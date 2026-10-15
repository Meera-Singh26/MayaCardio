"""Reads both sensors from the mock and persists every sample (and every loss) durably.

Rules: nothing is invented; every index is either a stored sample or a recorded loss.
Corrupt replies are re-read, duplicates dropped by index, session restarts close the old session with
an open-ended loss, and a full disk turns into explicit `device_disk_full` loss records.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time

from ..intervals import INF, IntervalSet
from ..protocol import ProtocolError, parse_reply
from ..store import Change, DiskFull, Store

log = logging.getLogger("collector")


class SessionState:
    def __init__(self, sid: str, state: str = "active"):
        self.id, self.state = sid, state
        self.data = {1: IntervalSet(), 2: IntervalSet()}
        self.resolved = {1: IntervalSet(), 2: IntervalSet()}
        self.ends = {1: None, 2: None}

    def frontier(self, s: int) -> int:
        return self.resolved[s].frontier()

    def apply(self, ch: Change) -> None:
        if ch.rows:
            self.data[ch.sensor].add(ch.rows[0][0], ch.rows[-1][0])
            self.resolved[ch.sensor].add(ch.rows[0][0], ch.rows[-1][0])
        for s, a, b, _ in ch.losses:
            self.resolved[s].add(a, INF if b is None else b)
        if ch.end_index is not None:
            self.ends[ch.sensor] = ch.end_index
        if ch.state:
            self.state = ch.state


def apply_change(store: Store, ch: Change) -> None:
    with store.tx():
        store.ensure_session(ch.session)
        if ch.rows:
            store.insert_samples(ch.session, ch.sensor, ch.rows)
            store.merge_range(ch.session, ch.sensor, "data", ch.rows[0][0], ch.rows[-1][0])
        for s, a, b, reason in ch.losses:
            store.add_loss(ch.session, s, a, b, reason)
        if ch.end_index is not None:
            store.set_end(ch.session, ch.sensor, ch.end_index)
        if ch.state:
            store.set_session_state(ch.session, ch.state)


class Collector(threading.Thread):
    def __init__(self, cfg, stop: threading.Event, wake: threading.Event):
        super().__init__(name="collector", daemon=True)
        self.cfg, self.stop_ev, self.wake = cfg, stop, wake
        self.store: Store | None = None
        self.sessions: dict[str, SessionState] = {}
        self.cur: SessionState | None = None
        self.resync = {1: True, 2: True}
        self.sock = None
        self.rf = None
        self.connected = False
        self.last_data_mono = 0.0
        self.mem_losses: list = []     # losses we could not persist yet (disk full)

    # ---- state ---------------------------------------------------------------------------
    def _load(self) -> None:
        for row in self.store.sessions():
            st = SessionState(row["session"], row["state"])
            for s in (1, 2):
                st.data[s] = self.store.load_ranges(st.id, s, "data")
                st.resolved[s] = IntervalSet(st.data[s].iv)
                for a, b in self.store.load_ranges(st.id, s, "lost").iv:
                    st.resolved[s].add(a, b)
                e = self.store.ends(st.id).get(str(s))
                st.ends[s] = e
            self.sessions[st.id] = st
            if st.state == "active":
                self.cur = st

    def current_session(self):
        return self.cur.id if self.cur else None

    # ---- socket --------------------------------------------------------------------------
    def _connect(self):
        self._close()
        self.sock = socket.create_connection((self.cfg.mock_host, self.cfg.mock_port), timeout=self.cfg.read_timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(self.cfg.read_timeout)
        self.rf = self.sock.makefile("rb")
        self.resync = {1: True, 2: True}
        self.connected = True
        self.store.event("mock_connected", f"{self.cfg.mock_host}:{self.cfg.mock_port}")

    def _close(self):
        self.connected = False
        for o in (self.rf, self.sock):
            try:
                if o:
                    o.close()
            except OSError:
                pass
        self.rf = self.sock = None

    def _exchange(self, sensor: int, from_index):
        req = {"cmd": "read", "sensor": sensor}
        if from_index is not None:
            req["from_index"] = from_index
        self.sock.sendall((__import__("json").dumps(req) + "\n").encode())
        for _ in range(6):
            line = self.rf.readline()
            if not line:
                raise ConnectionError("mock closed the connection")
            rp = parse_reply(line)
            if rp.error:
                return rp
            if rp.sensor != sensor:
                self.store.event("out_of_order_or_duplicate_reply", f"got sensor {rp.sensor}, wanted {sensor}")
                continue
            return rp
        raise ProtocolError("no matching reply")

    def _read_sensor(self, sensor: int):
        for attempt in range(5):
            fi = None
            if self.resync[sensor] and self.cur is not None:
                fi = self.cur.frontier(sensor)
                if fi >= INF:
                    fi = None
            rp = self._exchange(sensor, fi)
            if rp.error:
                self.store.event("mock_error", rp.error)
                self.resync[sensor] = False
                continue
            if not rp.crc_ok:
                self.store.event("corrupt_reply", f"sensor {sensor} first {rp.first_index} count {rp.count}; re-reading")
                self.resync[sensor] = True
                continue
            if fi is not None and self.cur is not None and rp.session not in self.sessions:
                # our explicit from_index referred to the OLD session; the mock has restarted.
                # Close the old session, then re-read the new one from its own index 0.
                self._switch_session(rp.session)
                self.resync = {1: True, 2: True}
                continue
            self.resync[sensor] = False
            return rp
        return None

    # ---- planning / committing ----------------------------------------------------------------
    def _switch_session(self, sid: str) -> SessionState:
        old = self.cur
        if old is not None and old.state == "active":
            ch = Change(session=old.id, state="interrupted")
            for s in (1, 2):
                if old.ends[s] is None:
                    ch.losses.append((s, old.frontier(s), None, "session_restart_unread_data_unknown_end"))
            self._commit(ch)
            self.store.event("session_interrupted", f"{old.id} -> {sid}")
        st = SessionState(sid)
        self.sessions[sid] = st
        self._commit(Change(session=sid))
        self.cur = st
        self.resync = {1: False, 2: False}
        self.store.event("session_start", sid)
        return st

    def _plan(self, st: SessionState, rp) -> Change:
        s = rp.sensor
        ch = Change(session=st.id, sensor=s)
        cursor = st.frontier(s)
        items = [(a, b, 0, r) for a, b, r in rp.lost]
        if rp.count:
            items.append((rp.first_index, rp.first_index + rp.count - 1, 1, None))
        items.sort(key=lambda t: (t[0], t[2]))
        for a, b, kind, reason in items:
            if cursor < INF and a > cursor:
                ch.losses.append((s, cursor, a - 1, "gap_not_reported_by_source"))
                ch.events.append(("gap_not_reported", f"sensor {s} {cursor}-{a - 1}"))
                cursor = a
            if kind == 0:
                if b is None:
                    ch.losses.append((s, max(a, cursor), None, reason))
                    cursor = INF
                elif b >= cursor:
                    ch.losses.append((s, max(a, cursor), b, reason))
                    cursor = b + 1
            else:
                if cursor > a:                                  # duplicate / overlap: verify, never overwrite
                    hi = min(cursor - 1, b)
                    old = self.store.fetch(st.id, s, a, hi)
                    for k, row in enumerate(old):
                        if row is not None and tuple(rp.data[k]) != row:
                            ch.events.append(("conflicting_duplicate", f"sensor {s} index {a + k} kept stored value"))
                            break
                if b >= cursor:
                    start = max(a, cursor)
                    for k in range(start - a, rp.count):
                        v = rp.data[k]
                        ch.rows.append((a + k, v[0], v[1], v[2] if s == 1 else None))
                    cursor = b + 1
        if rp.session_end and cursor < INF:
            ch.end_index = cursor
        return ch

    def _commit(self, ch: Change) -> None:
        try:
            apply_change(self.store, ch)
        except DiskFull:
            ch = self._handle_disk_full(ch)
            if ch is None:
                return
        for kind, detail in ch.events:
            self.store.event(kind, detail)
        st = self.sessions.get(ch.session)
        if st:
            st.apply(ch)

    def _free_reserve(self) -> None:
        p = os.path.join(self.cfg.data_dir, "reserve.bin")
        try:
            os.remove(p)
            log.error("disk full: released reserve file")
        except OSError:
            pass

    def _handle_disk_full(self, ch: Change):
        """Retry briefly, then convert unsavable rows into explicit loss records. Never silent."""
        self._free_reserve()
        deadline = time.monotonic() + self.cfg.disk_full_retry_s
        while time.monotonic() < deadline and not self.stop_ev.is_set():
            try:
                apply_change(self.store, ch)
                return ch
            except DiskFull:
                time.sleep(0.5)
        lossy = Change(session=ch.session, sensor=ch.sensor, losses=list(ch.losses), end_index=ch.end_index, state=ch.state)
        if ch.rows:
            lossy.losses.append((ch.sensor, ch.rows[0][0], ch.rows[-1][0], "device_disk_full"))
        lossy.events.append(("disk_full", f"{len(ch.rows)} rows of sensor {ch.sensor} not stored"))
        try:
            apply_change(self.store, lossy)
        except DiskFull:
            self.mem_losses.append(lossy)
            log.error("disk full: loss record kept in memory only (%d)", len(self.mem_losses))
            st = self.sessions.get(ch.session)
            if st:
                st.apply(lossy)
            return None
        return lossy

    def _flush_mem_losses(self):
        while self.mem_losses:
            try:
                apply_change(self.store, self.mem_losses[0])
            except DiskFull:
                return
            self.mem_losses.pop(0)

    def _process(self, rp) -> int:
        st = self.sessions.get(rp.session)
        if st is None:
            st = self._switch_session(rp.session)
        elif self.cur is None or (st is not self.cur and st.state == "active"):
            self.cur = st
        ch = self._plan(st, rp)
        if ch.rows or ch.losses or ch.end_index is not None or ch.events:
            self._commit(ch)
            if ch.end_index is not None and all(st.ends[s] is not None for s in (1, 2)) and st.state == "active":
                self._commit(Change(session=st.id, state="ended"))
                self.store.event("session_ended", st.id)
        if rp.count:
            self.last_data_mono = time.monotonic()
        return rp.count

    # ---- thread ----------------------------------------------------------------------------------
    def run(self):
        self.store = Store(self.cfg.db_path, self.cfg.sync)
        self._load()
        backoff = 0.2
        while not self.stop_ev.is_set():
            try:
                self._connect()
                backoff = 0.2
                self._loop()
            except (OSError, ProtocolError, ConnectionError) as e:
                if self.connected:
                    self.store.event("mock_connection_lost", repr(e))
                log.warning("mock connection problem: %r", e)
                self._close()
                self.stop_ev.wait(backoff)
                backoff = min(backoff * 2, 2.0)
        self._close()

    def _loop(self):
        while not self.stop_ev.is_set():
            self._flush_mem_losses()
            full = False
            idle_ended = self.cur is not None and self.cur.state == "ended"
            for s in (1, 2):
                rp = self._read_sensor(s)
                if rp is None:
                    continue
                n = self._process(rp)
                full = full or n >= 400
            self.wake.set()
            if not full:
                self.stop_ev.wait(1.0 if idle_ended else self.cfg.poll_interval)
