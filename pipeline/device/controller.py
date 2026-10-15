"""Device brain: offline processing, live streaming, catch-up (backfill) and result sync."""
from __future__ import annotations

import json
import logging
import os
import threading
import time

from ..intervals import INF
from ..store import Store, StoreSource
from ..windowing import SR, is_ready, LOOKBACK_WINDOWS, run_windows
from .cloudclient import CloudClient, NetError

log = logging.getLogger("controller")


class LiveState:
    def __init__(self, session, epoch, own_from):
        self.session, self.epoch, self.own_from = session, epoch, own_from
        self.cur = {1: SR * max(0, own_from - LOOKBACK_WINDOWS), 2: SR * own_from}
        self.ends_sent: dict = {}


def _runs(sensor, rows):
    """rows -> contiguous chunks [{'sensor','first','values'}]"""
    chunks, cur, prev = [], None, None
    for r in rows:
        v = [r[1], r[2], r[3]] if sensor == 1 else [r[1], r[2]]
        if cur is None or r[0] != prev + 1:
            cur = {"sensor": sensor, "first": r[0], "values": []}
            chunks.append(cur)
        cur["values"].append(v)
        prev = r[0]
    return chunks


class Controller(threading.Thread):
    def __init__(self, cfg, mode, wake, stop, collector, heartbeat):
        super().__init__(name="controller", daemon=True)
        self.cfg, self.mode, self.wake, self.stop_ev = cfg, mode, wake, stop
        self.collector, self.heartbeat = collector, heartbeat
        self.client = CloudClient(cfg.cloud_url, cfg.api_key, timeout=cfg.request_timeout)
        self.live: dict[str, LiveState] = {}
        self.gen = -1
        self.stuck: dict = {}
        self._last_state = 0.0

    # ---- helpers ---------------------------------------------------------------------------
    def _nw(self, sid) -> int:
        return int(self.store.get_meta(f"nw:{sid}", 0))

    def _compute(self, sid, mode, limit=100) -> int:
        n = 0
        while True:
            nw = self._nw(sid)
            batch = run_windows(StoreSource(self.store, sid), sid, nw, self.store.max_known(sid), mode, limit)
            if not batch:
                return n
            with self.store.tx():
                for b in batch:
                    self.store.put_result(b, synced=0)
                self.store.set_meta(f"nw:{sid}", batch[-1]["window"] + 1)
            n += len(batch)
            if self.stop_ev.is_set() or len(batch) < limit:
                return n

    def _compute_all_offline(self):
        for s in self.store.sessions()[-4:]:
            self._compute(s["session"], "offline")

    def _pending_losses(self, sid):
        out = []
        cov = {s: self.store.load_ranges(sid, s, "lost", "cloud_ranges") for s in (1, 2)}
        for sensor, a, b, reason in self.store.losses(sid):
            if not cov[sensor].covers(a, INF if b is None else b):
                out.append({"sensor": sensor, "from": a, "to": b, "reason": reason})
        return out

    def _post_ingest(self, payload, timeout):
        return self.client.post_json("/v1/ingest", payload, timeout=timeout)

    def _ack(self, sid, payload, resp):
        with self.store.tx():
            for ch in payload["chunks"]:
                self.store.merge_range(sid, ch["sensor"], "data", ch["first"], ch["first"] + len(ch["values"]) - 1, "cloud_ranges")
            for L in payload["losses"]:
                self.store.merge_range(sid, L["sensor"], "lost", L["from"], INF if L["to"] is None else L["to"], "cloud_ranges")
            self.store.set_meta(f"ends_acked:{sid}", json.dumps(payload["ends"], sort_keys=True))
            for b in resp.get("results", []):
                self.store.put_result(b, synced=1)
            dt = resp.get("done_through")
            if payload["kind"] == "live" and dt is not None and dt + 1 > self._nw(sid):
                self.store.set_meta(f"nw:{sid}", dt + 1)

    # ---- offline -------------------------------------------------------------------------------
    def _offline_tick(self):
        self._compute_all_offline()

    # ---- online ----------------------------------------------------------------------------------
    def _reconcile(self):
        for s in self.store.sessions():
            sid = s["session"]
            r = self.client.get_json(f"/v1/sessions/{sid}/ranges", timeout=self.cfg.request_timeout)
            with self.store.tx():
                for sensor in (1, 2):
                    for kind in ("data", "lost"):
                        self.store.replace_ranges("cloud_ranges", sid, sensor, kind, r["sensors"][str(sensor)][kind])
                self.store.set_meta(f"ends_acked:{sid}", json.dumps(r["ends"], sort_keys=True))
            after = max([b["window"] for b in self.store.results(sid, "online")] or [-1])
            for ln in self.client.stream_lines(f"/v1/sessions/{sid}/results?mode=online&after={after}"):
                if ln:
                    with self.store.tx():
                        self.store.put_result(json.loads(ln), synced=1)

    def _begin_online(self):
        log.info("entering ONLINE")
        self._compute_all_offline()                    # windows ready now stay on the device
        self._reconcile()
        self.live.clear()
        self.stuck.clear()
        self.gen = self.mode.generation

    def _ensure_live(self):
        sessions = self.store.sessions()
        if not sessions:
            return
        last = sessions[-1]
        if last["state"] == "active" and last["session"] not in self.live:
            sid = last["session"]
            ep = int(self.store.get_meta(f"epoch:{sid}", 0)) + 1
            self.store.set_meta(f"epoch:{sid}", ep)
            self.live[sid] = LiveState(sid, ep, self._nw(sid))
            self.store.event("online_epoch", f"{sid} epoch {ep} own_from {self._nw(sid)}")

    def _push_live(self, ls: LiveState) -> bool:
        sid = ls.session
        chunks = []
        nxt = dict(ls.cur)
        for s in (1, 2):
            rows = self.store.fetch_after(sid, s, ls.cur[s], self.cfg.live_batch)
            if rows:
                chunks += _runs(s, rows)
                nxt[s] = rows[-1][0] + 1
        losses = self._pending_losses(sid)
        ends = self.store.ends(sid)
        if not chunks and not losses and ends == ls.ends_sent:
            return False
        payload = {"session": sid, "kind": "live", "epoch": ls.epoch, "own_from": ls.own_from,
                   "chunks": chunks, "losses": losses, "ends": ends}
        resp = self._post_ingest(payload, self.cfg.request_timeout)
        self.mode.report(True)
        self._ack(sid, payload, resp)
        ls.cur, ls.ends_sent = nxt, ends
        return True

    def _live_finished(self, ls: LiveState) -> bool:
        st = [s for s in self.store.sessions() if s["session"] == ls.session][0]["state"]
        if st == "active":
            return False
        return (not any(self.store.fetch_after(ls.session, s, ls.cur[s], 1) for s in (1, 2))
                and not self._pending_losses(ls.session))

    def _backfill(self) -> bool:
        for srow in self.store.sessions():
            sid = srow["session"]
            for s in (1, 2):
                dev = self.store.load_ranges(sid, s, "data")
                cl = self.store.load_ranges(sid, s, "data", "cloud_ranges")
                for a, b in dev.iv:
                    gaps = cl.gaps(a, b)
                    if not gaps:
                        continue
                    ga, gb = gaps[0]
                    rows = [r for r in self.store.fetch_after(sid, s, ga, self.cfg.backfill_batch) if r[0] <= gb]
                    payload = {"session": sid, "kind": "backfill", "chunks": _runs(s, rows),
                               "losses": self._pending_losses(sid), "ends": self.store.ends(sid)}
                    resp = self._post_ingest(payload, self.cfg.backfill_timeout)
                    self.mode.report(True)
                    self._ack(sid, payload, resp)
                    return True
            losses, ends = self._pending_losses(sid), self.store.ends(sid)
            acked = self.store.get_meta(f"ends_acked:{sid}", "{}")
            if losses or json.dumps(ends, sort_keys=True) != acked:
                payload = {"session": sid, "kind": "backfill", "chunks": [], "losses": losses, "ends": ends}
                self._ack(sid, payload, self._post_ingest(payload, self.cfg.request_timeout))
                return True
        items = self.store.unsynced_results(500)
        if items:
            by: dict = {}
            for b in items:
                by.setdefault(b["session"], []).append(b)
            for sid, lst in by.items():
                self.client.post_json(f"/v1/sessions/{sid}/results", {"results": lst}, timeout=self.cfg.request_timeout)
            with self.store.tx():
                self.store.mark_results_synced(items)
            return True
        return False

    def _watchdog(self):
        """Safety net: a window the device already has all data for, but the cloud has not confirmed."""
        for sid in list(self.live):
            nw = self._nw(sid)
            mk = self.store.max_known(sid)
            if mk is not None and SR * nw <= mk and is_ready(nw, StoreSource(self.store, sid)):
                t0 = self.stuck.get(sid)
                if t0 is None or t0[0] != nw:
                    self.stuck[sid] = (nw, time.monotonic())
                elif time.monotonic() - t0[1] > self.cfg.stuck_s:
                    self.store.event("window_taken_over_by_device", f"{sid} window {nw} not confirmed by cloud")
                    batch = run_windows(StoreSource(self.store, sid), sid, nw, mk, "offline", 1)
                    with self.store.tx():
                        for b in batch:
                            self.store.put_result(b, synced=0)
                        self.store.set_meta(f"nw:{sid}", nw + 1)
                    self.stuck.pop(sid, None)
            else:
                self.stuck.pop(sid, None)

    def _online_tick(self):
        self._ensure_live()
        did = False
        for ls in list(self.live.values()):
            did |= self._push_live(ls)
            if self._live_finished(ls):
                del self.live[ls.session]
        if not did:
            did = self._backfill()
        self._watchdog()
        if not did:
            self.wake.wait(0.25)
            self.wake.clear()

    # ---- state file --------------------------------------------------------------------------------
    def _write_state(self, force=False):
        now = time.time()
        if not force and now - self._last_state < 1.0:
            return
        self._last_state = now
        st = {"mode": self.mode.state, "since_wall": self.mode.since_wall, "heartbeat_wall": now,
              "pid": os.getpid(), "cloud_url": self.cfg.cloud_url,
              "mock_connected": self.collector.connected, "session": self.collector.current_session(),
              "bytes_sent": self.client.bytes_sent + self.heartbeat.client.bytes_sent,
              "bytes_recv": self.client.bytes_recv + self.heartbeat.client.bytes_recv}
        tmp = self.cfg.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f)
        os.replace(tmp, self.cfg.state_path)

    def run(self):
        self.store = Store(self.cfg.db_path, self.cfg.sync)
        while not self.stop_ev.is_set():
            try:
                for wall, m, why in self.mode.drain():
                    self.store.event(f"mode_{m}", why)
                    log.info("MODE -> %s (%s)", m, why)
                self._write_state()
                if self.mode.state == "offline":
                    self.live.clear()
                    self.gen = -1
                    self._offline_tick()
                    self.mode.wake.wait(0.25)
                    self.mode.wake.clear()
                else:
                    if self.gen != self.mode.generation:
                        self._begin_online()
                    self._online_tick()
            except NetError as e:
                log.warning("cloud request failed: %s", e)
                self.mode.report(False, str(e)[:120])
                self.gen = -1 if self.mode.state == "offline" else self.gen
            except Exception:  # noqa: BLE001
                log.exception("controller error")
                self.stop_ev.wait(0.5)
        self._write_state(True)
