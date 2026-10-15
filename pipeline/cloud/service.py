"""Cloud-side business logic (storage, online processing). HTTP lives in server.py."""
from __future__ import annotations

import threading

from ..intervals import INF
from ..store import Store, StoreSource
from ..windowing import run_windows


class CloudService:
    def __init__(self, db_path: str, sync: str = "FULL"):
        self.db_path, self.sync = db_path, sync
        self.local = threading.local()
        self.write_lock = threading.Lock()
        Store(db_path, sync).close()

    @property
    def store(self) -> Store:
        s = getattr(self.local, "s", None)
        if s is None:
            s = self.local.s = Store(self.db_path, self.sync)
        return s

    # ---- ingest ---------------------------------------------------------------------------
    def ingest(self, p: dict) -> dict:
        st, sid = self.store, str(p["session"])
        live = p.get("kind") == "live"
        stored = dups = conflicts = 0
        with self.write_lock:
            with st.tx():
                st.ensure_session(sid)
                for ch in p.get("chunks", []):
                    sensor, first, vals = int(ch["sensor"]), int(ch["first"]), ch["values"]
                    if not vals:
                        continue
                    rows = [(first + k, v[0], v[1], v[2] if sensor == 1 else None) for k, v in enumerate(vals)]
                    n = st.insert_samples(sid, sensor, rows)
                    stored += n
                    if n < len(rows):
                        dups += len(rows) - n
                        old = st.fetch(sid, sensor, first, first + len(rows) - 1)
                        for k, o in enumerate(old):
                            if o is not None and tuple(vals[k]) != o:
                                conflicts += 1
                                st.event("conflicting_duplicate", f"{sid} sensor {sensor} index {first + k}")
                    st.merge_range(sid, sensor, "data", first, first + len(rows) - 1)
                for L in p.get("losses", []):
                    st.add_loss(sid, int(L["sensor"]), int(L["from"]), None if L["to"] is None else int(L["to"]), L["reason"])
                for s, e in (p.get("ends") or {}).items():
                    st.set_end(sid, int(s), int(e))
                if live:
                    ep, own = int(p.get("epoch", 0)), int(p.get("own_from", 0))
                    if ep > int(st.get_meta(f"epoch:{sid}", -1)):
                        st.set_meta(f"epoch:{sid}", ep)
                        st.set_meta(f"nw:{sid}", own)
            new_results, done = [], None
            if live:
                new_results, done = self._process(sid)
            else:
                done = int(st.get_meta(f"nw:{sid}", 0)) - 1
        return {"ok": True, "stored": stored, "duplicates": dups, "conflicts": conflicts,
                "done_through": done, "results": new_results}

    def _process(self, sid: str):
        st = self.store
        nw = int(st.get_meta(f"nw:{sid}", 0))
        out = []
        while True:
            batch = run_windows(StoreSource(st, sid), sid, nw, st.max_known(sid), "online", limit=100)
            if not batch:
                break
            with st.tx():
                for b in batch:
                    st.put_result(b, synced=1)
                nw = batch[-1]["window"] + 1
                st.set_meta(f"nw:{sid}", nw)
            out += batch
        return out, nw - 1

    # ---- queries ----------------------------------------------------------------------------
    def ranges(self, sid: str) -> dict:
        st = self.store
        out = {"session": sid, "sensors": {}, "ends": st.ends(sid),
               "done_through": int(st.get_meta(f"nw:{sid}", 0)) - 1}
        for s in (1, 2):
            out["sensors"][str(s)] = {k: st.load_ranges(sid, s, k).to_list() for k in ("data", "lost")}
        return out

    def put_results(self, sid: str, items: list) -> int:
        n = 0
        with self.write_lock, self.store.tx():
            for b in items:
                n += self.store.put_result(b, synced=1)
        return n

    def status(self, sid: str) -> dict:
        st = self.store
        return {"session": sid, "samples": {str(s): st.count_samples(sid, s) for s in (1, 2)},
                "lost_samples": st.lost_total(sid),
                "results": len(st.results(sid)), "done_through": int(st.get_meta(f"nw:{sid}", 0)) - 1}
