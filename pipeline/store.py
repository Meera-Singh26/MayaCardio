"""SQLite storage shared by the device and the cloud service.

Durability: WAL + synchronous=FULL -> a committed transaction survives process kill, reboot
and power loss (modulo storage hardware honouring fsync).  One sample == one row keyed by
(session, sensor, idx), so "exactly once" is enforced by the database itself.
"""
from __future__ import annotations

import contextlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Iterator, Optional

from .intervals import INF, IntervalSet

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions(
  session TEXT PRIMARY KEY, seq INTEGER NOT NULL, created REAL, state TEXT NOT NULL DEFAULT 'active');
CREATE TABLE IF NOT EXISTS samples(
  session TEXT NOT NULL, sensor INTEGER NOT NULL, idx INTEGER NOT NULL,
  v1 INTEGER NOT NULL, v2 INTEGER NOT NULL, v3 INTEGER,
  PRIMARY KEY(session, sensor, idx)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ranges(
  session TEXT NOT NULL, sensor INTEGER NOT NULL, kind TEXT NOT NULL, a INTEGER NOT NULL, b INTEGER NOT NULL,
  PRIMARY KEY(session, sensor, kind, a)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS cloud_ranges(
  session TEXT NOT NULL, sensor INTEGER NOT NULL, kind TEXT NOT NULL, a INTEGER NOT NULL, b INTEGER NOT NULL,
  PRIMARY KEY(session, sensor, kind, a)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS losses(
  session TEXT NOT NULL, sensor INTEGER NOT NULL, from_index INTEGER NOT NULL, to_index INTEGER,
  reason TEXT NOT NULL, ts REAL,
  PRIMARY KEY(session, sensor, from_index)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ends(
  session TEXT NOT NULL, sensor INTEGER NOT NULL, end_index INTEGER NOT NULL,
  PRIMARY KEY(session, sensor)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS results(
  session TEXT NOT NULL, window INTEGER NOT NULL, mode TEXT NOT NULL, body TEXT NOT NULL,
  synced INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(session, window, mode)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, kind TEXT, detail TEXT);
"""


class DiskFull(Exception):
    pass


def _is_full(e: Exception) -> bool:
    m = str(e).lower()
    return "disk is full" in m or "no space" in m or "database or disk is full" in m


@dataclass
class Change:
    """Everything one collector reply changes, committed atomically."""
    session: str
    sensor: int = 0
    rows: list = field(default_factory=list)        # (idx, v1, v2, v3|None)
    losses: list = field(default_factory=list)      # (sensor, from, to|None, reason)
    end_index: Optional[int] = None
    state: Optional[str] = None
    events: list = field(default_factory=list)      # (kind, detail)


class Store:
    def __init__(self, path: str, sync: str = "FULL", init: bool = True):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=30.0, isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA busy_timeout=30000")
        if init:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute(f"PRAGMA synchronous={sync}")
            self.conn.execute("PRAGMA wal_autocheckpoint=2000")
            self.conn.executescript(SCHEMA)

    def close(self):
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    @contextlib.contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except BaseException as e:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            if isinstance(e, sqlite3.OperationalError) and _is_full(e):
                raise DiskFull(str(e)) from e
            raise

    # ---- sessions / meta / events -------------------------------------------------------
    def ensure_session(self, session: str, state: str = "active") -> None:
        seq = (self.conn.execute("SELECT COALESCE(MAX(seq),0)+1 FROM sessions").fetchone()[0])
        self.conn.execute("INSERT OR IGNORE INTO sessions(session,seq,created,state) VALUES(?,?,?,?)",
                          (session, seq, time.time(), state))

    def set_session_state(self, session: str, state: str) -> None:
        self.conn.execute("UPDATE sessions SET state=? WHERE session=?", (state, session))

    def sessions(self) -> list[dict]:
        return [dict(session=r[0], seq=r[1], created=r[2], state=r[3]) for r in
                self.conn.execute("SELECT session,seq,created,state FROM sessions ORDER BY seq")]

    def latest_session(self) -> Optional[str]:
        r = self.conn.execute("SELECT session FROM sessions ORDER BY seq DESC LIMIT 1").fetchone()
        return r[0] if r else None

    def get_meta(self, k: str, default=None):
        r = self.conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return r[0] if r else default

    def set_meta(self, k: str, v) -> None:
        self.conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (k, str(v)))

    def event(self, kind: str, detail: str = "") -> None:
        try:
            self.conn.execute("INSERT INTO events(ts,kind,detail) VALUES(?,?,?)", (time.time(), kind, detail[:2000]))
        except sqlite3.Error:
            pass

    # ---- samples ---------------------------------------------------------------------------
    def insert_samples(self, session: str, sensor: int, rows) -> int:
        cur = self.conn.executemany(
            "INSERT OR IGNORE INTO samples(session,sensor,idx,v1,v2,v3) VALUES(?,?,?,?,?,?)",
            ((session, sensor, r[0], r[1], r[2], r[3] if len(r) > 3 else None) for r in rows))
        return cur.rowcount

    def fetch(self, session: str, sensor: int, lo: int, hi: int, ch: Optional[int] = None) -> list:
        """List aligned to lo..hi; None where a sample is missing. ch=None -> tuples."""
        n = hi - lo + 1
        out: list = [None] * n
        nch = 3 if sensor == 1 else 2
        cols = "idx,v1,v2,v3" if sensor == 1 else "idx,v1,v2"
        if ch is not None:
            cols = f"idx,v{ch + 1}"
        for r in self.conn.execute(
                f"SELECT {cols} FROM samples WHERE session=? AND sensor=? AND idx BETWEEN ? AND ?",
                (session, sensor, lo, hi)):
            out[r[0] - lo] = r[1] if ch is not None else tuple(r[1:1 + nch])
        return out

    def fetch_after(self, session: str, sensor: int, idx: int, limit: int) -> list:
        return self.conn.execute(
            "SELECT idx,v1,v2,v3 FROM samples WHERE session=? AND sensor=? AND idx>=? ORDER BY idx LIMIT ?",
            (session, sensor, idx, limit)).fetchall()

    # ---- ranges ------------------------------------------------------------------------------
    def load_ranges(self, session: str, sensor: int, kind: str, table: str = "ranges") -> IntervalSet:
        s = IntervalSet()
        s.iv = [[a, b] for a, b in self.conn.execute(
            f"SELECT a,b FROM {table} WHERE session=? AND sensor=? AND kind=? ORDER BY a", (session, sensor, kind))]
        return s

    def merge_range(self, session: str, sensor: int, kind: str, a: int, b: int, table: str = "ranges") -> None:
        rows = self.conn.execute(
            f"SELECT a,b FROM {table} WHERE session=? AND sensor=? AND kind=? AND b>=? AND a<=?",
            (session, sensor, kind, a - 1, min(b + 1, INF))).fetchall()
        for x, y in rows:
            a, b = min(a, x), max(b, y)
        if rows:
            self.conn.executemany(f"DELETE FROM {table} WHERE session=? AND sensor=? AND kind=? AND a=?",
                                  [(session, sensor, kind, x) for x, _ in rows])
        self.conn.execute(f"INSERT OR REPLACE INTO {table}(session,sensor,kind,a,b) VALUES(?,?,?,?,?)",
                          (session, sensor, kind, a, b))

    def replace_ranges(self, table: str, session: str, sensor: int, kind: str, ivs) -> None:
        self.conn.execute(f"DELETE FROM {table} WHERE session=? AND sensor=? AND kind=?", (session, sensor, kind))
        self.conn.executemany(f"INSERT INTO {table}(session,sensor,kind,a,b) VALUES(?,?,?,?,?)",
                              [(session, sensor, kind, a, b) for a, b in ivs])

    def resolved(self, session: str, sensor: int, lo: int, hi: int) -> bool:
        """Every index of lo..hi is either stored, declared lost, or beyond the declared session end."""
        s = self.load_ranges(session, sensor, "data")
        for a, b in self.conn.execute(
                "SELECT a,b FROM ranges WHERE session=? AND sensor=? AND kind='lost'", (session, sensor)):
            s.add(a, b)
        r = self.conn.execute("SELECT end_index FROM ends WHERE session=? AND sensor=?", (session, sensor)).fetchone()
        if r:
            s.add(r[0], INF)
        return s.covers(lo, hi)

    def max_known(self, session: str) -> Optional[int]:
        r = self.conn.execute("SELECT MAX(b) FROM ranges WHERE session=? AND b<?", (session, INF)).fetchone()
        return r[0]

    def count_samples(self, session: str, sensor: Optional[int] = None, table: str = "ranges") -> int:
        q = f"SELECT COALESCE(SUM(b-a+1),0) FROM {table} WHERE session=? AND kind='data'"
        args = [session]
        if sensor:
            q += " AND sensor=?"
            args.append(sensor)
        return self.conn.execute(q, args).fetchone()[0]

    # ---- losses / ends --------------------------------------------------------------------
    def add_loss(self, session: str, sensor: int, a: int, b: Optional[int], reason: str) -> None:
        self.conn.execute("INSERT OR IGNORE INTO losses(session,sensor,from_index,to_index,reason,ts) VALUES(?,?,?,?,?,?)",
                          (session, sensor, a, b, reason, time.time()))
        self.merge_range(session, sensor, "lost", a, INF if b is None else b)

    def losses(self, session: str) -> list[tuple]:
        return self.conn.execute(
            "SELECT sensor,from_index,to_index,reason FROM losses WHERE session=? ORDER BY sensor,from_index",
            (session,)).fetchall()

    def lost_total(self, session: str) -> int:
        return sum((b - a + 1) for _, a, b, _ in self.losses(session) if b is not None)

    def set_end(self, session: str, sensor: int, end_index: int) -> None:
        self.conn.execute("INSERT OR REPLACE INTO ends(session,sensor,end_index) VALUES(?,?,?)",
                          (session, sensor, end_index))

    def ends(self, session: str) -> dict:
        return {str(s): e for s, e in self.conn.execute("SELECT sensor,end_index FROM ends WHERE session=?", (session,))}

    # ---- results -----------------------------------------------------------------------------
    def put_result(self, body: dict, synced: int = 0) -> bool:
        cur = self.conn.execute("INSERT OR IGNORE INTO results(session,window,mode,body,synced) VALUES(?,?,?,?,?)",
                                (body["session"], body["window"], body["mode"],
                                 json.dumps(body, separators=(",", ":")), synced))
        return cur.rowcount > 0

    def results(self, session: str, mode: Optional[str] = None, after: int = -1) -> list[dict]:
        q = "SELECT body FROM results WHERE session=? AND window>?"
        args: list = [session, after]
        if mode:
            q += " AND mode=?"
            args.append(mode)
        q += " ORDER BY window,mode"
        return [json.loads(r[0]) for r in self.conn.execute(q, args)]

    def unsynced_results(self, limit: int = 500) -> list[dict]:
        return [json.loads(r[0]) for r in self.conn.execute(
            "SELECT body FROM results WHERE synced=0 AND mode='offline' ORDER BY session,window LIMIT ?", (limit,))]

    def mark_results_synced(self, items) -> None:
        self.conn.executemany("UPDATE results SET synced=1 WHERE session=? AND window=? AND mode=?",
                              [(b["session"], b["window"], b["mode"]) for b in items])

    # ---- export ------------------------------------------------------------------------------
    def iter_export(self, session: str) -> Iterator[str]:
        for sensor in (1, 2):
            cols = "idx,v1,v2,v3" if sensor == 1 else "idx,v1,v2"
            cur = self.conn.execute(f"SELECT {cols} FROM samples WHERE session=? AND sensor=? ORDER BY idx",
                                    (session, sensor))
            while True:
                chunk = cur.fetchmany(5000)
                if not chunk:
                    break
                for r in chunk:
                    yield json.dumps({"sensor": sensor, "index": r[0], "values": list(r[1:])})
        for sensor, a, b, reason in self.losses(session):
            yield json.dumps({"sensor": sensor, "from_index": a, "to_index": b, "reason": reason})


class StoreSource:
    """Adapter giving windowing.py access to one session of a Store."""

    def __init__(self, store: Store, session: str):
        self.store, self.session = store, session

    def fetch(self, sensor, lo, hi, ch=None):
        return self.store.fetch(self.session, sensor, lo, hi, ch)

    def resolved(self, sensor, lo, hi):
        return self.store.resolved(self.session, sensor, lo, hi)
