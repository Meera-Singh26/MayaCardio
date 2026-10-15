#!/usr/bin/env python3
"""Local re-implementation of `biosignal-mock` (section 6 of the brief) used for testing.

NOT the judges' binary. Adds test-only switches: --speed (time compression), --faults (injection
schedule), --truth (ground-truth log consumed by tools/ground_truth.py).

Fault spec (comma separated, times in real seconds since start):
  stall:T:D        stop answering for D seconds starting at T
  corrupt:T        next reply after T has a wrong crc32
  dup:T            next reply after T is sent twice
  drop:T:D:S       sensor S delivers nothing during [T,T+D]; those samples are reported lost
  restart:T        mock restarts: new session id, indexes from 0, unread data lost
"""
from __future__ import annotations

import argparse
import bisect
import json
import math
import random
import socket
import threading
import time
import zlib

SR = 400
MOD = 8388608


def _mix(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return x ^ (x >> 31)


def noise(seed: int, k: int, i: int, amp: int) -> int:
    return (_mix((seed << 40) ^ (k << 32) ^ i) % (2 * amp + 1)) - amp


def _beats():
    pos, t = [], 0
    for bpm, minutes in ((60, 2), (90, 2), (72, 2)):
        step = SR * 60 / bpm
        n = int(minutes * 60 * SR / step)
        pos += [t + round(j * step) for j in range(n)]
        t += minutes * 60 * SR
    return pos, t


_CYCLE, _CYCLE_LEN = _beats()


def _beat_near(i: int):
    c, r = divmod(i, _CYCLE_LEN)
    j = bisect.bisect_left(_CYCLE, r)
    out = []
    for jj in (j - 1, j):
        if 0 <= jj < len(_CYCLE):
            out.append(c * _CYCLE_LEN + _CYCLE[jj])
    if j == len(_CYCLE):
        out.append((c + 1) * _CYCLE_LEN + _CYCLE[0])
    if j == 0:
        out.append((c - 1) * _CYCLE_LEN + _CYCLE[-1])
    return out


def ecg(seed: int, i: int) -> int:
    v = 80 * math.sin(2 * math.pi * i / (SR * 5)) + noise(seed, 1, i, 12)
    for b in _beat_near(i):
        d = i - b
        if abs(d) > 150:
            continue
        v += 2000 * math.exp(-(d * d) / 32.0)
        v += 250 * math.exp(-((d - 80) ** 2) / 650.0)
        v += 120 * math.exp(-((d + 60) ** 2) / 300.0)
        v -= 180 * math.exp(-((d + 6) ** 2) / 8.0)
    return int(round(v))


def sample(mode: str, seed: int, sensor: int, i: int):
    if mode == "ramp":
        n = 3 if sensor == 1 else 2
        k = 3 if sensor == 1 else 2
        return [(k * i + c) % MOD for c in range(1, n + 1)]
    if sensor == 1:
        c2 = ecg(seed, i)
        return [int(round(c2 * 0.6)), c2, int(round(c2 * 1.3))]
    return [400000 + int(2000 * math.sin(i / 9000.0)) + noise(seed, 2, i, 40),
            150000 + int(900 * math.sin(i / 14000.0)) + noise(seed, 3, i, 30)]


class Session:
    def __init__(self, sid, speed, total, now):
        self.id, self.speed, self.total, self.t0 = sid, speed, total, now
        self.cursor = {1: 0, 2: 0}

    def produced(self, now):
        return min(self.total, int((now - self.t0) * self.speed * SR))


class Mock:
    def __init__(self, a):
        self.a = a
        self.rng = random.Random(a.seed)
        self.faults = self._parse(a.faults)
        self.t_start = time.monotonic()
        self.truth = open(a.truth, "a", buffering=1) if a.truth else None
        self.total = int(a.duration * SR)
        self.sess = None
        self.pending_log = []
        self.restarts = 0
        self.stall_done = set()
        self.pending = {"corrupt": [], "dup": []}
        self._new_session()

    @staticmethod
    def _parse(spec):
        out = []
        for part in (spec or "").split(","):
            if part.strip():
                f = part.split(":")
                out.append((f[0], [float(x) for x in f[1:]]))
        return out

    def log(self, **kw):
        if self.truth:
            kw["wall"] = time.time()
            self.truth.write(json.dumps(kw) + "\n")

    def _new_session(self):
        now = time.monotonic()
        if self.sess is not None:
            self.log(ev="restart", session=self.sess.id, produced=self.sess.produced(now))
        sid = "%08x" % random.SystemRandom().getrandbits(32)
        self.sess = Session(sid, self.a.speed, self.total, now)
        self.drop_state = {}
        self.log(ev="session_start", session=sid, mode=self.a.mode, seed=self.a.seed, speed=self.a.speed,
                 total=self.total)

    def _elapsed(self):
        return time.monotonic() - self.t_start

    def _faults_tick(self):
        e = self._elapsed()
        for k, (kind, args) in enumerate(self.faults):
            if kind == "restart" and e >= args[0] and ("r", k) not in self.stall_done:
                self.stall_done.add(("r", k))
                self._new_session()
            elif kind == "stall" and e >= args[0] and ("s", k) not in self.stall_done:
                self.stall_done.add(("s", k))
                self.log(ev="stall", seconds=args[1])
                time.sleep(args[1])
            elif kind in ("corrupt", "dup") and e >= args[0] and (kind, k) not in self.stall_done:
                self.stall_done.add((kind, k))
                self.pending[kind].append(1)

    def _dropping(self, sensor):
        e = self._elapsed()
        for kind, args in self.faults:
            if kind == "drop" and int(args[2]) == sensor and args[0] <= e < args[0] + args[1]:
                return True
        return False

    def read(self, sensor, from_index=None):
        s = self.sess
        now = time.monotonic()
        prod = s.produced(now)
        lost = []
        buf_start = max(0, prod - 10 * SR)
        cur = s.cursor[sensor] if from_index is None else from_index
        if self._dropping(sensor):
            self.drop_state[sensor] = True
            return self._reply(sensor, cur, [], lost, False)
        if self.drop_state.pop(sensor, False):
            # samples produced during the dropout are never delivered
            lo, hi = cur, prod - 1
            if hi >= lo and from_index is None:
                lost.append({"from_index": lo, "to_index": hi, "reason": "sensor_dropout"})
                self.log(ev="lost", session=s.id, sensor=sensor, **{"from": lo, "to": hi})
                cur = prod
        if cur < buf_start:
            lost.append({"from_index": cur, "to_index": buf_start - 1, "reason": "buffer_overflow"})
            self.log(ev="lost", session=s.id, sensor=sensor, **{"from": cur, "to": buf_start - 1})
            cur = buf_start
        n = max(0, min(SR, prod - cur))
        rows = [sample(self.a.mode, self.a.seed, sensor, cur + k) for k in range(n)]
        s.cursor[sensor] = cur + n
        end = s.cursor[sensor] >= s.total
        return self._reply(sensor, cur, rows, lost, end)

    def _reply(self, sensor, first, rows, lost, end):
        text = json.dumps(rows)
        crc = zlib.crc32(text.encode()) & 0xFFFFFFFF
        corrupt = bool(self.pending["corrupt"]) and rows
        if corrupt:
            self.pending["corrupt"].pop()
            crc ^= 0xDEADBEEF
        head = json.dumps({"session_id": self.sess.id, "sensor": sensor, "first_index": first, "count": len(rows),
                           "lost": lost, "session_end": end, "crc32": "%08x" % crc})[:-1]
        line = (head + ', "data": ' + text + "}\n").encode()
        if rows:
            self.pending_log.append(dict(ev="deliver", session=self.sess.id, sensor=sensor, first=first,
                                         count=len(rows), corrupt=corrupt))
        if self.pending["dup"] and rows:
            self.pending["dup"].pop()
            return line + line
        return line

    def handle(self, conn):
        f = conn.makefile("rb")
        for raw in f:
            try:
                req = json.loads(raw)
            except ValueError:
                continue
            self._faults_tick()
            if req.get("cmd") == "read" and req.get("sensor") in (1, 2):
                self.pending_log = []
                line = self.read(req["sensor"], req.get("from_index"))
                conn.sendall(line)               # only a successful send counts as delivered
                for e in self.pending_log:
                    self.log(**e)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", default="signal", choices=["signal", "ramp"])
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--duration", type=float, default=3600)
    p.add_argument("--port", type=int, default=7100)
    p.add_argument("--speed", type=float, default=1.0)
    p.add_argument("--faults", default="")
    p.add_argument("--truth", default="")
    a = p.parse_args()
    m = Mock(a)
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", a.port))
    srv.listen(8)
    while True:
        c, _ = srv.accept()  # one client at a time
        c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            m.handle(c)
        except OSError:
            pass
        finally:
            c.close()


if __name__ == "__main__":
    main()
