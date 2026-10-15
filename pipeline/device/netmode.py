"""Online/offline decision with hysteresis, driven by a dedicated heartbeat thread."""
from __future__ import annotations

import threading
import time

from .cloudclient import CloudClient, NetError


class ModeController:
    def __init__(self, fail_threshold=2, ok_threshold=2):
        self.fail_t, self.ok_t = fail_threshold, ok_threshold
        self.state = "offline"
        self.generation = 0                 # bumps every time we (re)enter online
        self.since_mono = time.monotonic()
        self.since_wall = time.time()
        self._fails = self._oks = 0
        self.lock = threading.Lock()
        self.transitions: list = []         # (wall, mode, why) drained by the controller thread
        self.wake = threading.Event()

    def report(self, ok: bool, why: str = "") -> None:
        with self.lock:
            if ok:
                self._fails = 0
                self._oks += 1
                if self.state == "offline" and self._oks >= self.ok_t:
                    self._flip("online", why or "cloud reachable")
            else:
                self._oks = 0
                self._fails += 1
                if self.state == "online" and self._fails >= self.fail_t:
                    self._flip("offline", why or "cloud unreachable")

    def _flip(self, mode, why):
        self.state = mode
        if mode == "online":
            self.generation += 1
        self.since_mono, self.since_wall = time.monotonic(), time.time()
        self.transitions.append((self.since_wall, mode, why))
        self.wake.set()

    def drain(self):
        with self.lock:
            out, self.transitions = self.transitions, []
        return out


class Heartbeat(threading.Thread):
    def __init__(self, cfg, mode: ModeController, stop: threading.Event):
        super().__init__(name="heartbeat", daemon=True)
        self.client = CloudClient(cfg.cloud_url, cfg.api_key, timeout=cfg.probe_timeout)
        self.mode, self.stop_ev, self.interval = mode, stop, cfg.probe_interval

    def run(self):
        while not self.stop_ev.is_set():
            try:
                self.client.request("GET", "/v1/health", timeout=self.client.timeout)
                self.mode.report(True)
            except NetError as e:
                self.client.close()
                self.mode.report(False, str(e)[:120])
            self.stop_ev.wait(self.interval)
