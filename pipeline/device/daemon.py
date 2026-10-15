from __future__ import annotations

import logging
import os
import signal
import sys
import threading

from ..store import Store
from .collector import Collector
from .controller import Controller
from .netmode import Heartbeat, ModeController


def _lock(f) -> bool:
    """Single-instance lock; works on Linux/macOS (flock) and Windows (msvcrt)."""
    try:
        import fcntl
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError:
        import msvcrt
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
    except OSError:
        return False
    return True


def _reserve(cfg):
    p = os.path.join(cfg.data_dir, "reserve.bin")
    if os.path.exists(p) or cfg.reserve_mb <= 0:
        return
    try:
        with open(p, "wb") as f:
            os.posix_fallocate(f.fileno(), 0, cfg.reserve_mb * 1024 * 1024)
    except (OSError, AttributeError):
        pass


def run(cfg) -> int:
    os.makedirs(cfg.data_dir, exist_ok=True)
    lock = open(os.path.join(cfg.data_dir, "pipeline.lock"), "w")
    if not _lock(lock):
        print("another pipeline instance is already running on this data dir", file=sys.stderr)
        return 1
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                        handlers=[logging.StreamHandler(sys.stderr),
                                  logging.FileHandler(os.path.join(cfg.data_dir, "pipeline.log"))])
    Store(cfg.db_path, cfg.sync).close()
    _reserve(cfg)
    stop, wake = threading.Event(), threading.Event()
    mode = ModeController()
    collector = Collector(cfg, stop, wake)
    hb = Heartbeat(cfg, mode, stop)
    ctl = Controller(cfg, mode, wake, stop, collector, hb)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    for t in (collector, hb, ctl):
        t.start()
    stop.wait()
    for t in (collector, hb, ctl):
        t.join(timeout=5)
    return 0
