"""`pipeline` command line: run | cloud | status | export | results | verify | sessions"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from .config import Config
from .store import Store, StoreSource
from .windowing import compute_window


def _env(name, default):
    return os.environ.get(name, default)


def _common(p):
    p.add_argument("--data-dir", default=_env("PIPELINE_DATA_DIR", "./data"))
    p.add_argument("--cloud-url", default=_env("PIPELINE_CLOUD_URL", "http://127.0.0.1:8080"))
    p.add_argument("--api-key", default=_env("PIPELINE_API_KEY", ""))


def _cfg(a) -> Config:
    c = Config(data_dir=a.data_dir, cloud_url=a.cloud_url, api_key=a.api_key)
    for k in ("mock_host", "mock_port", "sync"):
        if hasattr(a, k) and getattr(a, k) is not None:
            setattr(c, k, getattr(a, k))
    return c


def _resolve_session(store: Store, sid: str) -> str:
    if sid in (None, "latest"):
        s = store.latest_session()
        if not s:
            print("no sessions yet", file=sys.stderr)
            sys.exit(2)
        return s
    return sid


def cmd_run(a):
    from .device import daemon
    return daemon.run(_cfg(a))


def cmd_cloud(a):
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    from .cloud import server
    server.serve(a.host, a.port, a.db, a.api_key, a.sync)


def cmd_status(a):
    cfg = _cfg(a)
    state = {}
    try:
        state = json.load(open(cfg.state_path))
    except (OSError, ValueError):
        pass
    alive = bool(state) and time.time() - state.get("heartbeat_wall", 0) < 5
    out = {"mode": state.get("mode", "stopped") if alive else "stopped",
           "last_mode": state.get("mode"), "daemon_running": alive, "session": None,
           "samples_saved": 0, "samples_in_cloud": 0, "backlog": 0, "lost_samples": 0}
    if os.path.exists(cfg.db_path):
        st = Store(cfg.db_path, init=False)
        sid = st.latest_session()
        out["session"] = sid
        if sid:
            out["samples_saved"] = st.count_samples(sid)
            out["samples_in_cloud"] = st.count_samples(sid, table="cloud_ranges")
            out["backlog"] = max(0, out["samples_saved"] - out["samples_in_cloud"])
            out["lost_samples"] = st.lost_total(sid)
            out["per_sensor_saved"] = {str(s): st.count_samples(sid, s) for s in (1, 2)}
        tot_saved = sum(st.count_samples(s["session"]) for s in st.sessions())
        tot_cloud = sum(st.count_samples(s["session"], table="cloud_ranges") for s in st.sessions())
        out["all_sessions"] = {"count": len(st.sessions()), "samples_saved": tot_saved,
                               "samples_in_cloud": tot_cloud, "backlog": max(0, tot_saved - tot_cloud)}
        out["mode_since"] = state.get("since_wall")
        out["bytes_sent"], out["bytes_recv"] = state.get("bytes_sent"), state.get("bytes_recv")
    print(json.dumps(out))


def _cloud_client(a):
    from .device.cloudclient import CloudClient
    return CloudClient(a.cloud_url, a.api_key, timeout=30)


def cmd_export(a):
    out = sys.stdout
    if a.source == "device":
        st = Store(_cfg(a).db_path, init=False)
        for ln in st.iter_export(_resolve_session(st, a.session)):
            out.write(ln + "\n")
    else:
        for ln in _cloud_client(a).stream_lines(f"/v1/sessions/{a.session}/export"):
            out.write(ln + "\n")


def cmd_results(a):
    if a.source == "device":
        st = Store(_cfg(a).db_path, init=False)
        for b in st.results(_resolve_session(st, a.session)):
            print(json.dumps(b, separators=(",", ":")))
    else:
        for ln in _cloud_client(a).stream_lines(f"/v1/sessions/{a.session}/results"):
            print(ln)


def cmd_verify(a):
    """Recompute every stored result from the stored samples and compare (all fields but mode/time)."""
    st = Store(a.db, init=False)
    sid = _resolve_session(st, a.session)
    src, bad, n = StoreSource(st, sid), [], 0
    for b in st.results(sid):
        ref = compute_window(b["window"], src)
        n += 1
        for k, v in ref.items():
            if b.get(k) != v:
                bad.append({"window": b["window"], "mode": b["mode"], "field": k, "stored": b.get(k), "recomputed": v})
    print(json.dumps({"session": sid, "windows_checked": n, "mismatches": len(bad), "examples": bad[:5]}))
    return 1 if bad else 0


def cmd_sessions(a):
    st = Store(_cfg(a).db_path, init=False)
    for s in st.sessions():
        print(json.dumps(s))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="device daemon (collector + uploader + offline processing)")
    _common(r)
    r.add_argument("--mock-host", default=_env("PIPELINE_MOCK_HOST", "127.0.0.1"))
    r.add_argument("--mock-port", type=int, default=int(_env("PIPELINE_MOCK_PORT", "7100")))
    r.add_argument("--sync", default=_env("PIPELINE_SYNC", "FULL"))
    r.set_defaults(fn=cmd_run)
    c = sub.add_parser("cloud", help="cloud service")
    c.add_argument("--host", default=_env("HOST", "0.0.0.0"))
    c.add_argument("--port", type=int, default=int(_env("PORT", "8080")))
    c.add_argument("--db", default=_env("PIPELINE_CLOUD_DB", "./cloud.db"))
    c.add_argument("--api-key", default=_env("PIPELINE_API_KEY", ""))
    c.add_argument("--sync", default=_env("PIPELINE_SYNC", "FULL"))
    c.set_defaults(fn=cmd_cloud)
    s = sub.add_parser("status")
    _common(s)
    s.set_defaults(fn=cmd_status)
    e = sub.add_parser("export")
    _common(e)
    e.add_argument("--session", default="latest")
    e.add_argument("--from", dest="source", choices=["device", "cloud"], default="device")
    e.set_defaults(fn=cmd_export)
    rs = sub.add_parser("results")
    _common(rs)
    rs.add_argument("--session", default="latest")
    rs.add_argument("--from", dest="source", choices=["device", "cloud"], default="device")
    rs.set_defaults(fn=cmd_results)
    v = sub.add_parser("verify")
    v.add_argument("--db", required=True)
    v.add_argument("--session", default="latest")
    v.set_defaults(fn=cmd_verify)
    ss = sub.add_parser("sessions")
    _common(ss)
    ss.set_defaults(fn=cmd_sessions)
    a = p.parse_args(argv)
    return a.fn(a) or 0


if __name__ == "__main__":
    sys.exit(main())
