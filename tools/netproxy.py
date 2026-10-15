#!/usr/bin/env python3
"""TCP proxy with a control file, used to cut the "network" between device and cloud in tests.
mode file contents: ok | cut (refuse/close everything, fast errors) | blackhole (accept, swallow, never answer)"""
import argparse, os, select, socket, threading, time


def mode(path):
    try:
        return open(path).read().strip() or "ok"
    except OSError:
        return "ok"


def pipe(a, b, path, conns):
    try:
        while True:
            r, _, _ = select.select([a, b], [], [], 0.2)
            m = mode(path)
            if m != "ok":
                if m == "cut":
                    return
                for s in r:            # blackhole: read and drop
                    if s.recv(65536) == b"":
                        return
                continue
            for s in r:
                d = s.recv(65536)
                if not d:
                    return
                (b if s is a else a).sendall(d)
    except OSError:
        pass
    finally:
        for s in (a, b):
            try:
                s.close()
            except OSError:
                pass


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--listen", type=int, required=True)
    p.add_argument("--target", type=int, required=True)
    p.add_argument("--control", required=True)
    a = p.parse_args()
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", a.listen))
    srv.listen(64)
    while True:
        c, _ = srv.accept()
        m = mode(a.control)
        if m == "cut":
            c.close()
            continue
        try:
            u = socket.create_connection(("127.0.0.1", a.target), timeout=2) if m == "ok" else c
        except OSError:
            c.close()
            continue
        threading.Thread(target=pipe, args=(c, u, a.control, None), daemon=True).start()


if __name__ == "__main__":
    main()
