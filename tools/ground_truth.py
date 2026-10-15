#!/usr/bin/env python3
"""Compare a pipeline export with the mock's ground-truth log (our stand-in for check_ground_truth).

Checks per session/sensor:
 1. every exported value equals the generator value for its index        (no wrong values)
 2. no index is exported twice, and no index is both sample and loss      (exactly once)
 3. every index the mock delivered with a good CRC is a sample or a declared loss (nothing silently dropped)
 4. every index below the known produced count is a sample or in a loss   (no silent loss)
"""
import argparse, json, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.biosignal_mock import sample

INF = 1 << 62


def load_truth(path):
    sess = {}
    order = []
    for ln in open(path):
        e = json.loads(ln)
        s = sess.setdefault(e["session"], {"deliv": {1: set(), 2: set()}, "produced": None, "meta": None, "mock_lost": {1: [], 2: []}}) if "session" in e else None
        if e["ev"] == "session_start":
            s["meta"] = e
            order.append(e["session"])
        elif e["ev"] == "deliver" and not e["corrupt"]:
            s["deliv"][e["sensor"]].update(range(e["first"], e["first"] + e["count"]))
        elif e["ev"] == "restart":
            s["produced"] = e["produced"]
        elif e["ev"] == "lost":
            s["mock_lost"][e["sensor"]].append((e["from"], e["to"]))
    return sess, order


def check(export_lines, truth_path, session, produced_hint=None):
    sess, _ = load_truth(truth_path)
    t = sess[session]
    meta = t["meta"]
    errs = []
    data = {1: {}, 2: {}}
    losses = {1: [], 2: []}
    for ln in export_lines:
        o = json.loads(ln)
        if "index" in o:
            d = data[o["sensor"]]
            if o["index"] in d:
                errs.append(f"duplicate sample s{o['sensor']} idx {o['index']}")
            d[o["index"]] = o["values"]
        else:
            if not o.get("reason"):
                errs.append(f"loss without reason {o}")
            losses[o["sensor"]].append((o["from_index"], INF if o["to_index"] is None else o["to_index"]))
    summary = {}
    for s in (1, 2):
        bad = sum(1 for i, v in data[s].items() if v != sample(meta["mode"], meta["seed"], s, i))
        if bad:
            errs.append(f"s{s}: {bad} wrong values")
        lost_idx = set()
        for a, b in losses[s]:
            if b >= INF:
                if any(i >= a for i in data[s]):
                    errs.append(f"s{s}: open-ended loss from {a} overlaps stored samples")
                continue
            lost_idx.update(range(a, b + 1))
        both = lost_idx & data[s].keys()
        if both:
            errs.append(f"s{s}: {len(both)} indices are both sample and loss")
        miss = t["deliv"][s] - data[s].keys() - lost_idx   # sent into a dead socket => mock re-reports it lost
        if miss:
            errs.append(f"s{s}: {len(miss)} delivered samples missing (e.g. {min(miss)})")
        top = (t["produced"] or {}).get(str(s))
        if top is None:
            top = min(meta["total"], (max(list(data[s]) + [i for a, b in losses[s] for i in (a, b) if b < INF] + [-1]) + 1))
        open_from = min([a for a, b in losses[s] if b >= INF] or [INF])
        unacc = [i for i in range(min(top, open_from)) if i not in data[s] and i not in lost_idx]
        if unacc:
            errs.append(f"s{s}: {len(unacc)} indices neither stored nor listed lost (first {unacc[0]})")
        summary[f"s{s}"] = {"samples": len(data[s]), "lost_idx": len(lost_idx), "open_loss_from": None if open_from == INF else open_from}
    return errs, summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--export", required=True)
    ap.add_argument("--truth", required=True)
    ap.add_argument("--session", required=True)
    a = ap.parse_args()
    errs, summ = check(open(a.export), a.truth, a.session)
    print(json.dumps({"session": a.session, "ok": not errs, "summary": summ, "errors": errs[:20]}))
    sys.exit(1 if errs else 0)


if __name__ == "__main__":
    main()
