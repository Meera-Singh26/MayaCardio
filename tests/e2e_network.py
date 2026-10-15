#!/usr/bin/env python3
"""Real-time end-to-end failure test (speed 1). Produces evidence/e2e_network.json.
Timeline: blackhole network 40 s -> restore -> hard refusal 20 s -> restore -> kill -9 pipeline -> restart.
Checks: mode switch latency (<10 s), online result latency (<5 s), exactly-once cloud copy, results verify."""
import json, os, signal, sqlite3, subprocess, sys, time

R = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
W = sys.argv[1] if len(sys.argv) > 1 else "/tmp/e2e"
DUR = 200
env = dict(os.environ, PYTHONPATH=R)
sh = lambda *a: subprocess.run(a, env=env, capture_output=True, text=True)
sh(f"{R}/scripts/stack.sh", "stop", W); subprocess.run(["rm", "-rf", W])
subprocess.run([f"{R}/scripts/stack.sh", "start", W, f"--mode signal --seed 7 --duration {DUR} --speed 1", "proxy"], env=env)
net = lambda m: open(f"{W}/net.mode", "w").write(m)
log = []
t0 = time.time()
def at(t):
    time.sleep(max(0, t0 + t - time.time()))
def mark(kind):
    log.append((kind, time.time())); print(f"{time.time()-t0:6.1f}s {kind}", flush=True)

at(25); net("blackhole"); mark("cut_blackhole")
at(65); net("ok"); mark("restore_1")
at(85); net("cut"); mark("cut_refuse")
at(105); net("ok"); mark("restore_2")
at(125)
pid = int(open(f"{W}/run.pid").read()); os.kill(pid, signal.SIGKILL); mark("kill9_pipeline")
time.sleep(2)
subprocess.Popen([sys.executable, "-m", "pipeline", "run", "--data-dir", f"{W}/dev", "--cloud-url", "http://127.0.0.1:8082", "--api-key", "k"],
                 env=env, cwd=W, stdout=open(f"{W}/run2.log", "w"), stderr=subprocess.STDOUT, start_new_session=True)
mark("restart_pipeline")
at(DUR + 40)

db = sqlite3.connect(f"{W}/dev/device.db")
ev = db.execute("select ts,kind,detail from events where kind like 'mode_%' order by id").fetchall()
res = {"mode_events": [(round(t - t0, 1), k, d) for t, k, d in ev], "marks": [(k, round(t - t0, 1)) for k, t in log]}
def first_after(kind, t):
    for ts, k, _ in ev:
        if k == kind and ts >= t:
            return round(ts - t, 1)
cm = dict(log)
res["detect_offline_after_blackhole_s"] = first_after("mode_offline", cm["cut_blackhole"])
res["back_online_after_restore_1_s"] = first_after("mode_online", cm["restore_1"])
res["detect_offline_after_refuse_s"] = first_after("mode_offline", cm["cut_refuse"])
res["back_online_after_restore_2_s"] = first_after("mode_online", cm["restore_2"])
truth = [json.loads(l) for l in open(f"{W}/truth.jsonl")]
start = [e for e in truth if e["ev"] == "session_start"][0]
sid = start["session"]
lat = []
cl = sqlite3.connect(f"{W}/cloud.db")
from datetime import datetime
for (body,) in cl.execute("select body from results where mode='online' and session=?", (sid,)):
    b = json.loads(body)
    pa = datetime.fromisoformat(b["processed_at"]).timestamp()
    lat.append(pa - (start["wall"] + (b["window"] + 1)))
res["online_windows"] = len(lat)
res["online_latency_s"] = {"max": round(max(lat), 2), "p50": round(sorted(lat)[len(lat) // 2], 2), "over_5s": sum(1 for x in lat if x > 5)} if lat else None
res["offline_windows"] = db.execute("select count(*) from results where mode='offline' and session=?", (sid,)).fetchone()[0]
exp = {}
for src in ("device", "cloud"):
    out = sh(sys.executable, "-m", "pipeline", "export", "--session", sid, "--from", src, "--data-dir", f"{W}/dev", "--cloud-url", "http://127.0.0.1:8081", "--api-key", "k").stdout
    open(f"{W}/exp_{src}.jsonl", "w").write(out); exp[src] = out
    r = sh(sys.executable, f"{R}/tools/ground_truth.py", "--export", f"{W}/exp_{src}.jsonl", "--truth", f"{W}/truth.jsonl", "--session", sid)
    res[f"ground_truth_{src}"] = json.loads(r.stdout)
res["cloud_equals_device_export"] = sorted(exp["device"].splitlines()) == sorted(exp["cloud"].splitlines())
res["verify_device"] = json.loads(sh(sys.executable, "-m", "pipeline", "verify", "--db", f"{W}/dev/device.db", "--session", sid).stdout)
res["verify_cloud"] = json.loads(sh(sys.executable, "-m", "pipeline", "verify", "--db", f"{W}/cloud.db", "--session", sid).stdout)
res["status"] = json.loads(sh(sys.executable, "-m", "pipeline", "status", "--data-dir", f"{W}/dev").stdout)
res["events_sample"] = [(round(t - t0, 1), k, d[:90]) for t, k, d in db.execute("select ts,kind,detail from events order by id")][:60]
os.makedirs(f"{R}/evidence", exist_ok=True)
json.dump(res, open(f"{R}/evidence/e2e_network.json", "w"), indent=1)
print(json.dumps({k: v for k, v in res.items() if k not in ("events_sample", "mode_events")}, indent=1))
sh(f"{R}/scripts/stack.sh", "stop", W)
