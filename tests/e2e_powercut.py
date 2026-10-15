#!/usr/bin/env python3
"""Hard-crash test: SIGKILL pipeline AND mock together (what a power cut does to the board), then boot both
again. The old session must be closed with an open-ended loss; every committed sample must survive.
Also covers a plain pipeline crash while the mock keeps running (re-read from the 10 s buffer)."""
import json, os, signal, subprocess, sys, time

R = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
W = sys.argv[1] if len(sys.argv) > 1 else "/tmp/pc"
env = dict(os.environ, PYTHONPATH=R)
sh = lambda *a: subprocess.run(a, env=env, capture_output=True, text=True)
subprocess.run(["rm", "-rf", W]); os.makedirs(W)
def popen(args, log):
    return subprocess.Popen(args, env=env, cwd=W, stdout=open(f"{W}/{log}", "a"), stderr=subprocess.STDOUT, start_new_session=True)
mock = lambda dur: popen([sys.executable, f"{R}/tools/biosignal_mock.py", "--port", "7100", "--mode", "ramp", "--seed", "7",
                          "--duration", str(dur), "--speed", "2", "--truth", f"{W}/truth.jsonl"], "mock.log")
cloud = popen([sys.executable, "-m", "pipeline", "cloud", "--port", "8081", "--db", f"{W}/cloud.db", "--api-key", "k"], "cloud.log")
pipe = lambda: popen([sys.executable, "-m", "pipeline", "run", "--data-dir", f"{W}/dev", "--cloud-url", "http://127.0.0.1:8081", "--api-key", "k"], "run.log")
m = mock(600); time.sleep(1); p = pipe()
time.sleep(30)
# phase 1: pipeline-only crash (mock keeps its buffer) ----------------------------------------
os.kill(p.pid, signal.SIGKILL); time.sleep(3); p = pipe(); time.sleep(15)
# phase 2: power cut = kill both -----------------------------------------------------------------
os.kill(p.pid, signal.SIGKILL); os.kill(m.pid, signal.SIGKILL); time.sleep(2)
m = mock(100); time.sleep(1); p = pipe(); time.sleep(60)
for q in (p, m, cloud):
    q.terminate()
sessions = [json.loads(l)["session"] for l in sh(sys.executable, "-m", "pipeline", "sessions", "--data-dir", f"{W}/dev").stdout.splitlines()]
out = {"sessions": sessions}
for sid in sessions:
    exp = sh(sys.executable, "-m", "pipeline", "export", "--session", sid, "--from", "device", "--data-dir", f"{W}/dev").stdout
    open(f"{W}/exp_{sid}.jsonl", "w").write(exp)
    r = json.loads(sh(sys.executable, f"{R}/tools/ground_truth.py", "--export", f"{W}/exp_{sid}.jsonl", "--truth", f"{W}/truth.jsonl", "--session", sid).stdout)
    losses = [json.loads(l) for l in exp.splitlines() if "from_index" in l]
    out[sid] = {"ground_truth_ok": r["ok"], "errors": r["errors"], "summary": r["summary"], "loss_lines": losses}
os.makedirs(f"{R}/evidence", exist_ok=True)
json.dump(out, open(f"{R}/evidence/e2e_powercut.json", "w"), indent=1)
print(json.dumps(out, indent=1))
