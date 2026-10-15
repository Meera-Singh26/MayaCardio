#!/usr/bin/env python3
"""Catch-up benchmark: a device that holds a 2-hour outage (2 x 2.88M samples) syncs to a fresh cloud.
Loopback only: WAN latency adds ~ (#requests x RTT) on top (see DESIGN.md)."""
import json, os, sqlite3, subprocess, sys, time

R = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
W = sys.argv[1] if len(sys.argv) > 1 else "/tmp/bench"
HOURS = float(sys.argv[2]) if len(sys.argv) > 2 else 2
N = int(HOURS * 3600 * 400)
env = dict(os.environ, PYTHONPATH=R)
subprocess.run(["rm", "-rf", W]); os.makedirs(W + "/dev")
sys.path.insert(0, R)
from pipeline.store import Store
st = Store(W + "/dev/device.db", sync="OFF")
sid = "bench0001"
t = time.time()
with st.tx():
    st.ensure_session(sid, "ended")
    for sensor, k, nch in ((1, 3, 3), (2, 2, 2)):
        M = 8388608
        st.conn.executemany("INSERT INTO samples VALUES(?,?,?,?,?,?)",
                            ((sid, sensor, i, (k * i + 1) % M, (k * i + 2) % M, (k * i + 3) % M if nch == 3 else None) for i in range(N)))
        st.merge_range(sid, sensor, "data", 0, N - 1)
        st.set_end(sid, sensor, N)
    st.set_meta(f"nw:{sid}", N // 400)           # windows were already computed offline during the outage
print(f"generated {2*N} samples in {time.time()-t:.0f}s", flush=True)
st.close()
cloud = subprocess.Popen([sys.executable, "-m", "pipeline", "cloud", "--port", "8091", "--db", W + "/cloud.db", "--api-key", "k"], env=env,
                         stdout=open(W + "/cloud.log", "w"), stderr=subprocess.STDOUT)
time.sleep(1)
t0 = time.time()
dev = subprocess.Popen([sys.executable, "-m", "pipeline", "run", "--data-dir", W + "/dev", "--cloud-url", "http://127.0.0.1:8091", "--api-key", "k"],
                       env=env, stdout=open(W + "/run.log", "w"), stderr=subprocess.STDOUT)
while True:
    time.sleep(5)
    s = json.loads(subprocess.run([sys.executable, "-m", "pipeline", "status", "--data-dir", W + "/dev"], env=env, capture_output=True, text=True).stdout)
    print(f"{time.time()-t0:5.0f}s backlog={s['backlog']}", flush=True)
    if s["backlog"] == 0 and s["samples_in_cloud"] == 2 * N:
        break
    if time.time() - t0 > 1500:
        break
el = time.time() - t0
cdb = sqlite3.connect(W + "/cloud.db")
rows = cdb.execute("select count(*) from samples").fetchone()[0]
res = {"hours_of_backlog": HOURS, "samples": 2 * N, "cloud_rows": rows, "seconds_to_sync": round(el), "samples_per_second": round(2 * N / el),
       "within_15_min": el < 900, "exactly_once": rows == 2 * N}
json.dump(res, open(R + "/evidence/bench_backlog.json", "w"), indent=1)
print(json.dumps(res, indent=1))
dev.terminate(); cloud.terminate()
