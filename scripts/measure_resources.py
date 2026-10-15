#!/usr/bin/env python3
"""Resource report for the device pipeline (reads /proc; stdlib only; works on the Pi).
usage: measure_resources.py --seconds 3600 --data-dir DIR [--interval 5]
Reports average CPU %, peak RSS, MB written to disk, MB sent/received over the network."""
import argparse, glob, json, os, time


def pids(pattern):
    out = []
    for p in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            c = open(p, "rb").read().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if pattern in c and "measure_resources" not in c:
            out.append(int(p.split("/")[2]))
    return out


def stat(pid):
    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    ticks = int(f[11]) + int(f[12])
    rss = int(open(f"/proc/{pid}/statm").read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    io = dict(l.split(": ") for l in open(f"/proc/{pid}/io").read().splitlines())
    return ticks, rss, int(io["write_bytes"])


ap = argparse.ArgumentParser()
ap.add_argument("--seconds", type=int, default=3600)
ap.add_argument("--interval", type=float, default=5)
ap.add_argument("--data-dir", required=True)
ap.add_argument("--match", default="pipeline run")
a = ap.parse_args()
hz = os.sysconf("SC_CLK_TCK")
t0, base = time.time(), {}
peak_rss, samples, last_t = 0, [], {}
w0 = {}
end = t0 + a.seconds
while time.time() < end:
    tot_rss = 0
    for pid in pids(a.match):
        try:
            ticks, rss, wb = stat(pid)
        except (OSError, IndexError):
            continue
        base.setdefault(pid, (ticks, wb))
        last_t[pid] = (ticks, wb)
        tot_rss += rss
    peak_rss = max(peak_rss, tot_rss)
    samples.append(tot_rss)
    time.sleep(a.interval)
el = time.time() - t0
cpu_s = sum((last_t[p][0] - base[p][0]) / hz for p in last_t)
wr = sum(last_t[p][1] - base[p][1] for p in last_t)
st = {}
try:
    st = json.load(open(os.path.join(a.data_dir, "state.json")))
except OSError:
    pass
print(json.dumps({"seconds": round(el), "avg_cpu_percent_of_one_core": round(100 * cpu_s / el, 2),
                  "peak_rss_mb": round(peak_rss / 1048576, 1),
                  "disk_written_mb_(process write_bytes)": round(wr / 1048576, 1),
                  "db_size_mb": round(sum(os.path.getsize(p) for p in glob.glob(os.path.join(a.data_dir, "device.db*"))) / 1048576, 1),
                  "network_sent_mb_(app level, cumulative since start)": round(st.get("bytes_sent", 0) / 1048576, 2),
                  "network_recv_mb_(app level, cumulative since start)": round(st.get("bytes_recv", 0) / 1048576, 2)}, indent=1))
