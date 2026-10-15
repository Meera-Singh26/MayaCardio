# Never Lose a Heartbeat: device + cloud pipeline

Stdlib-only Python 3.9+ (no pip install, no compiler). Device side runs on a Raspberry Pi 4 (ARM64),
cloud side is the same package behind a tiny HTTP server (any free tier / any Linux box).

```
biosignal-mock --TCP--> [collector] --fsync--> device.db (SQLite WAL) --HTTPS--> [cloud service] --> cloud.db
                                   \-> offline window processing (same code as cloud) 
```

## 1. Run on a fresh board
```bash
sudo apt-get update && sudo apt-get install -y python3        # already present on Raspberry Pi OS
git clone <this repo> /opt/mayacardio-pipeline && cd /opt/mayacardio-pipeline
biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100 &

export PIPELINE_CLOUD_URL=https://YOUR-CLOUD-HOST PIPELINE_API_KEY=your-secret PIPELINE_DATA_DIR=/var/lib/mayacardio
./bin/pipeline run                      # collector + uploader + offline processor (Ctrl-C / SIGTERM to stop)
```
Auto-start after reboot / power cut: `deploy/pipeline.service` (systemd, `Restart=always`).

## 2. Deploy the cloud (free tier)
Any of these work; the service is one process, SQLite file, `PORT` env var:
* **Render (free web service)**: `deploy/render.yaml` + `deploy/Dockerfile`; set `PIPELINE_API_KEY`.
* **Oracle Cloud Always Free (ARM VM)**: `PIPELINE_API_KEY=... ./bin/pipeline cloud --port 8080` (put Caddy/nginx in front for TLS).
* **Locally for testing**: `./bin/pipeline cloud --port 8081 --db cloud.db --api-key k`.
Ephemeral-disk hosts are fine: after a cloud wipe the device notices (reconcile on reconnect) and re-sends everything missing.

## 3. Required commands
```bash
./bin/pipeline status                                  # one JSON line: mode, samples_saved, samples_in_cloud, backlog, ...
./bin/pipeline export  --session ID --from device      # JSON lines: samples, then one line per loss
./bin/pipeline export  --session ID --from cloud
./bin/pipeline results --session ID                    # one JSON line per window and mode (add --from cloud)
./bin/pipeline verify  --db data/device.db             # recompute every stored result from stored samples
./bin/pipeline sessions                                # --session latest is the default everywhere
```
`status`: `samples_saved`, `samples_in_cloud`, `backlog` refer to the latest session (both sensors);
`all_sessions` has the totals; `lost_samples` counts finite listed losses.

Result line (per window): `session, window, missing_1, missing_2, complete, mean/min/max {"1":[3 ch],"2":[2 ch]},
magnitude, heart_rate, mode (online|offline), processed_at`. `null` where the brief says null.

## 4. Tests
```bash
python3 -m unittest tests.test_windowing                 # window logic vs an independent naive global implementation
python3 tests/e2e_network.py /tmp/e2e                    # ~4 min: network blackhole/refusal, kill -9, latency, exactly-once
python3 tests/e2e_powercut.py /tmp/pc                    # ~2 min: kill -9 pipeline, then pipeline+mock together
python3 tests/bench_backlog.py /tmp/bench 2              # 2 h outage backlog -> fresh cloud
scripts/pi_demo.sh                                       # network cut + process kill + power cut for the demo video
scripts/pi_resource_1h.sh                                # the 1-hour resource figures on the Pi
```
`tools/` holds my own stand-ins for the provided mock (`biosignal_mock.py`, with fault injection and time compression),
a ground-truth checker and a TCP proxy used to cut the network. They are test aids, not the judges' tools.

## 5. Configuration (env or flags)
`PIPELINE_DATA_DIR`, `PIPELINE_CLOUD_URL`, `PIPELINE_API_KEY`, `PIPELINE_MOCK_HOST/PORT`, `PIPELINE_SYNC` (SQLite synchronous; keep `FULL`).
Timing constants live in `pipeline/config.py` (poll 0.3 s, mock timeout 3 s, heartbeat 1 s / 2 s timeout, 2 failures => offline, 2 successes => online).
