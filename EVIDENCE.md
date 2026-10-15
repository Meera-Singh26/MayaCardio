# Evidence of failure tests

All runs below were done in my sandbox (x86-64 Linux, 1 core), with my own mock (`tools/biosignal_mock.py`), NOT the judges' mock,
NOT on a Raspberry Pi 4 and with a local cloud process. Raw outputs are in `evidence/*.json`. Re-run on the Pi before submitting.

| Test | What happened | Result |
|---|---|---|
| Window logic (`tests.test_windowing`) | Compared with an independent naive global implementation, clean data + random gaps | pass |
| Mock faults (corrupt CRC, duplicate reply, 3 s stall, sensor dropout, 4x speed) | Corrupt re-read, duplicate dropped, reconnect after stall, losses listed with reason | ground truth ok, device export == cloud export, 240 windows verified |
| Network: silent drop 40 s, then refused connections 20 s (`e2e_network.json`) | Offline detected after 4.1 s / 0.1 s, online again after 3.2 s / 1.2 s | limit 10 s: pass |
| Same run: online result latency | 132 online windows, max 0.36 s, median 0.22 s, 0 over 5 s (loopback) | limit 5 s: pass on loopback |
| Same run: kill -9 of the pipeline + restart | 80 000 samples per sensor stored, 0 lost, 0 duplicates, cloud == device, 68 windows offline + 132 online, all re-verified | pass |
| Pipeline crash, then pipeline+mock killed together (`e2e_powercut.json`) | No loss on pipeline-only crash; after joint kill old session closed with `to_index: null` losses; new session complete | pass |
| Catch-up of 2 h outage (`bench_backlog.json`) | 5 760 000 samples, fresh cloud, loopback | 61 s (limit 15 min), exactly once |

## Resource figures (NOT the required 1 h on a Pi)
4 minutes, online mode, real-time rate, x86-64 sandbox (`evidence/resources_sandbox_4min.json`): average CPU 1.5 % of one core,
peak RSS 25.9 MB, 66 MB written to disk by the process (database file grew 12.9 MB), 1.19 MB sent and 0.29 MB received.
Finding: with `fsync` on every commit the write volume is about 5x the data size (WAL + checkpoint, about 1 GB/hour).
Fine for an SSD or industrial card; for a cheap SD card consider one commit per second (still <= 1 s power-cut loss) or a log-structured file.
**To do on the Pi:** `scripts/pi_resource_1h.sh` produces the required 1 h figures (state the environment used).

## Not yet done (cannot be done in my environment)
* Real power cut, real Raspberry Pi 4 run, free-tier cloud deployment, the 5 minute demo video (`scripts/pi_demo.sh` is the script).
* Testing against the provided `biosignal-mock` and `check_ground_truth` (not supplied to me): wire format implemented from section 6 of the brief.
