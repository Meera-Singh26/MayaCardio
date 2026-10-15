#!/bin/bash
# The required 1-hour ONLINE-mode resource figures. Run ON the Raspberry Pi 4 (ARM64), mock + pipeline + cloud URL configured.
# usage: PIPELINE_CLOUD_URL=https://... PIPELINE_API_KEY=... scripts/pi_resource_1h.sh /path/to/biosignal-mock
set -e
R="$(cd "$(dirname "$0")/.." && pwd)"; MOCK=${1:-biosignal-mock}; D=${PIPELINE_DATA_DIR:-/tmp/res1h}
rm -rf "$D"; mkdir -p "$D"
$MOCK --mode signal --seed 7 --duration 3700 --port 7100 & MP=$!
sleep 1; "$R/bin/pipeline" run --data-dir "$D" & PP=$!
sleep 10
python3 "$R/scripts/measure_resources.py" --seconds 3600 --data-dir "$D" | tee "$R/evidence/resources_pi_1h.json"
echo "environment: $(uname -m), $(grep -m1 'Model' /proc/cpuinfo || echo unknown), $(nproc) cores, $(free -m | awk '/Mem/{print $2}') MB RAM" | tee -a "$R/evidence/resources_pi_1h.json"
kill $PP $MP
