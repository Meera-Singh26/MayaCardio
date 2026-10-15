#!/bin/bash
# Test helper: start/stop a local mock + cloud (+ optional proxy) + device daemon under a work dir.
# usage: stack.sh start <workdir> "<mock args>" [proxy]   |   stack.sh stop <workdir>
R="$(cd "$(dirname "$0")/.." && pwd)"; export PYTHONPATH="$R"
cmd=$1; W=$2
case $cmd in
start)
  mkdir -p "$W"; cd "$W"
  setsid python3 $R/tools/biosignal_mock.py --port 7100 --truth "$W/truth.jsonl" $3 > mock.log 2>&1 < /dev/null & echo $! > mock.pid
  setsid python3 -m pipeline cloud --port 8081 --db "$W/cloud.db" --api-key k > cloud.log 2>&1 < /dev/null & echo $! > cloud.pid
  URL=http://127.0.0.1:8081
  if [ "$4" = "proxy" ]; then
    echo ok > "$W/net.mode"
    setsid python3 $R/tools/netproxy.py --listen 8082 --target 8081 --control "$W/net.mode" > proxy.log 2>&1 < /dev/null & echo $! > proxy.pid
    URL=http://127.0.0.1:8082
  fi
  sleep 1
  setsid python3 -m pipeline run --data-dir "$W/dev" --cloud-url $URL --api-key k > run.log 2>&1 < /dev/null & echo $! > run.pid
  ;;
stop)
  cd "$W"; for f in run mock cloud proxy; do [ -f $f.pid ] && kill $(cat $f.pid) 2>/dev/null; done ;;
esac
