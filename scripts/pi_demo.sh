#!/bin/bash
# Demo for the <=5 min video: network cut, process kill, power cut. Run on the Pi (mock + cloud configured).
# Power cut: pull the plug (or `sudo systemctl reboot -f`), then show `bin/pipeline status` and the exports after boot.
R="$(cd "$(dirname "$0")/.." && pwd)"; P="$R/bin/pipeline"
say(){ echo; echo "=== $* ==="; }
say "1. status (online)";            $P status
say "2. cut the network (30 s)";     sudo iptables -A OUTPUT -p tcp --dport 443 -j DROP; sleep 15; $P status; sleep 15
say "3. restore network";            sudo iptables -D OUTPUT -p tcp --dport 443 -j DROP; sleep 8; $P status; sleep 20; $P status
say "4. kill -9 the pipeline";       kill -9 "$(pgrep -f 'pipeline run' | head -1)"; sleep 3; sudo systemctl restart pipeline; sleep 10; $P status
say "5. power cut: pull the plug now, then after boot run:  $P status ; $P export --session <old id> --from device | tail -3"
