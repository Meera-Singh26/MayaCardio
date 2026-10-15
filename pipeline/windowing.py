"""Section 3 processing logic.

Pure functions only: the very same code runs on the device (offline mode) and in the
cloud (online mode).  A window result depends only on stored samples, never on timing.

Peak rule (global definition, see DESIGN.md "Heart rate"):
  candidate i  <=>  samples i-800..i-1, i and i+1 available, x[i] > x[i-1], x[i] > x[i+1],
                    2*x[i] > max(x[i-800..i-1])
  peak         <=>  candidate with  i - previous_accepted_peak >= 100   (greedy, in order)
The greedy chain is evaluated on a local slice that starts CHAIN_MARGIN samples before the
5-second range; it is anchored at the first candidate that no earlier candidate can block,
which makes the local result identical to a scan from sample 0.
"""
from __future__ import annotations

import math
from collections import deque
from datetime import datetime, timezone

SR = 400                    # samples / second / sensor
HR_SECONDS = 5              # window w plus the 4 before it
PEAK_HISTORY = 800
MIN_PEAK_GAP = 100
CHAIN_MARGIN = 1600         # extra scan before the 5 s range (anchoring the refractory chain)
LOOKBACK_WINDOWS = 10       # sensor-1 context needed before window w: SR*(w-10) .. SR*(w+1)
# Reading of "if any sample in those 5 seconds is missing": sensor 1 only (HR is computed from
# sensor 1). Flip to True to also require sensor 2 to be complete.
HR_REQUIRES_SENSOR2 = False


def need_ranges(w: int):
    """(sensor, lo, hi) index ranges (inclusive) that must be *resolved* to compute window w."""
    return ((1, SR * max(0, w - LOOKBACK_WINDOWS), SR * (w + 1)),
            (2, SR * w, SR * w + SR - 1))


def is_ready(w: int, src) -> bool:
    return all(src.resolved(s, lo, hi) for s, lo, hi in need_ranges(w))


def _stats(rows, nch):
    n = 0
    sums = [0] * nch
    mins = [None] * nch
    maxs = [None] * nch
    for r in rows:
        if r is None:
            continue
        n += 1
        for c in range(nch):
            v = r[c]
            sums[c] += v
            if mins[c] is None or v < mins[c]:
                mins[c] = v
            if maxs[c] is None or v > maxs[c]:
                maxs[c] = v
    if n == 0:
        return 0, [None] * nch, [None] * nch, [None] * nch
    return n, [round(sums[c] / n, 1) for c in range(nch)], mins, maxs


def candidate_peaks(x, base, c_lo):
    """Candidate peak indices (absolute) >= c_lo in list x (x[k] is sample base+k, None=missing)."""
    n = len(x)
    miss = [0] * (n + 1)
    for k, v in enumerate(x):
        miss[k + 1] = miss[k] + (v is None)
    cands = []
    dq: deque = deque()
    for k in range(n):
        if k >= 1:
            v = x[k - 1]
            if v is not None:
                while dq and x[dq[-1]] <= v:
                    dq.pop()
                dq.append(k - 1)
        while dq and dq[0] < k - PEAK_HISTORY:
            dq.popleft()
        if k < PEAK_HISTORY or k + 1 >= n or base + k < c_lo:
            continue
        xk = x[k]
        if xk is None or x[k + 1] is None:
            continue
        if miss[k] - miss[k - PEAK_HISTORY] != 0:
            continue
        if xk > x[k - 1] and xk > x[k + 1] and 2 * xk > x[dq[0]]:
            cands.append(base + k)
    return cands


def greedy_peaks(cands, c_lo):
    start = 0
    for j, c in enumerate(cands):
        if (j == 0 or c - cands[j - 1] >= MIN_PEAK_GAP) and (c_lo == 0 or c - c_lo >= MIN_PEAK_GAP):
            start = j
            break
    peaks, last = [], None
    for c in cands[start:]:
        if last is None or c - last >= MIN_PEAK_GAP:
            peaks.append(c)
            last = c
    return peaks


def heart_rate(w: int, src):
    if w < HR_SECONDS - 1:
        return None
    r_lo = SR * (w - (HR_SECONDS - 1))
    r_hi = SR * (w + 1) - 1
    c_lo = max(0, r_lo - CHAIN_MARGIN)
    base = max(0, c_lo - PEAK_HISTORY)
    x = src.fetch(1, base, r_hi + 1, ch=1)          # sensor 1, channel 2
    if any(v is None for v in x[r_lo - base: r_hi - base + 1]):
        return None
    if HR_REQUIRES_SENSOR2:
        if any(v is None for v in src.fetch(2, r_lo, r_hi, ch=0)):
            return None
    peaks = [p for p in greedy_peaks(candidate_peaks(x, base, c_lo), c_lo) if r_lo <= p <= r_hi]
    if len(peaks) < 2:
        return None
    return round(60.0 * SR * (len(peaks) - 1) / (peaks[-1] - peaks[0]), 1)


def compute_window(w: int, src) -> dict:
    lo, hi = SR * w, SR * w + SR - 1
    a = src.fetch(1, lo, hi)
    b = src.fetch(2, lo, hi)
    n1, mean1, min1, max1 = _stats(a, 3)
    n2, mean2, min2, max2 = _stats(b, 2)
    mags = [math.sqrt(r[0] * r[0] + r[1] * r[1]) for r in b if r is not None]
    return {
        "window": w,
        "missing_1": SR - n1,
        "missing_2": SR - n2,
        "complete": n1 == SR and n2 == SR,
        "mean": {"1": mean1, "2": mean2},
        "min": {"1": min1, "2": min2},
        "max": {"1": max1, "2": max2},
        "magnitude": round(math.fsum(mags) / len(mags), 1) if mags else None,
        "heart_rate": heart_rate(w, src),
    }


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def run_windows(src, session: str, start_w: int, last_index, mode: str, limit: int = 100):
    """Compute consecutive ready windows from start_w. Returns list of result bodies."""
    out = []
    w = start_w
    if last_index is None:
        return out
    while SR * w <= last_index and len(out) < limit and is_ready(w, src):
        body = compute_window(w, src)
        body["session"] = session
        body["mode"] = mode
        body["processed_at"] = now_iso()
        out.append(body)
        w += 1
    return out
