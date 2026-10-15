"""Independent, deliberately naive implementation of section 3 (global scan) used only by tests."""
import math


def reference_window(w, s1, s2):
    """s1/s2: dict idx -> tuple. Returns dict of outputs (no mode/session)."""
    SR = 400
    def stats(d, n):
        rows = [d[i] for i in range(SR * w, SR * w + SR) if i in d]
        if not rows:
            return len(rows), [None] * n, [None] * n, [None] * n
        return (len(rows), [round(sum(r[c] for r in rows) / len(rows), 1) for c in range(n)],
                [min(r[c] for r in rows) for c in range(n)], [max(r[c] for r in rows) for c in range(n)])
    n1, a1, b1, c1 = stats(s1, 3)
    n2, a2, b2, c2 = stats(s2, 2)
    mags = [math.sqrt(d[0] ** 2 + d[1] ** 2) for i, d in s2.items() if SR * w <= i < SR * w + SR]
    return {"missing_1": SR - n1, "missing_2": SR - n2, "complete": n1 == SR and n2 == SR,
            "mean": {"1": a1, "2": a2}, "min": {"1": b1, "2": b2}, "max": {"1": c1, "2": c2},
            "magnitude": round(sum(mags) / len(mags), 1) if mags else None}


def reference_peaks(s1, upto):
    """Global greedy scan from sample 0."""
    peaks, last = [], None
    for i in range(800, upto):
        if i not in s1 or (i + 1) not in s1:
            continue
        if any((j not in s1) for j in range(i - 800, i)):
            continue
        x = s1[i][1]
        if x > s1[i - 1][1] and x > s1[i + 1][1] and 2 * x > max(s1[j][1] for j in range(i - 800, i)):
            if last is None or i - last >= 100:
                peaks.append(i)
                last = i
    return peaks


def reference_hr(w, peaks, s1):
    SR = 400
    if w < 4:
        return None
    lo, hi = SR * (w - 4), SR * (w + 1) - 1
    if any(i not in s1 for i in range(lo, hi + 1)):
        return None
    p = [x for x in peaks if lo <= x <= hi]
    if len(p) < 2:
        return None
    gaps = [(p[k + 1] - p[k]) / SR for k in range(len(p) - 1)]
    return round(60 / (sum(gaps) / len(gaps)), 1)
