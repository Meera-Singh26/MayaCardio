import random
import unittest

from pipeline import windowing as W
from tools.biosignal_mock import sample
from tools.naive_reference import reference_hr, reference_peaks, reference_window


class MemSource:
    def __init__(self, s1, s2, lost1=(), lost2=()):
        self.s = {1: s1, 2: s2}
        self.lost = {1: list(lost1), 2: list(lost2)}

    def fetch(self, sensor, lo, hi, ch=None):
        d = self.s[sensor]
        return [(d[i][ch] if ch is not None else d[i]) if i in d else None for i in range(lo, hi + 1)]

    def resolved(self, sensor, lo, hi):
        return all(i in self.s[sensor] or any(a <= i <= b for a, b in self.lost[sensor]) for i in range(lo, hi + 1))


def build(n_windows, gaps, seed=7, mode="signal"):
    s1 = {i: tuple(sample(mode, seed, 1, i)) for i in range(400 * n_windows)}
    s2 = {i: tuple(sample(mode, seed, 2, i)) for i in range(400 * n_windows)}
    lost1, lost2 = [], []
    for sensor, a, b in gaps:
        d, l = (s1, lost1) if sensor == 1 else (s2, lost2)
        for i in range(a, b + 1):
            d.pop(i, None)
        l.append((a, b))
    return MemSource(s1, s2, lost1, lost2)


class T(unittest.TestCase):
    def check(self, gaps, nw=40):
        src = build(nw, gaps)
        peaks = reference_peaks(src.s[1], 400 * nw)
        for w in range(nw - 1):
            got = W.compute_window(w, src)
            ref = reference_window(w, src.s[1], src.s[2])
            for k, v in ref.items():
                self.assertEqual(got[k], v, (w, k))
            self.assertEqual(got["heart_rate"], reference_hr(w, peaks, src.s[1]), ("hr", w))

    def test_clean(self):
        self.check([])

    def test_gaps(self):
        self.check([(1, 3000, 3399), (2, 5000, 5100), (1, 9000, 9001), (1, 12345, 14000)])

    def test_random_gaps(self):
        rnd = random.Random(3)
        for _ in range(3):
            g = [(rnd.choice((1, 2)), a, a + rnd.randint(0, 900)) for a in sorted(rnd.randrange(800, 14000) for _ in range(5))]
            self.check(g)

    def test_hr_values(self):
        src = build(150, [])
        self.assertAlmostEqual(W.compute_window(60, src)["heart_rate"], 60.0, delta=0.5)
        self.assertAlmostEqual(W.compute_window(140, src)["heart_rate"], 90.0, delta=1.5)


if __name__ == "__main__":
    unittest.main()
