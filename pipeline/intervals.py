"""Inclusive integer interval sets (sorted, disjoint, merged when adjacent)."""
from __future__ import annotations

INF = 1 << 62  # "open ended" upper bound (unknown end of a lost range)


class IntervalSet:
    __slots__ = ("iv",)

    def __init__(self, items=()):
        self.iv: list[list[int]] = []
        for a, b in items:
            self.add(a, b)

    def add(self, a: int, b: int) -> None:
        if b < a:
            return
        out: list[list[int]] = []
        placed = False
        for x, y in self.iv:
            if y < a - 1:
                out.append([x, y])
            elif x > b + 1:
                if not placed:
                    out.append([a, b])
                    placed = True
                out.append([x, y])
            else:
                a = min(a, x)
                b = max(b, y)
        if not placed:
            out.append([a, b])
        self.iv = out

    def covers(self, a: int, b: int) -> bool:
        """True when every integer of [a, b] is inside the set."""
        if b < a:
            return True
        for x, y in self.iv:
            if y < a:
                continue
            return x <= a and y >= b
        return False

    def gaps(self, a: int, b: int) -> list[tuple[int, int]]:
        """Sub-ranges of [a, b] NOT covered by the set."""
        out: list[tuple[int, int]] = []
        cur = a
        for x, y in self.iv:
            if y < cur:
                continue
            if x > b:
                break
            if x > cur:
                out.append((cur, min(x - 1, b)))
            cur = max(cur, y + 1)
            if cur > b:
                break
        if cur <= b:
            out.append((cur, b))
        return out

    def total(self) -> int:
        return sum(min(y, INF - 1) - x + 1 for x, y in self.iv if y < INF)

    def frontier(self) -> int:
        """First index not covered when counting contiguously from 0."""
        if self.iv and self.iv[0][0] == 0:
            return INF if self.iv[0][1] >= INF else self.iv[0][1] + 1
        return 0

    def to_list(self) -> list[list[int]]:
        return [list(p) for p in self.iv]

    def __iter__(self):
        return iter(self.iv)

    def __bool__(self):
        return bool(self.iv)
