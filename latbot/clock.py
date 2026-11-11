"""Time helpers.

Two clocks are used everywhere:
  * wall  - time.time(), seconds since epoch, for storage / display
  * mono  - time.monotonic(), for durations / ages / latency (never jumps)

Every market event carries a Timestamps object (see models.Timestamps) that
distinguishes exchange timestamp, local receive time and processing time.
"""
from __future__ import annotations

import time
from dataclasses import dataclass


class Clock:
    """Injectable clock so backtests can replay recorded data deterministically."""

    def wall(self) -> float:
        return time.time()

    def mono(self) -> float:
        return time.monotonic()


class SimClock(Clock):
    """Deterministic clock for tests / backtests.  Advance it manually."""

    def __init__(self, wall: float = 1_700_000_000.0, mono: float = 1000.0) -> None:
        self._wall = wall
        self._mono = mono

    def wall(self) -> float:
        return self._wall

    def mono(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        self._wall += seconds
        self._mono += seconds

    def set(self, wall: float | None = None, mono: float | None = None) -> None:
        if wall is not None:
            self._wall = wall
        if mono is not None:
            self._mono = mono


@dataclass(slots=True)
class Ewma:
    """Exponentially weighted moving average for latency tracking (ms)."""

    alpha: float = 0.2
    value: float | None = None
    samples: int = 0

    def update(self, x: float) -> float:
        if self.value is None:
            self.value = x
        else:
            self.value = self.alpha * x + (1.0 - self.alpha) * self.value
        self.samples += 1
        return self.value

    def get(self, default: float) -> float:
        return default if self.value is None else self.value


def ms(seconds: float) -> float:
    return seconds * 1000.0
