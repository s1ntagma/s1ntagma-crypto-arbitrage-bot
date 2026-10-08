"""DurationEstimator - how long an opportunity window lasts (seconds from
first_seen).  Base = learned median duration (or median reaction delay, or
the default); refined with the live decay of ``net_history`` once enough
points exist.  Clamped to ``[min_opp_duration_s, max_opp_duration_s]`` and
never below the current age + 0.1 s.
"""
from __future__ import annotations

import logging
from typing import Any

from ..config import Settings
from ..models import LagSignal, Opportunity

log = logging.getLogger(__name__)

_FIT_POINTS = 30
_MIN_SPAN_S = 1.0


def _slope(points: list[tuple[float, float]]) -> float | None:
    """Least-squares slope of y over x; None when x has no spread."""
    n = len(points)
    mx = sum(p[0] for p in points) / n
    my = sum(p[1] for p in points) / n
    sxx = sum((p[0] - mx) ** 2 for p in points)
    if sxx <= 0:
        return None
    sxy = sum((p[0] - mx) * (p[1] - my) for p in points)
    return sxy / sxx


class DurationEstimator:
    """Estimates the total window length of an opportunity."""

    def __init__(self, settings: Settings, stats: Any | None) -> None:
        self._settings = settings
        self._stats = stats

    def base_duration(self, asset: str, target: str) -> float:
        base: float | None = None
        if self._stats is not None:
            try:
                base = self._stats.median_duration(asset, target)
                if not base:
                    base = self._stats.median_reaction_delay(asset, target)
            except Exception:  # noqa: BLE001
                log.exception("stats lookup failed")
                base = None
        if not base or base <= 0:
            base = self._settings.default_opp_duration_s
        return float(base)

    def estimate(self, signal: LagSignal, opp: Opportunity | None, now_mono: float) -> float:
        s = self._settings
        base = self.base_duration(signal.asset, signal.target)
        result = base
        age = 0.0
        if opp is not None:
            age = opp.age_s(now_mono)
            pts = list(opp.net_history[-_FIT_POINTS:])
            if len(pts) >= 3 and (pts[-1][0] - pts[0][0]) >= _MIN_SPAN_S:
                slope = _slope(pts)
                if slope is not None and slope < 0:
                    net_now = max(0.0, pts[-1][1])
                    remaining = net_now / (-slope)
                    live = age + remaining
                    result = 0.5 * base + 0.5 * live
        result = max(s.min_opp_duration_s, min(s.max_opp_duration_s, result))
        if opp is not None:
            result = max(result, age + 0.1)
        if getattr(s, "simple_mode", False):
            result = min(result, float(s.simple_max_hold_s))  # simple mode: the window never exceeds the hold limit
        return result
