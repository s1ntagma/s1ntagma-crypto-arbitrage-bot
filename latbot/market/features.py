"""Pure feature functions over MarketState data (easy to test offline)."""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:  # pragma: no cover
    from .state import MarketState


def _mono_of(point: tuple[float, float]) -> float:
    return point[0]


def move_over_window(
    series: Sequence[tuple[float, float]], now_mono: float, window_s: float
) -> tuple[float, float, float, float] | None:
    """Price move over ``[now_mono - window_s, now_mono]``.

    Returns ``(move_pct, start_price, end_price, start_mono)`` using the oldest
    point inside the window and the last point at or before ``now_mono``;
    ``None`` when fewer than two points fall inside the window.
    """
    if not series:
        return None
    start = now_mono - window_s
    i = bisect_left(series, start, key=_mono_of)  # first point with mono >= start
    j = bisect_right(series, now_mono, key=_mono_of)  # points with mono <= now are series[:j]
    if j - i < 2:
        return None
    start_mono, start_price = series[i]
    _, end_price = series[j - 1]
    if start_price <= 0:
        return None
    move_pct = (end_price / start_price - 1.0) * 100.0
    return move_pct, start_price, end_price, start_mono


def velocity_pct_s(move_pct: float, start_mono: float, end_mono: float) -> float:
    """Move speed in percent per second (0 when the interval is empty)."""
    dt = end_mono - start_mono
    if dt <= 0:
        return 0.0
    return move_pct / dt


def volume_acceleration(
    state: "MarketState", exchange: str, asset: str, now_mono: float, short_s: float = 5, long_s: float = 60
) -> float | None:
    """Recent traded quote volume rate vs the baseline rate.

    ``(short_vol / short_s) / (long_vol / long_s)``; ``None`` when no baseline
    volume exists (no trades) or the windows are invalid.
    """
    if short_s <= 0 or long_s <= 0:
        return None
    long_vol = state.recent_quote_volume(exchange, asset, long_s, now_mono)
    if long_vol <= 0:
        return None
    short_vol = state.recent_quote_volume(exchange, asset, short_s, now_mono)
    return (short_vol / short_s) / (long_vol / long_s)
