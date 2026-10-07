"""Offline tests for latbot.market.features."""
from __future__ import annotations

import pytest

from latbot.market.features import move_over_window, velocity_pct_s, volume_acceleration


def test_move_over_window_selects_oldest_point_in_window():
    series = [(1000.0, 100.0), (1002.0, 101.0), (1004.0, 102.0), (1006.0, 103.0)]
    r = move_over_window(series, now_mono=1006.0, window_s=5.0)
    assert r is not None
    move, start_price, end_price, start_mono = r
    assert start_mono == 1002.0 and start_price == 101.0 and end_price == 103.0
    assert move == pytest.approx((103.0 / 101.0 - 1) * 100)
    # points after now are ignored
    r2 = move_over_window(series, now_mono=1004.0, window_s=10.0)
    assert r2 is not None and r2[2] == 102.0 and r2[3] == 1000.0


def test_move_over_window_needs_two_points():
    assert move_over_window([], 1000.0, 5.0) is None
    assert move_over_window([(1000.0, 100.0)], 1000.0, 5.0) is None
    assert move_over_window([(900.0, 100.0), (1000.0, 101.0)], 1000.0, 5.0) is None
    assert move_over_window([(999.0, 0.0), (1000.0, 101.0)], 1000.0, 5.0) is None


def test_velocity():
    assert velocity_pct_s(2.0, 1000.0, 1004.0) == 0.5
    assert velocity_pct_s(2.0, 1000.0, 1000.0) == 0.0
    assert velocity_pct_s(2.0, 1001.0, 1000.0) == 0.0


class _StubState:
    def __init__(self, volumes: dict[float, float]) -> None:
        self.volumes = volumes  # window -> volume

    def recent_quote_volume(self, exchange: str, asset: str, window_s: float, now_mono: float) -> float:
        return self.volumes.get(window_s, 0.0)


def test_volume_acceleration():
    st = _StubState({5.0: 500.0, 60.0: 1200.0})
    assert volume_acceleration(st, "binance", "BTC", 1000.0) == pytest.approx((500 / 5) / (1200 / 60))
    assert volume_acceleration(_StubState({}), "binance", "BTC", 1000.0) is None
    assert volume_acceleration(st, "binance", "BTC", 1000.0, short_s=0) is None
