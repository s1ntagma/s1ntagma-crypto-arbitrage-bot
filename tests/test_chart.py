"""GET /api/opportunities/{id}/chart: real mid-price history of the movers and
the target, normalised to % from the window start.  No data -> no line."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from latbot.api import create_app
from latbot.config import Settings
from latbot.market.state import MarketState
from latbot.models import OppState
from tests.test_api import NOW, make_ctx, make_opp


def _ctx(tmp_path, series: dict[tuple[str, str], list[tuple[float, float]]], opps=None):
    ctx = make_ctx(tmp_path, opps=opps)
    state = MarketState(Settings(), ctx.clock)
    for key, pts in series.items():
        state._series[key] = list(pts)  # real series layout: [(recv_mono, mid)]
    ctx.state = state
    return ctx


def _line(body: dict, exchange: str) -> dict:
    return next(s for s in body["series"] if s["exchange"] == exchange)


def test_chart_returns_normalised_real_series(tmp_path):
    ctx = _ctx(tmp_path, {
        ("binance", "BTC"): [(NOW - 20, 100.0), (NOW - 10, 102.0), (NOW - 2, 104.0)],
        ("bybit", "BTC"): [(NOW - 25, 100.0), (NOW - 10, 100.5), (NOW - 1, 101.0)],
    })
    with TestClient(create_app(ctx)) as c:
        r = c.get("/api/opportunities/opp_1/chart?seconds=30")
    assert r.status_code == 200
    body = r.json()
    assert body["has_data"] is True and body["seconds"] == 30
    mover = _line(body, "binance")
    target = _line(body, "bybit")
    assert mover["role"] == "signal" and target["role"] == "target"
    assert mover["points"][0] == [-20.0, 0.0]
    assert mover["points"][-1][1] == pytest.approx(4.0)
    assert target["points"][-1][1] == pytest.approx(1.0)
    assert all(-30.0 <= p[0] <= 0.0 for s in body["series"] for p in s["points"])
    # detection marker: the opportunity was first seen at NOW -> 0 s ago
    assert body["detected_at_s"] == pytest.approx(0.0)


def test_chart_window_cuts_old_points(tmp_path):
    ctx = _ctx(tmp_path, {
        ("bybit", "BTC"): [(NOW - 50, 90.0), (NOW - 12, 100.0), (NOW - 1, 101.0)],
        ("binance", "BTC"): [(NOW - 11, 100.0), (NOW - 1, 103.0)],
    })
    with TestClient(create_app(ctx)) as c:
        body = c.get("/api/opportunities/opp_1/chart?seconds=15").json()
    target = _line(body, "bybit")
    assert [p[0] for p in target["points"]] == [-12.0, -1.0]
    assert target["points"][0][1] == 0.0  # base = first point INSIDE the window, not the older one


def test_chart_no_data_is_reported_not_invented(tmp_path):
    ctx = _ctx(tmp_path, {})
    with TestClient(create_app(ctx)) as c:
        body = c.get("/api/opportunities/opp_1/chart").json()
    assert body["has_data"] is False
    assert body["series"] == []


def test_chart_single_point_is_not_a_line(tmp_path):
    ctx = _ctx(tmp_path, {("bybit", "BTC"): [(NOW - 1, 100.0)], ("binance", "BTC"): [(NOW - 1, 100.0)]})
    with TestClient(create_app(ctx)) as c:
        body = c.get("/api/opportunities/opp_1/chart").json()
    assert body["has_data"] is False


def test_chart_unknown_opportunity_404(tmp_path):
    ctx = _ctx(tmp_path, {})
    with TestClient(create_app(ctx)) as c:
        assert c.get("/api/opportunities/nope/chart").status_code == 404


def test_chart_seconds_clamped_and_validated(tmp_path):
    ctx = _ctx(tmp_path, {("bybit", "BTC"): [(NOW - 59, 100.0), (NOW - 1, 101.0)], ("binance", "BTC"): [(NOW - 59, 100.0), (NOW - 1, 102.0)]})
    with TestClient(create_app(ctx)) as c:
        assert c.get("/api/opportunities/opp_1/chart?seconds=9999").json()["seconds"] == 60
        assert c.get("/api/opportunities/opp_1/chart?seconds=1").json()["seconds"] == 5
        assert c.get("/api/opportunities/opp_1/chart?seconds=abc").status_code == 400


def test_chart_terminal_opportunity_anchored_at_expiry(tmp_path):
    opp = make_opp("opp_x", state=OppState.EXPIRED)
    opp.expired_mono = NOW - 30.0
    ctx = _ctx(tmp_path, {
        ("bybit", "BTC"): [(NOW - 50, 100.0), (NOW - 40, 100.2), (NOW - 31, 100.9)],
        ("binance", "BTC"): [(NOW - 50, 100.0), (NOW - 40, 102.0), (NOW - 31, 103.0)],
    }, opps=[opp])
    with TestClient(create_app(ctx)) as c:
        body = c.get("/api/opportunities/opp_x/chart?seconds=30").json()
    assert body["has_data"] is True
    assert _line(body, "bybit")["points"][-1][0] == pytest.approx(-1.0)


def test_chart_decimates_long_series_keeping_last_point(tmp_path):
    pts = [(NOW - 59 + i * 0.1, 100.0 + i * 0.001) for i in range(580)]
    ctx = _ctx(tmp_path, {("bybit", "BTC"): pts, ("binance", "BTC"): pts})
    with TestClient(create_app(ctx)) as c:
        body = c.get("/api/opportunities/opp_1/chart?seconds=60").json()
    line = _line(body, "bybit")
    assert len(line["points"]) <= 121
    assert line["points"][-1][0] == pytest.approx(pts[-1][0] - NOW)


def test_chart_without_market_state_is_empty_not_error(tmp_path):
    ctx = make_ctx(tmp_path)  # state=None
    with TestClient(create_app(ctx)) as c:
        body = c.get("/api/opportunities/opp_1/chart").json()
    assert body["has_data"] is False
