"""GET /api/opportunities/{id}/chart: real mid-price history of the movers and
the target.  One shared pre-move reference, real movers only, no data -> no line."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from latbot.api import create_app
from latbot.config import Settings
from latbot.market.state import MarketState
from latbot.models import OppState
from tests.test_api import NOW, make_ctx, make_opp


def _ctx(tmp_path, series, opps=None):
    ctx = make_ctx(tmp_path, opps=opps)
    state = MarketState(Settings(), ctx.clock)
    for key, pts in series.items():
        state._series[key] = list(pts)  # real series layout: [(recv_mono, mid)]
    ctx.state = state
    return ctx


def _opp(move_start=NOW - 1.8, reacted=None, **kw):
    o = make_opp("opp_1", **kw)
    o.signal.first_move_start_mono = move_start
    if reacted is not None:
        o.signal.reacted = reacted
    return o


def _get(ctx, opp_id="opp_1", seconds=30):
    with TestClient(create_app(ctx)) as c:
        r = c.get(f"/api/opportunities/{opp_id}/chart?seconds={seconds}")
    assert r.status_code == 200, r.text
    return r.json()


def _line(body, exchange):
    return next(s for s in body["series"] if s["exchange"] == exchange)


def test_lines_share_the_pre_move_reference_so_the_gap_is_the_lag(tmp_path):
    # move window 5 s: reference = move start (NOW-20) - 5 s = NOW-25
    ctx = _ctx(tmp_path, {
        ("binance", "BTC"): [(NOW - 28, 100.0), (NOW - 20, 101.0), (NOW - 10, 102.0), (NOW - 2, 104.0)],
        ("bybit", "BTC"): [(NOW - 29, 200.0), (NOW - 10, 201.0), (NOW - 1, 202.0)],
    }, opps=[_opp(move_start=NOW - 20)])
    body = _get(ctx)
    assert body["has_data"] is True and body["unit"] == "pct_from_pre_move"
    mover, target = _line(body, "binance"), _line(body, "bybit")
    assert mover["role"] == "signal" and target["role"] == "target"
    assert mover["points"][-1][1] == pytest.approx(4.0)
    assert target["points"][-1][1] == pytest.approx(1.0)
    assert body["move_started_at_s"] == pytest.approx(-20.0)
    assert body["detected_at_s"] == pytest.approx(0.0)
    assert all(-30.0 <= p[0] <= 0.0 for s in body["series"] for p in s["points"])


def test_move_older_than_the_window_still_shows_the_gap(tmp_path):
    # reviewer repro: binance jumped 100 -> 102 at -35 s, kraken 100 -> 100.3; window 15 s
    opp = _opp(move_start=NOW - 35, target="kraken")
    ctx = _ctx(tmp_path, {
        ("binance", "BTC"): [(NOW - 50, 100.0), (NOW - 35, 102.0), (NOW - 5, 102.0)],
        ("kraken", "BTC"): [(NOW - 50, 100.0), (NOW - 33, 100.3), (NOW - 4, 100.3)],
    }, opps=[opp])
    body = _get(ctx, seconds=15)
    mover, target = _line(body, "binance"), _line(body, "kraken")
    assert mover["points"][0] == [-15.0, pytest.approx(2.0)]  # price in effect at the window edge, carried forward
    assert mover["points"][-1][1] == pytest.approx(2.0)
    assert target["points"][-1][1] == pytest.approx(0.3)


def test_only_real_movers_in_the_signal_direction_are_drawn(tmp_path):
    # MoveDetector puts EVERY fresh exchange into reacted (flat, opposite, other targets)
    reacted = {"binance": 1.0, "bitget": 0.0, "bitstamp": 0.0, "bybit": 0.0, "kucoin": -0.8, "mexc": 0.9, "htx": 0.1}
    pts = [(NOW - 20, 100.0), (NOW - 1, 101.0)]
    ctx = _ctx(tmp_path, {(ex, "BTC"): pts for ex in reacted}, opps=[_opp(move_start=NOW - 10, reacted=reacted)])
    body = _get(ctx)
    assert [(s["exchange"], s["role"]) for s in body["series"]] == [("binance", "signal"), ("mexc", "signal"), ("bybit", "target")]


def test_no_reference_in_history_falls_back_to_window_start(tmp_path):
    ctx = _ctx(tmp_path, {
        ("bybit", "BTC"): [(NOW - 12, 100.0), (NOW - 1, 101.0)],
        ("binance", "BTC"): [(NOW - 11, 100.0), (NOW - 1, 103.0)],
    }, opps=[_opp(move_start=NOW - 59)])
    body = _get(ctx, seconds=15)
    assert body["unit"] == "pct_from_window_start"
    target = _line(body, "bybit")
    assert [p[0] for p in target["points"]] == [-12.0, -1.0]
    assert target["points"][0][1] == 0.0


def test_no_data_is_reported_not_invented(tmp_path):
    body = _get(_ctx(tmp_path, {}))
    assert body["has_data"] is False
    assert body["series"] == []


def test_single_point_is_not_a_line(tmp_path):
    ctx = _ctx(tmp_path, {("bybit", "BTC"): [(NOW - 1, 100.0)], ("binance", "BTC"): [(NOW - 1, 100.0)]})
    assert _get(ctx)["has_data"] is False


def test_unknown_opportunity_404(tmp_path):
    with TestClient(create_app(_ctx(tmp_path, {}))) as c:
        assert c.get("/api/opportunities/nope/chart").status_code == 404


def test_seconds_clamped_and_validated(tmp_path):
    pts = [(NOW - 59, 100.0), (NOW - 1, 101.0)]
    ctx = _ctx(tmp_path, {("bybit", "BTC"): pts, ("binance", "BTC"): pts})
    with TestClient(create_app(ctx)) as c:
        assert c.get("/api/opportunities/opp_1/chart?seconds=9999").json()["seconds"] == 60
        assert c.get("/api/opportunities/opp_1/chart?seconds=1").json()["seconds"] == 5
        assert c.get("/api/opportunities/opp_1/chart?seconds=-5").json()["seconds"] == 5
        assert c.get("/api/opportunities/opp_1/chart?seconds=abc").status_code == 400


def test_terminal_opportunity_anchored_at_expiry(tmp_path):
    opp = make_opp("opp_x", state=OppState.EXPIRED)
    opp.expired_mono = NOW - 30.0
    opp.signal.first_move_start_mono = NOW - 45.0
    ctx = _ctx(tmp_path, {
        ("bybit", "BTC"): [(NOW - 55, 100.0), (NOW - 40, 100.2), (NOW - 31, 100.9)],
        ("binance", "BTC"): [(NOW - 55, 100.0), (NOW - 40, 102.0), (NOW - 31, 103.0)],
    }, opps=[opp])
    body = _get(ctx, "opp_x", 30)
    assert body["has_data"] is True
    assert _line(body, "bybit")["points"][-1][0] == pytest.approx(-1.0)
    assert _line(body, "binance")["points"][-1][1] == pytest.approx(3.0)


def test_decimates_long_series_keeping_last_point(tmp_path):
    pts = [(NOW - 59 + i * 0.1, 100.0 + i * 0.001) for i in range(580)]
    ctx = _ctx(tmp_path, {("bybit", "BTC"): pts, ("binance", "BTC"): pts})
    line = _line(_get(ctx, seconds=60), "bybit")
    assert len(line["points"]) <= 121
    assert line["points"][-1][0] == pytest.approx(pts[-1][0] - NOW)


def test_without_market_state_is_empty_not_error(tmp_path):
    ctx = make_ctx(tmp_path)  # state=None
    with TestClient(create_app(ctx)) as c:
        assert c.get("/api/opportunities/opp_1/chart").json()["has_data"] is False


def test_woff2_fonts_are_served_with_a_font_type(tmp_path):
    with TestClient(create_app(make_ctx(tmp_path))) as c:
        r = c.get("/static/fonts/space-grotesk-latin.woff2")
    assert r.status_code == 200 and r.headers["content-type"].startswith("font/woff2")
