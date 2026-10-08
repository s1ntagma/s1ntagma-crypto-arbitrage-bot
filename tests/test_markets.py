"""Offline tests for latbot.context.markets.TradFiMarkets (Yahoo v8 chart API).

Fixtures (tests/fixtures/context/calendar-markets/, captured 2026-10-08 ~13:01 UTC): ES=F and
^TNX 1-minute charts (unmodified), DX-Y.NYB (last 240 bars), the "No data found" answer for
DX=F and the 429 body Yahoo returns without a browser User-Agent.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from latbot.clock import SimClock
from latbot.context.base import HttpResponse, SourceError
from latbot.context.markets import (
    BROWSER_UA,
    DEFAULT_SYMBOLS,
    YAHOO_CHART_URL,
    TradFiMarkets,
    parse_chart,
)

FIX = Path(__file__).parent / "fixtures" / "context" / "calendar-markets"
FETCH_WALL = 1791464509.0  # ~13:01:49 UTC, when the fixtures were captured
ES_URL = YAHOO_CHART_URL.format(symbol="ES=F")
TNX_URL = YAHOO_CHART_URL.format(symbol="%5ETNX")
DXY_URL = YAHOO_CHART_URL.format(symbol="DX-Y.NYB")
DXF_URL = YAHOO_CHART_URL.format(symbol="DX=F")


def fx(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def ok(text: str) -> HttpResponse:
    return HttpResponse(200, text, {"content-type": "application/json"})


class FakeHttp:
    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = dict(routes)
        self.calls: list[tuple[str, dict | None, dict | None]] = []

    async def get(self, url: str, *, params: dict | None = None, headers: dict | None = None,
                  timeout_s: float = 10.0) -> HttpResponse:
        self.calls.append((url, params, headers))
        r = self.routes.get(url)
        if r is None:
            return HttpResponse(404, '{"chart":{"result":null,"error":{"code":"Not Found","description":"x"}}}', {})
        if isinstance(r, list):
            r = r.pop(0) if len(r) > 1 else r[0]
        if isinstance(r, Exception):
            raise r
        return r

    async def close(self) -> None:
        pass


def settings(**over: Any) -> SimpleNamespace:
    base: dict[str, Any] = dict(markets_enabled=True, markets_poll_s=60,
                                markets_symbols={"SPX_FUT": "ES=F", "US10Y": "^TNX", "DXY": "DX-Y.NYB"})
    base.update(over)
    return SimpleNamespace(**base)


def routes() -> dict[str, Any]:
    return {ES_URL: ok(fx("yahoo_chart_ESF_1m_1d.json")), TNX_URL: ok(fx("yahoo_chart_TNX_1m_1d.json")),
            DXY_URL: ok(fx("yahoo_chart_DXY_1m_1d_trimmed.json"))}


async def loaded(**over: Any) -> tuple[TradFiMarkets, FakeHttp, SimClock]:
    clock = SimClock(wall=FETCH_WALL, mono=100.0)
    http = FakeHttp(routes())
    m = TradFiMarkets(settings(**over), clock, http)
    for _ in range(len(m.symbols)):
        assert await m.step() is True
    return m, http, clock


def test_parse_chart_fixture_and_errors() -> None:
    meta, bars = parse_chart(json.loads(fx("yahoo_chart_ESF_1m_1d.json")))
    assert meta["symbol"] == "ES=F" and meta["regularMarketTime"] == 1791463907
    assert len(bars) == 523  # 524 points, the trailing live point folds into its minute
    assert bars[-1] == (1791463860, 7817.25)  # the latest point inside the minute wins
    assert all(b[0] % 60 == 0 for b in bars) and [b[0] for b in bars] == sorted(b[0] for b in bars)
    _, tnx = parse_chart(json.loads(fx("yahoo_chart_TNX_1m_1d.json")))
    assert len(tnx) == 27  # one null close dropped, never filled
    with pytest.raises(SourceError, match="No data found"):
        parse_chart(json.loads(fx("yahoo_chart_DXF_not_found.json")))
    with pytest.raises(SourceError):
        parse_chart({"unexpected": True})


async def test_staggered_round_robin_with_browser_user_agent() -> None:
    m, http, clock = await loaded()
    assert m.interval_s == pytest.approx(20.0)  # 60 s / 3 symbols
    assert [c[0] for c in http.calls] == [ES_URL, TNX_URL, DXY_URL]
    assert TNX_URL.endswith("/%5ETNX")
    for _, params, headers in http.calls:
        assert params == {"interval": "1m", "range": "1d"}
        assert headers["User-Agent"] == BROWSER_UA and "latbot" not in headers["User-Agent"]
    assert not m.due(clock.mono() + 19.9) and m.due(clock.mono() + 20.0)
    assert m.status.ok and m.status.items == 3


async def test_quote_futures_delayed_with_age_and_changes() -> None:
    m, _, _ = await loaded()
    q = m.quote("SPX_FUT")
    assert q is not None
    assert q["symbol"] == "ES=F" and q["last"] == 7817.25 and q["prev_close"] == 7852.75
    assert q["delayed"] is True and q["source"] == "yahoo"
    assert q["data_age_s"] == pytest.approx(FETCH_WALL - 1791463907)  # ~10 min CME delay
    assert q["ts"] == 1791463907
    assert q["chg_1d_pct"] == pytest.approx((7817.25 / 7852.75 - 1) * 100)
    assert q["chg_15m_pct"] == pytest.approx((7817.25 / 7819.5 - 1) * 100)
    assert q["chg_1h_pct"] == pytest.approx((7817.25 / 7818.75 - 1) * 100)
    assert q["market_open"] is True and q["is_yield"] is False
    assert "chg_15m_bp" not in q


async def test_yield_quote_in_percent_with_basis_points() -> None:
    m, _, _ = await loaded()
    q = m.quote("US10Y")
    assert q is not None and q["is_yield"] is True
    assert q["last"] == pytest.approx(5.324)  # ^TNX is already in percent
    assert q["chg_1d_bp"] == pytest.approx((5.324 - 5.277) * 100)
    assert q["chg_15m_bp"] == pytest.approx((5.324000358581543 - 5.320000171661377) * 100)
    assert q["chg_1h_pct"] is None and q["chg_1h_bp"] is None  # only 27 minutes of session data: no guess
    assert q["data_age_s"] == pytest.approx(FETCH_WALL - 1791463595)  # ~15 min Cboe delay


async def test_market_open_false_when_stale_or_outside_session() -> None:
    m, _, clock = await loaded()
    assert m.quote("SPX_FUT", FETCH_WALL + 1800)["market_open"] is False  # data too old (default 1800 s)
    m2, _, _ = await loaded(markets_stale_s=7200)
    assert m2.quote("SPX_FUT", FETCH_WALL + 1800)["market_open"] is True
    assert m2.quote("US10Y", 1791486000 + 60)["market_open"] is False  # after the Cboe session end
    assert m.quote("US10Y", FETCH_WALL + 3600)["data_age_s"] == pytest.approx(FETCH_WALL + 3600 - 1791463595)


async def test_returns_1m_consecutive_minutes_only() -> None:
    m, _, _ = await loaded()
    r = m.returns_1m("SPX_FUT", 30)
    assert 0 < len(r) <= 30
    assert all(b[0] - a[0] >= 60 for a, b in zip(r, r[1:]))
    _, bars = parse_chart(json.loads(fx("yahoo_chart_ESF_1m_1d.json")))
    closes = dict(bars)
    last_minute, pct = r[-1]
    assert last_minute == bars[-1][0]
    assert pct == pytest.approx((closes[last_minute] / closes[last_minute - 60] - 1) * 100)
    # a missing minute is skipped, never interpolated
    tnx = m.returns_1m("US10Y", 60)
    tnx_minutes = {b[0] for b in parse_chart(json.loads(fx("yahoo_chart_TNX_1m_1d.json")))[1]}
    assert all(t in tnx_minutes and t - 60 in tnx_minutes for t, _ in tnx)
    assert len(tnx) == 26  # 27 consecutive bars (the null bar's minute is covered by the live point)
    assert m.returns_1m("SPX_FUT", 0) == [] and m.returns_1m("NOPE", 10) == []


async def test_returns_1m_never_bridges_a_gap() -> None:
    payload = json.loads(fx("yahoo_chart_TNX_1m_1d.json"))
    res = payload["chart"]["result"][0]
    res["timestamp"] = [1791462000, 1791462060, 1791462120, 1791462300, 1791462360]
    res["indicators"]["quote"][0]["close"] = [5.30, 5.31, None, 5.33, 5.34]
    clock = SimClock(wall=FETCH_WALL)
    m = TradFiMarkets(settings(markets_symbols={"US10Y": "^TNX"}), clock, FakeHttp({TNX_URL: ok(json.dumps(payload))}))
    assert await m.step() is True
    r = m.returns_1m("US10Y", 60)
    assert [t for t, _ in r] == [1791462060, 1791462360]
    assert r[0][1] == pytest.approx((5.31 / 5.30 - 1) * 100) and r[1][1] == pytest.approx((5.34 / 5.33 - 1) * 100)


async def test_rate_limit_backs_off_and_keeps_quotes() -> None:
    m, http, clock = await loaded()
    http.routes[ES_URL] = HttpResponse(429, fx("yahoo_429_body.txt"), {})
    assert await m.step() is False
    assert "429" in m.status.last_error and "SPX_FUT" in m.errors
    assert not m.due(clock.mono() + 299)  # Retry-After absent -> 300 s
    assert m.quote("SPX_FUT")["last"] == 7817.25  # last data kept, its age keeps growing


async def test_one_bad_symbol_does_not_stop_the_others() -> None:
    clock = SimClock(wall=FETCH_WALL, mono=100.0)
    r = routes()
    r[DXF_URL] = HttpResponse(404, fx("yahoo_chart_DXF_not_found.json"), {})
    http = FakeHttp(r)
    m = TradFiMarkets(settings(markets_symbols={"SPX_FUT": "ES=F", "DXF": "DX=F"}), clock, http)
    assert await m.step() is True
    assert await m.step() is True  # DX=F failed but ES=F is fine
    assert "No data found" in m.errors["DXF"]
    assert m.quote("DXF") is None and m.quote("SPX_FUT") is not None
    # once every symbol fails the status turns red, even though old quotes are kept
    http.routes[ES_URL] = OSError("network down")
    assert await m.step() is False
    assert m.status.ok is False and m.quote("SPX_FUT") is not None
    snap = m.snapshot()
    json.dumps(snap)
    assert snap["quotes"]["DXF"] is None and snap["errors"]["DXF"] and snap["delayed"] is True
    assert "задержка" in snap["note"]


async def test_nothing_fetched_yet_reports_error() -> None:
    clock = SimClock(wall=FETCH_WALL)
    http = FakeHttp({DXF_URL: HttpResponse(404, fx("yahoo_chart_DXF_not_found.json"), {})})
    m = TradFiMarkets(settings(markets_symbols={"DXF": "DX=F"}), clock, http)
    assert await m.step() is False
    assert "No data found" in m.status.last_error
    http.routes[DXF_URL] = OSError("connection reset")
    clock.advance(600)
    assert await m.step() is False and "OSError" in m.status.last_error


async def test_bars_merge_across_polls_and_are_trimmed() -> None:
    m, http, clock = await loaded(markets_keep_minutes=100)
    payload = json.loads(fx("yahoo_chart_ESF_1m_1d.json"))
    res = payload["chart"]["result"][0]
    nxt = 1791463920
    res["timestamp"] = [nxt, nxt + 60]
    res["indicators"]["quote"][0]["close"] = [7820.0, 7821.0]
    res["meta"]["regularMarketTime"] = nxt + 60
    res["meta"]["regularMarketPrice"] = 7821.0
    http.routes[ES_URL] = ok(json.dumps(payload))
    clock.advance(60)
    assert await m.step() is True
    store_len = len(m.returns_1m("SPX_FUT", 10_000))
    assert store_len <= 100
    r = m.returns_1m("SPX_FUT", 2)
    assert r[-1][0] == nxt + 60 and r[-1][1] == pytest.approx((7821.0 / 7820.0 - 1) * 100)
    assert r[-2][1] == pytest.approx((7820.0 / 7817.25 - 1) * 100)
    assert m.quote("SPX_FUT")["last"] == 7821.0


async def test_defaults_and_config_settings() -> None:
    from latbot.config import Settings

    m = TradFiMarkets(Settings(), SimClock(), FakeHttp({}))
    assert m.symbols == DEFAULT_SYMBOLS and m.interval_s == pytest.approx(7.5)
    assert m.quote("DXY") is None
    assert m.is_yield("US10Y") and m.is_yield("US5Y") and m.is_yield("US3M") and not m.is_yield("VIX")
    snap = m.snapshot()
    assert set(snap["quotes"]) == set(DEFAULT_SYMBOLS) and all(v is None for v in snap["quotes"].values())
    off = TradFiMarkets(settings(markets_enabled=False), SimClock(), FakeHttp({}))
    assert off.due() is False
