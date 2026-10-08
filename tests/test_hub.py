"""ContextHub: every factor relative to the trade direction, vetoes, honest "no data"."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from latbot.clock import SimClock
from latbot.config import Settings
from latbot.context.base import MacroEvent, NewsItem, SourceStatus
from latbot.context.hub import DEFAULT_CONTEXT_WEIGHTS, ContextHub
from latbot.context.reactions import EventReactionStore

NOW = 1_800_000_000.0


def ok_status(name: str) -> SourceStatus:
    return SourceStatus(name=name, ok=True, last_ok_ts=NOW)


class FakeCalendar:
    def __init__(self, events=None, up=True):
        self.events_ = events or []
        self.up = up
        self.status = ok_status("macro_calendar")
        self.surprises: dict[str, int] = {}

    def available(self, now=None):
        return self.up

    def event_window(self, now):
        out = []
        for e in self.events_:
            if e.impact == "high" and 0 <= e.ts - now <= 300:
                out.append((e, "before"))
            elif e.impact == "high" and 0 <= now - e.ts <= 3600:
                out.append((e, "after"))
        return out

    def upcoming(self, now, within_s, min_impact="high"):
        return [e for e in self.events_ if 0 < e.ts - now <= within_s and (min_impact == "medium" or e.impact == "high")]

    def recent(self, now, within_s, min_impact="high"):
        return [e for e in self.events_ if 0 <= now - e.ts <= within_s and (min_impact == "medium" or e.impact == "high")]

    def next_event(self, now, min_impact="high"):
        nxt = [e for e in self.events_ if e.ts > now and e.impact == "high"]
        return min(nxt, key=lambda e: e.ts) if nxt else None

    def set_surprise(self, event_id, surprise, **_):
        self.surprises[event_id] = surprise
        return True


class FakeNews:
    def __init__(self, items=None, whales=None, surprises=None):
        self._items = items or []
        self._whales = whales or []
        self._surprises = surprises or []
        self.status = ok_status("news")

    def relevant(self, asset, now, within_s, min_importance=0.5):
        return [i for i in self._items if now - i.ts <= within_s and i.importance >= min_importance and (asset in i.assets or "*" in i.assets)]

    def whale_flows(self, asset, now, within_s):
        return [w for w in self._whales if w["asset"] == asset]

    def items(self, since_wall=None, limit=100):
        return [i for i in self._items if since_wall is None or i.ts >= since_wall][:limit]

    def macro_surprises(self, now, within_s):
        return list(self._surprises)


class FakeDerivs:
    def __init__(self, info=None, quad=None):
        self.info = info or {}
        self.quad = quad
        self.status = ok_status("derivatives")

    def get(self, asset):
        return self.info.get(asset)

    def oi_quadrant(self, asset, ret):
        return self.quad


class FakeBars:
    def __init__(self, stats=None, erratic=None):
        self._stats = stats or {}
        self._erratic = erratic or {}
        self.status = ok_status("okx_candles")
        self.sampled = 0

    def stats(self, asset):
        return self._stats.get(asset, {})

    def erratic(self, asset):
        return self._erratic.get(asset, {"available": True, "suspended": False})

    def returns(self, asset, minutes):
        return []

    def track(self, assets):
        pass

    def sample(self, now=None):
        self.sampled += 1


def hub(**kw) -> ContextHub:
    clock = SimClock(wall=NOW)
    return ContextHub(Settings(), clock, **kw)


def factor(h: ContextHub, key: str, asset="SOL", d=1):
    return next(f for f in h.factors(asset, d, NOW) if f.key == key)


def test_every_factor_present_and_unavailable_without_components():
    h = hub()
    fs = h.factors("SOL", 1, NOW)
    assert {f.key for f in fs} == set(DEFAULT_CONTEXT_WEIGHTS)
    assert all(not f.available and f.score == 0.0 and not f.veto for f in fs)
    assert all(f.detail for f in fs)  # every "no data" says why (Russian)


def test_macro_veto_before_a_high_impact_release():
    ev = MacroEvent(id="cpi", title="CPI m/m", country="USD", ts=NOW + 90, impact="high", event_type="CPI")
    f = factor(hub(calendar=FakeCalendar([ev])), "macro_event")
    assert f.available and f.veto and "CPI" in f.detail


def test_macro_after_release_uses_reaction_history_or_vetoes():
    ev = MacroEvent(id="cpi", title="CPI m/m", country="USD", ts=NOW - 30, impact="high", event_type="CPI", surprise=-1)
    clock = SimClock(wall=NOW)
    reactions = EventReactionStore(Settings(), clock, lambda a: None)
    h = ContextHub(Settings(), clock, calendar=FakeCalendar([ev]), reactions=reactions)
    assert factor(h, "macro_event").veto  # no history yet: unpredictable
    # 6 past cooler CPIs, BTC up within 60 s in 5 of them
    prof = {60: {"n": 6, "mean_pct": 0.4, "median_pct": 0.3, "up_share": 5 / 6, "mean_abs_pct": 0.5}}
    reactions.profile = lambda et, a, s=None: prof if et == "CPI" else {}
    f_long = factor(h, "macro_event", d=1)
    f_short = factor(h, "macro_event", d=-1)
    assert not f_long.veto and f_long.score > 0.5 and f_short.score < -0.5


def test_calendar_down_is_unavailable():
    f = factor(hub(calendar=FakeCalendar(up=False)), "macro_event")
    assert not f.available and not f.veto


def test_news_direction_relative_to_trade():
    item = NewsItem(id="n1", ts=NOW - 60, source="telegram", channel="WatcherGuru", title="SEC approves SOL ETF",
                    assets=["SOL"], category="etf", importance=0.9, direction=1, confidence=0.8)
    h = hub(news=FakeNews([item]))
    assert factor(h, "news", d=1).score > 0.5
    assert factor(h, "news", d=-1).score < -0.5
    assert factor(h, "news", asset="ADA").score == 0.0 and factor(h, "news", asset="ADA").available


def test_whale_inflow_to_exchanges_argues_against_longs():
    h = hub(news=FakeNews(whales=[{"asset": "SOL", "usd": 1e8, "flow": "to_exchange"}]))
    assert factor(h, "whale", d=1).score < -0.5 and factor(h, "whale", d=-1).score > 0.5


def test_funding_needs_history_then_crowding_matters():
    h = hub(derivs=FakeDerivs({"SOL": {"funding_rate_8h": 0.001, "funding_pct": None, "funding_n": 40, "funding_min_n": 288}}))
    f = factor(h, "funding")
    assert not f.available and "40/288" in f.detail
    h = hub(derivs=FakeDerivs({"SOL": {"funding_rate_8h": 0.001, "funding_pct": 98.0}}))
    assert factor(h, "funding", d=1).score < 0 and factor(h, "funding", d=-1).score > 0


def test_open_interest_quadrant_and_liquidations():
    h = hub(derivs=FakeDerivs({"SOL": {"liq_long_usd_5m": 0.0, "liq_short_usd_5m": 4e5}}, quad="NEW_LONGS"),
            bars=FakeBars({"SOL": {"ret_15m_pct": 1.0}}))
    assert factor(h, "open_interest", d=1).score == 0.5 and factor(h, "open_interest", d=-1).score == -0.5
    assert factor(h, "liquidations", d=1).score > 0.5 and factor(h, "liquidations", d=-1).score < -0.5


def test_btc_regime_and_erratic_veto():
    bars = FakeBars({"BTC": {"minutes": 60, "ret_15m_pct": 0.5, "shock": 0, "vol_pctile": 0.5}},
                    erratic={"SOL": {"available": True, "suspended": True, "reason": "2 скачка >= 1% за час"}})
    h = hub(bars=bars)
    assert factor(h, "btc_regime", d=1).score == pytest.approx(1.0)
    assert factor(h, "btc_regime", d=-1).score == pytest.approx(-1.0)
    e = factor(h, "erratic")
    assert e.veto and "скач" in e.detail
    # BTC itself is judged against ETH; missing ETH data is honest "no data"
    assert not factor(h, "btc_regime", asset="BTC").available


def test_sentiment_extremes_are_contrarian_and_small():
    fg = SimpleNamespace(get=lambda: {"value": 85, "classification": "Extreme Greed"}, status=ok_status("fear_greed"))
    h = hub(fear_greed=fg)
    assert factor(h, "sentiment", d=1).score == -0.3 and factor(h, "sentiment", d=-1).score == 0.2


def test_funding_and_asset_stats_queries():
    h = hub(derivs=FakeDerivs({"BTC": {"funding_rate_8h": 0.0001, "funding_interval_h": 8, "next_funding_ts": NOW + 100}}),
            bars=FakeBars({"BTC": {"vol_1m_pct": 0.04}}))
    assert h.funding("BTC") == (0.0001, 8, NOW + 100) and h.funding("ETH") is None
    assert h.asset_stats("BTC")["vol_1m_pct"] == 0.04


def test_housekeeping_registers_release_and_squawk_surprise():
    ev = MacroEvent(id="nfp", title="Non-Farm Employment Change", country="USD", ts=NOW + 60, impact="high", event_type="NFP")
    clock = SimClock(wall=NOW)
    cal = FakeCalendar([ev])
    reactions = EventReactionStore(Settings(), clock, lambda a: 100.0)
    news = FakeNews(surprises=[{"id": "sq1", "event_type": "NFP", "surprise": 1, "ts": NOW + 75}])
    h = ContextHub(Settings(), clock, calendar=cal, news=news, reactions=reactions, bars=FakeBars())
    h.housekeeping(NOW)
    assert "macro:nfp" in h._watched
    clock.advance(90)
    h.housekeeping(NOW + 90)
    assert cal.surprises.get("nfp") == 1


async def test_run_starts_collectors_and_stops_cleanly():
    class Col:
        name = "col"

        def __init__(self):
            self.started = asyncio.Event()

        async def run(self, stop):
            self.started.set()
            await stop.wait()

    c = Col()
    h = hub(pool=None, calendar=None)
    h.news = c  # any collector with run()
    stop = asyncio.Event()
    task = asyncio.create_task(h.run(stop))
    await asyncio.wait_for(c.started.wait(), 2.0)
    stop.set()
    await asyncio.wait_for(task, 2.0)


def test_snapshot_is_json_safe():
    import json
    ev = MacroEvent(id="cpi", title="CPI m/m", country="USD", ts=NOW + 3600, impact="high", event_type="CPI")
    h = hub(calendar=FakeCalendar([ev]), bars=FakeBars())
    snap = h.snapshot()
    json.dumps(snap, default=str)
    assert snap["macro"]["next"][0]["in_s"] == 3600 and "weights" in snap
