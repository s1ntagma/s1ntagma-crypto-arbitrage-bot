"""Phase 3 backend review fixes: hot-path cost, pool follows at runtime, stability retry,
surprise propagation, honest news availability, stale shadow closes, DIAG keys, table pruning."""
from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace

from latbot import app as app_mod
from latbot.clock import SimClock
from latbot.config import Settings
from latbot.context.base import HttpResponse, MacroEvent
from latbot.context.bars import MinuteBars
from latbot.context.derivs import Derivatives
from latbot.context.hub import ContextHub
from latbot.engine.metalabel import MetaLabelStore
from latbot.engine.pipeline import _quality_reason_key
from latbot.engine.shadow import ShadowTrader, summarize
from latbot.storage.db import Database
from latbot.universe.pool import AssetPool
from test_bars import _hourly_http, _settings as bars_settings
from test_hub import NOW, FakeBars, FakeCalendar
from test_news import FakeHttp, make_feed, ok, settings as news_settings, tg_page
from test_news import NOW as NEWS_NOW
from test_pipeline import Env, detect


# 1. hot path ----------------------------------------------------------------------------------
def test_derivatives_get_is_memoised_and_invalidated_by_new_data():
    clock = SimClock(wall=NOW)
    d = Derivatives(Settings(), clock, None)
    calls: list[str] = []
    d._get = lambda a: calls.append(a) or {"asset": a, "funding_rate_8h": 0.0001}
    assert d.get("BTC") == d.get("btc") and len(calls) == 1  # one computation per second
    clock.advance(1.5)
    d.get("BTC")
    assert len(calls) == 2
    d._ingest("BTC", [{"asset": "BTC", "ts": NOW, "funding_rate_8h": 0.0002}])  # new samples: memo dropped
    d.get("BTC")
    assert len(calls) == 3


def test_funding_stats_single_pass_matches_definition():
    clock = SimClock(wall=NOW)
    d = Derivatives(Settings(), clock, None)
    d.min_samples = 4
    d._ingest("SOL", [{"asset": "SOL", "ts": NOW - 300 * i, "funding_rate_8h": v} for i, v in enumerate([1, 2, 3, 4, 5])])
    pct, n = d._funding_stats("SOL", NOW, {"funding_rate_8h": 3})
    assert n == 5 and pct == 50.0  # 2 below + half of 1 equal over 5
    assert d._funding_stats("SOL", NOW, None) == (None, 5)


# 2/3. pool follows the stability measurement and the REST poll set follows the pool -----------
VENUES = {a: {"binance": 1e9, "kucoin": 1e9, "okx": 1e9, "kraken": 1e8} for a in
          "BTC ETH XRP SOL BNB LINK ADA LTC AVAX SUI HBAR AAVE DOT BCH ETC RENDER XLM UNI ALGO ATOM POL APT CRV TAO ONDO INJ".split()}
GOOD = {"days": 30.0, "btc_corr": 0.8, "hour_jumps": 0.0, "max_7d_move_pct": 10.0, "beta": 1.0}


def test_pool_does_not_collapse_while_stability_is_being_measured():
    clock = SimClock(wall=1_791_471_600.0)
    measured: dict[str, dict] = {}
    tried: set[str] = set()
    pool = AssetPool(Settings(), clock, None, cache_path=Path(tempfile.mkdtemp()) / "pool.json",
                     listings_fn=lambda: VENUES, stability_fn=lambda a: measured.get(a),
                     stability_pending_fn=lambda a: a not in measured and a not in tried)
    full = len(pool.assets())
    for a in ("BTC", "ETH", "XRP"):
        measured[a] = dict(GOOD, ts=clock.wall())
        tried.add(a)
    pool.reevaluate()
    assert len(pool.assets()) == full > 3  # untried candidates wait for their measurement
    assert any("замер ещё не выполнялся" in n for n in pool.get("SOL").reasons)
    tried.add("SOL")  # tried and failed: no data -> out (hard rule 1)
    pool.reevaluate()
    assert "SOL" not in pool.assets() and "нет данных о стабильности" in pool.get("SOL").reasons
    tried.discard("SOL")  # a NEW candidate never enters unmeasured, even while its measurement is queued
    pool.reevaluate()
    assert "SOL" not in pool.assets() and any("замер в очереди" in r for r in pool.get("SOL").reasons)
    measured["SOL"] = dict(GOOD, ts=clock.wall())
    pool.reevaluate()
    assert "SOL" in pool.assets()


async def test_bars_stability_pending_until_tried():
    http, last_ts = _hourly_http()
    clock = SimClock(wall=last_ts + 1800)
    bars = MinuteBars(bars_settings(), clock, lambda a: None, http)
    assert bars.stability_pending("ETH") and bars.stability_pending("TON")
    http.override["ETH-USDT"] = HttpResponse(503, "down")
    await bars.refresh_stability(["ETH", "TON"])
    assert not bars.stability_pending("ETH") and bars.stability("ETH") is None  # failed try: no longer pending
    assert not bars.stability_pending("TON")  # no instrument on OKX
    del http.override["ETH-USDT"]
    assert await bars.refresh_stability(["ETH"]) == 1  # retried later, measured


class StabilityBars(FakeBars):
    def __init__(self):
        super().__init__()
        self.runs: list[list[str]] = []

    async def refresh_stability(self, assets):
        self.runs.append(list(assets))
        return 0


class PoolStub:
    def __init__(self, cands):
        self.cands = list(cands)
        self.reevaluated = 0

    def assets(self):
        return list(self.cands)

    def candidates(self):
        return list(self.cands)

    def reevaluate(self):
        self.reevaluated += 1


async def test_hub_retries_stability_and_measures_new_candidates_early():
    clock = SimClock(wall=NOW)
    bars, pool = StabilityBars(), PoolStub(["BTC", "ETH", "SOL"])
    h = ContextHub(Settings(), clock, pool=pool, bars=bars)
    h.housekeeping(NOW)
    await asyncio.sleep(0)
    assert bars.runs == [["BTC", "ETH", "SOL"]] and pool.reevaluated >= 1
    h.housekeeping(NOW)
    await asyncio.sleep(0)
    assert len(bars.runs) == 1  # nothing new, retry not due
    pool.cands.append("ADA")  # CoinGecko answered: a new candidate is measured at once
    h.housekeeping(NOW)
    await asyncio.sleep(0)
    assert len(bars.runs) == 2
    clock.advance(601)  # failed / pending assets are retried every 10 min (bars fetches only what is due)
    h.housekeeping(NOW + 601)
    await asyncio.sleep(0)
    assert len(bars.runs) == 3


async def test_follow_pool_updates_the_poll_set(monkeypatch):
    monkeypatch.setattr(app_mod, "_POOL_FOLLOW_S", 0.01)
    pool = PoolStub(["BTC", "ETH"])
    applied: list[list[str]] = []
    ctx = SimpleNamespace(clock=SimClock(wall=NOW), adapters={}, pipeline=SimpleNamespace(pool=pool), pool=pool,
                          poller=SimpleNamespace(set_assets=applied.append), settings=SimpleNamespace(ticker_poll_enabled=True),
                          watchlist=["BTC"], poll_assets=[], listings={})
    stop = asyncio.Event()
    task = asyncio.create_task(app_mod._follow_pool(ctx, stop))
    await asyncio.sleep(0.05)
    pool.cands += ["SOL", "XRP"]  # assets entered the pool after start
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert applied[0] == ["BTC", "ETH"] and applied[-1] == ["BTC", "ETH", "SOL", "XRP"] and len(applied) == 2
    assert ctx.poll_assets == ["ETH", "SOL", "XRP"]


# 4. surprise signs -----------------------------------------------------------------------------
class Reactions:
    def __init__(self):
        self.set: list[tuple[str, int]] = []

    def watch(self, *a, **k):
        return True

    def tick(self, now):
        pass

    def set_surprise(self, key, s):
        self.set.append((key, s))


def test_calendar_surprise_reaches_reactions_and_refused_squawk_does_not():
    ev = MacroEvent(id="cpi", title="CPI m/m", country="USD", ts=NOW - 60, impact="high", event_type="CPI", surprise=-1)
    cal = FakeCalendar([ev])
    cal.set_surprise = lambda *a, **k: False  # an actual-based (BLS) sign exists: squawk refused
    news = SimpleNamespace(items=lambda **k: [], macro_surprises=lambda now, w: [{"id": "sq", "event_type": "CPI", "surprise": 1, "ts": NOW - 50}],
                           relevant=lambda *a, **k: [], status=None)
    r = Reactions()
    h = ContextHub(Settings(), SimClock(wall=NOW), calendar=cal, news=news, reactions=r)
    h._watched.add("macro:cpi")
    h.housekeeping(NOW)
    assert ("macro:cpi", -1) in r.set and ("macro:cpi", 1) not in r.set


# 5. shadow closes on stale data are not learned -------------------------------------------------
async def test_shadow_close_on_stale_book_is_no_data_and_not_learned():
    env = Env()
    ml = MetaLabelStore(env.settings, env.clock)
    sh = ShadowTrader(env.settings, env.bus, env.clock, metalabel=ml)
    opp = await detect(env)
    await env.opps.expire(opp.id, "target book stale")
    row = sh.trades()[0]
    assert row["barrier"] == "no_data" and row["actual_net_pct"] is None and len(ml) == 0
    s = summarize(sh.trades())
    assert s["no_data"] == 1 and s["closed"] == 0


# 6. news availability ------------------------------------------------------------------------------
async def test_news_down_is_no_data_not_no_news():
    good, bad = "WatcherGuru", "markettwits"
    http = FakeHttp({f"https://t.me/s/{good}": ok(tg_page(good, [(1, NEWS_NOW - 10, "Bitcoin ETF inflows")])),
                     f"https://t.me/s/{bad}": ConnectionError("reset")})
    feed, clock = make_feed(news_settings(news_telegram_channels=[good, bad]), http)
    assert not feed.available()
    assert await feed.step() and feed.available()
    h = ContextHub(Settings(), clock, news=feed, reactions=SimpleNamespace(profile=lambda *a, **k: {}))
    keys = {f.key: f for f in h.factors("BTC", 1, clock.wall())}
    assert keys["news"].available and keys["event_reaction"].available
    http.routes[f"https://t.me/s/{good}"] = ConnectionError("reset")  # every source down from now on
    for _ in range(6):
        clock.advance(60)
        await feed.step()
    assert not feed.available() and not feed.status.ok
    keys = {f.key: f for f in h.factors("BTC", 1, clock.wall())}
    assert not keys["news"].available and not keys["event_reaction"].available and not keys["whale"].available


# 7. DIAG keys -----------------------------------------------------------------------------------------
def test_quality_reason_key_groups_by_cause():
    a = _quality_reason_key({"vetoes": ["Макро-события: CPI через 90 с"]})
    b = _quality_reason_key({"vetoes": ["Макро-события: CPI через 30 с"]})
    assert a == b == "вето: Макро-события"
    assert _quality_reason_key({"vetoes": ["NET после издержек -0.12% ≤ 0"]}) == "вето: NET после издержек ≤ 0"
    assert _quality_reason_key({"vetoes": [], "score": 41, "min_score": 60}) == _quality_reason_key({"score": 55, "min_score": 60})


# 8. pruning ------------------------------------------------------------------------------------------
def test_prune_context_tables():
    db = Database(":memory:")
    for ts in (NOW - 40 * 86400, NOW - 3600):
        db.save_deriv_sample({"asset": "BTC", "venue": "okx", "ts": ts, "funding_rate_8h": 0.0001})
        db.save_news({"id": f"n{ts}", "ts": ts, "title": "x"})
    for i in range(5):
        db.save_metalabel_outcome({"id": f"o{i}", "ts": NOW, "outcome": "win"})
    out = db.prune_context(NOW, deriv_days=31, news_days=30, metalabel_keep=3)
    assert out["deriv_samples"] == 1 and out["news_items"] == 1 and out["metalabel_outcomes"] == 2
    assert len(db.load_deriv_samples("BTC", 0)) == 1 and len(db.load_metalabel_outcomes()) == 3
