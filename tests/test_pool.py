"""universe/pool.py - AssetPool (ARCHITECTURE P3.1 + P3.12), fully offline.

Fixtures (tests/fixtures/context/pool/) are trimmed real samples captured on
2026-10-08: CoinGecko /coins/markets top-250 and category pages, listings of 12
venues, and stability metrics measured on OKX USDT 1H candles over 30 days.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from latbot.clock import SimClock
from latbot.config import Settings
from latbot.context.base import HttpResponse
from latbot.universe.pool import (
    DEFAULT_EXCLUDE_CATEGORIES,
    DEFAULT_LIVE_CATEGORIES,
    FALLBACK_PATH,
    POOL_DEFAULTS,
    AssetPool,
    PoolAsset,
    jumpiness,
    normalise_asset,
)

FIX = Path(__file__).parent / "fixtures" / "context" / "pool"
HEADERS = json.loads((FIX / "headers.json").read_text(encoding="utf-8"))
CAT_FILES = {
    "meme-token": "cat_meme_token_p1",
    "stablecoins": "cat_stablecoins",
    "bridged-tokens": "cat_bridged_tokens",
    "wrapped-tokens": "cat_wrapped_tokens",
    "liquid-staking-tokens": "cat_liquid_staking_tokens",
}
LISTINGS = json.loads((FIX / "listings_top250.json").read_text(encoding="utf-8"))
STABILITY = json.loads((FIX / "stability_okx_1h_30d.json").read_text(encoding="utf-8"))
SECRET = "CG-demo-secret-key-123"
NOW = 1_791_460_000.0  # 2026-10-08


def _text(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


ROLES = {
    "kraken": "execution", "coinbase": "execution", "bitvavo": "execution", "bitstamp": "execution",
    "cryptocom": "execution", "gate": "execution",
    "binance": "signal", "kucoin": "signal", "bitget": "signal", "mexc": "signal", "htx": "signal", "okx": "signal",
}


def make_settings(**kw):
    exchanges = [SimpleNamespace(name=n, role=r, enabled=True) for n, r in ROLES.items()]
    return SimpleNamespace(exchanges=exchanges, **kw)


class FakeHttp:
    """Answers /coins/markets from fixtures; ``override(key, page)`` may return a response first."""

    def __init__(self, override=None) -> None:
        self.calls: list[tuple[str, dict, dict]] = []
        self.override = override

    async def get(self, url, *, params=None, headers=None, timeout_s=10.0):
        params = dict(params or {})
        self.calls.append((url, params, dict(headers or {})))
        key = params.get("category") or "markets"
        page = int(params.get("page", 1))
        if self.override is not None:
            r = self.override(key, page)
            if r is not None:
                return r
        if key == "markets":
            return HttpResponse(200, _text("markets_top250.json"), dict(HEADERS["markets_p1"]))
        if key in CAT_FILES and page == 1:
            return HttpResponse(200, _text(CAT_FILES[key] + ".json"), dict(HEADERS[CAT_FILES[key]]))
        return HttpResponse(200, "[]", {"total": "0", "per-page": "250"})

    async def close(self) -> None:
        pass

    def categories_called(self) -> list[str]:
        return [p["category"] for _, p, _ in self.calls if "category" in p]


def make_pool(settings=None, http=None, clock=None, **kw) -> AssetPool:
    return AssetPool(settings if settings is not None else make_settings(), clock or SimClock(wall=NOW), http, **kw)


def research_stability(asset: str):
    return STABILITY.get(asset)


# --------------------------------------------------------------------------- #
# bundled fallback
# --------------------------------------------------------------------------- #
TIER_A = ["BTC", "ETH", "XRP", "SOL", "BNB", "LINK", "ADA", "LTC", "AVAX", "SUI", "HBAR", "AAVE", "DOT", "BCH", "ETC",
          "RENDER"]
TIER_B = ["XLM", "UNI", "ALGO", "ATOM", "POL", "APT", "CRV", "TAO", "ONDO", "INJ"]


def test_fallback_json_contract():
    doc = json.loads(FALLBACK_PATH.read_text(encoding="utf-8"))
    assert doc["tier_a"] == TIER_A and doc["tier_b"] == TIER_B
    assert doc["pool"] == TIER_A + TIER_B
    assert set(doc["cg_ids"]) == set(doc["pool"]) and doc["cg_ids"]["BTC"] == "bitcoin"
    # frozen category ids: every group the pool relies on
    groups = doc["exclude_categories"]
    for g in ("meme", "stablecoin", "wrapped", "liquid_staking", "bridged", "gold_commodities", "fan_token"):
        assert groups[g], g
    flat = [c for cats in groups.values() for c in cats]
    assert tuple(flat) == DEFAULT_EXCLUDE_CATEGORIES
    assert set(DEFAULT_LIVE_CATEGORIES) <= set(DEFAULT_EXCLUDE_CATEGORIES)
    assert "liquid-staking" not in flat  # governance tokens of staking protocols, not receipts
    # frozen members never contain a pool asset's id
    pool_ids = set(doc["cg_ids"].values())
    for cat, ids in doc["category_members"].items():
        assert cat in flat
        assert not pool_ids & set(ids), cat
    # meme / hype blocklist keyed by CoinGecko id; stables too
    meme = {e["id"]: e["symbol"] for e in doc["meme_blocklist"]}
    assert {"dogecoin", "pepe", "shiba-inu", "official-trump"} <= set(meme)
    assert not pool_ids & set(meme)
    assert {"tether", "usd-coin"} <= {e["id"] for e in doc["stable_blocklist"]}
    # futures aliases, never the ambiguous tickers
    for alias in ("1000PEPE", "1000SHIB", "1000BONK", "1000FLOKI", "1000SATS", "1000RATS", "1000CAT", "1MBABYDOGE",
                  "1000CHEEMS"):
        assert alias in doc["symbol_aliases"]
    assert set(doc["ambiguous_symbols"]) == {"AI", "BABY", "B", "M", "LION", "DOG", "LUNA"}
    assert not set(doc["ambiguous_symbols"]) & set(doc["symbol_aliases"])
    assert FALLBACK_PATH.stat().st_size < 64_000


def test_fallback_usable_right_after_construction():
    pool = make_pool()  # no cache, no network
    assert pool.source == "fallback"
    assert pool.contains("BTC") and pool.contains("eth") and pool.contains("XBT")  # Kraken name
    assert pool.contains("SOL") and pool.contains("RENDER")
    assert not pool.contains("DOGE") and not pool.contains("PEPE") and not pool.contains("1000PEPE")
    a = pool.assets()
    assert a[:2] == ["BTC", "ETH"]
    assert len(a) == 25  # 26 bundled, pool_max_assets=25 -> the smallest drops out
    crv = pool.get("CRV")
    assert crv is not None and not crv.included
    assert any("лимит пула: 25" in r for r in crv.reasons)
    sol = pool.get("SOL")
    assert sol.included and sol.tier == "A" and sol.source == "fallback" and sol.cg_id == "solana"
    assert sol.reasons[0].startswith("в пуле:")
    assert any("офлайн-список" in r for r in sol.reasons)
    snap = pool.snapshot()
    assert "пропущен" in snap["steps"]["stability"]
    assert snap["source"] == "fallback" and snap["fallback_generated"] == "2026-10-08"


def test_works_with_real_settings_object():
    s = Settings()  # no pool_* fields yet: every default comes from POOL_DEFAULTS
    pool = AssetPool(s, SimClock(wall=NOW), None)
    assert pool.contains("BTC") and pool.assets()[:2] == ["BTC", "ETH"]
    assert pool.criteria()["pool_min_signal_venues"] == POOL_DEFAULTS["pool_min_signal_venues"] == 3
    assert pool.interval_s == 12 * 3600


def test_pool_disabled_contains_everything():
    pool = make_pool(make_settings(pool_enabled=False))
    assert pool.contains("PEPE") and pool.contains("ANYTHING")
    assert pool.snapshot()["enabled"] is False


# --------------------------------------------------------------------------- #
# live CoinGecko refresh
# --------------------------------------------------------------------------- #
async def test_poll_live_markets_categories_listings_stability(tmp_path):
    http = FakeHttp()
    cache = tmp_path / "pool.json"
    pool = make_pool(http=http, cache_path=cache, api_key=SECRET, listings_fn=lambda: LISTINGS,
                     stability_fn=research_stability)
    assert await pool.step() is True
    assert pool.source == "coingecko" and pool.status.ok

    # request shape: top 250 by market cap with the 7 d change, Demo key header on every call
    url, params, headers = http.calls[0]
    assert url.endswith("/api/v3/coins/markets")
    assert params["order"] == "market_cap_desc" and params["per_page"] == 250 and params["page"] == 1
    assert "7d" in params["price_change_percentage"] and params["vs_currency"] == "usd"
    assert all(h.get("x-cg-demo-api-key") == SECRET for _, _, h in http.calls)
    assert set(http.categories_called()) == set(DEFAULT_LIVE_CATEGORIES)  # 1 page each (60/min with a key)

    a = pool.assets()
    assert a[:2] == ["BTC", "ETH"]
    assert {"XRP", "SOL", "BNB", "LINK", "ADA"} <= set(a)
    ranks = [pool.get(x).rank for x in a[2:]]
    assert ranks == sorted(ranks)

    def reasons(x):
        return " | ".join(pool.get(x).reasons)

    assert "стейблкоин" in reasons("USDT") and not pool.contains("USDT")
    assert "мем-монета" in reasons("DOGE") and "meme-token" in reasons("DOGE")
    assert "мем-монета" in reasons("PEPE")
    assert "токенизированное золото" in reasons("XAUT")
    # erratic / weakly linked names, with the numbers behind the verdict
    assert "40 за 30 дн > 20" in reasons("NEAR")
    assert "корреляция 0.27 < 0.55" in reasons("TRX")
    assert "179% > 100%" in reasons("ICP")
    # not tradable on any target venue (XMR) / thin venues
    assert "мало целевых площадок: 0 < 1" in reasons("XMR")
    for pa in pool.snapshot()["excluded"]:
        assert pa["reasons"] and all(isinstance(r, str) and r for r in pa["reasons"])
    btc = pool.get("BTC")
    assert btc.included and btc.forced and btc.venues_target and btc.target_volumes["kraken"] > 1e8
    assert btc.stability["btc_corr"] == 1.0 and btc.stability["origin"] == "live"
    assert pool.candidates()[:2] == ["BTC", "ETH"] and "NEAR" in pool.candidates()

    # cache file with fetched_ts
    doc = json.loads(cache.read_text(encoding="utf-8"))
    assert doc["fetched_ts"] == NOW and len(doc["markets"]) == 250
    assert "dogecoin" in doc["categories"]["meme-token"]["ids"]
    assert SECRET not in cache.read_text(encoding="utf-8")
    # nothing secret leaves through the snapshot
    snap = pool.snapshot()
    text = json.dumps(snap, ensure_ascii=False)
    assert SECRET not in text and snap["api_key"] is True
    for k in ("enabled", "updated_ts", "source", "criteria", "included", "excluded", "status"):
        assert k in snap
    assert snap["size"] == len(a) and snap["status"]["name"] == "asset_pool"
    assert snap["pending_categories"] == []


async def test_symbol_collisions_and_ambiguous_tickers():
    rows = [
        {"id": "bitcoin", "symbol": "btc", "market_cap": 1.6e12, "market_cap_rank": 1, "total_volume": 3e10,
         "price_change_percentage_24h": -1.0, "price_change_percentage_7d_in_currency": -2.0},
        # a bridged BTC reusing the ticker must never knock out BTC
        {"id": "near-intents-bridged-btc", "symbol": "btc", "market_cap": 2e9, "market_cap_rank": 40,
         "total_volume": 1e8, "price_change_percentage_24h": 0.0, "price_change_percentage_7d_in_currency": 0.0},
        # "AI" is a legit project here (not the meme coin artificial-inu-3 with the same ticker)
        {"id": "sleepless-ai", "symbol": "ai", "market_cap": 3e9, "market_cap_rank": 30, "total_volume": 5e8,
         "price_change_percentage_24h": 1.0, "price_change_percentage_7d_in_currency": 2.0},
        # "M" resolves by id to memecore (meme) -> excluded by id, not by ticker
        {"id": "memecore", "symbol": "m", "market_cap": 2.4e9, "market_cap_rank": 46, "total_volume": 3e7,
         "price_change_percentage_24h": 1.0, "price_change_percentage_7d_in_currency": 2.0},
    ]

    def override(key, page):
        if key == "markets":
            return HttpResponse(200, json.dumps(rows), {})
        if key == "bridged-tokens":
            bridged = [{"id": "near-intents-bridged-btc", "symbol": "btc", "market_cap": 2e9}]
            return HttpResponse(200, json.dumps(bridged), {})
        return None

    pool = make_pool(make_settings(pool_include=[]), http=FakeHttp(override), api_key="k")
    await pool.poll_once()
    assert pool.get("BTC").cg_id == "bitcoin" and pool.contains("BTC")
    assert pool.contains("AI") and pool.get("AI").categories == []
    assert not pool.contains("M") and "мем-монета" in pool.get("M").reasons[0]
    assert pool.blocked_symbol("1000PEPE") and "PEPE" in pool.blocked_symbol("1000PEPE")
    for amb in ("AI", "BABY", "B", "M", "LION", "DOG", "LUNA"):
        assert pool.blocked_symbol(amb) is None


async def test_market_criteria_reasons_with_numbers():
    base = {"market_cap_rank": 10, "total_volume": 5e8, "price_change_percentage_24h": 1.0,
            "price_change_percentage_7d_in_currency": 2.0}
    rows = [
        dict(base, id="small", symbol="sml", market_cap=4e8, market_cap_rank=150),
        dict(base, id="thin", symbol="thn", market_cap=5e9, total_volume=1e6),
        dict(base, id="pumped", symbol="pmp", market_cap=5e9, price_change_percentage_24h=22.5),
        dict(base, id="weekly", symbol="wkl", market_cap=5e9, price_change_percentage_7d_in_currency=-140.0),
        dict(base, id="nodata", symbol="nod", market_cap=5e9, price_change_percentage_7d_in_currency=None),
        dict(base, id="zero-cap", symbol="zcp", market_cap=0),
        dict(base, id="good", symbol="gud", market_cap=5e9),
    ]
    http = FakeHttp(lambda k, p: HttpResponse(200, json.dumps(rows), {}) if k == "markets" else None)
    pool = make_pool(make_settings(pool_include=[]), http=http, api_key="k")
    await pool.poll_once()
    r = {a: " | ".join(pool.get(a).reasons) for a in ("SML", "THN", "PMP", "WKL", "NOD", "ZCP")}
    assert "капитализация $400.0 млн < $1.0 млрд" in r["SML"]
    assert "объём за 24 ч $1.0 млн < $20.0 млн" in r["THN"]
    assert "+22.5% (лимит ±15%)" in r["PMP"]
    assert "-140.0% (лимит ±100%)" in r["WKL"]
    assert "нет данных: изменение цены за 7 дн" in r["NOD"]
    assert "нет данных: капитализация" in r["ZCP"]
    assert pool.assets() == ["GUD"]


async def test_pool_include_always_in_exclude_wins():
    s = make_settings(pool_include=["BTC", "ETH", "DOGE"], pool_exclude=["SOL"])
    pool = make_pool(s, http=FakeHttp(), api_key="k", listings_fn=lambda: LISTINGS)
    await pool.poll_once()
    assert pool.assets()[:3] == ["BTC", "ETH", "DOGE"]
    doge = pool.get("DOGE")
    assert doge.included and doge.forced
    assert any(r.startswith("не прошла бы отбор: категория CoinGecko") for r in doge.reasons)
    assert not pool.contains("SOL") and "исключена владельцем (pool_exclude)" in pool.get("SOL").reasons
    # include + exclude at once -> exclude wins
    s.pool_exclude = ["SOL", "DOGE"]
    pool.reevaluate()
    assert not pool.contains("DOGE")


async def test_pool_cap_keeps_includes_and_largest():
    pool = make_pool(make_settings(pool_max_assets=4), http=FakeHttp(), api_key="k", listings_fn=lambda: LISTINGS)
    await pool.poll_once()
    assert pool.assets() == ["BTC", "ETH", "BNB", "XRP"]
    assert any("лимит пула: 4" in r for r in pool.get("SOL").reasons)


# --------------------------------------------------------------------------- #
# stability / funding / venues / 1-minute jumps
# --------------------------------------------------------------------------- #
async def test_missing_stability_excludes_only_when_fn_given():
    # the source answers for NEAR but has nothing for SOL -> SOL stays out (missing data never passes)
    stab = {"NEAR": {"days": 30, "btc_corr": 0.9, "hour_jumps": 1, "max_7d_move_pct": 20}}
    pool = make_pool(http=FakeHttp(), api_key="k", listings_fn=lambda: LISTINGS, stability_fn=stab.get)
    await pool.poll_once()
    assert pool.contains("NEAR") and not pool.contains("SOL")
    assert "нет данных о стабильности" in pool.get("SOL").reasons
    assert "SOL" in pool.candidates()  # it passed everything before stability: bars should fetch its candles

    pool2 = make_pool(http=FakeHttp(), api_key="k", listings_fn=lambda: LISTINGS)  # no stability_fn
    await pool2.poll_once()
    assert pool2.contains("SOL") and pool2.contains("NEAR")
    assert "пропущен" in pool2.snapshot()["steps"]["stability"]


async def test_stability_criteria_reasons():
    stab = {
        "SOL": {"days": 12, "btc_corr": 0.9, "hour_jumps": 1, "max_7d_move_pct": 20},
        "XRP": {"days": 30, "btc_corr": 0.31, "hour_jumps": 25, "max_7d_move_pct": 150},
        "ADA": {"days": 30, "btc_corr": None, "hour_jumps": 2},
        # limits are inclusive; "ts" = when context/bars.py measured it
        "LINK": {"days": 30, "btc_corr": 0.8, "hour_jumps": 20, "max_7d_move_pct": 100, "ts": NOW - 7200.0},
    }
    pool = make_pool(http=FakeHttp(), api_key="k", listings_fn=lambda: LISTINGS, stability_fn=stab.get)
    await pool.poll_once()
    r = {a: " | ".join(pool.get(a).reasons) for a in ("SOL", "XRP", "ADA")}
    assert "мало истории свечей: 12 дн < 20 дн" in r["SOL"]
    assert "корреляция 0.31 < 0.55" in r["XRP"]
    assert "скачков ≥3%: 25 за 30 дн > 20" in r["XRP"]
    assert "макс. движение за 7 дн 150% > 100%" in r["XRP"]
    assert "нет данных о стабильности: корреляция с BTC" in r["ADA"]
    assert "нет данных о стабильности: макс. движение за 7 дн" in r["ADA"]
    assert pool.contains("LINK") and pool.get("LINK").stability["age_s"] == 7200.0
    assert set(pool.assets()) == {"BTC", "ETH", "LINK"}


async def test_funding_interval_below_8h_excluded():
    iv = {"SOL": 4.0, "XRP": 8.0}
    pool = make_pool(http=FakeHttp(), api_key="k", listings_fn=lambda: LISTINGS, funding_interval_fn=iv.get)
    await pool.poll_once()
    assert not pool.contains("SOL")
    assert any("фандинг каждые 4 ч (< 8 ч)" in r for r in pool.get("SOL").reasons)
    assert pool.contains("XRP") and pool.get("XRP").funding_interval_h == 8.0
    # no funding data is noted, not a verdict
    assert pool.contains("ADA") and "интервал фандинга: нет данных" in pool.get("ADA").reasons


def test_venue_presence():
    listings = {
        "XBT": {"kraken": 3e8, "binance": None, "okx": None},  # Kraken name -> BTC
        "SOL": {"binance": None, "okx": None, "kucoin": None},  # no target venue
        "XRP": {"kraken": 1e7, "binance": None},  # only 2 venues
        "ADA": {"kraken": 2e6, "binance": None, "kucoin": None, "okx": None},
    }
    pool = make_pool(listings_fn=lambda: listings)
    assert pool.get("BTC").venues_signal == ["binance", "kraken", "okx"] and pool.get("BTC").venues_target == ["kraken"]
    assert "мало целевых площадок: 0 < 1" in pool.get("SOL").reasons
    assert any(r.startswith("мало площадок: 2 < 3") for r in pool.get("XRP").reasons)
    assert "нет ни на одной включённой бирже" in pool.get("LINK").reasons
    assert pool.contains("ADA") and pool.get("ADA").target_volumes == {"kraken": 2e6}
    assert pool.assets() == ["BTC", "ETH", "ADA"]
    # no listings at all (adapters not loaded yet) -> the step is skipped, not a verdict
    empty = make_pool(listings_fn=lambda: {})
    assert empty.contains("SOL") and "пропущен" in empty.snapshot()["steps"]["venues"]

    def broken():
        raise RuntimeError("adapters not ready")

    assert make_pool(listings_fn=broken).contains("SOL")


def test_bars_fn_one_minute_spikes():
    calm = [100.0 + 0.01 * i for i in range(200)]
    spiky = list(calm)
    for i in (20, 60, 100, 140, 180):
        spiky[i] = spiky[i - 1] * 1.03
    bars = {"SOL": spiky, "XRP": calm, "ADA": calm[:50]}
    pool = make_pool(bars_fn=lambda a: bars.get(a, calm))
    sol = pool.get("SOL")
    assert not sol.included and sol.jump["spikes"] >= 5
    assert any("1-мин. скачков ≥2%" in r for r in sol.reasons)
    assert pool.contains("XRP") and pool.get("XRP").jump["spikes"] == 0
    assert pool.contains("ADA") and any("не оценены: 49 мин < 120" in r for r in pool.get("ADA").reasons)


def test_jumpiness():
    assert jumpiness([], 2.0) == {"minutes": 0, "spikes": 0, "max_1m_pct": None, "tail_ratio": None}
    assert jumpiness([100.0], 2.0)["minutes"] == 0
    j = jumpiness([100.0, 101.0, 100.0, 103.0, 103.0], 2.0)
    assert j["minutes"] == 4 and j["spikes"] == 1
    assert j["max_1m_pct"] == pytest.approx(3.0, abs=1e-3)
    assert j["tail_ratio"] is not None and j["tail_ratio"] > 0
    # bad closes break the chain (no gap filling)
    assert jumpiness([100.0, 0.0, 150.0, float("nan"), 151.5], 2.0)["minutes"] == 0
    j2 = jumpiness([100.0, -1.0, 150.0, 153.0], 2.0)  # no spurious +50 % across the bad close
    assert j2["minutes"] == 1 and j2["spikes"] == 1 and j2["max_1m_pct"] == pytest.approx(2.0)
    assert jumpiness([100.0, 100.0, 100.0], 2.0)["tail_ratio"] is None


def test_normalise_asset():
    assert normalise_asset(" xbt ") == "BTC" and normalise_asset("XDG") == "DOGE" and normalise_asset("eth") == "ETH"


# --------------------------------------------------------------------------- #
# rate limits, call budget, paging
# --------------------------------------------------------------------------- #
async def test_429_honours_retry_after_and_keeps_pool():
    clock = SimClock(wall=NOW)
    r429 = json.loads(_text("coingecko_429.json"))
    http = FakeHttp(lambda k, p: HttpResponse(429, r429["body"], dict(r429["headers"])))
    pool = make_pool(http=http, clock=clock)
    assert await pool.step() is False
    assert not pool.status.ok and "429" in pool.status.last_error
    assert len(http.calls) == 1  # no retry inside the poll
    assert not pool.due(clock.mono() + 59.0)  # Retry-After: 60 is honoured (never earlier)
    assert pool.source == "fallback" and pool.contains("BTC") and pool.contains("SOL")


async def test_429_mid_categories_keeps_progress(tmp_path):
    clock = SimClock(wall=NOW)

    def override(key, page):
        if key == "stablecoins":
            return HttpResponse(429, "{}", {"retry-after": "60"})
        return None

    http = FakeHttp(override)
    cache = tmp_path / "c.json"
    pool = make_pool(http=http, clock=clock, cache_path=cache, api_key="k")
    assert await pool.step() is False
    assert pool.source == "coingecko"  # markets refreshed before the 429
    doc = json.loads(cache.read_text(encoding="utf-8"))
    assert len(doc["markets"]) == 250 and "meme-token" in doc["categories"]
    pending = pool.snapshot()["pending_categories"]
    assert "stablecoins" in pending and "meme-token" not in pending
    # the next poll (after the backoff) does not refetch fresh markets, it continues the categories
    clock.advance(900.0)
    http.override = None
    n_before = len(http.calls)
    assert await pool.step() is True
    later = http.calls[n_before:]
    assert all("category" in p for _, p, _ in later) and "meme-token" not in [p["category"] for _, p, _ in later]
    assert pool.snapshot()["pending_categories"] == []


async def test_keyless_call_budget_spreads_categories():
    clock = SimClock(wall=NOW)
    http = FakeHttp()
    pool = make_pool(http=http, clock=clock)  # keyless: 5 calls / minute
    assert await pool.step() is True
    assert len(http.calls) == 5
    assert all("x-cg-demo-api-key" not in h for _, _, h in http.calls)
    assert not pool.due(clock.mono() + 30.0) and pool.due(clock.mono() + 61.0)  # continues in a minute
    for _ in range(3):
        clock.advance(61.0)
        if pool.due():
            await pool.step()
    assert sorted(set(http.categories_called())) == sorted(DEFAULT_LIVE_CATEGORIES)
    assert len(http.calls) == 1 + len(DEFAULT_LIVE_CATEGORIES)  # markets once, each category once
    assert pool.snapshot()["pending_categories"] == []
    assert not pool.due(clock.mono() + 3600.0) and pool.due(clock.mono() + 12 * 3600.0)  # then pool_refresh_h


async def test_category_paging_follows_total_and_market_cap():
    big = [{"id": f"coin-{i}", "symbol": f"c{i}", "market_cap": 5e9 - i * 1e6} for i in range(250)]
    tail = [{"id": "coin-tail", "symbol": "ct", "market_cap": 1e9}]

    def override(key, page):
        if key == "wrapped-tokens":
            return HttpResponse(200, json.dumps(big if page == 1 else tail), {"total": "251", "per-page": "250"})
        return None

    http = FakeHttp(override)
    pool = make_pool(http=http, api_key="k")
    await pool.poll_once()
    pages = {c: sorted(p["page"] for _, p, _ in http.calls if p.get("category") == c)
             for c in ("wrapped-tokens", "meme-token")}
    assert pages["wrapped-tokens"] == [1, 2]
    assert pages["meme-token"] == [1]  # 6429 members, but page 1 already ends far below any candidate's cap
    assert "coin-tail" in pool._live_cats["wrapped-tokens"]["ids"]


async def test_bad_payload_is_an_error_not_data():
    http = FakeHttp(lambda k, p: HttpResponse(200, '{"status": {"error_code": 10002}}', {}) if k == "markets" else None)
    pool = make_pool(http=http)
    assert await pool.step() is False
    assert "10002" in pool.status.last_error and pool.source == "fallback"
    http2 = FakeHttp(lambda k, p: HttpResponse(401, "{}", {}))
    pool2 = make_pool(http=http2, api_key=SECRET)
    assert await pool2.step() is False
    assert "401" in pool2.status.last_error and SECRET not in pool2.status.last_error


# --------------------------------------------------------------------------- #
# cache
# --------------------------------------------------------------------------- #
async def test_cache_roundtrip_and_no_refetch_after_restart(tmp_path):
    clock = SimClock(wall=NOW)
    cache = tmp_path / "pool.json"
    first = make_pool(http=FakeHttp(), clock=clock, cache_path=cache, api_key="k", listings_fn=lambda: LISTINGS,
                      stability_fn=research_stability)
    await first.poll_once()
    clock.advance(3600.0)
    # restart: bars has no stability yet -> the cached measurement (1 h old) keeps the pool
    second = make_pool(http=FakeHttp(), clock=clock, cache_path=cache, listings_fn=lambda: LISTINGS,
                       stability_fn=lambda a: None)
    assert second.source == "cache"
    assert second.assets() == first.assets()
    xrp = second.get("XRP")
    assert xrp.stability["origin"] == "cache" and any("стабильность из кэша" in r for r in xrp.reasons)
    assert not second.due() and second.due(clock.mono() + 11 * 3600.0 + 1.0)  # fresh cache: no call on restart
    assert "кэш" in second.snapshot()["steps"]["market"]
    # a stale measurement is no longer data; with the source still silent the step is skipped (outage),
    # it does not empty the pool
    clock.advance(POOL_DEFAULTS["stability_max_age_h"] * 3600.0)
    second.reevaluate()
    assert "временно пропущен" in second.snapshot()["steps"]["stability"]


def test_corrupt_cache_falls_back(tmp_path):
    cache = tmp_path / "pool.json"
    cache.write_text("{not json", encoding="utf-8")
    pool = make_pool(cache_path=cache)
    assert pool.source == "fallback" and pool.contains("BTC") and pool.due()


def test_pool_asset_to_dict():
    d = PoolAsset(asset="BTC", rank=1, included=True, reasons=["в пуле: ранг #1"]).to_dict()
    assert d["asset"] == "BTC" and d["included"] is True and d["source"] == "fallback" and d["jump"] == {}


def test_stability_source_down_does_not_empty_the_pool(tmp_path):
    from latbot.clock import SimClock
    from latbot.config import Settings
    from latbot.universe.pool import AssetPool

    pool = AssetPool(Settings(), SimClock(), None, cache_path=tmp_path / "p.json", stability_fn=lambda a: None)
    pool.reevaluate()
    assert len(pool.assets()) > 2  # not just BTC/ETH
    assert "временно пропущен" in pool.snapshot()["steps"]["stability"]
    measured = {"SOL": {"days": 30, "hours": 720, "btc_corr": 0.1, "hour_jumps": 99, "max_7d_move_pct": 20.0, "ts": 1_700_000_000.0}}
    pool2 = AssetPool(Settings(), SimClock(), None, cache_path=tmp_path / "q.json", stability_fn=measured.get)
    pool2.reevaluate()
    assert "SOL" not in pool2.assets()  # once the source answers, the step applies again
