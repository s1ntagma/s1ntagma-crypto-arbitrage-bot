"""context/derivs.py: OKX derivatives (funding / OI / liquidations / long-short),
Fear & Greed and spot-ETF flows - offline, recorded fixtures + a fake HttpGetter."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from latbot.clock import SimClock
from latbot.context.base import HttpResponse
from latbot.context.derivs import Derivatives, EtfFlows, FearGreed, parse_sosovalue_v2

FIX = Path(__file__).parent / "fixtures" / "context" / "derivs"
NOW = 1791464000.0  # 2026-10-08 13:33 UTC, right after the fixtures were captured


def fx(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def resp(text: str, status: int = 200, headers: dict[str, str] | None = None) -> HttpResponse:
    return HttpResponse(status, text, headers or {})


def okx(data: list[Any], code: str = "0", msg: str = "") -> HttpResponse:
    return resp(json.dumps({"code": code, "data": data, "msg": msg}))


def ms(ts: float) -> str:
    return str(int(round(ts * 1000)))


class FakeHttp:
    """Routes by URL substring (the latest matching route wins).  A route is an
    HttpResponse, an Exception, or a callable(url, params_or_json_body)."""

    def __init__(self) -> None:
        self.routes: list[tuple[str, Any]] = []
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def route(self, needle: str, answer: Any) -> None:
        self.routes.append((needle, answer))

    def _answer(self, method: str, url: str, params: dict[str, Any] | None) -> HttpResponse:
        self.calls.append((method, url, dict(params or {})))
        for needle, answer in reversed(self.routes):
            if needle in url:
                if callable(answer):
                    answer = answer(url, dict(params or {}))
                if isinstance(answer, Exception):
                    raise answer
                return answer
        return HttpResponse(404, "not found")

    async def get(self, url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None,
                  timeout_s: float = 10.0) -> HttpResponse:
        return self._answer("GET", url, params)

    async def close(self) -> None:
        pass

    def count(self, needle: str) -> int:
        return sum(1 for _, url, _ in self.calls if needle in url)

    def params(self, needle: str) -> list[dict[str, Any]]:
        return [p for _, url, p in self.calls if needle in url]


class FakePostHttp(FakeHttp):
    async def post(self, url: str, *, json: Any = None, headers: dict[str, str] | None = None,
                   timeout_s: float = 10.0) -> HttpResponse:
        return self._answer("POST", url, json)


def _default_liq(url: str, p: dict[str, Any]) -> HttpResponse:
    if p.get("instFamily") == "BTC-USDT":
        return resp(fx("okx_liquidation_btc.json"))
    return okx([])


def _default_ls(url: str, p: dict[str, Any]) -> HttpResponse:
    if p.get("instId") == "BTC-USDT-SWAP":
        return resp(fx("okx_ls_ratio_btc.json"))
    return okx([[ms(NOW - 200), "1.5"]])


def okx_http() -> FakeHttp:
    h = FakeHttp()
    h.route("/api/v5/public/instruments", resp(fx("okx_instruments_swap.json")))
    h.route("/api/v5/public/funding-rate", resp(fx("okx_funding_any.json")))
    h.route("/api/v5/public/open-interest", resp(fx("okx_open_interest_swap.json")))
    h.route("/api/v5/public/liquidation-orders", _default_liq)
    h.route("/long-short-account-ratio-contract", _default_ls)
    return h


def make(assets: list[str], http: FakeHttp | None = None, clock: SimClock | None = None, db: Any = None,
         **settings: Any) -> tuple[Derivatives, FakeHttp, SimClock]:
    http = http or okx_http()
    clock = clock or SimClock(wall=NOW, mono=1000.0)
    d = Derivatives(SimpleNamespace(**settings), clock, http, assets_fn=lambda: list(assets), db=db)
    return d, http, clock


class FakeDb:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])
        self.saved: list[dict[str, Any]] = []
        self.load_calls: list[tuple[str, float]] = []

    def save_deriv_sample(self, row: dict[str, Any]) -> None:
        self.saved.append(dict(row))
        self.rows.append(dict(row))

    def load_deriv_samples(self, asset: str, since_ts: float) -> list[dict[str, Any]]:
        self.load_calls.append((asset, since_ts))
        return [dict(r) for r in self.rows if r["asset"] == asset and r["ts"] >= since_ts]


class AsyncDb(FakeDb):
    async def save_deriv_sample(self, row: dict[str, Any]) -> None:  # type: ignore[override]
        FakeDb.save_deriv_sample(self, row)

    async def load_deriv_samples(self, asset: str, since_ts: float) -> list[dict[str, Any]]:  # type: ignore[override]
        return FakeDb.load_deriv_samples(self, asset, since_ts)


# --------------------------------------------------------------------------- #
# Derivatives: funding / OI
# --------------------------------------------------------------------------- #
async def test_funding_normalised_per_8h_with_interval_and_next_funding():
    d, http, _ = make(["btc", "TIA", "BTC"])
    assert await d.step() is True
    assert d.status.ok and d.status.items == 2

    btc = d.get("BTC")
    assert btc is not None
    assert btc["source"] == "okx" and btc["inst_id"] == "BTC-USDT-SWAP"
    assert btc["funding_interval_h"] == 8.0
    assert btc["funding_rate"] == pytest.approx(0.0000555670777269)
    assert btc["funding_rate_8h"] == pytest.approx(0.0000555670777269)
    assert btc["next_funding_ts"] == 1791475200.0
    assert btc["funding_cap"] == pytest.approx(0.00375)
    assert btc["funding_at_cap"] is False

    tia = d.get("tia")  # case-insensitive
    assert tia["funding_interval_h"] == 4.0
    assert tia["funding_rate"] == pytest.approx(0.00005)
    assert tia["funding_rate_8h"] == pytest.approx(0.0001)  # 4 h rate doubled to 8 h

    # every OKX USDT perp's interval is known (pool filter), not only the tracked ones;
    # inverse (BTC-USD-SWAP) and X-perps futures rows are ignored
    assert d.funding_interval_h("AI") == 4.0
    assert d.funding_interval_h("XAU") is None
    assert d.get("XAU") is None

    # one call each covers every perpetual
    assert http.count("/public/funding-rate") == 1 and http.params("/public/funding-rate") == [{"instId": "ANY"}]
    assert http.count("/public/open-interest") == 1 and http.params("/public/open-interest") == [{"instType": "SWAP"}]
    assert [r["asset"] for r in d.snapshot()["assets"]] == ["BTC", "TIA"]


async def test_open_interest_snapshot_and_unlisted_asset():
    d, http, _ = make(["BTC", "NOPE"])
    assert await d.step() is True
    btc = d.get("BTC")
    assert btc["oi_usd"] == pytest.approx(2538703056.8675507)
    assert btc["oi_ccy"] == pytest.approx(30928.1486000001307)
    assert btc["oi_chg_15m_pct"] is None and btc["oi_chg_1h_pct"] is None  # no history yet: unknown, not 0

    assert d.get("NOPE") is None
    snap = d.snapshot()
    assert snap["missing"] == ["NOPE"]
    nope = [r for r in snap["assets"] if r["asset"] == "NOPE"][0]
    assert nope["available"] is False and "OKX" in nope["reason"]
    assert snap["funding_intervals"] == {"8": 5, "4": 2}
    assert snap["status"]["name"] == "derivatives"
    # no per-asset calls for an asset OKX does not list
    assert not [p for p in http.params("liquidation-orders") if p.get("instFamily") == "NOPE-USDT"]


async def test_oi_changes_use_coins_and_quadrants():
    clock = SimClock(wall=NOW, mono=1000.0)
    state = {"k": 0}

    def oi_route(url: str, p: dict[str, Any]) -> HttpResponse:
        k = state["k"]
        ts = ms(clock.wall())
        return okx([
            # USD value swings wildly (price), coins grow 3 % over 15 min
            {"instId": "BTC-USDT-SWAP", "instType": "SWAP", "oi": "1", "oiCcy": str(1000 + 2 * k),
             "oiUsd": str(1e9 * (1 + 0.1 * k)), "ts": ts},
            {"instId": "ETH-USDT-SWAP", "instType": "SWAP", "oi": "1", "oiCcy": str(1000 - 3 * k),
             "oiUsd": "5e8", "ts": ts},
            {"instId": "SOL-USDT-SWAP", "instType": "SWAP", "oi": "1", "oiCcy": "500", "oiUsd": "5e7", "ts": ts},
        ])

    http = okx_http()
    http.route("/api/v5/public/open-interest", oi_route)
    d, _, _ = make(["BTC", "ETH", "SOL"], http=http, clock=clock)
    for k in range(16):
        state["k"] = k
        if k:
            clock.advance(60.0)
        assert await d.step() is True
        if k == 10:
            assert d.get("BTC")["oi_chg_15m_pct"] is None  # 10 min of history is not a 15 min change
            assert d.oi_quadrant("BTC", 1.0) is None

    btc = d.get("BTC")
    assert btc["oi_chg_15m_pct"] == pytest.approx(3.0)
    assert btc["oi_chg_1h_pct"] is None
    assert d.get("ETH")["oi_chg_15m_pct"] == pytest.approx(-4.5)

    assert d.oi_quadrant("BTC", 0.8) == "NEW_LONGS"
    assert d.oi_quadrant("BTC", -0.8) == "NEW_SHORTS"
    assert d.oi_quadrant("BTC", 0.2) == "QUIET"  # price below +-0.5 %
    assert d.oi_quadrant("ETH", 0.6) == "SHORT_COVERING"
    assert d.oi_quadrant("ETH", -0.6) == "LONG_LIQUIDATION"
    assert d.oi_quadrant("SOL", 2.0) == "QUIET"  # OI flat
    assert d.oi_quadrant("BTC", None) is None
    assert d.oi_quadrant("BTC", float("nan")) is None
    assert d.oi_quadrant("NOPE", 1.0) is None


async def test_stale_data_is_no_data():
    d, _, clock = make(["BTC"])
    assert await d.step() is True
    # funding ts = NOW-6 s, OI ts = NOW-156 s; stale after derivs_stale_s=300
    clock.advance(200.0)
    btc = d.get("BTC")
    assert btc["funding_rate_8h"] is not None
    assert btc["oi_usd"] is None and btc["oi_chg_15m_pct"] is None
    clock.advance(200.0)
    assert d.get("BTC") is None
    assert d.funding_percentile("BTC") is None
    assert d.oi_quadrant("BTC", 1.0) is None
    assert d.funding_interval_h("BTC") == 8.0  # structural info survives a short outage


# --------------------------------------------------------------------------- #
# funding percentiles + persistence
# --------------------------------------------------------------------------- #
def _funding_route(clock: SimClock, rates: dict[str, float]) -> Any:
    def route(url: str, p: dict[str, Any]) -> HttpResponse:
        rows = []
        for asset, rate in rates.items():
            rows.append({"instId": f"{asset}-USDT-SWAP", "instType": "SWAP", "fundingRate": repr(rate),
                         "fundingTime": "1791475200000", "nextFundingTime": "1791504000000",
                         "maxFundingRate": "0.00375", "minFundingRate": "-0.00375", "ts": ms(clock.wall())})
        return okx(rows)
    return route


async def test_funding_percentile_needs_min_samples_and_persists_samples():
    clock = SimClock(wall=NOW, mono=1000.0)
    rates = {"BTC": 1e-4}
    http = okx_http()
    http.route("/api/v5/public/funding-rate", _funding_route(clock, rates))
    db = FakeDb([{"asset": "BTC", "ts": NOW - 50.0, "source": "binance", "funding_rate_8h": 9e-4}])
    d, _, _ = make(["BTC"], http=http, clock=clock, db=db, derivs_min_samples=4, derivs_sample_every_s=300)
    assert db.load_calls and db.load_calls[0][0] == "BTC"
    assert db.load_calls[0][1] == pytest.approx(NOW - 30 * 86400)
    assert d.funding_samples("BTC") == 0  # the binance row is never mixed into the OKX history

    for i, rate in enumerate([1e-4, 2e-4, 3e-4]):
        rates["BTC"] = rate
        if i:
            clock.advance(300.0)
        assert await d.step() is True
        assert d.funding_percentile("BTC") is None
    g = d.get("BTC")
    assert g["funding_pct"] is None and g["funding_n"] == 3 and g["funding_min_n"] == 4

    clock.advance(60.0)  # between samples: nothing is saved
    assert await d.step() is True
    assert len(db.saved) == 3

    rates["BTC"] = 4e-4
    clock.advance(240.0)
    assert await d.step() is True
    assert len(db.saved) == 4
    assert d.funding_percentile("BTC") == (87.5, 4)  # 3 below + half of 1 equal, of 4
    assert d.get("BTC")["funding_pct"] == 87.5

    s = db.saved[-1]
    assert set(s) == {"asset", "ts", "venue", "source", "inst_id", "funding_rate", "funding_rate_8h",
                      "funding_interval_h", "oi_usd", "oi_ccy", "ls_ratio"}
    assert s["asset"] == "BTC" and s["venue"] == s["source"] == "okx" and s["ts"] == clock.wall()
    assert s["funding_rate_8h"] == pytest.approx(4e-4) and s["funding_interval_h"] == 8.0

    # a restart loads the persisted history at construction: no waiting for 288 new samples
    d2, _, _ = make(["BTC"], http=http, clock=clock, db=db, derivs_min_samples=4, derivs_sample_every_s=300)
    assert d2.funding_samples("BTC") == 4
    assert await d2.step() is True
    assert d2.funding_percentile("BTC") == (87.5, 4)
    assert len(db.saved) == 4  # last sample is younger than derivs_sample_every_s


async def test_samples_round_trip_through_the_real_database():
    from latbot.storage.db import Database

    clock = SimClock(wall=NOW, mono=1000.0)
    rates = {"BTC": 1e-4}
    http = okx_http()
    http.route("/api/v5/public/funding-rate", _funding_route(clock, rates))
    db = Database(":memory:", clock=clock)
    db.save_deriv_sample({"asset": "BTC", "venue": "binance", "ts": NOW - 10.0, "funding_rate_8h": 5e-3})
    d, _, _ = make(["BTC"], http=http, clock=clock, db=db, derivs_min_samples=3)
    for i in range(3):
        rates["BTC"] = (i + 1) * 1e-4
        if i:
            clock.advance(300.0)
        assert await d.step() is True
    assert d.funding_percentile("BTC") == (83.3, 3)

    d2, _, _ = make(["BTC"], http=http, clock=clock, db=db, derivs_min_samples=3)
    assert d2.funding_samples("BTC") == 3  # the binance sample stays out of the OKX history
    assert await d2.step() is True
    assert d2.funding_percentile("BTC") == (83.3, 3)


async def test_funding_percentile_against_loaded_history():
    rows = [{"asset": "BTC", "ts": NOW - 100.0 - 300.0 * i, "source": "okx", "funding_rate_8h": i * 1e-6,
             "oi_ccy": 1.0} for i in range(300)]
    d, _, _ = make(["BTC"], db=FakeDb(rows))
    assert d.min_samples == 288  # documented default
    assert await d.step() is True
    # current BTC 8h funding 5.5567e-5: 56 of 300 samples are below
    assert d.funding_percentile("BTC") == (18.7, 300)


async def test_default_min_samples_means_history_is_building():
    d, _, _ = make(["BTC"], db=FakeDb())
    assert await d.step() is True
    assert d.funding_percentile("BTC") is None
    g = d.get("BTC")
    assert g["funding_n"] == 1 and g["funding_min_n"] == 288 and g["funding_pct"] is None


async def test_async_db_is_loaded_on_first_poll():
    rows = [{"asset": "BTC", "ts": NOW - 100.0 - 300.0 * i, "source": "okx", "funding_rate_8h": 1e-4}
            for i in range(5)]
    db = AsyncDb(rows)
    d, _, _ = make(["BTC"], db=db, derivs_min_samples=5)
    assert d.funding_samples("BTC") == 0
    assert await d.step() is True
    assert d.funding_samples("BTC") == 5
    assert d.funding_percentile("BTC") is not None


async def test_db_errors_never_break_polling():
    class BrokenDb:
        def load_deriv_samples(self, asset: str, since_ts: float) -> list[dict[str, Any]]:
            raise RuntimeError("db locked")

        def save_deriv_sample(self, row: dict[str, Any]) -> None:
            raise RuntimeError("db locked")

    d, _, _ = make(["BTC"], db=BrokenDb())
    assert await d.step() is True
    assert d.get("BTC")["funding_rate_8h"] is not None


# --------------------------------------------------------------------------- #
# liquidations
# --------------------------------------------------------------------------- #
async def test_liquidations_usd_and_side_from_recorded_page():
    d, http, _ = make(["BTC", "ETH"])
    assert await d.step() is True
    btc = d.get("BTC")
    # 5 long liquidations (side=sell, posSide=long): USD = sz * ctVal(0.01) * ctMult(1) * bkPx
    expected = 0.01 * (1 * 82056.5 + 0.49 * 81956.5 + 0.53 * 81958.5 + 0.45 * 81947.7 + 0.01 * 81958.5)
    assert btc["liq_long_usd_5m"] == pytest.approx(expected, abs=0.01)
    assert btc["liq_long_usd_15m"] == pytest.approx(expected, abs=0.01)
    assert btc["liq_short_usd_5m"] == 0.0 and btc["liq_short_usd_15m"] == 0.0
    # OKX answered an empty (not full) page: measured zero, not unknown
    eth = d.get("ETH")
    assert eth["liq_long_usd_5m"] == 0.0 and eth["liq_short_usd_15m"] == 0.0
    assert isinstance(eth["liq_long_usd_5m"], float)

    p = [x for x in http.params("liquidation-orders") if x.get("instFamily") == "BTC-USDT"][0]
    assert p == {"instType": "SWAP", "instFamily": "BTC-USDT", "state": "filled", "limit": "100"}


async def test_liquidation_side_mapping_and_windows():
    def liq(url: str, p: dict[str, Any]) -> HttpResponse:
        if p.get("instFamily") != "SOL-USDT":
            return okx([])
        details = [
            {"side": "buy", "posSide": "short", "sz": "2", "bkPx": "100", "ts": ms(NOW - 30)},  # short liquidated
            {"side": "buy", "posSide": "long", "sz": "7", "bkPx": "100", "ts": ms(NOW - 31)},  # inconsistent: skipped
            {"side": "sell", "posSide": "net", "sz": "1", "bkPx": "100", "ts": ms(NOW - 400)},  # long, 15 m only
            {"side": "sell", "posSide": "long", "sz": "", "bkPx": "100", "ts": ms(NOW - 40)},  # no size: skipped
            {"side": "sell", "posSide": "long", "sz": "3", "bkPx": "100", "ts": ms(NOW - 1000)},  # outside 15 m
        ]
        return okx([{"instId": "SOL-USDT-SWAP", "instFamily": "SOL-USDT", "instType": "SWAP", "details": details}])

    http = okx_http()
    http.route("/api/v5/public/liquidation-orders", liq)
    d, _, _ = make(["SOL"], http=http)
    assert await d.step() is True
    sol = d.get("SOL")
    assert sol["liq_short_usd_5m"] == 200.0 and sol["liq_long_usd_5m"] == 0.0  # ctVal(SOL) = 1
    assert sol["liq_short_usd_15m"] == 200.0 and sol["liq_long_usd_15m"] == 100.0


async def test_liquidation_coverage_pagination_overlap_and_gap():
    clock = SimClock(wall=NOW, mono=1000.0)
    page: dict[str, Any] = {}

    def details(ts_list: list[float]) -> list[dict[str, Any]]:
        return [{"side": "sell", "posSide": "long", "sz": "1", "bkPx": "100", "ts": ms(t)} for t in ts_list]

    def liq(url: str, p: dict[str, Any]) -> HttpResponse:
        if p.get("instFamily") != "BTC-USDT":
            return okx([])
        rows = page["older"] if "after" in p else page["latest"]
        return okx([{"instId": "BTC-USDT-SWAP", "instType": "SWAP", "details": details(rows)}])

    http = okx_http()
    http.route("/api/v5/public/liquidation-orders", liq)
    http.route("/api/v5/public/funding-rate", _funding_route(clock, {"BTC": 1e-4}))  # stays fresh
    d, _, _ = make(["BTC"], http=http, clock=clock)

    # poll 1: a full page (100 events in the last 100 s) + an older non-full page -> complete
    t0 = NOW
    page["latest"] = [t0 - 1 - i for i in range(100)]
    page["older"] = [t0 - 400 - i for i in range(10)]
    assert await d.step() is True
    after = [p["after"] for p in http.params("liquidation-orders") if "after" in p]
    assert after == [ms(t0 - 100)]
    btc = d.get("BTC")
    assert btc["liq_long_usd_5m"] == pytest.approx(100.0)  # 1 contract * 0.01 BTC * 100 USD each
    assert btc["liq_long_usd_15m"] == pytest.approx(110.0)

    # poll 2 (+60 s): a full page that overlaps the previous poll -> still continuous, duplicates counted once
    clock.advance(60.0)
    t1 = clock.wall()
    page["latest"] = [t1 - 1 - i for i in range(100)]  # 60 new + 40 already seen
    n_after = len(after)
    assert await d.step() is True
    assert len([p for p in http.params("liquidation-orders") if "after" in p]) == n_after  # no pagination
    btc = d.get("BTC")
    assert btc["liq_long_usd_5m"] == pytest.approx(160.0)
    assert btc["liq_long_usd_15m"] == pytest.approx(170.0)

    # poll 3 (+600 s gap, one page only): the page does not reach the previous poll -> windows unknown
    d.liq_max_pages = 1
    clock.advance(600.0)
    t2 = clock.wall()
    page["latest"] = [t2 - 1 - i for i in range(100)]
    assert await d.step() is True
    btc = d.get("BTC")
    assert btc["liq_long_usd_5m"] is None and btc["liq_long_usd_15m"] is None
    assert btc["funding_rate_8h"] == pytest.approx(1e-4)  # other fields unaffected


# --------------------------------------------------------------------------- #
# long / short ratio
# --------------------------------------------------------------------------- #
async def test_long_short_ratio_fetched_every_5_minutes():
    d, http, clock = make(["BTC"])
    assert await d.step() is True
    btc = d.get("BTC")
    assert btc["ls_ratio"] == pytest.approx(1.822068218115446)
    assert btc["ls_ts"] == 1791463800.0
    assert http.params("long-short-account-ratio-contract") == [
        {"instId": "BTC-USDT-SWAP", "period": "5m", "limit": "1"}]

    clock.advance(60.0)
    assert await d.step() is True
    assert http.count("long-short-account-ratio-contract") == 1
    clock.advance(240.0)
    assert await d.step() is True
    assert http.count("long-short-account-ratio-contract") == 2


# --------------------------------------------------------------------------- #
# failures
# --------------------------------------------------------------------------- #
async def test_core_failure_is_an_error_and_no_data():
    http = okx_http()
    http.route("/api/v5/public/funding-rate", resp("oops", 500))
    http.route("/api/v5/public/open-interest", resp("oops", 500))
    d, _, _ = make(["BTC"], http=http)
    assert await d.step() is False
    assert d.status.ok is False and "HTTP 500" in d.status.last_error
    assert d.get("BTC") is None
    assert http.count("liquidation-orders") == 0  # no per-asset calls without the core data


async def test_one_core_part_failing_keeps_the_other():
    http = okx_http()
    http.route("/api/v5/public/open-interest", resp("oops", 500))
    d, _, _ = make(["BTC"], http=http)
    assert await d.step() is True
    btc = d.get("BTC")
    assert btc["funding_rate_8h"] is not None and btc["oi_usd"] is None
    assert "open_interest" in d.status.note
    assert d.snapshot()["parts"]["open_interest"]["ok"] is False


async def test_okx_error_codes_and_rate_limits():
    http = okx_http()
    http.route("/api/v5/public/funding-rate", okx([], code="51000", msg="Parameter instId error"))
    http.route("/api/v5/public/open-interest", okx([], code="51000", msg="Parameter error"))
    d, _, _ = make(["BTC"], http=http)
    assert await d.step() is False
    assert "code 51000" in d.status.last_error

    http.route("/api/v5/public/funding-rate", resp("{}", 429, {"retry-after": "120"}))
    http.route("/api/v5/public/open-interest", okx([], code="50011", msg="Too Many Requests"))
    d, _, clock = make(["BTC"], http=http)
    assert await d.step() is False
    assert "rate limited" in d.status.last_error
    assert d.due(clock.mono() + 119.0) is False
    assert d.due(clock.mono() + 121.0) is True


async def test_optional_parts_fail_softly():
    http = okx_http()
    http.route("/api/v5/public/liquidation-orders", okx([], code="51000", msg="Parameter limit error"))
    http.route("/long-short-account-ratio-contract", resp("oops", 500))
    d, _, clock = make(["BTC", "ETH"], http=http)
    assert await d.step() is True
    btc = d.get("BTC")
    assert btc["liq_long_usd_5m"] is None and btc["liq_short_usd_15m"] is None
    assert btc["ls_ratio"] is None
    parts = d.snapshot()["parts"]
    assert parts["liquidations"]["failed"] == 2 and "51000" in parts["liquidations"]["error"]
    assert parts["long_short"]["failed"] == 2
    assert "ликвидации" in d.status.note and "пауза" in d.status.note

    # every call failed: the broken best-effort source is paused instead of hammered each minute
    n_liq, n_ls = http.count("liquidation-orders"), http.count("long-short-account-ratio-contract")
    clock.advance(60.0)
    assert await d.step() is True
    assert http.count("liquidation-orders") == n_liq
    clock.advance(600.0)
    assert await d.step() is True
    assert http.count("liquidation-orders") > n_liq
    assert http.count("long-short-account-ratio-contract") > n_ls


async def test_optional_rate_limit_pauses_per_asset_calls():
    http = okx_http()
    http.route("/api/v5/public/liquidation-orders", resp("{}", 429, {"retry-after": "180"}))
    d, _, clock = make(["BTC"], http=http)
    assert await d.step() is True
    n = http.count("liquidation-orders")
    clock.advance(60.0)
    assert await d.step() is True
    assert http.count("liquidation-orders") == n  # paused while rate limited
    assert "пауза" in d.status.note
    clock.advance(130.0)
    assert await d.step() is True
    assert http.count("liquidation-orders") > n


async def test_missing_instruments_means_no_liquidation_usd():
    http = okx_http()
    http.route("/api/v5/public/instruments", resp("oops", 503))
    d, _, _ = make(["BTC"], http=http)
    assert await d.step() is True
    btc = d.get("BTC")
    assert btc["funding_rate_8h"] is not None
    assert btc["liq_long_usd_5m"] is None  # no ctVal -> no USD, never a guess
    assert "instruments" in d.status.note


async def test_assets_fn_failure_is_safe_and_disabled_collector():
    def boom() -> list[str]:
        raise RuntimeError("pool not ready")

    clock = SimClock(wall=NOW, mono=1000.0)
    d = Derivatives(SimpleNamespace(), clock, okx_http(), assets_fn=boom)
    assert await d.step() is True
    assert d.snapshot()["assets"] == []
    assert d.funding_interval_h("BTC") == 8.0

    off = Derivatives(SimpleNamespace(derivs_enabled=False), clock, okx_http(), assets_fn=lambda: ["BTC"])
    assert off.due() is False and off.status.note == "disabled in settings"
    assert off.get("BTC") is None


# --------------------------------------------------------------------------- #
# Fear & Greed
# --------------------------------------------------------------------------- #
async def test_fear_greed_parses_and_ages_out():
    http = FakeHttp()
    http.route("api.alternative.me/fng", resp(fx("fng.json")))
    clock = SimClock(wall=NOW, mono=1000.0)
    fg = FearGreed(SimpleNamespace(), clock, http)
    assert fg.interval_s == 3600.0 and fg.name == "fear_greed"
    assert fg.get() is None
    assert await fg.step() is True
    g = fg.get()
    assert g["value"] == 64 and g["classification"] == "Greed" and g["ts"] == 1791417600.0
    assert g["prev_value"] == 71 and g["source"] == "alternative.me"
    assert fg.latest() == g
    assert http.params("fng") == [{"limit": "2"}]
    assert fg.snapshot()["latest"] == g

    # a failed refresh keeps the last good (still fresh) value
    http.route("api.alternative.me/fng", resp("oops", 502))
    assert await fg.step() is False
    assert fg.get()["value"] == 64

    clock.advance(2 * 86400.0)
    assert fg.get() is None  # daily index older than fear_greed_max_age_s


@pytest.mark.parametrize("payload, needle", [
    (fx("fng_world_format.json"), "timestamp"),
    (json.dumps({"name": "x", "data": [], "metadata": {"error": "Service unavailable"}}), "Service unavailable"),
    (json.dumps({"data": [{"value": "150", "value_classification": "?", "timestamp": "1791417600"}]}), "bad value"),
    ("<html>", "not JSON"),
])
async def test_fear_greed_rejects_bad_payloads(payload: str, needle: str):
    http = FakeHttp()
    http.route("api.alternative.me/fng", resp(payload))
    fg = FearGreed(SimpleNamespace(), SimClock(wall=NOW), http)
    assert await fg.step() is False
    assert needle in fg.status.last_error
    assert fg.get() is None


# --------------------------------------------------------------------------- #
# ETF flows
# --------------------------------------------------------------------------- #
def etf_http(answers: dict[str, Any]) -> FakePostHttp:
    http = FakePostHttp()
    http.route("api.sosovalue.xyz/openapi/v2/etf/historicalInflowChart",
               lambda url, body: answers.get(body.get("type"), resp("nope", 404)))
    return http


async def test_etf_flows_latest_per_asset():
    http = etf_http({"us-btc-spot": resp(fx("sosovalue_us-btc-spot.json")),
                     "us-eth-spot": resp(fx("sosovalue_us-eth-spot.json"))})
    etf = EtfFlows(SimpleNamespace(), SimClock(wall=NOW), http)
    assert etf.name == "etf_flows"
    assert await etf.step() is True
    assert etf.latest("BTC") == {"date": "2026-10-07", "net_usd": -487067411.5, "source": "sosovalue_v2"}
    assert etf.latest("eth") == {"date": "2026-10-07", "net_usd": -160773554.86, "source": "sosovalue_v2"}
    assert etf.latest("SOL") is None
    assert [r["date"] for r in etf.history("BTC", 3)] == ["2026-10-07", "2026-10-06", "2026-10-05"]
    assert sorted(b["type"] for m, _, b in http.calls if m == "POST") == ["us-btc-spot", "us-eth-spot"]
    snap = etf.snapshot()
    assert snap["assets"]["BTC"]["latest"]["net_usd"] == -487067411.5 and snap["assets"]["BTC"]["days"] == 20


async def test_etf_flows_without_post_is_no_data():
    http = FakeHttp()  # GET-only HttpGetter: SoSoValue v2 needs POST
    etf = EtfFlows(SimpleNamespace(), SimClock(wall=NOW), http)
    assert await etf.step() is False
    assert "нет данных" in etf.status.note
    assert etf.latest("BTC") is None
    assert http.calls == []


async def test_etf_flows_partial_failure_empty_series_and_bad_code():
    http = etf_http({"us-btc-spot": resp(fx("sosovalue_xrp_empty.json")), "us-eth-spot": resp("oops", 500)})
    etf = EtfFlows(SimpleNamespace(), SimClock(wall=NOW), http)
    assert await etf.step() is True  # one series answered (empty), the other failed
    assert etf.latest("BTC") is None and etf.latest("ETH") is None
    assert "пустой ответ" in etf.status.note and "нет данных" in etf.status.note

    http = etf_http({"us-btc-spot": resp(json.dumps({"code": 40001, "msg": "need key", "data": None})),
                     "us-eth-spot": resp("{}", 429, {"retry-after": "600"})})
    etf = EtfFlows(SimpleNamespace(), SimClock(wall=NOW), http)
    assert await etf.step() is False
    assert "rate limited" in etf.status.last_error
    assert etf.due(etf._clock.mono() + 599.0) is False


async def test_etf_flows_missing_day_is_not_zero_and_stale_is_none():
    payload = json.loads(fx("sosovalue_us-btc-spot.json"))
    payload["data"][0]["totalNetInflow"] = None  # 2026-10-07 not reported yet
    payload["data"][1]["totalNetInflow"] = "-"  # 2026-10-06 dash
    http = etf_http({"us-btc-spot": resp(json.dumps(payload))})
    clock = SimClock(wall=NOW)
    etf = EtfFlows(SimpleNamespace(etf_types={"BTC": "us-btc-spot"}), clock, http)
    assert await etf.step() is True
    assert etf.latest("BTC") == {"date": "2026-10-05", "net_usd": -89897783.0, "source": "sosovalue_v2"}

    # a failed refresh keeps the series; a too-old latest day is no data
    http.route("historicalInflowChart", resp("oops", 502))
    assert await etf.step() is False
    assert etf.latest("BTC")["date"] == "2026-10-05"
    clock.advance(6 * 86400.0)
    assert etf.latest("BTC") is None


def test_parse_sosovalue_sorts_and_dedupes():
    rows = parse_sosovalue_v2(resp(json.dumps({"code": 0, "data": [
        {"date": "2026-10-01", "totalNetInflow": 1},
        {"date": "2026-10-03", "totalNetInflow": "2.5"},
        {"date": "2026-10-03", "totalNetInflow": 99},
        {"date": "bad", "totalNetInflow": 3},
        {"date": "2026-10-02"},
    ]})), "test")
    assert [(r["date"], r["net_usd"]) for r in rows] == [("2026-10-03", 2.5), ("2026-10-01", 1.0)]


async def test_snapshots_are_json_ready_before_and_after_polls():
    d, _, clock = make(["BTC", "NOPE"])
    fg_http = FakeHttp()
    fg_http.route("api.alternative.me/fng", resp(fx("fng.json")))
    fg = FearGreed(SimpleNamespace(), clock, fg_http)
    etf = EtfFlows(SimpleNamespace(), clock, etf_http({"us-btc-spot": resp(fx("sosovalue_us-btc-spot.json"))}))
    before = [d.snapshot(), fg.snapshot(), etf.snapshot()]
    assert [r["available"] for r in before[0]["assets"]] == [False, False]
    assert before[0]["missing"] == []  # nothing is known yet: not "unlisted"
    json.dumps(before)
    for c in (d, fg, etf):
        await c.step()
    json.dumps([d.snapshot(), fg.snapshot(), etf.snapshot()])
