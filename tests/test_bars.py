"""Offline tests for latbot.context.bars (MinuteBars, erratic gate, stability, helpers)."""
from __future__ import annotations

import json
import math
from pathlib import Path
from types import SimpleNamespace

import pytest

from latbot.clock import SimClock
from latbot.context import bars as bars_mod
from latbot.context.base import HttpResponse
from latbot.context.bars import (
    OKX_CANDLES_URL,
    OKX_HISTORY_CANDLES_URL,
    MinuteBars,
    bar_returns,
    beta,
    correlation,
    max_window_move_pct,
    parse_okx_candles,
    stability_metrics,
)

FIX = Path(__file__).parent / "fixtures" / "context" / "bars"
T0 = 1_791_400_000 - (1_791_400_000 % 3600)  # an hour boundary (epoch seconds)


def _settings(**kw):
    base = {"bars_request_interval_s": 0.0}
    base.update(kw)
    return SimpleNamespace(**base)


class Prices:
    """Mutable price source standing in for the integration's median fresh mid."""

    def __init__(self) -> None:
        self.p: dict[str, float | None] = {}

    def __call__(self, asset: str) -> float | None:
        return self.p.get(asset)


def _feed(bars: MinuteBars, clock: SimClock, prices: Prices, series: dict[str, list[float | None]], start: int = T0) -> int:
    """One late sample (+58 s) per minute; returns the epoch of the first minute after the series."""
    n = len(next(iter(series.values())))
    for i in range(n):
        for a, seq in series.items():
            prices.p[a] = seq[i]
        clock.set(wall=start + i * 60 + 58)
        bars.sample()
    end = start + n * 60
    for a in series:
        prices.p[a] = None
    clock.set(wall=end + 1)
    bars.sample()  # commits the last minute
    return end


def _okx_row(ts_s: int, close: float | str, confirm: str = "1") -> list[str]:
    c = str(close)
    return [str(ts_s * 1000), c, c, c, c, "1", "1", "1", confirm]


class FakeOkx:
    """Emulates OKX: /market/candles serves the newest ``recent`` bars (<= 300/page),
    /market/history-candles all bars (<= 100/page); ``after`` = strictly older than."""

    def __init__(self, series: dict[tuple[str, str], list[list[str]]] | None = None, *, recent: int = 1440) -> None:
        self.series = series or {}  # (instId, bar) -> rows oldest first
        self.recent = recent
        self.calls: list[tuple[str, dict]] = []
        self.override: dict[str, HttpResponse] = {}  # instId -> fixed response

    async def get(self, url, *, params=None, headers=None, timeout_s=10.0):
        params = dict(params or {})
        self.calls.append((url, params))
        inst, bar, limit = params["instId"], params["bar"], int(params["limit"])
        if inst in self.override:
            return self.override[inst]
        rows = self.series.get((inst, bar))
        if rows is None:
            return HttpResponse(200, json.dumps({"code": "51001", "msg": "Instrument ID does not exist", "data": []}))
        newest_first = list(reversed(rows))
        if url == OKX_CANDLES_URL:
            pool, cap = newest_first[: self.recent], 300
        else:
            assert url == OKX_HISTORY_CANDLES_URL
            pool, cap = newest_first, 100
        if "after" in params:
            pool = [r for r in pool if int(r[0]) < int(params["after"])]
        return HttpResponse(200, json.dumps({"code": "0", "msg": "", "data": pool[: min(limit, cap)]}))

    async def close(self) -> None:
        pass


# --------------------------------------------------------------------------- #
# live sampling
# --------------------------------------------------------------------------- #
def test_last_fresh_sample_of_a_minute_is_its_close():
    clock, prices = SimClock(wall=T0 + 5), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    bars.track(["btc"])
    for off, p in ((5, 100.0), (30, 101.0), (57, 102.0)):
        prices.p["BTC"] = p
        clock.set(wall=T0 + off)
        bars.sample()
    assert bars.closes("BTC") == []  # minute not finished yet
    prices.p["BTC"] = 103.0
    clock.set(wall=T0 + 62)
    bars.sample()
    assert bars.closes("BTC") == [102.0]
    assert bars.tracked() == ["BTC"]


def test_minute_without_a_late_fresh_price_is_not_written():
    clock, prices = SimClock(wall=T0), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    bars.track(["BTC"])
    prices.p["BTC"] = 100.0
    clock.set(wall=T0 + 5)
    bars.sample()
    prices.p["BTC"] = None  # books went stale for the rest of the minute
    for off in (10, 20, 30, 40, 50, 55):
        clock.set(wall=T0 + off)
        bars.sample()
    clock.set(wall=T0 + 61)
    bars.sample()
    assert bars.closes("BTC") == []
    assert bars.counters["minutes_skipped"] == 1


def test_missing_minutes_stay_missing_and_gaps_are_not_bridged():
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    bars.track(["BTC"])
    end = _feed(bars, clock, prices, {"BTC": [100.0, 101.0, None, 103.0]})
    assert bars.closes("BTC") == [100.0, 101.0, 103.0]
    rets = bars.returns("BTC", 10)
    assert [m for m, _ in rets] == [T0 + 60]  # minute T0+180 has no predecessor
    assert rets[0][1] == pytest.approx(1.0)
    assert end == T0 + 240


def test_price_fn_errors_and_bad_values_count_as_missing():
    clock = SimClock(wall=T0 + 58)

    def broken(asset):
        if asset == "BTC":
            raise RuntimeError("no books")
        return float("nan") if asset == "ETH" else -5.0

    bars = MinuteBars(_settings(), clock, broken)
    bars.track(["BTC", "ETH", "SOL"])
    bars.sample()
    clock.set(wall=T0 + 61)
    bars.sample()
    assert bars.closes("BTC") == bars.closes("ETH") == bars.closes("SOL") == []


def test_keep_minutes_trims_history_and_minutes_window_uses_the_clock():
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(), clock, prices, keep_minutes=60)
    bars.track(["BTC"])
    end = _feed(bars, clock, prices, {"BTC": [100.0 + i for i in range(90)]})
    closes = bars.closes("BTC")
    assert len(closes) == 60 and closes[-1] == 189.0
    assert bars.closes("BTC", 5) == [185.0, 186.0, 187.0, 188.0, 189.0]
    clock.set(wall=end + 3 * 60 + 1)  # three minutes later, nothing new: minutes 88..92 -> 88, 89 exist
    assert bars.closes("BTC", 5) == [188.0, 189.0]
    bars.untrack(["BTC"])
    assert bars.tracked() == []


# --------------------------------------------------------------------------- #
# stats
# --------------------------------------------------------------------------- #
def test_stats_returns_on_a_known_series():
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    bars.track(["BTC"])
    series = [100.0 * 1.001 ** i for i in range(70)]
    _feed(bars, clock, prices, {"BTC": series})
    st = bars.stats("BTC")
    assert st["minutes"] == 70 and st["minutes_1h"] == 60 and st["fresh"] is True
    assert st["ret_5m_pct"] == pytest.approx((1.001 ** 5 - 1) * 100)
    assert st["ret_15m_pct"] == pytest.approx((1.001 ** 15 - 1) * 100)
    assert st["ret_1h_pct"] == pytest.approx((1.001 ** 60 - 1) * 100)
    assert st["vol_1m_pct"] == pytest.approx(0.0, abs=1e-9)  # constant 0.1 % steps
    assert st["vol_pctile"] is None  # fewer than 6 hours of history
    assert st["tail_1m_pct"] is None  # fewer than 120 returns
    assert st["shock"] == 0
    assert st["last_close"] == pytest.approx(series[-1])


def test_stats_without_data_or_stale_data_are_none():
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    st = bars.stats("XRP")
    assert st["minutes"] == 0 and st["ret_5m_pct"] is None and st["vol_1m_pct"] is None and st["shock"] == 0
    bars.track(["BTC"])
    end = _feed(bars, clock, prices, {"BTC": [100.0 + i * 0.01 for i in range(30)]})
    assert bars.stats("BTC")["ret_15m_pct"] is not None
    clock.set(wall=end + 10 * 60)
    st = bars.stats("BTC")
    assert st["fresh"] is False and st["ret_5m_pct"] is None and st["ret_15m_pct"] is None


def _alternating(n: int, start: float, amp_pct: list[float]) -> list[float]:
    """+a, -a, +a ... percent steps; amp_pct[i] is the step size of minute i."""
    out, p = [start], start
    for i in range(1, n):
        a = amp_pct[i] / 100.0
        p = p * (1 + a) if i % 2 else p / (1 + a)
        out.append(p)
    return out


def test_vol_percentile_tail_and_sigma_median():
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    bars.track(["ETH"])
    amps = [0.05] * (7 * 60) + [0.20] * 60  # 7 calm hours, then one 4x noisier hour
    _feed(bars, clock, prices, {"ETH": _alternating(8 * 60, 2000.0, amps)})
    st = bars.stats("ETH")
    assert st["vol_pctile"] == pytest.approx(1.0)
    assert st["sigma_1m_median_pct"] == pytest.approx(0.05, rel=0.05)
    assert st["vol_1m_pct"] == pytest.approx(0.20, rel=0.05)
    assert 0.05 < st["tail_1m_pct"] <= 0.2 + 1e-9


def test_shock_has_a_direction():
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    bars.track(["BTC"])
    calm = _alternating(120, 80000.0, [0.02] * 120)
    _feed(bars, clock, prices, {"BTC": calm + [calm[-1] * 0.992]})  # -0.8 % in one minute
    st = bars.stats("BTC")
    assert st["shock"] == -1 and st["shock_pct"] == pytest.approx(-0.8)
    clock2, prices2 = SimClock(), Prices()
    bars2 = MinuteBars(_settings(), clock2, prices2)
    bars2.track(["BTC"])
    _feed(bars2, clock2, prices2, {"BTC": calm + [calm[-1] * 1.002]})
    assert bars2.stats("BTC")["shock"] == 0


# --------------------------------------------------------------------------- #
# erratic gate
# --------------------------------------------------------------------------- #
def _bars_with(series: list[float], asset: str = "ALT", **kw) -> MinuteBars:
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(**kw), clock, prices)
    bars.track([asset])
    _feed(bars, clock, prices, {asset: series})
    return bars


def test_erratic_two_big_one_minute_bars_suspend():
    calm = _alternating(180, 10.0, [0.02] * 180)
    p = calm[-1]
    tail = [p * 1.012, p * 1.012, p * 1.012 * 0.988, p * 1.012 * 0.988]
    res = _bars_with(calm + tail).erratic("ALT")
    assert res["suspended"] is True and res["available"] is True
    assert res["bars_1pct"] == 2 and res["threshold_1m_pct"] == pytest.approx(1.0)
    assert "хаотичн" in res["reason"]
    one = _bars_with(calm + [p * 1.012, p * 1.012]).erratic("ALT")
    assert one["suspended"] is False and one["bars_1pct"] == 1
    assert one["max_5m_pct"] == pytest.approx(1.2, rel=0.05)


def test_erratic_five_minute_move_suspends_without_big_bars():
    calm = _alternating(180, 10.0, [0.02] * 180)
    p = calm[-1]
    creep = [p * 1.007 ** k for k in range(1, 6)]  # five 0.7 % bars = 3.55 % in 5 min
    res = _bars_with(calm + creep).erratic("ALT")
    assert res["suspended"] is True and res["bars_1pct"] == 0
    assert res["max_5m_pct"] == pytest.approx((1.007 ** 5 - 1) * 100)


def test_erratic_threshold_scales_with_the_assets_own_sigma():
    noisy = _alternating(240, 10.0, [0.2] * 240)  # sigma ~0.2 % -> 8 sigma ~1.6 %
    p = noisy[-1]
    res = _bars_with(noisy + [p * 1.012, p * 1.012 * 0.988]).erratic("ALT")
    assert res["threshold_1m_pct"] == pytest.approx(1.6, rel=0.05)
    assert res["bars_1pct"] == 0 and res["suspended"] is False


def test_erratic_without_enough_minutes_is_unavailable_not_suspended():
    res = _bars_with([10.0, 10.01, 10.0]).erratic("ALT")
    assert res["suspended"] is False and res["available"] is False
    assert "нет данных" in res["reason"]
    empty = MinuteBars(_settings(), SimClock(), Prices()).erratic("NOPE")
    assert empty["available"] is False and empty["max_5m_pct"] is None


def _bars_from_rows(rows: list[tuple[int, float]], asset: str) -> MinuteBars:
    """Replay real OKX closes through the live sampler (one late sample per minute)."""
    clock, prices = SimClock(), Prices()
    bars = MinuteBars(_settings(), clock, prices)
    bars.track([asset])
    for ts, close in rows:
        prices.p[asset] = close
        clock.set(wall=ts + 58)
        bars.sample()
    prices.p[asset] = None
    clock.set(wall=rows[-1][0] + 61)
    bars.sample()
    return bars


def test_erratic_real_algo_pump_is_suspended():
    rows = json.loads((FIX / "okx_ALGO-USDT_1m_pump.json").read_text(encoding="utf-8"))["rows"]
    bars = _bars_from_rows([(int(t) // 1000, float(c)) for t, c in rows], "ALGO")
    res = bars.erratic("ALGO")
    assert res["suspended"] is True
    assert res["bars_1pct"] >= 2 and res["max_5m_pct"] >= 3.0
    assert res["max_1m_pct"] == pytest.approx(4.51, abs=0.01)  # the measured 4.5 % bar


def test_erratic_real_calm_btc_is_not_suspended():
    page = json.loads((FIX / "okx_candles_BTC-USDT_1m_page.json").read_text(encoding="utf-8"))
    closes = parse_okx_candles(page["data"], 60, now_wall=2e9)
    bars = _bars_from_rows(sorted(closes.items()), "BTC")
    res = bars.erratic("BTC")
    assert res["suspended"] is False and res["available"] is True
    assert res["bars_1pct"] == 0 and res["max_5m_pct"] < 1.0


# --------------------------------------------------------------------------- #
# OKX parsing & bootstrap
# --------------------------------------------------------------------------- #
def test_parse_okx_candles_keeps_only_closed_bars():
    sample = json.loads((FIX / "okx_candles_BTC-USDT_1m_sample.json").read_text(encoding="utf-8"))
    closes = parse_okx_candles(sample["data"], 60, now_wall=2e9)
    assert closes == {1791465240: 82482.8, 1791465180: 82500.2}  # the open bar (confirm 0) is dropped
    rows = [["1791465300000", "1", "1", "1", "2"], ["bad"], ["1791465360000", "1", "1", "1", "x"]]
    assert parse_okx_candles(rows, 60, now_wall=1791465360.0) == {1791465300: 2.0}
    assert parse_okx_candles(rows, 60, now_wall=1791465359.0) == {}  # no confirm field: must have ended
    assert parse_okx_candles("nope", 60, now_wall=0) == {}


async def test_bootstrap_from_a_real_okx_page():
    page = json.loads((FIX / "okx_candles_BTC-USDT_1m_page.json").read_text(encoding="utf-8"))
    rows = list(reversed(page["data"]))
    last_ts = int(rows[-1][0]) // 1000  # the open bar
    clock = SimClock(wall=last_ts + 30)
    http = FakeOkx({("BTC-USDT", "1m"): rows})
    bars = MinuteBars(_settings(), clock, lambda a: 82481.0, http)
    n = await bars.bootstrap(["BTC"], limit=300)
    assert n == 299  # the still-open bar is not a close
    assert bars.closes("BTC")[-1] == 82482.8
    assert http.calls[0][0] == OKX_CANDLES_URL
    assert http.calls[0][1] == {"instId": "BTC-USDT", "bar": "1m", "limit": "300"}
    assert "BTC" in bars.tracked()
    assert bars.status.ok is True
    calls = len(http.calls)
    assert await bars.bootstrap(["BTC"], limit=300) == 0  # already covered: no new request
    assert len(http.calls) == calls


async def test_bootstrap_pages_back_into_history_candles():
    now_min = T0 + 600 * 60
    rows = [_okx_row(now_min - (600 - i) * 60, 100.0 + i * 0.01) for i in range(600)]
    clock = SimClock(wall=now_min + 5)
    http = FakeOkx({("ETH-USDT", "1m"): rows}, recent=250)
    bars = MinuteBars(_settings(), clock, lambda a: None, http)
    n = await bars.bootstrap(["ETH"], limit=450)
    assert n == 450
    urls = [u for u, _ in http.calls]
    assert urls[0] == OKX_CANDLES_URL and OKX_HISTORY_CANDLES_URL in urls
    afters = [int(p["after"]) for _, p in http.calls if "after" in p]
    assert afters == sorted(afters, reverse=True)  # always paging further back
    closes = bars.closes("ETH")
    assert len(closes) == 450 and closes[-1] == pytest.approx(100.0 + 599 * 0.01)
    rets = bars.returns("ETH", 450)
    assert len(rets) == 449


async def test_bootstrap_never_overwrites_own_minutes_and_checks_the_quote():
    clock, prices = SimClock(), Prices()
    rows = [_okx_row(T0 + i * 60, 100.0) for i in range(10)]
    http = FakeOkx({("SOL-USDT", "1m"): rows})
    bars = MinuteBars(_settings(), clock, prices, http)
    bars.track(["SOL"])
    _feed(bars, clock, prices, {"SOL": [None] * 8 + [100.2, 100.1]})  # own minutes 8 and 9
    n = await bars.bootstrap(["SOL"], limit=20)
    assert n == 8
    assert bars.closes("SOL")[-2:] == [100.2, 100.1]

    # price_fn quotes another currency (e.g. EUR) -> the OKX USDT history is refused
    clock2 = SimClock(wall=T0 + 10 * 60 + 5)
    eur = MinuteBars(_settings(), clock2, lambda a: 92.0, FakeOkx({("SOL-USDT", "1m"): rows}))
    assert await eur.bootstrap(["SOL"], limit=20) == 0
    assert eur.closes("SOL") == [] and eur.counters["bootstrap_rejected"] == 1


async def test_bootstrap_unknown_instrument_is_skipped_and_others_continue():
    rows = [_okx_row(T0 + i * 60, 5.0) for i in range(5)]
    clock = SimClock(wall=T0 + 5 * 60 + 1)
    bars = MinuteBars(_settings(), clock, lambda a: None, FakeOkx({("ADA-USDT", "1m"): rows}))
    assert await bars.bootstrap(["QNT", "ADA"], limit=5) == 5
    assert bars.unavailable == {"QNT": "OKX: нет инструмента"}
    assert bars.closes("QNT") == [] and len(bars.closes("ADA")) == 5


async def test_bootstrap_rate_limit_stops_and_backs_off():
    clock = SimClock(wall=T0)
    http = FakeOkx()
    http.override["BTC-USDT"] = HttpResponse(429, "{}", {"retry-after": "30"})
    bars = MinuteBars(_settings(), clock, lambda a: None, http)
    assert await bars.bootstrap(["BTC", "ETH"]) == 0
    assert len(http.calls) == 1 and bars.status.ok is False and "429" in bars.status.last_error
    assert await bars.bootstrap(["ETH"]) == 0
    assert len(http.calls) == 1  # still inside the Retry-After window
    clock.advance(31)
    await bars.bootstrap(["ETH"])
    assert len(http.calls) == 2


async def test_okx_error_code_and_bad_json_are_source_errors():
    clock = SimClock(wall=T0)
    http = FakeOkx()
    http.override["BTC-USDT"] = HttpResponse(200, "<html>Request Denied</html>")
    http.override["ETH-USDT"] = HttpResponse(200, json.dumps({"code": "50011", "msg": "Too Many Requests", "data": []}))
    bars = MinuteBars(_settings(), clock, lambda a: None, http)
    assert await bars.bootstrap(["BTC"]) == 0
    assert bars.status.ok is False and "bad JSON" in bars.status.last_error
    assert await bars.bootstrap(["ETH", "SOL"]) == 0
    assert [p["instId"] for _, p in http.calls] == ["BTC-USDT", "ETH-USDT"]  # 50011 = rate limit: stop


async def test_requests_are_paced(monkeypatch):
    slept: list[float] = []

    async def fake_sleep(s):
        slept.append(s)

    monkeypatch.setattr(bars_mod, "_sleep", fake_sleep)
    rows = {(f"{a}-USDT", "1m"): [_okx_row(T0 + i * 60, 1.0) for i in range(3)] for a in ("A", "B", "C")}
    clock = SimClock(wall=T0 + 3 * 60 + 1)
    bars = MinuteBars(_settings(bars_request_interval_s=0.25), clock, lambda a: None, FakeOkx(rows))
    assert await bars.bootstrap(["A", "B", "C"], limit=3) == 9
    assert slept == [0.25, 0.25]  # no wait before the first request
    assert MinuteBars(SimpleNamespace(), clock, lambda a: None)._cfg("bars_request_interval_s", 0.25) == 0.25


async def test_no_http_means_no_bootstrap_and_no_stability():
    bars = MinuteBars(SimpleNamespace(), SimClock(), lambda a: 1.0)
    assert await bars.bootstrap(["BTC"]) == 0
    assert await bars.refresh_stability(["BTC"]) == 0
    assert bars.stability("BTC") is None
    assert bars.status.enabled is False
    snap = bars.snapshot()
    assert snap["assets"]["BTC"]["tracked"] is True and snap["status"]["name"] == "okx_candles"


# --------------------------------------------------------------------------- #
# correlation / beta helpers
# --------------------------------------------------------------------------- #
def test_correlation_and_beta_align_by_epoch():
    btc = [(T0 + i * 3600, math.sin(i / 3.0)) for i in range(100)]
    alt = [(t, 2.0 * r + 0.1) for t, r in btc]
    assert correlation(alt, btc) == pytest.approx(1.0)
    assert beta(alt, btc) == pytest.approx(2.0)
    inverse = [(t, -0.5 * r) for t, r in btc]
    assert correlation(inverse, btc) == pytest.approx(-1.0)
    assert beta(inverse, btc) == pytest.approx(-0.5)
    # order does not matter, only the epoch keys
    assert correlation(list(reversed(alt)), btc) == pytest.approx(1.0)
    # shifted by one hour: only 99 common keys, still enough; with min_n=100 not
    shifted = [(t + 3600, r) for t, r in alt]
    assert correlation(shifted, btc, min_n=100) is None
    assert correlation(shifted, btc, min_n=50) is not None
    assert correlation(btc[:10], btc[:10]) is None  # fewer than min_n=30
    flat = [(t, 0.0) for t, _ in btc]
    assert correlation(flat, btc) is None and beta(alt, flat) is None
    assert correlation([(T0, float("nan"))] * 40, btc) is None


def test_bar_returns_only_between_consecutive_bars():
    closes = {0: 100.0, 3600: 110.0, 10800: 121.0}
    assert bar_returns(closes, 3600) == [(3600, pytest.approx(10.0))]


def test_max_window_move_pct():
    closes = {0: 100.0, 3600: 150.0, 7200: 75.0}
    assert max_window_move_pct(closes, 7 * 86400) == pytest.approx(50.0)
    far = {0: 100.0, 8 * 86400: 300.0, 8 * 86400 + 3600: 297.0}
    assert max_window_move_pct(far, 7 * 86400) == pytest.approx(1.0)  # 100 -> 300 is outside 7 days
    assert max_window_move_pct({0: 1.0}, 3600) is None


def test_stability_metrics_needs_candles():
    assert stability_metrics({}, {0: 1.0}) is None
    assert stability_metrics({0: 1.0}, {0: 1.0}) is None
    m = stability_metrics({0: 1.0, 3600: 1.1}, {0: 1.0, 3600: 1.0}, min_n=48)
    assert m["btc_corr"] is None and m["beta"] is None and m["hour_jumps"] is None and m["hour_moves"] == 1


# --------------------------------------------------------------------------- #
# stability (OKX 1H, 30 days)
# --------------------------------------------------------------------------- #
def _hourly_http() -> tuple[FakeOkx, int]:
    raw = json.loads((FIX / "okx_1H_closes_30d.json").read_text(encoding="utf-8"))["series"]
    series = {(f"{a}-USDT", "1H"): [_okx_row(int(ts) // 1000, c, conf) for ts, c, conf in rows] for a, rows in raw.items()}
    last_ts = int(raw["BTC"][-1][0]) // 1000
    return FakeOkx(series), last_ts


async def test_refresh_stability_on_real_okx_hourly_candles():
    http, last_ts = _hourly_http()
    clock = SimClock(wall=last_ts + 1800)
    bars = MinuteBars(_settings(), clock, lambda a: None, http)
    assert await bars.refresh_stability(["ETH", "NEAR", "BTC"]) == 3
    eth, near, btc = bars.stability("eth"), bars.stability("NEAR"), bars.stability("BTC")
    # research anchors (REPORT_pool-leverage): ETH hourly corr 0.85, 0 own 3 % jumps;
    # NEAR corr 0.37, ~40 own jumps, ~99 % max 7-day move
    assert eth["btc_corr"] == pytest.approx(0.85, abs=0.02) and eth["hour_jumps"] == 0
    assert eth["beta"] == pytest.approx(1.10, abs=0.05)
    assert near["btc_corr"] == pytest.approx(0.37, abs=0.02) and near["hour_jumps"] >= 35
    assert near["max_7d_move_pct"] == pytest.approx(99.1, abs=1.0)
    assert eth["days"] == pytest.approx(29.96, abs=0.05) and eth["days"] >= 20
    assert btc["btc_corr"] == pytest.approx(1.0) and btc["hour_jumps"] == 0
    assert set(eth) >= {"days", "btc_corr", "hour_jumps", "max_7d_move_pct", "beta", "ts"}
    assert eth["ts"] == clock.wall()
    # BTC is fetched once (the reference), every page within OKX's page limits
    per_asset: dict[str, int] = {}
    for url, p in http.calls:
        per_asset[p["instId"]] = per_asset.get(p["instId"], 0) + 1
        assert int(p["limit"]) <= (300 if url == OKX_CANDLES_URL else 100)
    assert set(per_asset) == {"BTC-USDT", "ETH-USDT", "NEAR-USDT"}
    assert per_asset["BTC-USDT"] == per_asset["ETH-USDT"] == per_asset["NEAR-USDT"] <= 6


async def test_stability_is_cached_refreshed_and_expires():
    http, last_ts = _hourly_http()
    clock = SimClock(wall=last_ts + 1800)
    bars = MinuteBars(_settings(), clock, lambda a: None, http)
    await bars.refresh_stability(["ETH"])
    calls = len(http.calls)
    per_asset = calls // 2  # BTC + ETH, same paging
    clock.advance(3600)
    assert await bars.refresh_stability(["ETH"]) == 0 and len(http.calls) == calls  # cached < 24 h
    assert await bars.refresh_stability(["ETH", "NEAR"]) == 1  # NEAR new, BTC reference reused
    assert len(http.calls) == calls + per_asset
    assert {p["instId"] for _, p in http.calls[calls:]} == {"NEAR-USDT"}
    clock.advance(24 * 3600)
    assert await bars.refresh_stability(["ETH"]) == 1  # due again (BTC refetched too)
    clock.advance(73 * 3600)
    assert bars.stability("ETH") is None  # too old to trust
    assert bars.stability("XRP") is None


async def test_stability_without_btc_or_instrument_is_none():
    http, last_ts = _hourly_http()
    clock = SimClock(wall=last_ts + 1800)
    http.override["BTC-USDT"] = HttpResponse(503, "down")
    bars = MinuteBars(_settings(), clock, lambda a: None, http)
    assert await bars.refresh_stability(["ETH"]) == 0
    assert bars.stability("ETH") is None and bars.status.ok is False
    del http.override["BTC-USDT"]
    assert await bars.refresh_stability(["ETH", "TON"]) == 1
    assert bars.stability("TON") is None and bars.unavailable["TON"] == "OKX: нет инструмента"
    assert bars.snapshot()["stability"]["ETH"]["btc_corr"] is not None
