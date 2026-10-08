"""Derivatives context: funding, open interest, liquidations, long/short (OKX public v5),
the Fear & Greed index (alternative.me) and US spot-ETF daily net flows (SoSoValue).

Everything here is a MEASUREMENT with a source and a timestamp.  Missing,
stale or unparseable data is ``None`` (never 0, never a neutral guess):

* ``Derivatives`` ("derivatives") - one ``funding-rate?instId=ANY`` and one
  ``open-interest?instType=SWAP`` call cover every OKX perpetual; funding is
  normalised per 8 h (``rate * 8 / interval_h``, interval = nextFundingTime -
  fundingTime); OI changes are measured in coins (``oiCcy``) so a price move
  never masquerades as an OI change; liquidations come from the undocumented
  REST ``liquidation-orders`` (best effort) and are reported for a window only
  when the fetched pages cover the whole window; the account long/short ratio
  comes from rubik (best effort).  Funding extremes are judged as percentiles
  of the asset's OWN sampled history (``db.save_deriv_sample`` /
  ``db.load_deriv_samples``); with fewer than ``derivs_min_samples`` samples the
  percentile is ``None`` ("история копится N/288").
* ``FearGreed`` ("fear_greed") - alternative.me, daily index polled hourly.
* ``EtfFlows`` ("etf_flows") - SoSoValue keyless v2 ``historicalInflowChart``
  (fragile, undocumented, needs HTTP POST); one source per series, a day
  without a number is "нет данных", not 0.

Contrarian funding / long-short readings are CONTEXT for the quality score,
never entry triggers.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import math
import re
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from ..logging_setup import redact
from .base import HttpGetter, HttpResponse, PollingCollector, RateLimitedSource, SourceError, check_response

log = logging.getLogger(__name__)

OKX_BASE = "https://www.okx.com"
OKX_FUNDING = "/api/v5/public/funding-rate"
OKX_OPEN_INTEREST = "/api/v5/public/open-interest"
OKX_INSTRUMENTS = "/api/v5/public/instruments"
OKX_LIQUIDATIONS = "/api/v5/public/liquidation-orders"  # undocumented since 2023-04, still answers
OKX_LS_RATIO = "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract"
OKX_RATE_LIMIT_CODES = frozenset({"50011", "50061"})

QUADRANTS = ("NEW_LONGS", "NEW_SHORTS", "SHORT_COVERING", "LONG_LIQUIDATION", "QUIET")

LIQ_PAGE_LIMIT = 100  # OKX max for liquidation-orders
LIQ_MAX_PAGES = 3  # per asset per poll (pagination with after=<oldest ts>)
_GET_TTL_S = 1.0  # get() memo: the hot path asks several times per book update
LIQ_KEEP_S = 1200.0  # keep events a bit longer than the 15 m window
LS_MAX_AGE_S = 900.0  # rubik 5 m bars: older than 3 bars -> no data
INSTRUMENTS_REFRESH_S = 3600.0
OPTIONAL_CONCURRENCY = 4
OPTIONAL_FAIL_PAUSE_S = 600.0  # every call of a best-effort part failed: stop hammering it for a while

FNG_URL = "https://api.alternative.me/fng/"

SOSOVALUE_V2_URL = "https://api.sosovalue.xyz/openapi/v2/etf/historicalInflowChart"
SOSOVALUE_V2_SOURCE = "sosovalue_v2"
DEFAULT_ETF_TYPES: dict[str, str] = {"BTC": "us-btc-spot", "ETH": "us-eth-spot"}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _num(v: Any) -> float | None:
    """Finite float from a number or numeric string; anything else -> None."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
    elif isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            x = float(s)
        except ValueError:
            return None
    else:
        return None
    return x if math.isfinite(x) else None


def _ms_to_s(v: Any) -> float | None:
    x = _num(v)
    return x / 1000.0 if x is not None and x > 0 else None


def _setting(settings: Any, name: str, default: Any, cast: Callable[[Any], Any]) -> Any:
    raw = getattr(settings, name, default)
    if raw is None:
        return default
    try:
        return cast(raw)
    except (TypeError, ValueError):
        log.warning("setting %s=%r is invalid, using default %r", name, raw, default)
        return default


def _err(e: BaseException) -> str:
    msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
    return redact(msg)[:200]


def _okx_asset(inst_id: Any) -> str | None:
    """'BTC-USDT-SWAP' -> 'BTC'; any other instrument (inverse, X-perps, futures) -> None."""
    if not isinstance(inst_id, str) or not inst_id.endswith("-USDT-SWAP"):
        return None
    asset = inst_id[: -len("-USDT-SWAP")]
    return asset if asset and "-" not in asset else None


def _okx_data(resp: HttpResponse, source: str) -> list[Any]:
    """OKX envelope {"code": "0", "data": [...], "msg": ""} -> data, or a SourceError."""
    check_response(resp, source)
    try:
        payload = resp.json()
    except ValueError:
        raise SourceError(f"{source}: response is not JSON") from None
    if not isinstance(payload, dict):
        raise SourceError(f"{source}: unexpected payload")
    code = str(payload.get("code", ""))
    if code != "0":
        msg = str(payload.get("msg") or "")[:120]
        if code in OKX_RATE_LIMIT_CODES:
            raise RateLimitedSource(2.0, f"{source}: code {code} {msg}".strip())
        raise SourceError(f"{source}: code {code} {msg}".strip())
    data = payload.get("data")
    if not isinstance(data, list):
        raise SourceError(f"{source}: no data list")
    return data


def _parse_funding(row: dict[str, Any], fetched_wall: float) -> dict[str, Any] | None:
    rate = _num(row.get("fundingRate"))
    if rate is None:
        return None
    ft = _num(row.get("fundingTime"))
    nft = _num(row.get("nextFundingTime"))
    pft = _num(row.get("prevFundingTime"))
    interval_h: float | None = None
    for later, earlier in ((nft, ft), (ft, pft)):
        if later is not None and earlier is not None and later > earlier:
            h = (later - earlier) / 3_600_000.0
            if 0.0 < h <= 24.0:
                interval_h = round(h, 4)
                break
    cap = _num(row.get("maxFundingRate"))
    floor = _num(row.get("minFundingRate"))
    at_cap: bool | None = None
    if cap is not None and floor is not None and cap > 0 > floor:
        at_cap = rate >= cap * 0.999 or rate <= floor * 0.999
    return {
        "inst_id": row.get("instId"),
        "funding_rate": rate,  # fraction per settlement interval (OKX forecast for fundingTime)
        "funding_rate_8h": rate * 8.0 / interval_h if interval_h else None,
        "funding_interval_h": interval_h,
        "next_funding_ts": ft / 1000.0 if ft else None,
        "funding_cap": cap,
        "funding_at_cap": at_cap,
        "funding_ts": _ms_to_s(row.get("ts")) or fetched_wall,
    }


def _as_dict(row: Any) -> dict[str, Any] | None:
    if isinstance(row, dict):
        return row
    try:
        return dict(row)  # sqlite3.Row and other mappings
    except (TypeError, ValueError):
        return None


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


# --------------------------------------------------------------------------- #
# Derivatives (OKX)
# --------------------------------------------------------------------------- #
class Derivatives(PollingCollector):
    """Funding / open interest / liquidations / long-short per pool asset from OKX public v5."""

    name = "derivatives"

    def __init__(
        self, settings: Any, clock: Any, http: HttpGetter | None, *,
        assets_fn: Callable[[], Iterable[str]] | None = None, db: Any = None,
    ) -> None:
        interval = _setting(settings, "derivs_poll_s", 60.0, float)
        super().__init__(settings, clock, http, interval_s=interval,
                         enabled=_setting(settings, "derivs_enabled", True, bool))
        self._assets_fn = assets_fn or (lambda: [])
        self._db = db
        self._base = str(_setting(settings, "derivs_okx_base", OKX_BASE, str)).rstrip("/")
        self.sample_every_s = max(1.0, _setting(settings, "derivs_sample_every_s", 300.0, float))
        self.min_samples = max(1, _setting(settings, "derivs_min_samples", 288, int))
        self.history_s = max(1.0, _setting(settings, "derivs_history_days", 30.0, float)) * 86400.0
        self.stale_s = max(1.0, _setting(settings, "derivs_stale_s", max(300.0, 3.0 * self.interval_s), float))
        self.quad_price_pct = abs(_setting(settings, "derivs_quadrant_price_pct", 0.5, float))
        self.quad_oi_pct = abs(_setting(settings, "derivs_quadrant_oi_pct", 2.0, float))
        self.liq_enabled = _setting(settings, "derivs_liquidations_enabled", True, bool)
        self.ls_enabled = _setting(settings, "derivs_ls_enabled", True, bool)
        self.ls_every_s = max(1.0, _setting(settings, "derivs_ls_every_s", 300.0, float))
        self.max_assets = max(0, _setting(settings, "derivs_max_assets", 40, int))
        self.liq_max_pages = LIQ_MAX_PAGES

        self._tracked: list[str] = []
        self._funding: dict[str, dict[str, Any]] = {}  # asset -> parsed row (every OKX USDT perp)
        self._oi: dict[str, dict[str, Any]] = {}
        self._inst: dict[str, dict[str, Any]] = {}  # instId -> {ct_val, ct_mult, ct_type, state}
        self._inst_next_mono = 0.0
        self._oi_hist: dict[str, deque[tuple[float, float]]] = {}  # asset -> (ts, oi_ccy), sorted
        self._fund_hist: dict[str, deque[tuple[float, float]]] = {}  # asset -> (ts, funding_rate_8h)
        self._last_sample_ts: dict[str, float] = {}
        self._liq_events: dict[str, dict[tuple[Any, ...], tuple[float, bool, float]]] = {}
        self._liq_cover_from: dict[str, float] = {}
        self._liq_last_poll: dict[str, float] = {}
        self._ls: dict[str, tuple[float, float]] = {}  # asset -> (ratio, ts)
        self._ls_next_mono: dict[str, float] = {}
        self._loaded: set[str] = set()
        self._get_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}  # asset -> (mono, get())
        self._opt_backoff_until = 0.0  # rate limited: all per-asset calls paused
        self._kind_pause_until = {"liquidations": 0.0, "long_short": 0.0}  # one part keeps failing
        self._db_warned = False
        self._parts: dict[str, dict[str, Any]] = {
            k: {"ok": None, "ts": None, "error": ""} for k in ("instruments", "funding", "open_interest")
        }
        self._parts["liquidations"] = {"ok": 0, "failed": 0, "ts": None, "error": ""}
        self._parts["long_short"] = {"ok": 0, "failed": 0, "ts": None, "error": ""}

        self._tracked = self._tracked_assets()
        for asset in self._tracked:
            self._load_history_sync(asset)

    # ------------------------------------------------------------------ public
    def get(self, asset: str) -> dict[str, Any] | None:
        """Latest fresh derivatives state of ``asset`` or None (no / stale data).

        Called on the hot path (quality score on every target-book refresh): the result is
        memoised for ``_GET_TTL_S`` (the inputs change at most every poll, >= 30 s)."""
        a = str(asset or "").strip().upper()
        mono = self._clock.mono()
        hit = self._get_cache.get(a)
        if hit is not None and 0.0 <= mono - hit[0] < _GET_TTL_S:
            return None if hit[1] is None else dict(hit[1])
        out = self._get(a)
        if len(self._get_cache) > 500:
            self._get_cache.clear()
        self._get_cache[a] = (mono, out)
        return None if out is None else dict(out)

    def _get(self, a: str) -> dict[str, Any] | None:
        now = self._clock.wall()
        f = self._fresh_funding(a, now)
        o = self._fresh_oi(a, now)
        if f is None and o is None:
            return None
        ts = max(x for x in ((f or {}).get("funding_ts"), (o or {}).get("oi_ts")) if x is not None)
        pct, n_samples = self._funding_stats(a, now, f)
        ls = self._fresh_ls(a, now)
        out: dict[str, Any] = {
            "asset": a,
            "inst_id": f"{a}-USDT-SWAP",
            "source": "okx",
            "ts": ts,
            "age_s": round(max(0.0, now - ts), 1),
            "funding_rate": f["funding_rate"] if f else None,
            "funding_rate_8h": f["funding_rate_8h"] if f else None,
            "funding_interval_h": f["funding_interval_h"] if f else None,
            "next_funding_ts": f["next_funding_ts"] if f else None,
            "funding_cap": f["funding_cap"] if f else None,
            "funding_at_cap": f["funding_at_cap"] if f else None,
            "funding_pct": pct,
            "funding_n": n_samples,
            "funding_min_n": self.min_samples,
            "oi_usd": o["oi_usd"] if o else None,
            "oi_ccy": o["oi_ccy"] if o else None,
            "oi_chg_15m_pct": self._oi_change(a, 900.0, now),
            "oi_chg_1h_pct": self._oi_change(a, 3600.0, now),
            "ls_ratio": ls[0] if ls else None,
            "ls_ts": ls[1] if ls else None,
        }
        out.update(self._liq_sums(a, now))
        return out

    def funding_percentile(self, asset: str) -> tuple[float, int] | None:
        """(percentile 0..100 of the current 8h funding within the asset's own history, n) or
        None when there is no fresh funding or fewer than ``derivs_min_samples`` samples."""
        a = str(asset or "").strip().upper()
        now = self._clock.wall()
        pct, n = self._funding_stats(a, now, self._fresh_funding(a, now))
        return None if pct is None else (pct, n)

    def _funding_stats(self, a: str, now: float, f: dict[str, Any] | None) -> tuple[float | None, int]:
        """(percentile of the current 8h funding or None, samples in the window) in ONE pass."""
        lo = now - self.history_s
        cur = None if f is None else f["funding_rate_8h"]
        tol = 1e-12
        n = below = equal = 0
        for ts, v in self._fund_hist.get(a, ()):
            if ts < lo:
                continue
            n += 1
            if cur is not None:
                if v < cur - tol:
                    below += 1
                elif v <= cur + tol:
                    equal += 1
        if cur is None or n < self.min_samples:
            return None, n
        return round(100.0 * (below + 0.5 * equal) / n, 1), n

    def funding_samples(self, asset: str) -> int:
        """Number of stored funding samples in the history window (for "история копится N/288")."""
        a = str(asset or "").strip().upper()
        return self._funding_stats(a, self._clock.wall(), None)[1]

    def funding_interval_h(self, asset: str) -> float | None:
        """Funding interval (hours) of the OKX USDT perpetual - known for EVERY listed perp, not only
        tracked assets (pool filter: exchanges move erratic coins to 4 h / 1 h)."""
        a = str(asset or "").strip().upper()
        row = self._funding.get(a)
        if row is None or self._clock.wall() - row["funding_ts"] > max(self.stale_s, 6 * 3600.0):
            return None
        return row["funding_interval_h"]

    def oi_quadrant(self, asset: str, price_change_pct: float | None) -> str | None:
        """OI/price quadrant over 15 min: price +-derivs_quadrant_price_pct, OI +-derivs_quadrant_oi_pct.
        None when the 15 m OI change or the price change is unknown."""
        p = _num(price_change_pct)
        oi = self._oi_change(str(asset or "").strip().upper(), 900.0, self._clock.wall())
        if p is None or oi is None:
            return None
        p_up, p_dn = p >= self.quad_price_pct, p <= -self.quad_price_pct
        oi_up, oi_dn = oi >= self.quad_oi_pct, oi <= -self.quad_oi_pct
        if p_up and oi_up:
            return "NEW_LONGS"
        if p_dn and oi_up:
            return "NEW_SHORTS"
        if p_up and oi_dn:
            return "SHORT_COVERING"
        if p_dn and oi_dn:
            return "LONG_LIQUIDATION"
        return "QUIET"

    def snapshot(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        missing: list[str] = []
        known = bool(self._funding or self._oi)
        for a in self._tracked:
            g = self.get(a)
            if g is None:
                unlisted = known and not self._has_swap(a)
                if unlisted:
                    missing.append(a)
                rows.append({"asset": a, "available": False,
                             "reason": "нет USDT-перпетуала на OKX" if unlisted else "нет данных"})
                continue
            g["available"] = True
            rows.append(g)
        intervals: dict[str, int] = {}
        for row in self._funding.values():
            h = row.get("funding_interval_h")
            if h:
                key = f"{h:g}"
                intervals[key] = intervals.get(key, 0) + 1
        return {
            "name": self.name,
            "source": "okx",
            "status": self.status.to_dict(),
            "assets": rows,
            "missing": missing,
            "parts": {k: dict(v) for k, v in self._parts.items()},
            "min_samples": self.min_samples,
            "sample_every_s": self.sample_every_s,
            "funding_intervals": intervals,
        }

    # ------------------------------------------------------------------ polling
    async def poll_once(self) -> int:
        try:
            return await self._poll_once()
        finally:
            self._get_cache.clear()  # new data: the hot-path memo must not serve the old state

    async def _poll_once(self) -> int:
        now_mono = self._clock.mono()
        tracked = self._tracked_assets()
        self._tracked = tracked
        for a in tracked:
            if a not in self._loaded:
                await self._load_history(a)

        errors: list[str] = []
        limited: RateLimitedSource | None = None

        if now_mono >= self._inst_next_mono:
            try:
                await self._poll_instruments()
                self._inst_next_mono = now_mono + INSTRUMENTS_REFRESH_S
                self._part_ok("instruments")
            except RateLimitedSource as e:
                limited = e
                self._part_fail("instruments", e, errors)
                self._inst_next_mono = now_mono + max(self.interval_s, e.retry_after_s)
            except Exception as e:  # noqa: BLE001 - keep the other parts alive
                self._part_fail("instruments", e, errors)
                self._inst_next_mono = now_mono + min(300.0, INSTRUMENTS_REFRESH_S)

        core_ok = 0
        for part, fn in (("funding", self._poll_funding), ("open_interest", self._poll_oi)):
            try:
                await fn()
                core_ok += 1
                self._part_ok(part)
            except RateLimitedSource as e:
                limited = limited or e
                self._part_fail(part, e, errors)
            except Exception as e:  # noqa: BLE001
                self._part_fail(part, e, errors)

        if core_ok and now_mono >= self._opt_backoff_until:
            per_asset = [a for a in tracked[: self.max_assets] if self._has_swap(a)]
            opt_limited = await self._poll_optional(per_asset)
            if opt_limited is not None:
                self._opt_backoff_until = now_mono + max(self.interval_s, opt_limited.retry_after_s)

        await self._persist_samples(tracked)
        self.status.note = self._note(errors)
        if core_ok == 0:
            if limited is not None:
                raise limited
            raise SourceError("; ".join(errors) or "OKX: no data")
        now = self._clock.wall()
        return sum(1 for a in tracked if self._fresh_funding(a, now) or self._fresh_oi(a, now))

    async def _okx(self, path: str, params: dict[str, Any], what: str) -> list[Any]:
        if self._http is None:
            raise SourceError("no HTTP client")
        resp = await self._http.get(self._base + path, params=params, timeout_s=10.0)
        return _okx_data(resp, f"OKX {what}")

    async def _poll_instruments(self) -> None:
        data = await self._okx(OKX_INSTRUMENTS, {"instType": "SWAP"}, "instruments")
        inst: dict[str, dict[str, Any]] = {}
        for row in data:
            if not isinstance(row, dict) or _okx_asset(row.get("instId")) is None:
                continue
            ct_val, ct_mult = _num(row.get("ctVal")), _num(row.get("ctMult"))
            if ct_val is None or ct_val <= 0:
                continue
            inst[row["instId"]] = {
                "ct_val": ct_val,
                "ct_mult": ct_mult if ct_mult is not None and ct_mult > 0 else 1.0,
                "ct_type": str(row.get("ctType") or ""),
                "state": str(row.get("state") or ""),
            }
        if not inst:
            raise SourceError("OKX instruments: no USDT perpetuals in the answer")
        self._inst = inst

    async def _poll_funding(self) -> None:
        data = await self._okx(OKX_FUNDING, {"instId": "ANY"}, "funding")
        fetched = self._clock.wall()
        fresh: dict[str, dict[str, Any]] = {}
        for row in data:
            if not isinstance(row, dict) or row.get("instType") not in (None, "SWAP"):
                continue
            asset = _okx_asset(row.get("instId"))
            if asset is None:
                continue
            parsed = _parse_funding(row, fetched)
            if parsed is not None:
                fresh[asset] = parsed
        if not fresh:
            raise SourceError("OKX funding: no USDT perpetuals in the answer")
        self._funding = fresh

    async def _poll_oi(self) -> None:
        data = await self._okx(OKX_OPEN_INTEREST, {"instType": "SWAP"}, "open interest")
        fetched = self._clock.wall()
        fresh: dict[str, dict[str, Any]] = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            asset = _okx_asset(row.get("instId"))
            oi_ccy = _num(row.get("oiCcy"))
            if asset is None or oi_ccy is None or oi_ccy < 0:
                continue
            oi_usd = _num(row.get("oiUsd"))
            fresh[asset] = {
                "oi_ccy": oi_ccy,
                "oi_usd": oi_usd if oi_usd is not None and oi_usd >= 0 else None,
                "oi_ts": _ms_to_s(row.get("ts")) or fetched,
            }
        if not fresh:
            raise SourceError("OKX open interest: no USDT perpetuals in the answer")
        self._oi = fresh
        keep_from = fetched - 3600.0 - self._oi_tolerance(3600.0) - 300.0
        for a in self._tracked:
            row = fresh.get(a)
            if row is None:
                continue
            hist = self._oi_hist.setdefault(a, deque())
            if not hist or row["oi_ts"] > hist[-1][0]:
                hist.append((row["oi_ts"], row["oi_ccy"]))
            while hist and hist[0][0] < keep_from:
                hist.popleft()

    async def _poll_optional(self, assets: list[str]) -> RateLimitedSource | None:
        """Liquidations + long/short per asset; failures never fail the collector."""
        sem = asyncio.Semaphore(OPTIONAL_CONCURRENCY)
        limited: list[RateLimitedSource] = []
        stats = {"liquidations": [0, 0, ""], "long_short": [0, 0, ""]}

        async def run(kind: str, asset: str, fn: Callable[[str], Any]) -> None:
            if limited:
                return
            async with sem:
                if limited:
                    return
                try:
                    await fn(asset)
                    stats[kind][0] += 1
                except RateLimitedSource as e:
                    limited.append(e)
                    stats[kind][1] += 1
                    stats[kind][2] = _err(e)
                except Exception as e:  # noqa: BLE001
                    stats[kind][1] += 1
                    stats[kind][2] = f"{asset}: {_err(e)}"

        tasks = []
        now_mono = self._clock.mono()
        if self.liq_enabled and now_mono >= self._kind_pause_until["liquidations"]:
            tasks += [run("liquidations", a, self._poll_liquidations) for a in assets]
        if self.ls_enabled and now_mono >= self._kind_pause_until["long_short"]:
            for a in assets:
                if now_mono >= self._ls_next_mono.get(a, 0.0):
                    self._ls_next_mono[a] = now_mono + self.ls_every_s
                    tasks.append(run("long_short", a, self._poll_ls))
        if tasks:
            await asyncio.gather(*tasks)
        now = self._clock.wall()
        for kind, (ok, failed, error) in stats.items():
            if ok or failed:
                self._parts[kind] = {"ok": ok, "failed": failed, "ts": now, "error": error}
            if failed and not ok and not limited:
                self._kind_pause_until[kind] = now_mono + OPTIONAL_FAIL_PAUSE_S
        return limited[0] if limited else None

    async def _poll_liquidations(self, asset: str) -> None:
        inst_id = f"{asset}-USDT-SWAP"
        meta = self._inst.get(inst_id)
        if meta is None or meta["ct_type"] != "linear":
            raise SourceError("no contract size (instruments) for liquidation USD")
        now = self._clock.wall()
        prev_poll = self._liq_last_poll.get(asset)
        prev_cover = self._liq_cover_from.get(asset)
        events: dict[tuple[Any, ...], tuple[float, bool, float]] = {}
        after: str | None = None
        complete = overlap = False
        oldest = now
        for _ in range(max(1, self.liq_max_pages)):
            params = {"instType": "SWAP", "instFamily": f"{asset}-USDT", "state": "filled",
                      "limit": str(LIQ_PAGE_LIMIT)}
            if after is not None:
                params["after"] = after
            data = await self._okx(OKX_LIQUIDATIONS, params, f"liquidations {asset}")
            details = [d for item in data if isinstance(item, dict) and item.get("instId") == inst_id
                       for d in (item.get("details") or []) if isinstance(d, dict)]
            page_ts: list[float] = []
            for d in details:
                ts_ms = _num(d.get("ts")) or _num(d.get("time"))
                if ts_ms is None or ts_ms <= 0:
                    continue
                page_ts.append(ts_ms)
                ev = self._liq_event(d, ts_ms, meta)
                if ev is not None:
                    events[ev[0]] = ev[1]
            if len(details) < LIQ_PAGE_LIMIT:
                complete = True  # OKX returned everything it keeps for this instrument
                break
            if not page_ts:
                break
            oldest_ms = min(page_ts)
            oldest = oldest_ms / 1000.0
            if prev_poll is not None and prev_cover is not None and oldest <= prev_poll:
                overlap = True  # continuous with the previous poll
                break
            if oldest <= now - 900.0:
                break  # the longest window is covered
            after = str(int(oldest_ms))

        if complete:
            cover = 0.0
        elif overlap and prev_cover is not None:
            cover = prev_cover
        else:
            cover = oldest  # a gap before the oldest fetched event: windows reaching past it are unknown
        store = self._liq_events.setdefault(asset, {})
        store.update(events)
        for k in [k for k, v in store.items() if v[0] < now - LIQ_KEEP_S]:
            del store[k]
        self._liq_cover_from[asset] = cover
        self._liq_last_poll[asset] = now

    @staticmethod
    def _liq_event(d: dict[str, Any], ts_ms: float, meta: dict[str, Any]) -> tuple[tuple[Any, ...], tuple[float, bool, float]] | None:
        side = str(d.get("side") or "").lower()
        pos = str(d.get("posSide") or "").lower()
        if side == "sell" and pos in ("long", "net", ""):
            is_long = True  # a long position was liquidated (sold)
        elif side == "buy" and pos in ("short", "net", ""):
            is_long = False
        else:
            return None
        sz, px = _num(d.get("sz")), _num(d.get("bkPx"))
        if sz is None or px is None or sz <= 0 or px <= 0:
            return None
        usd = sz * meta["ct_val"] * meta["ct_mult"] * px  # linear: contracts -> coins -> USD at bankruptcy price
        key = (int(ts_ms), side, pos, d.get("sz"), d.get("bkPx"))
        return key, (ts_ms / 1000.0, is_long, usd)

    async def _poll_ls(self, asset: str) -> None:
        data = await self._okx(OKX_LS_RATIO, {"instId": f"{asset}-USDT-SWAP", "period": "5m", "limit": "1"},
                               f"long/short {asset}")
        best: tuple[float, float] | None = None
        for row in data:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            ts, ratio = _ms_to_s(row[0]), _num(row[1])
            if ts is None or ratio is None or ratio <= 0:
                continue
            if best is None or ts > best[1]:
                best = (ratio, ts)
        if best is None:
            raise SourceError("OKX long/short: empty answer")
        self._ls[asset] = best

    # ------------------------------------------------------------------ history / persistence
    def _history_rows(self, asset: str) -> Any:
        fn = getattr(self._db, "load_deriv_samples", None) if self._db is not None else None
        if fn is None:
            return None
        return fn(asset, self._clock.wall() - self.history_s)

    def _load_history_sync(self, asset: str) -> None:
        try:
            rows = self._history_rows(asset)
        except Exception as e:  # noqa: BLE001
            log.warning("derivs: cannot load samples of %s: %s", asset, _err(e))
            self._loaded.add(asset)
            return
        if inspect.isawaitable(rows):
            close = getattr(rows, "close", None)
            if callable(close):
                close()  # async db: loaded on the first poll instead
            return
        self._ingest(asset, rows)
        self._loaded.add(asset)

    async def _load_history(self, asset: str) -> None:
        try:
            # 30 days of samples: SQLite read + JSON decoding off the event loop (the feeds keep flowing)
            rows = await _maybe_await(await asyncio.to_thread(self._history_rows, asset))
        except Exception as e:  # noqa: BLE001
            log.warning("derivs: cannot load samples of %s: %s", asset, _err(e))
            rows = None
        self._ingest(asset, rows)
        self._loaded.add(asset)

    def _ingest(self, asset: str, rows: Any) -> None:
        if not rows:
            return
        self._get_cache.pop(asset, None)
        fh: list[tuple[float, float]] = list(self._fund_hist.get(asset, ()))
        oh: list[tuple[float, float]] = list(self._oi_hist.get(asset, ()))
        last = self._last_sample_ts.get(asset)
        for r in rows:
            d = _as_dict(r)
            if d is None or str(d.get("asset") or asset).upper() != asset:
                continue
            if str(d.get("venue") or d.get("source") or "okx").lower() != "okx":
                continue  # never mix venues in one history
            ts = _num(d.get("ts"))
            if ts is None:
                continue
            f8 = _num(d.get("funding_rate_8h"))
            if f8 is not None:
                fh.append((ts, f8))
            oc = _num(d.get("oi_ccy"))
            if oc is not None and oc >= 0:
                oh.append((ts, oc))
            last = ts if last is None else max(last, ts)
        oi_from = self._clock.wall() - 3600.0 - self._oi_tolerance(3600.0) - 300.0
        self._fund_hist[asset] = deque(sorted(set(fh)))
        self._oi_hist[asset] = deque(sorted(x for x in set(oh) if x[0] >= oi_from))
        if last is not None:
            self._last_sample_ts[asset] = last

    async def _persist_samples(self, tracked: list[str]) -> None:
        now = self._clock.wall()
        save = getattr(self._db, "save_deriv_sample", None) if self._db is not None else None
        for a in tracked:
            last = self._last_sample_ts.get(a)
            if last is not None and now - last < self.sample_every_s:
                continue
            f = self._fresh_funding(a, now)
            o = self._fresh_oi(a, now)
            if f is None and o is None:
                continue
            ls = self._fresh_ls(a, now)
            sample = {
                "asset": a, "ts": now, "venue": "okx", "source": "okx", "inst_id": f"{a}-USDT-SWAP",
                "funding_rate": f["funding_rate"] if f else None,
                "funding_rate_8h": f["funding_rate_8h"] if f else None,
                "funding_interval_h": f["funding_interval_h"] if f else None,
                "oi_usd": o["oi_usd"] if o else None,
                "oi_ccy": o["oi_ccy"] if o else None,
                "ls_ratio": ls[0] if ls else None,
            }
            self._last_sample_ts[a] = now
            if sample["funding_rate_8h"] is not None:
                hist = self._fund_hist.setdefault(a, deque())
                hist.append((now, sample["funding_rate_8h"]))
                while hist and hist[0][0] < now - self.history_s:
                    hist.popleft()
            if save is not None:
                try:
                    await _maybe_await(save(dict(sample)))
                except Exception as e:  # noqa: BLE001
                    if not self._db_warned:
                        log.warning("derivs: cannot save sample: %s", _err(e))
                        self._db_warned = True

    # ------------------------------------------------------------------ internals
    def _tracked_assets(self) -> list[str]:
        try:
            raw = list(self._assets_fn() or [])
        except Exception as e:  # noqa: BLE001
            log.debug("derivs: assets_fn failed: %s", _err(e))
            raw = []
        out: list[str] = []
        seen: set[str] = set()
        for a in raw:
            if not isinstance(a, str):
                continue
            a = a.strip().upper()
            if a and a not in seen:
                seen.add(a)
                out.append(a)
        return out

    def _has_swap(self, asset: str) -> bool:
        return f"{asset}-USDT-SWAP" in self._inst or asset in self._funding or asset in self._oi

    def _fresh_funding(self, asset: str, now: float) -> dict[str, Any] | None:
        row = self._funding.get(asset)
        if row is None or now - row["funding_ts"] > self.stale_s:
            return None
        return row

    def _fresh_oi(self, asset: str, now: float) -> dict[str, Any] | None:
        row = self._oi.get(asset)
        if row is None or now - row["oi_ts"] > self.stale_s:
            return None
        return row

    def _fresh_ls(self, asset: str, now: float) -> tuple[float, float] | None:
        v = self._ls.get(asset)
        if v is None or now - v[1] > LS_MAX_AGE_S:
            return None
        return v

    def _oi_tolerance(self, window_s: float) -> float:
        return max(2.0 * self.interval_s, 0.2 * window_s)

    def _oi_change(self, asset: str, window_s: float, now: float) -> float | None:
        """OI change in percent (coins) between the latest sample and the one closest to
        ``window_s`` earlier (within tolerance); None without both."""
        hist = self._oi_hist.get(asset)
        if not hist:
            return None
        ts_c, oi_c = hist[-1]
        if now - ts_c > self.stale_s:
            return None
        target = ts_c - window_s
        tol = self._oi_tolerance(window_s)
        best: tuple[float, float] | None = None
        for ts, v in hist:
            dist = abs(ts - target)
            if dist <= tol and (best is None or dist < best[0]):
                best = (dist, v)
        if best is None or best[1] <= 0:
            return None
        return round((oi_c / best[1] - 1.0) * 100.0, 4)

    def _liq_sums(self, asset: str, now: float) -> dict[str, float | None]:
        out: dict[str, float | None] = {
            "liq_long_usd_5m": None, "liq_short_usd_5m": None,
            "liq_long_usd_15m": None, "liq_short_usd_15m": None,
        }
        last = self._liq_last_poll.get(asset)
        cover = self._liq_cover_from.get(asset)
        if last is None or cover is None or now - last > self.stale_s:
            return out
        events = self._liq_events.get(asset, {}).values()
        for window, suffix in ((300.0, "5m"), (900.0, "15m")):
            lo = now - window
            if cover > lo:
                continue  # the fetched pages do not reach back to the window start
            long_usd = sum(usd for ts, is_long, usd in events if ts >= lo and is_long)
            short_usd = sum(usd for ts, is_long, usd in events if ts >= lo and not is_long)
            out[f"liq_long_usd_{suffix}"] = round(float(long_usd), 2)
            out[f"liq_short_usd_{suffix}"] = round(float(short_usd), 2)
        return out

    def _part_ok(self, part: str) -> None:
        self._parts[part] = {"ok": True, "ts": self._clock.wall(), "error": ""}

    def _part_fail(self, part: str, e: BaseException, errors: list[str]) -> None:
        msg = _err(e)
        prev = self._parts.get(part, {})
        self._parts[part] = {"ok": False, "ts": prev.get("ts"), "error": msg}
        errors.append(f"{part}: {msg}")

    def _note(self, errors: list[str]) -> str:
        bits: list[str] = []
        if errors:
            bits.append("ошибки: " + "; ".join(errors))
        now_mono = self._clock.mono()
        for kind, label in (("liquidations", "ликвидации"), ("long_short", "L/S")):
            p = self._parts[kind]
            if p.get("failed"):
                pause = " - нет данных, пауза 10 мин" if now_mono < self._kind_pause_until[kind] else ""
                bits.append(f"{label}: {p['failed']} ошибок{pause} ({p.get('error', '')})")
        if now_mono < self._opt_backoff_until:
            bits.append("ликвидации/L/S: пауза (лимит запросов)")
        return "; ".join(bits)[:300]


# --------------------------------------------------------------------------- #
# Fear & Greed (alternative.me)
# --------------------------------------------------------------------------- #
class FearGreed(PollingCollector):
    """Crypto Fear & Greed index: value 0..100, classification, ts (start of the UTC day)."""

    name = "fear_greed"

    def __init__(self, settings: Any, clock: Any, http: HttpGetter | None) -> None:
        super().__init__(settings, clock, http, interval_s=_setting(settings, "fear_greed_poll_s", 3600.0, float),
                         enabled=_setting(settings, "fear_greed_enabled", True, bool))
        self.max_age_s = max(1.0, _setting(settings, "fear_greed_max_age_s", 172800.0, float))
        self._latest: dict[str, Any] | None = None

    async def poll_once(self) -> int:
        if self._http is None:
            raise SourceError("no HTTP client")
        resp = await self._http.get(FNG_URL, params={"limit": "2"}, timeout_s=10.0)
        check_response(resp, "alternative.me F&G")
        try:
            payload = resp.json()
        except ValueError:
            raise SourceError("alternative.me F&G: response is not JSON") from None
        if not isinstance(payload, dict):
            raise SourceError("alternative.me F&G: unexpected payload")
        meta = payload.get("metadata")
        if isinstance(meta, dict) and meta.get("error"):
            raise SourceError(f"alternative.me F&G: {str(meta['error'])[:120]}")
        data = payload.get("data")
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            raise SourceError("alternative.me F&G: empty data")
        row = data[0]
        value = _num(row.get("value"))
        ts = _num(row.get("timestamp"))
        if value is None or not 0 <= value <= 100 or value != int(value):
            raise SourceError(f"alternative.me F&G: bad value {row.get('value')!r}")
        if ts is None or ts <= 0:
            raise SourceError(f"alternative.me F&G: bad timestamp {row.get('timestamp')!r}")
        prev = None
        if len(data) > 1 and isinstance(data[1], dict):
            pv = _num(data[1].get("value"))
            prev = int(pv) if pv is not None and 0 <= pv <= 100 else None
        until = _num(row.get("time_until_update"))
        now = self._clock.wall()
        self._latest = {
            "value": int(value),
            "classification": str(row.get("value_classification") or ""),
            "ts": ts,
            "prev_value": prev,
            "next_update_ts": now + until if until is not None and until >= 0 else None,
            "fetched_ts": now,
        }
        self.status.note = ""
        return 1

    def get(self) -> dict[str, Any] | None:
        """{"value", "classification", "ts", "prev_value", "source", "age_s"} or None (no / stale data)."""
        if self._latest is None:
            return None
        now = self._clock.wall()
        age = now - self._latest["ts"]
        if age > self.max_age_s:
            return None
        return {
            "value": self._latest["value"],
            "classification": self._latest["classification"],
            "ts": self._latest["ts"],
            "prev_value": self._latest["prev_value"],
            "source": "alternative.me",
            "age_s": round(max(0.0, age), 1),
        }

    latest = get

    def snapshot(self) -> dict[str, Any]:
        return {"name": self.name, "source": "alternative.me", "status": self.status.to_dict(),
                "latest": self.get()}


# --------------------------------------------------------------------------- #
# Spot-ETF daily net flows (SoSoValue keyless v2)
# --------------------------------------------------------------------------- #
def _date_ts(date: str) -> float | None:
    try:
        return datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    except (TypeError, ValueError):
        return None


def parse_sosovalue_v2(resp: HttpResponse, source: str) -> list[dict[str, Any]]:
    """historicalInflowChart -> [{"date", "net_usd", "total_net_assets_usd", "value_traded_usd"}],
    newest first.  A day without a numeric net inflow is skipped ("нет данных", never 0)."""
    check_response(resp, source)
    try:
        payload = resp.json()
    except ValueError:
        raise SourceError(f"{source}: response is not JSON") from None
    if not isinstance(payload, dict):
        raise SourceError(f"{source}: unexpected payload")
    code = payload.get("code")
    if code not in (0, "0"):
        raise SourceError(f"{source}: code {code} {str(payload.get('msg') or '')[:120]}".strip())
    data = payload.get("data")
    if not isinstance(data, list):
        raise SourceError(f"{source}: no data list")
    by_date: dict[str, dict[str, Any]] = {}
    for r in data:
        if not isinstance(r, dict):
            continue
        date = r.get("date")
        if not isinstance(date, str) or not _DATE_RE.match(date) or _date_ts(date) is None:
            continue
        net = _num(r.get("totalNetInflow"))
        if net is None or date in by_date:
            continue
        by_date[date] = {
            "date": date,
            "net_usd": net,
            "total_net_assets_usd": _num(r.get("totalNetAssets")),
            "value_traded_usd": _num(r.get("totalValueTraded")),
        }
    return [by_date[d] for d in sorted(by_date, reverse=True)]


class EtfFlows(PollingCollector):
    """US spot-ETF daily net flows per asset from ONE source (SoSoValue keyless v2, fragile).

    The endpoint needs HTTP POST: the injected HttpGetter must provide
    ``async post(url, *, json=None, headers=None, timeout_s=10.0) -> HttpResponse``;
    without it the source reports "нет данных"."""

    name = "etf_flows"

    def __init__(self, settings: Any, clock: Any, http: HttpGetter | None) -> None:
        super().__init__(settings, clock, http, interval_s=_setting(settings, "etf_poll_s", 3600.0, float),
                         enabled=_setting(settings, "etf_enabled", True, bool))
        types = getattr(settings, "etf_types", None)
        if not isinstance(types, dict) or not types:
            types = DEFAULT_ETF_TYPES
        self._types = {str(k).strip().upper(): str(v) for k, v in types.items() if k and v}
        self.max_age_s = max(1.0, _setting(settings, "etf_max_age_days", 5.0, float)) * 86400.0
        self._series: dict[str, dict[str, Any]] = {}  # asset -> {"source", "type", "rows", "fetched_ts"}

    async def poll_once(self) -> int:
        post = getattr(self._http, "post", None) if self._http is not None else None
        if not callable(post):
            self.status.note = "нет данных: SoSoValue требует HTTP POST, а HttpGetter без post()"
            raise SourceError("SoSoValue v2 needs HTTP POST; the HttpGetter has no post()")
        ok = 0
        errors: list[str] = []
        limited: RateLimitedSource | None = None
        empty: list[str] = []
        for asset, etf_type in self._types.items():
            src = f"SoSoValue {etf_type}"
            try:
                resp = await post(SOSOVALUE_V2_URL, json={"type": etf_type},
                                  headers={"Content-Type": "application/json", "Accept": "application/json"},
                                  timeout_s=15.0)
                rows = parse_sosovalue_v2(resp, src)
            except RateLimitedSource as e:
                limited = limited or e
                errors.append(f"{asset}: {_err(e)}")
                continue
            except Exception as e:  # noqa: BLE001 - fragile source: one series failing keeps the others
                errors.append(f"{asset}: {_err(e)}")
                continue
            # the whole series is replaced by one source's answer: sources are never mixed
            self._series[asset] = {"source": SOSOVALUE_V2_SOURCE, "type": etf_type, "rows": rows,
                                   "fetched_ts": self._clock.wall()}
            ok += 1
            if not rows:
                empty.append(asset)
        notes: list[str] = []
        if errors:
            notes.append("нет данных: " + "; ".join(errors))
        if empty:
            notes.append("пустой ответ: " + ", ".join(empty))
        self.status.note = "; ".join(notes)[:300]
        if ok == 0:
            if limited is not None:
                raise limited
            raise SourceError("; ".join(errors) or "SoSoValue: no data")
        return sum(1 for a in self._series if self._series[a]["rows"])

    def latest(self, asset: str) -> dict[str, Any] | None:
        """{"date", "net_usd", "source"} of the newest reported day, or None (no data / older
        than ``etf_max_age_days``)."""
        s = self._series.get(str(asset or "").strip().upper())
        if not s:
            return None
        now = self._clock.wall()
        for row in s["rows"]:
            day = _date_ts(row["date"])
            if day is None or day > now:
                continue
            if now - day > self.max_age_s:
                return None
            return {"date": row["date"], "net_usd": row["net_usd"], "source": s["source"]}
        return None

    def history(self, asset: str, limit: int = 30) -> list[dict[str, Any]]:
        s = self._series.get(str(asset or "").strip().upper())
        if not s:
            return []
        return [dict(r, source=s["source"]) for r in s["rows"][: max(0, int(limit))]]

    def snapshot(self) -> dict[str, Any]:
        assets = {}
        for asset, etf_type in self._types.items():
            s = self._series.get(asset)
            assets[asset] = {"type": etf_type, "latest": self.latest(asset),
                             "days": len(s["rows"]) if s else 0,
                             "fetched_ts": s["fetched_ts"] if s else None}
        return {"name": self.name, "source": SOSOVALUE_V2_SOURCE, "status": self.status.to_dict(),
                "assets": assets}


__all__ = [
    "DEFAULT_ETF_TYPES", "Derivatives", "EtfFlows", "FearGreed", "QUADRANTS", "parse_sosovalue_v2",
]
