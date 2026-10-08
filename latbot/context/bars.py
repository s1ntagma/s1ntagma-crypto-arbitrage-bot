"""Per-asset minute closes, regime statistics, the live erratic gate and the
30-day hourly stability measurements (ARCHITECTURE P3.6 + P3.12).

* Live minutes come from OUR data: ``price_fn(asset)`` (median of fresh mids
  across signal venues, supplied by the integration) is sampled every ~5 s;
  the last fresh sample of a minute is its close.  No fresh price late in the
  minute -> the minute is NOT written (no gap filling, no interpolation).
* ``bootstrap()`` fills history from OKX spot ``{A}-USDT`` 1m candles (closed
  candles only, paced requests, never overwrites our own minutes, refused when
  the OKX quote does not match ``price_fn``'s quote).
* ``erratic()`` is the live gate: >= 2 one-minute bars beyond
  max(1 %, 8 * median sigma_1m) or a 5-minute move >= 3 % in the last hour.
* ``refresh_stability()`` / ``stability()`` measure the asset pool's stability
  inputs from OKX 1H candles over 30 days (correlation and beta to BTC,
  residual hourly jumps, max 7-day move, days of history).

Minute / hour keys are epoch SECONDS of the bar start (multiples of 60 / 3600),
the same convention as exchange candles; ``correlation``/``beta`` align two
``(epoch, pct)`` series by equal keys.
"""
from __future__ import annotations

import asyncio
import logging
import math
import statistics
from collections import deque
from collections.abc import Callable, Iterable
from typing import Any

from .base import HttpGetter, RateLimitedSource, SourceError, SourceStatus, check_response

log = logging.getLogger(__name__)

OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
OKX_HISTORY_CANDLES_URL = "https://www.okx.com/api/v5/market/history-candles"
OKX_CANDLES_MAX_LIMIT = 300  # /market/candles: latest 1440 bars, 300 per page
OKX_HISTORY_MAX_LIMIT = 100  # /market/history-candles: older bars, 100 per page
OKX_RATE_LIMIT_CODE = "50011"

MINUTE_S = 60
HOUR_S = 3600
WEEK_S = 7 * 86400

_sleep = asyncio.sleep  # request pacing (tests replace this hook, not asyncio.sleep)


# --------------------------------------------------------------------------- #
# pure helpers
# --------------------------------------------------------------------------- #
def _finite(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _price(v: Any) -> float | None:
    """A usable positive finite price, else None."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) and f > 0 else None


def _pct(new: float, old: float) -> float:
    return (new / old - 1.0) * 100.0


def _norm_asset(asset: Any) -> str:
    return str(asset or "").strip().upper()


def _series(a: Iterable[tuple[int, float]]) -> dict[int, float]:
    out: dict[int, float] = {}
    for item in a:
        try:
            k, v = item
        except (TypeError, ValueError):
            continue
        if _finite(v) and _finite(k):
            out[int(k)] = float(v)
    return out


def _aligned(a: Iterable[tuple[int, float]], b: Iterable[tuple[int, float]]) -> tuple[list[float], list[float]]:
    da, db = _series(a), _series(b)
    keys = sorted(da.keys() & db.keys())
    return [da[k] for k in keys], [db[k] for k in keys]


def correlation(a: list[tuple[int, float]], b: list[tuple[int, float]], min_n: int = 30) -> float | None:
    """Pearson correlation of two ``(epoch, pct)`` series aligned by epoch (minute or
    hour keys).  None with fewer than ``min_n`` common points or a constant series."""
    x, y = _aligned(a, b)
    n = len(x)
    if n < max(2, int(min_n)):
        return None
    mx, my = math.fsum(x) / n, math.fsum(y) / n
    sxx = math.fsum((v - mx) ** 2 for v in x)
    syy = math.fsum((v - my) ** 2 for v in y)
    if sxx <= 0.0 or syy <= 0.0:
        return None
    sxy = math.fsum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    return max(-1.0, min(1.0, sxy / math.sqrt(sxx * syy)))


def beta(asset_r: list[tuple[int, float]], btc_r: list[tuple[int, float]], min_n: int = 30) -> float | None:
    """OLS beta of the asset's returns on BTC's returns (aligned by epoch):
    cov(asset, btc) / var(btc).  None with too few points or constant BTC."""
    y, x = _aligned(asset_r, btc_r)
    n = len(x)
    if n < max(2, int(min_n)):
        return None
    mx, my = math.fsum(x) / n, math.fsum(y) / n
    sxx = math.fsum((v - mx) ** 2 for v in x)
    if sxx <= 0.0:
        return None
    sxy = math.fsum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    return sxy / sxx


def bar_returns(closes: dict[int, float], step_s: int) -> list[tuple[int, float]]:
    """Percent returns between CONSECUTIVE bars only (key difference == step_s),
    labelled with the later bar's start epoch.  Gaps produce no return."""
    out: list[tuple[int, float]] = []
    for k in sorted(closes):
        prev = closes.get(k - step_s)
        if prev is not None:
            out.append((k, _pct(closes[k], prev)))
    return out


def max_window_move_pct(closes: dict[int, float], window_s: int) -> float | None:
    """Largest |c_i / c_j - 1| (percent) over pairs with t_i - window_s <= t_j < t_i.
    O(n) with monotonic deques (window min and max)."""
    keys = sorted(closes)
    if len(keys) < 2:
        return None
    lo_q: deque[int] = deque()  # indices, increasing closes (window minimum at the left)
    hi_q: deque[int] = deque()  # indices, decreasing closes (window maximum at the left)
    best: float | None = None
    for i, k in enumerate(keys):
        while lo_q and keys[lo_q[0]] < k - window_s:
            lo_q.popleft()
        while hi_q and keys[hi_q[0]] < k - window_s:
            hi_q.popleft()
        c = closes[k]
        if lo_q:
            up = _pct(c, closes[keys[lo_q[0]]])
            down = -_pct(c, closes[keys[hi_q[0]]])
            m = max(up, down)
            best = m if best is None else max(best, m)
        while lo_q and closes[keys[lo_q[-1]]] >= c:
            lo_q.pop()
        lo_q.append(i)
        while hi_q and closes[keys[hi_q[-1]]] <= c:
            hi_q.pop()
        hi_q.append(i)
    return best


def stability_metrics(
    asset_closes: dict[int, float], btc_closes: dict[int, float], *, jump_pct: float = 3.0,
    min_n: int = 48, now_ts: float | None = None, asset: str = "",
) -> dict[str, Any] | None:
    """Stability inputs of the asset pool from hourly closes (keys = hour start epoch s).

    ``hour_jumps`` counts aligned hours with |r - beta * r_btc| >= ``jump_pct`` (the
    asset's OWN jumps, net of the market); ``hour_moves`` counts raw |r| >= jump_pct.
    Returns None without at least two candles."""
    if len(asset_closes) < 2:
        return None
    r_a = bar_returns(asset_closes, HOUR_S)
    r_b = bar_returns(btc_closes, HOUR_S)
    corr = correlation(r_a, r_b, min_n)
    b = beta(r_a, r_b, min_n)
    hour_jumps: int | None = None
    aligned_hours = 0
    if b is not None:
        ya, xb = _aligned(r_a, r_b)
        aligned_hours = len(ya)
        hour_jumps = sum(1 for ra, rb in zip(ya, xb) if abs(ra - b * rb) >= jump_pct)
    keys = sorted(asset_closes)
    return {
        "asset": asset,
        "days": round(len(keys) / 24.0, 2),
        "hours": len(keys),
        "aligned_hours": aligned_hours,
        "btc_corr": corr,
        "beta": b,
        "hour_jumps": hour_jumps,
        "hour_moves": sum(1 for _, r in r_a if abs(r) >= jump_pct),
        "jump_hour_pct": float(jump_pct),
        "max_7d_move_pct": max_window_move_pct(asset_closes, WEEK_S),
        "first_ts": float(keys[0]),
        "last_ts": float(keys[-1]),
        "source": "okx 1H",
        "ts": now_ts,
    }


def parse_okx_candles(data: Any, bar_s: int, now_wall: float) -> dict[int, float]:
    """OKX ``data`` rows ``[ts_ms, o, h, l, c, vol, volCcy, volCcyQuote, confirm]`` ->
    ``{bar_start_epoch_s: close}`` for CLOSED bars only (confirm == "1"; rows without
    the confirm field must have ended before ``now_wall``).  Bad rows are skipped."""
    out: dict[int, float] = {}
    if not isinstance(data, list):
        return out
    for row in data:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        try:
            ts = int(int(row[0]) // 1000)
        except (TypeError, ValueError):
            continue
        close = _price(row[4])
        if close is None:
            continue
        if len(row) >= 9:
            if str(row[8]) != "1":
                continue
        elif ts + bar_s > now_wall:
            continue
        out[ts] = close
    return out


# --------------------------------------------------------------------------- #
# MinuteBars
# --------------------------------------------------------------------------- #
class MinuteBars:
    """Minute closes per asset (own data + OKX bootstrap), statistics, erratic gate,
    hourly stability.  Never raises on missing data: values are None / unavailable."""

    def __init__(
        self, settings: Any, clock: Any, price_fn: Callable[[str], float | None],
        http: HttpGetter | None = None, *, keep_minutes: int = 1440,
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._price_fn = price_fn
        self._http = http
        self.keep_minutes = max(60, int(keep_minutes))
        self._tracked: dict[str, None] = {}  # ordered set
        self._closes: dict[str, dict[int, float]] = {}
        self._pending: dict[str, tuple[int, float, float]] = {}  # asset -> (minute, price, offset_s)
        self._version: dict[str, int] = {}
        self._sorted: dict[str, tuple[int, list[int]]] = {}
        self._stats_cache: dict[str, tuple[tuple[int, int], dict[str, Any]]] = {}
        self._erratic_cache: dict[str, tuple[tuple[int, int], dict[str, Any]]] = {}
        # hourly stability
        self._hourly: dict[str, tuple[float, dict[int, float]]] = {}  # "BTC" -> (fetched_mono, closes)
        self._stability: dict[str, tuple[float, dict[str, Any]]] = {}  # asset -> (computed_mono, metrics)
        self.unavailable: dict[str, str] = {}  # asset -> why OKX has no data for it
        # OKX pacing / backoff
        self._req_lock = asyncio.Lock()
        self._last_req_mono: float | None = None
        self._blocked_until_mono = 0.0
        self.status = SourceStatus(name="okx_candles", enabled=http is not None)
        if http is None:
            self.status.note = "no HTTP client: bootstrap/stability off"
        self.counters: dict[str, int] = {
            "minutes_written": 0, "minutes_skipped": 0, "bootstrap_minutes": 0, "bootstrap_rejected": 0,
            "requests": 0,
        }

    # ------------------------------------------------------------------ #
    # settings
    # ------------------------------------------------------------------ #
    def _cfg(self, name: str, default: float) -> float:
        v = getattr(self._settings, name, default)
        return float(v) if _finite(v) else float(default)

    # ------------------------------------------------------------------ #
    # tracking & live sampling
    # ------------------------------------------------------------------ #
    def track(self, assets: Iterable[str]) -> None:
        """Add assets to the sampled set (additive, idempotent)."""
        for a in assets:
            a = _norm_asset(a)
            if a:
                self._tracked.setdefault(a, None)

    def untrack(self, assets: Iterable[str]) -> None:
        """Stop sampling these assets (their stored history is kept until it ages out)."""
        for a in assets:
            a = _norm_asset(a)
            self._tracked.pop(a, None)
            self._pending.pop(a, None)

    def tracked(self) -> list[str]:
        return list(self._tracked)

    def _safe_price(self, asset: str) -> float | None:
        try:
            return _price(self._price_fn(asset))
        except Exception as e:  # noqa: BLE001 - a broken price source must not stop sampling
            log.debug("bars: price_fn(%s) failed: %s", asset, e)
            return None

    def sample(self, now_wall: float | None = None) -> None:
        """Called every ~5 s: the latest fresh price becomes the current minute's
        close candidate; a finished minute is committed when the next minute starts."""
        now = self._clock.wall() if now_wall is None else float(now_wall)
        minute = int(now // MINUTE_S) * MINUTE_S
        offset = now - minute
        for asset in list(self._tracked):
            pend = self._pending.get(asset)
            if pend is not None and pend[0] < minute:
                del self._pending[asset]
                self._commit(asset, pend, minute)
                pend = None
            if pend is not None and pend[0] > minute:
                continue  # wall clock stepped back: keep the newer candidate
            price = self._safe_price(asset)
            if price is None:
                continue  # no fresh price: nothing to record for this sample
            self._pending[asset] = (minute, price, offset)

    def _commit(self, asset: str, pend: tuple[int, float, float], cur_minute: int) -> None:
        minute, price, offset = pend
        window = max(1.0, min(60.0, self._cfg("bars_close_window_s", 15.0)))
        if offset < MINUTE_S - window:
            # the last fresh price came too early in the minute -> its close is unknown
            self.counters["minutes_skipped"] += 1
            log.debug("bars: %s minute %d not written (last fresh sample at +%.1f s)", asset, minute, offset)
            return
        closes = self._closes.setdefault(asset, {})
        closes[minute] = price  # our own observation wins over a bootstrap value
        self.counters["minutes_written"] += 1
        self._trim(asset, cur_minute)
        self._bump(asset)

    def _trim(self, asset: str, cur_minute: int) -> None:
        closes = self._closes.get(asset)
        if not closes:
            return
        lo = cur_minute - self.keep_minutes * MINUTE_S
        for k in [k for k in closes if k < lo]:
            del closes[k]

    def _bump(self, asset: str) -> None:
        self._version[asset] = self._version.get(asset, 0) + 1

    def _cur_minute(self) -> int:
        return int(self._clock.wall() // MINUTE_S) * MINUTE_S

    def _keys(self, asset: str) -> list[int]:
        ver = self._version.get(asset, 0)
        cached = self._sorted.get(asset)
        if cached is not None and cached[0] == ver:
            return cached[1]
        keys = sorted(self._closes.get(asset, {}))
        self._sorted[asset] = (ver, keys)
        return keys

    # ------------------------------------------------------------------ #
    # read API
    # ------------------------------------------------------------------ #
    def closes(self, asset: str, minutes: int | None = None) -> list[float]:
        """Completed minute closes, oldest -> newest.  ``minutes`` = only the minutes
        of the last N completed minutes by the clock (missing minutes stay missing)."""
        asset = _norm_asset(asset)
        closes = self._closes.get(asset)
        if not closes:
            return []
        cur = self._cur_minute()
        lo = None if minutes is None else cur - int(minutes) * MINUTE_S
        return [closes[k] for k in self._keys(asset) if k < cur and (lo is None or k >= lo)]

    def returns(self, asset: str, minutes: int) -> list[tuple[int, float]]:
        """1-minute percent returns ``(minute_epoch_s, pct)`` for the last N completed
        minutes; a minute without its predecessor yields no return (no gap bridging)."""
        asset = _norm_asset(asset)
        cur = self._cur_minute()
        return self._returns_window(asset, cur - int(minutes) * MINUTE_S, cur)

    def _returns_window(self, asset: str, lo: int, hi: int) -> list[tuple[int, float]]:
        closes = self._closes.get(asset)
        if not closes:
            return []
        out: list[tuple[int, float]] = []
        for k in self._keys(asset):
            if k < lo:
                continue
            if k >= hi:
                break
            prev = closes.get(k - MINUTE_S)
            if prev is not None:
                out.append((k, _pct(closes[k], prev)))
        return out

    def _hour_sigmas(self, asset: str) -> list[float]:
        """Std-dev of 1m returns per clock hour over the stored history (hours with
        at least ``bars_min_vol_returns`` returns)."""
        min_n = max(2, int(self._cfg("bars_min_vol_returns", 30)))
        groups: dict[int, list[float]] = {}
        for k, r in self._returns_window(asset, -1, 1 << 62):
            groups.setdefault(k // HOUR_S, []).append(r)
        return [statistics.stdev(v) for _, v in sorted(groups.items()) if len(v) >= min_n]

    def stats(self, asset: str) -> dict[str, Any]:
        """Regime numbers (percent values; None = no data):
        minutes, ret_5m_pct, ret_15m_pct, ret_1h_pct, vol_1m_pct (sigma of 1m returns,
        last 60 min), vol_pctile (0..1 vs hourly sigmas of the stored history),
        tail_1m_pct (p99 of |1m return|), shock (+1 / -1 / 0: sudden move direction)."""
        asset = _norm_asset(asset)
        cur = self._cur_minute()
        key = (self._version.get(asset, 0), cur)
        cached = self._stats_cache.get(asset)
        if cached is not None and cached[0] == key:
            return dict(cached[1])
        res = self._compute_stats(asset, cur)
        self._stats_cache[asset] = (key, res)
        return dict(res)

    def _compute_stats(self, asset: str, cur: int) -> dict[str, Any]:
        closes = self._closes.get(asset, {})
        keys = self._keys(asset)
        max_lag = max(0, int(self._cfg("bars_max_lag_min", 2)))
        last = keys[-1] if keys else None
        fresh = last is not None and cur - MINUTE_S * (1 + max_lag) <= last < cur
        res: dict[str, Any] = {
            "asset": asset, "minutes": len(keys), "minutes_1h": 0, "fresh": fresh,
            "last_minute_ts": float(last) if last is not None else None,
            "last_close": closes.get(last) if last is not None else None,
            "ret_5m_pct": None, "ret_15m_pct": None, "ret_1h_pct": None,
            "vol_1m_pct": None, "vol_pctile": None, "sigma_1m_median_pct": None, "tail_1m_pct": None,
            "shock": 0, "shock_pct": None,
        }
        if last is None:
            return res
        res["minutes_1h"] = sum(1 for k in keys if cur - HOUR_S <= k < cur)
        if fresh:
            for n, name in ((5, "ret_5m_pct"), (15, "ret_15m_pct"), (60, "ret_1h_pct")):
                base = closes.get(last - n * MINUTE_S)
                if base is not None:
                    res[name] = _pct(closes[last], base)
        min_n = max(2, int(self._cfg("bars_min_vol_returns", 30)))
        r60 = [r for _, r in self._returns_window(asset, cur - HOUR_S, cur)]
        vol = statistics.stdev(r60) if len(r60) >= min_n else None
        res["vol_1m_pct"] = vol
        sigmas = self._hour_sigmas(asset)
        sigma_med = statistics.median(sigmas) if sigmas else None
        res["sigma_1m_median_pct"] = sigma_med
        if vol is not None and len(sigmas) >= max(1, int(self._cfg("bars_vol_pctile_min_hours", 6))):
            res["vol_pctile"] = sum(1 for s in sigmas if s <= vol) / len(sigmas)
        all_abs = [abs(r) for _, r in self._returns_window(asset, -1, cur)]
        if len(all_abs) >= max(2, int(self._cfg("bars_tail_min_returns", 120))):
            res["tail_1m_pct"] = statistics.quantiles(all_abs, n=100, method="inclusive")[98]
        if fresh:
            shock, shock_pct = self._shock(asset, closes, last, cur, sigma_med)
            res["shock"], res["shock_pct"] = shock, shock_pct
        return res

    def _shock(self, asset: str, closes: dict[int, float], last: int, cur: int,
               sigma_med: float | None) -> tuple[int, float | None]:
        k = self._cfg("bars_shock_sigma", 6.0)
        thr1 = self._cfg("bars_shock_min_1m_pct", 0.5)
        thr5 = self._cfg("bars_shock_min_5m_pct", 1.0)
        if sigma_med is not None:
            thr1 = max(thr1, k * sigma_med)
            thr5 = max(thr5, k * sigma_med * math.sqrt(5.0))
        recent = self._returns_window(asset, cur - 3 * MINUTE_S, cur)
        best = max(recent, key=lambda kr: abs(kr[1]), default=None)
        if best is not None and abs(best[1]) >= thr1:
            return (1 if best[1] > 0 else -1), best[1]
        base = closes.get(last - 5 * MINUTE_S)
        if base is not None:
            r5 = _pct(closes[last], base)
            if abs(r5) >= thr5:
                return (1 if r5 > 0 else -1), r5
        return 0, None

    # ------------------------------------------------------------------ #
    # live erratic gate (P3.12)
    # ------------------------------------------------------------------ #
    def erratic(self, asset: str) -> dict[str, Any]:
        """Suspend new signals on an erratic asset: in the last ``erratic_window_min``
        minutes >= ``erratic_min_bars`` 1m bars with |r| >= max(erratic_min_1m_pct,
        erratic_sigma_mult * median sigma_1m) OR max |r5m| >= erratic_max_5m_pct.
        ``available`` is False when the window has too few minutes to judge calm."""
        asset = _norm_asset(asset)
        cur = self._cur_minute()
        key = (self._version.get(asset, 0), cur)
        cached = self._erratic_cache.get(asset)
        if cached is not None and cached[0] == key:
            return dict(cached[1])
        res = self._compute_erratic(asset, cur)
        self._erratic_cache[asset] = (key, res)
        return dict(res)

    def _compute_erratic(self, asset: str, cur: int) -> dict[str, Any]:
        win = max(5, int(self._cfg("erratic_window_min", 60)))
        floor_1m = self._cfg("erratic_min_1m_pct", 1.0)
        mult = self._cfg("erratic_sigma_mult", 8.0)
        min_bars = max(1, int(self._cfg("erratic_min_bars", 2)))
        lim_5m = self._cfg("erratic_max_5m_pct", 3.0)
        min_minutes = max(1, int(self._cfg("erratic_min_minutes", 30)))
        lo = cur - win * MINUTE_S
        closes = self._closes.get(asset, {})
        sigmas = self._hour_sigmas(asset) if closes else []
        sigma_med = statistics.median(sigmas) if sigmas else None
        thr = max(floor_1m, mult * sigma_med) if sigma_med is not None else floor_1m
        rets = self._returns_window(asset, lo, cur)
        big = [r for _, r in rets if abs(r) >= thr]
        minutes_in = [k for k in self._keys(asset) if lo <= k < cur]
        moves_5m = [abs(_pct(closes[k], closes[k - 5 * MINUTE_S])) for k in minutes_in if (k - 5 * MINUTE_S) in closes]
        max5 = max(moves_5m) if moves_5m else None
        max1 = max((abs(r) for _, r in rets), default=None)
        reasons: list[str] = []
        if len(big) >= min_bars:
            reasons.append(f"{len(big)} мин. баров с |r| ≥ {thr:.2f} % за {win} мин")
        if max5 is not None and max5 >= lim_5m:
            reasons.append(f"движение {max5:.2f} % за 5 мин (порог {lim_5m:.1f} %)")
        suspended = bool(reasons)
        available = suspended or len(minutes_in) >= min_minutes
        if suspended:
            reason = "хаотичное движение: " + "; ".join(reasons)
        elif not available:
            reason = f"нет данных: {len(minutes_in)}/{win} мин"
        else:
            m5 = f"{max5:.2f} %" if max5 is not None else "н/д"
            reason = f"спокойно: {len(big)} бар(ов) ≥ {thr:.2f} %, макс. за 5 мин {m5}"
        return {
            "asset": asset, "suspended": suspended, "available": available, "reason": reason,
            "bars_1pct": len(big), "threshold_1m_pct": thr, "max_1m_pct": max1, "max_5m_pct": max5,
            "minutes": len(minutes_in), "window_min": win, "sigma_1m_median_pct": sigma_med,
        }

    # ------------------------------------------------------------------ #
    # OKX access (paced, single place)
    # ------------------------------------------------------------------ #
    def _blocked(self) -> bool:
        return self._clock.mono() < self._blocked_until_mono

    async def _okx_get(self, url: str, params: dict[str, Any]) -> list[Any]:
        assert self._http is not None
        async with self._req_lock:
            interval = max(0.0, self._cfg("bars_request_interval_s", 0.25))
            if self._last_req_mono is not None and interval > 0:
                wait = interval - (self._clock.mono() - self._last_req_mono)
                if wait > 0:
                    await _sleep(min(wait, interval))
            self._last_req_mono = self._clock.mono()
            self.counters["requests"] += 1
            resp = await self._http.get(url, params=params, timeout_s=10.0)
        check_response(resp, "okx candles", default_retry_s=10.0)
        try:
            payload = resp.json()
        except ValueError as e:
            raise SourceError(f"okx candles: bad JSON ({e})") from e
        if not isinstance(payload, dict):
            raise SourceError("okx candles: unexpected payload")
        code = str(payload.get("code", ""))
        if code == OKX_RATE_LIMIT_CODE:
            raise RateLimitedSource(2.0, "okx candles: too many requests (50011)")
        if code != "0":
            raise SourceError(f"okx candles: code {code} {str(payload.get('msg', ''))[:80]}".strip())
        data = payload.get("data")
        if not isinstance(data, list):
            raise SourceError("okx candles: no data list")
        return data

    async def _fetch_closes(self, asset: str, bar: str, bar_s: int, n: int) -> dict[int, float]:
        """Newest ``n`` CLOSED bars of OKX ``{asset}-USDT``: /market/candles first (the
        latest 1440 bars), then /market/history-candles, paging back with ``after``."""
        n = max(1, int(n))
        out: dict[int, float] = {}
        after: int | None = None
        url = OKX_CANDLES_URL
        max_pages = n // OKX_HISTORY_MAX_LIMIT + 4
        for _ in range(max_pages):
            if len(out) >= n:
                break
            page_max = OKX_CANDLES_MAX_LIMIT if url == OKX_CANDLES_URL else OKX_HISTORY_MAX_LIMIT
            limit = min(page_max, n - len(out) + 1)  # +1: the newest row may still be open
            params: dict[str, Any] = {"instId": f"{asset}-USDT", "bar": bar, "limit": str(limit)}
            if after is not None:
                params["after"] = str(after)
            data = await self._okx_get(url, params)
            if not data:
                if url == OKX_CANDLES_URL:
                    url = OKX_HISTORY_CANDLES_URL
                    continue
                break
            out.update(parse_okx_candles(data, bar_s, self._clock.wall()))
            try:
                oldest = min(int(r[0]) for r in data if isinstance(r, (list, tuple)) and r)
            except ValueError:
                break
            if after is not None and oldest >= after:
                break  # no progress: stop instead of looping
            after = oldest
        keys = sorted(out)[-n:]
        return {k: out[k] for k in keys}

    def _on_rate_limited(self, e: RateLimitedSource) -> None:
        self._blocked_until_mono = self._clock.mono() + e.retry_after_s
        self.status.ok = False
        self.status.last_error = str(e)[:240]
        log.warning("bars: OKX rate limited, pausing %.0f s", e.retry_after_s)

    def _on_asset_error(self, asset: str, e: Exception) -> None:
        msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
        if "code 51001" in msg:  # instrument does not exist on OKX spot
            self.unavailable[asset] = "OKX: нет инструмента"
            log.info("bars: %s-USDT not on OKX spot", asset)
            return
        self.status.ok = False
        self.status.last_error = msg[:240]
        log.warning("bars: OKX candles for %s failed: %s", asset, msg[:240])

    def _on_ok(self) -> None:
        self.status.ok = True
        self.status.last_ok_ts = self._clock.wall()
        self.status.last_error = ""

    # ------------------------------------------------------------------ #
    # bootstrap (1m history)
    # ------------------------------------------------------------------ #
    async def bootstrap(self, assets: Iterable[str], *, limit: int = 300) -> int:
        """Fill up to ``limit`` past minutes per asset from OKX 1m candles (also tracks
        the assets).  Only missing minutes are filled; our own minutes win.  Assets
        already >= 90 % covered are skipped.  Returns minutes written."""
        names = [a for a in dict.fromkeys(_norm_asset(a) for a in assets) if a]
        self.track(names)
        if self._http is None or not names:
            return 0
        if self._blocked():
            log.debug("bars: bootstrap skipped (OKX backoff)")
            return 0
        limit = max(1, min(int(limit), self.keep_minutes))
        written = 0
        for asset in names:
            cur = self._cur_minute()
            have = sum(1 for k in self._closes.get(asset, {}) if cur - limit * MINUTE_S <= k < cur)
            if have >= 0.9 * limit:
                continue
            self.status.last_attempt_ts = self._clock.wall()
            try:
                rows = await self._fetch_closes(asset, "1m", MINUTE_S, limit)
            except RateLimitedSource as e:
                self._on_rate_limited(e)
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - one asset must not stop the rest
                self._on_asset_error(asset, e)
                continue
            self._on_ok()
            written += self._merge_bootstrap(asset, rows, self._cur_minute())
        self.counters["bootstrap_minutes"] += written
        return written

    def _merge_bootstrap(self, asset: str, rows: dict[int, float], cur: int) -> int:
        lo = cur - self.keep_minutes * MINUTE_S
        rows = {k: v for k, v in rows.items() if lo <= k < cur}
        if not rows:
            return 0
        closes = self._closes.setdefault(asset, {})
        # The OKX USDT quote must match price_fn's quote, otherwise the junction between
        # bootstrap and live minutes would be a fake return.  Compare common minutes
        # (median), else the newest closed OKX minute with the live price.
        max_basis = self._cfg("bars_bootstrap_max_basis_pct", 1.0)
        overlap = [k for k in rows if k in closes]
        if overlap:
            basis = statistics.median(abs(_pct(rows[k], closes[k])) for k in overlap)
            check = f"median {basis:.3f} % over {len(overlap)} common minutes"
        else:
            live = self._safe_price(asset)
            basis = abs(_pct(rows[max(rows)], live)) if live is not None else 0.0
            check = f"{basis:.3f} % vs live price"
        if basis > max_basis:
            self.counters["bootstrap_rejected"] += 1
            log.warning("bars: OKX %s-USDT bootstrap rejected: quote differs from price_fn (%s > %.2f %%)",
                        asset, check, max_basis)
            return 0
        n = 0
        for k, v in rows.items():
            if k not in closes:
                closes[k] = v
                n += 1
        if n:
            self._bump(asset)
        return n

    # ------------------------------------------------------------------ #
    # hourly stability (P3.12 pool step 4)
    # ------------------------------------------------------------------ #
    async def refresh_stability(self, assets: Iterable[str]) -> int:
        """Fetch OKX 1H candles over ``stability_days`` for assets whose measurement
        is older than ``stability_refresh_h`` (BTC is always fetched as the reference)
        and recompute their stability.  Paced; returns assets (re)measured."""
        if self._http is None:
            return 0
        names = [a for a in dict.fromkeys(_norm_asset(a) for a in assets) if a]
        now_mono = self._clock.mono()
        refresh_s = max(60.0, self._cfg("stability_refresh_h", 24.0) * 3600.0)
        due = [a for a in names if a not in self._stability or now_mono - self._stability[a][0] >= refresh_s]
        if not due or self._blocked():
            return 0
        n_hours = max(48, int(self._cfg("stability_days", 30.0) * 24))
        self.status.last_attempt_ts = self._clock.wall()

        async def hourly(asset: str) -> dict[int, float]:
            cached = self._hourly.get(asset)
            if cached is not None and now_mono - cached[0] < refresh_s:
                return cached[1]
            rows = await self._fetch_closes(asset, "1H", HOUR_S, n_hours)
            if asset == "BTC":  # the reference series is reused by every asset
                self._hourly[asset] = (self._clock.mono(), rows)
            return rows

        try:
            btc = await hourly("BTC")
        except RateLimitedSource as e:
            self._on_rate_limited(e)
            return 0
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            self._on_asset_error("BTC", e)
            return 0
        if not btc:
            self.status.ok = False
            self.status.last_error = "OKX BTC-USDT 1H: нет данных"
            return 0
        self._on_ok()
        jump = self._cfg("pool_jump_hour_pct", 3.0)
        min_n = max(2, int(self._cfg("stability_min_hours", 48)))
        done = 0
        for asset in due:
            try:
                rows = await hourly(asset)
            except RateLimitedSource as e:
                self._on_rate_limited(e)
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self._on_asset_error(asset, e)
                continue
            self._on_ok()
            metrics = stability_metrics(rows, btc, jump_pct=jump, min_n=min_n,
                                        now_ts=self._clock.wall(), asset=asset)
            if metrics is None:
                self.unavailable.setdefault(asset, "OKX: нет часовых свечей")
                continue
            self.unavailable.pop(asset, None)
            self._stability[asset] = (self._clock.mono(), metrics)
            done += 1
        self.status.items = len(self._stability)
        return done

    def stability(self, asset: str) -> dict[str, Any] | None:
        """Last stability measurement: {"days", "btc_corr", "hour_jumps", "max_7d_move_pct",
        "beta", "ts", ...} or None (never fetched, no OKX data, or older than
        ``stability_max_age_h``)."""
        asset = _norm_asset(asset)
        item = self._stability.get(asset)
        if item is None:
            return None
        max_age = self._cfg("stability_max_age_h", 72.0) * 3600.0
        if self._clock.mono() - item[0] > max_age:
            return None
        return dict(item[1])

    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict[str, Any]:
        cur = self._cur_minute()
        assets = {}
        for a in sorted(set(self._tracked) | set(self._closes)):
            keys = self._keys(a)
            assets[a] = {
                "tracked": a in self._tracked,
                "minutes": len(keys),
                "minutes_1h": sum(1 for k in keys if cur - HOUR_S <= k < cur),
                "last_minute_ts": float(keys[-1]) if keys else None,
            }
        return {
            "assets": assets,
            "stability": {a: self.stability(a) for a in sorted(self._stability)},
            "unavailable": dict(self.unavailable),
            "status": self.status.to_dict(),
            "counters": dict(self.counters),
        }


__all__ = [
    "MinuteBars", "bar_returns", "beta", "correlation", "max_window_move_pct", "parse_okx_candles",
    "stability_metrics",
]
