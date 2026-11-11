"""TradFi market context from the Yahoo Finance v8 chart API (collector ``"tradfi"``).

Measured delays (2026-10-08): CME/ICE futures and DXY ~600 s, Cboe VIX / yield
indices ~905 s.  These quotes are therefore REGIME context only, never a
trigger: every quote carries ``data_age_s = now - meta.regularMarketTime`` and
``delayed = True``; the UI labels them "задержка".

* One symbol per ``poll_once`` (round-robin, "staggered"): with 8 symbols and
  ``markets_poll_s = 60`` a request goes out every 7.5 s and every symbol is
  refreshed once a minute.  A browser User-Agent is required (Yahoo answers
  429 "Edge: Too Many Requests" to non-browser agents).
* Minute closes are merged into a per-symbol store (no gap filling) so 15 min /
  1 h changes and 1-minute returns survive session boundaries.
* Cboe yield indices (^TNX, ^FVX, ^IRX, ^TYX) are quoted in PERCENT; their
  changes are also given in basis points (``chg_15m_bp`` ...).
"""
from __future__ import annotations

import logging
import math
from typing import Any
from urllib.parse import quote as _urlquote

from .base import PollingCollector, RateLimitedSource, SourceError, check_response

log = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
# A plain browser UA (no bot suffix): Yahoo rejects non-browser agents with 429.
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0 Safari/537.36")

DEFAULT_SYMBOLS: dict[str, str] = {
    "DXY": "DX-Y.NYB", "US10Y": "^TNX", "US5Y": "^FVX", "US3M": "^IRX",
    "SPX_FUT": "ES=F", "NDX_FUT": "NQ=F", "VIX": "^VIX", "GOLD": "GC=F",
}
YIELD_SYMBOLS = frozenset({"^TNX", "^FVX", "^IRX", "^TYX"})
SOURCE = "yahoo"


def _f(v: Any) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    v = float(v)
    return v if math.isfinite(v) else None


def parse_chart(payload: Any) -> tuple[dict[str, Any], list[tuple[int, float]]]:
    """Yahoo v8 chart JSON -> (meta, [(minute_epoch, close)]) with null bars dropped.

    Several points inside one minute (the trailing live point) keep the latest close.
    Raises SourceError for ``chart.error`` / an unexpected payload."""
    if not isinstance(payload, dict) or not isinstance(payload.get("chart"), dict):
        raise SourceError("yahoo: unexpected payload")
    chart = payload["chart"]
    err = chart.get("error")
    result = chart.get("result")
    if err or not result:
        desc = (err or {}).get("description") if isinstance(err, dict) else None
        code = (err or {}).get("code") if isinstance(err, dict) else None
        raise SourceError(f"yahoo: {code or 'error'}: {desc or 'no result'}")
    r0 = result[0] if isinstance(result, list) and result and isinstance(result[0], dict) else None
    if r0 is None or not isinstance(r0.get("meta"), dict):
        raise SourceError("yahoo: no meta")
    meta = r0["meta"]
    ts_list = r0.get("timestamp") or []
    try:
        closes = (r0.get("indicators") or {}).get("quote", [{}])[0].get("close") or []
    except (AttributeError, IndexError, TypeError):
        closes = []
    by_minute: dict[int, tuple[int, float]] = {}
    for t, c in zip(ts_list, closes):
        tt, cc = _f(t), _f(c)
        if tt is None or cc is None or cc <= 0:
            continue
        minute = int(tt) - int(tt) % 60
        prev = by_minute.get(minute)
        if prev is None or int(tt) >= prev[0]:
            by_minute[minute] = (int(tt), cc)
    bars = sorted((m, v[1]) for m, v in by_minute.items())
    return meta, bars


class TradFiMarkets(PollingCollector):
    """DXY, US yields, S&P / Nasdaq futures, VIX, gold - delayed regime context."""

    name = "tradfi"

    def __init__(self, settings: Any, clock: Any, http: Any) -> None:
        symbols = getattr(settings, "markets_symbols", None)
        self.symbols: dict[str, str] = dict(symbols) if isinstance(symbols, dict) and symbols else dict(DEFAULT_SYMBOLS)
        poll = _num(getattr(settings, "markets_poll_s", 60.0), 60.0)
        super().__init__(settings, clock, http, interval_s=max(1.0, poll / max(1, len(self.symbols))),
                         enabled=bool(getattr(settings, "markets_enabled", True)))
        self.poll_s = poll
        self._rr = 0
        self._meta: dict[str, dict[str, Any]] = {}
        self._bars: dict[str, dict[int, float]] = {}
        self._fetched: dict[str, float] = {}
        self.errors: dict[str, str] = {}

    # ------------------------------------------------------------------ polling
    def _next_key(self) -> str | None:
        keys = list(self.symbols)
        if not keys:
            return None
        key = keys[self._rr % len(keys)]
        self._rr += 1
        return key

    async def poll_once(self) -> int:
        if self._http is None:
            raise SourceError("no HTTP client")
        key = self._next_key()
        if key is None:
            return 0
        sym = self.symbols[key]
        try:
            await self._fetch(key, sym)
        except RateLimitedSource as e:
            self.errors[key] = str(e)
            raise
        except Exception as e:  # noqa: BLE001 - one bad symbol must not stop the others
            msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            self.errors[key] = msg[:200]
            if not self._meta or all(k in self.errors for k in self.symbols):  # nothing healthy left
                raise SourceError(f"{sym}: {msg}") from None
            log.debug("tradfi %s (%s) failed: %s", key, sym, msg)
        return len(self._meta)

    async def _fetch(self, key: str, sym: str) -> None:
        url = YAHOO_CHART_URL.format(symbol=_urlquote(sym, safe="=.-"))
        resp = await self._http.get(url, params={"interval": "1m", "range": "1d"},
                                    headers={"User-Agent": BROWSER_UA, "Accept": "application/json"}, timeout_s=10.0)
        if resp.status >= 400 and resp.status not in (418, 429):
            try:
                payload = resp.json()
            except ValueError:
                payload = None
            if isinstance(payload, dict) and isinstance(payload.get("chart"), dict):
                parse_chart(payload)  # raises with Yahoo's own error text ("No data found, symbol may be delisted")
        check_response(resp, f"yahoo {sym}", default_retry_s=300.0)
        try:
            payload = resp.json()
        except ValueError:
            raise SourceError(f"yahoo {sym}: not JSON") from None
        meta, bars = parse_chart(payload)
        if _f(meta.get("regularMarketPrice")) is None and not bars:
            raise SourceError(f"yahoo {sym}: no price")
        self._meta[key] = meta
        self._fetched[key] = self._clock.wall()
        store = self._bars.setdefault(key, {})
        for m, c in bars:
            store[m] = c
        keep = max(60, int(_num(getattr(self._settings, "markets_keep_minutes", 1440), 1440)))
        if len(store) > keep:
            for m in sorted(store)[:-keep]:
                del store[m]
        self.errors.pop(key, None)

    # ------------------------------------------------------------------ queries
    def is_yield(self, key: str) -> bool:
        return self.symbols.get(key, "") in YIELD_SYMBOLS

    def _ref_close(self, key: str, last_minute: int, horizon_s: int) -> float | None:
        """Close at the latest minute <= last - horizon, if it is close enough to that target."""
        store = self._bars.get(key) or {}
        target = last_minute - horizon_s
        tol = max(180, int(0.2 * horizon_s))
        m = target - target % 60
        while m >= target - tol:
            c = store.get(m)
            if c is not None:
                return c
            m -= 60
        return None

    def quote(self, key: str, now_wall: float | None = None) -> dict[str, Any] | None:
        """Latest (delayed) quote; None when the symbol was never fetched successfully."""
        meta = self._meta.get(key)
        if meta is None:
            return None
        now = self._clock.wall() if now_wall is None else now_wall
        store = self._bars.get(key) or {}
        last_minute = max(store) if store else None
        last_bar = store[last_minute] if last_minute is not None else None
        last = _f(meta.get("regularMarketPrice"))
        if last is None:
            last = last_bar
        ts = _f(meta.get("regularMarketTime"))
        if ts is None and last_minute is not None:
            ts = float(last_minute)
        prev_close = _f(meta.get("previousClose")) or _f(meta.get("chartPreviousClose"))
        is_yield = self.is_yield(key)

        def chg(ref: float | None, cur: float | None) -> tuple[float | None, float | None]:
            if ref is None or cur is None or ref <= 0:
                return None, None
            return (cur / ref - 1.0) * 100.0, ((cur - ref) * 100.0 if is_yield else None)

        r15 = self._ref_close(key, last_minute, 900) if last_minute is not None else None
        r60 = self._ref_close(key, last_minute, 3600) if last_minute is not None else None
        c15, b15 = chg(r15, last_bar)
        c60, b60 = chg(r60, last_bar)
        c1d, b1d = chg(prev_close, last)
        age = (now - ts) if ts is not None else None
        stale_s = _num(getattr(self._settings, "markets_stale_s", 1800.0), 1800.0)
        reg = ((meta.get("currentTradingPeriod") or {}).get("regular") or {})
        start, end = _f(reg.get("start")), _f(reg.get("end"))
        in_session = start is not None and end is not None and start <= now <= end
        q: dict[str, Any] = {
            "key": key, "symbol": self.symbols.get(key, ""), "name": meta.get("shortName") or meta.get("longName") or "",
            "last": last, "prev_close": prev_close,
            "chg_15m_pct": c15, "chg_1h_pct": c60, "chg_1d_pct": c1d,
            "ts": ts, "data_age_s": age, "delayed": True,
            "market_open": bool(in_session and age is not None and age <= stale_s),
            "is_yield": is_yield, "fetched_ts": self._fetched.get(key), "source": SOURCE,
        }
        if is_yield:
            q.update(chg_15m_bp=b15, chg_1h_bp=b60, chg_1d_bp=b1d)
        return q

    def returns_1m(self, key: str, minutes: int) -> list[tuple[int, float]]:
        """Consecutive 1-minute returns (minute_epoch, pct) over the last ``minutes`` minutes
        of available data (the data is delayed; the window ends at the newest bar).  A minute
        whose previous minute is missing is skipped - never interpolated."""
        store = self._bars.get(key) or {}
        if not store or minutes <= 0:
            return []
        last = max(store)
        start = last - int(minutes) * 60
        out: list[tuple[int, float]] = []
        for m in sorted(store):
            if m <= start:
                continue
            p = store.get(m - 60)
            if p is not None and p > 0:
                out.append((m, (store[m] / p - 1.0) * 100.0))
        return out

    def snapshot(self, now_wall: float | None = None) -> dict[str, Any]:
        now = self._clock.wall() if now_wall is None else now_wall
        return {
            "enabled": self.status.enabled,
            "source": SOURCE,
            "delayed": True,
            "note": "Yahoo: задержка ~10 мин (фьючерсы, DXY) и ~15 мин (VIX, доходности) - только режим рынка",
            "symbols": dict(self.symbols),
            "poll_s": self.poll_s,
            "quotes": {k: self.quote(k, now) for k in self.symbols},
            "errors": dict(self.errors),
            "status": self.status.to_dict(),
        }


def _num(v: Any, default: float) -> float:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return float(default)
    return f if math.isfinite(f) else float(default)


__all__ = ["BROWSER_UA", "DEFAULT_SYMBOLS", "YAHOO_CHART_URL", "YIELD_SYMBOLS", "TradFiMarkets", "parse_chart"]
