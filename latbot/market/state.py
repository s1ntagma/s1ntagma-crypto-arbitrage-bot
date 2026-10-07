"""MarketState - the in-memory view of live market data.

Keeps, per ``(exchange, asset)``:
  * the latest OrderBook / Ticker,
  * a bounded deque of recent trades,
  * a rolling mid-price series ``[(recv_mono, mid), ...]`` (oldest -> newest)
    trimmed to ``settings.price_history_s`` and throttled to one sample per
    ``settings.price_sample_min_interval_ms`` per stream,
  * the 24h quote volume reported by the last ticker,
and per exchange the latest ExchangeHealth.

Nothing here guesses: a stream without data is simply not fresh.
"""
from __future__ import annotations

import logging
from bisect import bisect_left, bisect_right
from collections import deque
from typing import Deque, Sequence

from ..clock import Clock
from ..config import Settings
from ..links import split_symbol
from ..models import ExchangeHealth, OrderBook, Ticker, Trade

log = logging.getLogger(__name__)

_TRADES_MAXLEN = 500

Key = tuple[str, str]  # (exchange, asset)


def _mono_of(point: tuple[float, float]) -> float:
    return point[0]


class MarketState:
    """Latest market data + mid-price history per (exchange, asset)."""

    def __init__(self, settings: Settings, clock: Clock) -> None:
        self._settings = settings
        self._clock = clock
        self._books: dict[Key, OrderBook] = {}
        self._tickers: dict[Key, Ticker] = {}
        self._trades: dict[Key, Deque[Trade]] = {}
        self._series: dict[Key, list[tuple[float, float]]] = {}
        self._last_append_mono: dict[Key, float] = {}  # throttle anchor per stream
        self._last_data_mono: dict[Key, float] = {}  # last book OR ticker recv_mono
        self._volume24h: dict[Key, float] = {}
        self._health: dict[str, ExchangeHealth] = {}
        self._assets: dict[str, set[str]] = {}

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def asset_of(obj: OrderBook | Ticker | Trade) -> str:
        """Canonical asset key of a market object (``asset`` or base of symbol)."""
        return obj.asset or split_symbol(obj.symbol)[0]

    def _register(self, exchange: str, asset: str) -> None:
        self._assets.setdefault(asset, set()).add(exchange)

    def _trim(self, series: list[tuple[float, float]], now_mono: float) -> None:
        cutoff = now_mono - self._settings.price_history_s
        idx = bisect_left(series, cutoff, key=_mono_of)
        if idx > 0:
            del series[:idx]

    def _append_mid(self, key: Key, mono: float, mid: float) -> None:
        """Append ``(mono, mid)`` with per-stream throttling.

        Within ``price_sample_min_interval_ms`` of the last accepted sample the
        newest point is *replaced* (so the latest price is always visible while
        the sampling cadence is preserved); a lone first point is never
        replaced so the pre-move price is not lost.  Out-of-order samples are
        ignored.
        """
        if mid <= 0:
            return
        series = self._series.setdefault(key, [])
        interval_s = max(0.0, self._settings.price_sample_min_interval_ms) / 1000.0
        if series:
            last_mono = series[-1][0]
            if mono < last_mono:
                return
            anchor = self._last_append_mono.get(key, last_mono)
            if len(series) >= 2 and (mono - anchor) < interval_s:
                series[-1] = (mono, mid)
                self._trim(series, mono)
                return
        series.append((mono, mid))
        self._last_append_mono[key] = mono
        self._trim(series, mono)

    # ------------------------------------------------------------------ #
    # ingestion
    # ------------------------------------------------------------------ #
    def on_book(self, book: OrderBook) -> None:
        """Store the latest book and append its mid to the price series."""
        asset = self.asset_of(book)
        if not asset:
            log.debug("book without asset/symbol ignored exchange=%s", book.exchange)
            return
        key = (book.exchange, asset)
        self._books[key] = book
        self._register(book.exchange, asset)
        mono = book.ts.recv_mono
        prev = self._last_data_mono.get(key)
        if prev is None or mono >= prev:
            self._last_data_mono[key] = mono
        mid = book.mid
        if mid:
            self._append_mid(key, mono, mid)

    def on_ticker(self, ticker: Ticker, *, append_mid: bool = True) -> None:
        """Store the ticker, its 24h volume and (``append_mid``) its mid.

        ``append_mid=False`` is for polled REST tickers of assets the same
        exchange already streams over WebSocket: a REST price is 1-2 s old and
        must never become the newest point of the live series (it would read
        as a move that is not there); volume, ticker and liveness still count.
        """
        asset = self.asset_of(ticker)
        if not asset:
            return
        key = (ticker.exchange, asset)
        self._tickers[key] = ticker
        self._register(ticker.exchange, asset)
        mono = ticker.ts.recv_mono
        prev = self._last_data_mono.get(key)
        if prev is None or mono >= prev:
            self._last_data_mono[key] = mono
        mid: float | None = None
        if ticker.bid and ticker.ask and ticker.bid > 0 and ticker.ask > 0:
            mid = (ticker.bid + ticker.ask) / 2.0
        elif ticker.last and ticker.last > 0:
            mid = ticker.last
        vol: float | None = None
        if ticker.quote_volume is not None:
            vol = float(ticker.quote_volume)
        elif ticker.base_volume is not None and mid:
            vol = float(ticker.base_volume) * mid
        if vol is not None and vol >= 0:
            self._volume24h[key] = vol
        if mid and append_mid:
            self._append_mid(key, mono, mid)

    def on_trade(self, trade: Trade) -> None:
        asset = self.asset_of(trade)
        if not asset:
            return
        key = (trade.exchange, asset)
        dq = self._trades.get(key)
        if dq is None:
            dq = deque(maxlen=_TRADES_MAXLEN)
            self._trades[key] = dq
        dq.append(trade)

    def on_health(self, h: ExchangeHealth) -> None:
        """Keep the (adapter-owned) health object so ``stale`` is live."""
        self._health[h.exchange] = h

    # ------------------------------------------------------------------ #
    # queries
    # ------------------------------------------------------------------ #
    def book(self, exchange: str, asset: str) -> OrderBook | None:
        return self._books.get((exchange, asset))

    def books_for(self, asset: str) -> dict[str, OrderBook]:
        out: dict[str, OrderBook] = {}
        for ex in self._assets.get(asset, ()):
            b = self._books.get((ex, asset))
            if b is not None:
                out[ex] = b
        return out

    def ticker(self, exchange: str, asset: str) -> Ticker | None:
        return self._tickers.get((exchange, asset))

    def symbol(self, exchange: str, asset: str) -> str | None:
        """Market symbol of ``asset`` on ``exchange`` (book first, then ticker)."""
        b = self._books.get((exchange, asset))
        if b is not None:
            return b.symbol
        t = self._tickers.get((exchange, asset))
        return t.symbol if t is not None else None

    def volume24h(self, exchange: str, asset: str) -> float | None:
        return self._volume24h.get((exchange, asset))

    def health(self, exchange: str) -> ExchangeHealth | None:
        return self._health.get(exchange)

    def last_data_mono(self, exchange: str, asset: str) -> float | None:
        return self._last_data_mono.get((exchange, asset))

    def is_fresh(self, exchange: str, asset: str, now_mono: float, max_age_ms: float) -> bool:
        """True when the stream has data not older than ``max_age_ms`` and the
        exchange is not flagged stale / in maintenance."""
        last = self._last_data_mono.get((exchange, asset))
        if last is None:
            return False
        if (now_mono - last) * 1000.0 > max_age_ms:
            return False
        h = self._health.get(exchange)
        if h is not None and (h.stale or h.maintenance):
            return False
        return True

    def mid_series(self, exchange: str, asset: str) -> Sequence[tuple[float, float]]:
        """Internal series (oldest -> newest).  Callers must not mutate it."""
        return self._series.get((exchange, asset), ())

    def mid_at(self, exchange: str, asset: str, mono: float) -> float | None:
        series = self._series.get((exchange, asset))
        if not series:
            return None
        idx = bisect_right(series, mono, key=_mono_of)
        if idx == 0:
            return None
        return series[idx - 1][1]

    def assets(self) -> set[str]:
        return set(self._assets)

    def exchanges_for(self, asset: str) -> list[str]:
        return sorted(self._assets.get(asset, ()))

    def recent_quote_volume(self, exchange: str, asset: str, window_s: float, now_mono: float) -> float:
        dq = self._trades.get((exchange, asset))
        if not dq:
            return 0.0
        cutoff = now_mono - window_s
        total = 0.0
        for t in reversed(dq):
            if t.ts.recv_mono < cutoff:
                break
            if t.ts.recv_mono <= now_mono:
                total += t.price * t.amount
        return total
