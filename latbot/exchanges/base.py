"""ExchangeAdapter interface.

Every exchange (real via ccxt.pro, Bybit demo, internal paper simulator, fake
adapter for tests) implements this.  Market data is pushed onto the EventBus
(events.BOOK / TRADE / TICKER / HEALTH); trading methods are awaited directly.

Rules for implementations
-------------------------
* Normalise everything into models.* objects.  Prices/amounts are floats.
* Every event carries Timestamps(exchange_ts, recv_ts, recv_mono).
* Never log or expose credentials.
* place_order must set Order.sent_mono before the network call and Order.ack_mono
  when the exchange acknowledges; update health.order_latency_ms (EWMA).
* Raise the typed errors below so the execution engine can react.
* An adapter is "usable" only if health().is_usable(now_mono).
"""
from __future__ import annotations

import abc
import logging
from typing import Iterable

from ..clock import Clock, Ewma
from ..config import ExchangeConfig
from ..events import EventBus
from ..models import Balance, ExchangeHealth, ExchangeRole, Fees, Order, OrderBook, OrderRequest, Ticker

log = logging.getLogger(__name__)


class ExchangeError(Exception):
    pass


class RateLimited(ExchangeError):
    def __init__(self, retry_after_s: float = 1.0, msg: str = "rate limited") -> None:
        super().__init__(msg)
        self.retry_after_s = retry_after_s


class InsufficientBalance(ExchangeError):
    pass


class OrderRejected(ExchangeError):
    pass


class StaleData(ExchangeError):
    pass


class ExchangeAdapter(abc.ABC):
    """Base class.  Subclasses must set ``name`` and implement the abstract API."""

    def __init__(self, cfg: ExchangeConfig, bus: EventBus, clock: Clock | None = None) -> None:
        self.cfg = cfg
        self.name = cfg.name
        self.bus = bus
        self.clock = clock or Clock()
        self.health = ExchangeHealth(exchange=cfg.name)
        self._ws_lat = Ewma()
        self._rest_lat = Ewma()
        self._order_lat = Ewma()
        self._fill_lat = Ewma()
        self._fees_cache: dict[str, Fees] = {}

    # ---- identity ------------------------------------------------------
    @property
    def role(self) -> ExchangeRole:
        return self.cfg.role

    @property
    def can_execute(self) -> bool:
        return self.cfg.role == ExchangeRole.EXECUTION

    # ---- lifecycle -----------------------------------------------------
    @abc.abstractmethod
    async def start(self, symbols: Iterable[str]) -> None:
        """Connect and start streaming order books (+ trades if available) for
        symbols.  Must return quickly; streaming runs in background tasks.
        Symbols the exchange does not list are skipped (log once)."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Cancel background tasks, close connections."""

    @abc.abstractmethod
    def supported_symbols(self) -> set[str]:
        """Symbols actually streamed (subset of requested)."""

    # ---- market data ---------------------------------------------------
    @abc.abstractmethod
    def order_book(self, symbol: str) -> OrderBook | None:
        """Latest cached book or None."""

    @abc.abstractmethod
    async def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        """REST snapshot (used for revalidation when WS looks stale)."""

    @abc.abstractmethod
    async def fetch_ticker(self, symbol: str) -> Ticker:
        ...

    # ---- account -------------------------------------------------------
    @abc.abstractmethod
    async def fetch_balances(self) -> dict[str, Balance]:
        """asset -> Balance.  Paper adapters return virtual balances."""

    @abc.abstractmethod
    async def fees(self, symbol: str) -> Fees:
        """Taker/maker fee fractions for symbol (cached; falls back to cfg)."""

    # ---- trading -------------------------------------------------------
    @abc.abstractmethod
    async def place_order(self, req: OrderRequest) -> Order:
        """Submit order.  Returns Order with status >= ACKNOWLEDGED (or raises).
        For IOC orders the returned Order may already be FILLED/PARTIALLY_FILLED/
        CANCELLED (unfilled remainder)."""

    @abc.abstractmethod
    async def fetch_order(self, order_id: str, symbol: str) -> Order:
        """Refresh order status / fills."""

    @abc.abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        ...

    async def cancel_all(self, symbol: str | None = None) -> int:
        """Best-effort cancel of all open orders.  Returns count.  Default: 0."""
        return 0

    async def open_orders(self, symbol: str | None = None) -> list[Order]:
        return []

    # ---- health helpers (for subclasses) ---------------------------------
    def _mark_msg(self, exchange_ts: float | None, recv_ts: float, recv_mono: float) -> None:
        self.health.ws_connected = True
        self.health.last_msg_mono = recv_mono
        self.health.stale = False
        if exchange_ts is not None:
            lat = (recv_ts - exchange_ts) * 1000.0
            if -5000 < lat < 60_000:
                self.health.ws_latency_ms = self._ws_lat.update(max(0.0, lat))

    def _mark_rest_latency(self, ms: float) -> None:
        self.health.rest_latency_ms = self._rest_lat.update(ms)

    def _mark_order_latency(self, ms: float) -> None:
        self.health.order_latency_ms = self._order_lat.update(ms)

    def _mark_fill_latency(self, ms: float) -> None:
        self.health.fill_latency_ms = self._fill_lat.update(ms)

    def _mark_error(self, reason: str = "") -> None:
        self.health.error_count += 1
        if reason:
            self.health.reason = reason

    def check_staleness(self, now_mono: float, stale_after_s: float) -> bool:
        """Update health.stale based on last message age; returns stale flag."""
        if self.health.last_msg_mono is None:
            self.health.stale = True
        else:
            self.health.stale = (now_mono - self.health.last_msg_mono) > stale_after_s
        return self.health.stale

    async def publish_health(self) -> None:
        await self.bus.publish("health", self.health)

    def estimated_bot_latency_ms(self, default_ms: float) -> float:
        """Round-trip estimate for one order: ws + order ack (+ fill if known)."""
        order = self.health.order_latency_ms if self.health.order_latency_ms is not None else default_ms
        ws = self.health.ws_latency_ms or 0.0
        return order + ws
