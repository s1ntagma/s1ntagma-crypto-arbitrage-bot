"""Paper-trading adapter: simulated execution on top of a real market-data feed.

``PaperAdapter`` wraps a market-data adapter (``inner``: usually a
``CcxtAdapter`` without credentials) and shares its books and health.  Orders
are filled against the inner adapter's LATEST order book after a simulated
network latency (inner order-latency EWMA or ``settings.default_api_latency_ms``
with +-20 % jitter from a seeded RNG) - and only while that book is no older
than ``settings.max_data_age_ms``.  A real venue has no price when the feed is
down, so neither has the paper venue (hard rule 1): on a missing or stale
book MARKET / IOC / FOK orders are rejected (``OrderRejected``) before any
balance moves, and a resting GTC simply waits for fresh data.

* LIMIT IOC  - fills the levels within the limit price, the rest is CANCELLED
  (``filled`` and ``fills`` are kept; a PARTIALLY_FILLED update is published first).
* LIMIT GTC  - fills what is immediately available, the remainder rests and is
  re-checked against the latest book on every ``fetch_order`` (so an exit limit
  order fills when the market reaches it).
* MARKET     - walks the whole book; an exhausted book fills what exists.

Fees = inner taker fee x notional, paid in quote.  Spot balances (quote / base)
or swap positions (1x margin, SHORT allowed, realized P&L on reduce) live in a
``Ledger`` persisted to ``db.kv`` under ``paper:<exchange>`` when a db is given.
"""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import replace
from typing import Any, Iterable

from .. import events
from ..clock import Clock
from ..config import Settings
from ..events import EventBus
from ..models import Balance, ExchangeHealth, Fees, Order, OrderBook, OrderRequest, OrderStatus, OrderType, Ticker, TimeInForce
from ._fill import Ledger, PositionBook, SimBroker
from .base import ExchangeAdapter, OrderRejected

log = logging.getLogger(__name__)


class PaperAdapter(ExchangeAdapter):
    """Simulated execution venue sharing ``inner``'s market data (see module doc)."""

    def __init__(
        self,
        inner: ExchangeAdapter,
        settings: Settings,
        bus: EventBus,
        clock: Clock,
        db: Any | None = None,
        initial_quote: float | None = None,
        quote_asset: str = "USDT",
        *,
        seed: int | None = None,
        quote_assets: Iterable[str] | None = None,
        manage_inner: bool = False,
    ) -> None:
        self.inner = inner
        super().__init__(inner.cfg, bus, clock)
        self.settings = settings
        self.db = db
        self.quote_asset = quote_asset.upper()
        self.quote_assets = [q.upper() for q in (quote_assets or [])]
        self.manage_inner = manage_inner
        self._rng = random.Random(seed)
        self._initial_quote = initial_quote
        self._kv_key = f"paper:{self.name}"
        self._ledger = self._load_or_new(initial_quote)
        self._broker = SimBroker(self.name, self.cfg.market_type, clock, self._ledger, id_prefix="p")

    # ---- ledger ------------------------------------------------------------------
    def _new_ledger(self, initial_quote: float | None) -> Ledger:
        led = Ledger(self.cfg.market_type)
        amount = float(self.settings.paper_balance_quote if initial_quote is None else initial_quote)
        for asset in dict.fromkeys([self.quote_asset, *self.quote_assets]):
            led.credit(asset, amount)
        return led

    def _load_or_new(self, initial_quote: float | None) -> Ledger:
        if self.db is not None and initial_quote is None:
            try:
                data = self.db.kv_get(self._kv_key, None)
            except Exception as exc:  # noqa: BLE001
                log.warning("%s: paper state load failed (%s)", self.name, type(exc).__name__)
                data = None
            if isinstance(data, dict) and data.get("balances"):
                led = Ledger.from_dict(data, self.cfg.market_type)
                for asset in dict.fromkeys([self.quote_asset, *self.quote_assets]):
                    led.balance(asset)  # make sure every quote exists (zero if new)
                log.info("%s: paper state restored (%d balances, %d positions)", self.name, len(led.balances), len(led.positions))
                return led
        return self._new_ledger(initial_quote)

    def _persist(self) -> None:
        if self.db is None:
            return
        try:
            self.db.kv_set(self._kv_key, self._ledger.to_dict())
        except Exception as exc:  # noqa: BLE001
            log.warning("%s: paper state save failed (%s)", self.name, type(exc).__name__)

    def reset(self, initial_quote: float | None = None) -> dict[str, Balance]:
        """Reset balances/positions to ``initial_quote`` (default: settings).
        Open paper orders are dropped.  Publishes BALANCE when a loop runs."""
        self._ledger = self._new_ledger(initial_quote)
        self._broker = SimBroker(self.name, self.cfg.market_type, self.clock, self._ledger, id_prefix="p")
        self._persist()
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            self.bus.publish_nowait(events.BALANCE, self._balance_event())
        return self._broker.balances_copy()

    @property
    def positions(self) -> PositionBook:
        """``{symbol: {"side", "amount", "entry"}}`` (callable too: ``positions()``)."""
        return self._ledger.positions

    @property
    def realized_pnl_quote(self) -> float:
        return self._ledger.realized_pnl_quote

    # ---- health (inner's, with our own order/fill latency overlaid) ---------------
    @property
    def health(self) -> ExchangeHealth:
        h = self.inner.health
        return replace(
            h,
            order_latency_ms=self._order_lat.value if self._order_lat.value is not None else h.order_latency_ms,
            fill_latency_ms=self._fill_lat.value if self._fill_lat.value is not None else h.fill_latency_ms,
        )

    @health.setter
    def health(self, value: ExchangeHealth) -> None:  # base __init__ assigns; inner owns health
        return None

    def check_staleness(self, now_mono: float, stale_after_s: float) -> bool:
        return self.inner.check_staleness(now_mono, stale_after_s)

    # ---- delegation to the market-data adapter -------------------------------------
    async def start(self, symbols: Iterable[str]) -> None:
        if self.manage_inner:
            await self.inner.start(symbols)

    async def stop(self) -> None:
        if self.manage_inner:
            await self.inner.stop()

    def supported_symbols(self) -> set[str]:
        return self.inner.supported_symbols()

    def symbol_for(self, asset: str) -> str | None:
        fn = getattr(self.inner, "symbol_for", None)
        return fn(asset) if fn else None

    def asset_for(self, symbol: str) -> str | None:
        fn = getattr(self.inner, "asset_for", None)
        return fn(symbol) if fn else None

    def market(self, asset: str) -> dict[str, Any] | None:
        fn = getattr(self.inner, "market", None)
        return fn(asset) if fn else None

    def order_book(self, symbol: str) -> OrderBook | None:
        return self.inner.order_book(symbol)

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        return await self.inner.fetch_order_book(symbol, limit)

    async def fetch_ticker(self, symbol: str) -> Ticker:
        return await self.inner.fetch_ticker(symbol)

    async def fetch_tickers(self, *args: Any, **kwargs: Any) -> dict[str, Ticker]:
        fn = getattr(self.inner, "fetch_tickers", None)
        return await fn(*args, **kwargs) if fn else {}

    async def fees(self, symbol: str) -> Fees:
        return await self.inner.fees(symbol)

    async def fetch_balances(self) -> dict[str, Balance]:
        return self._broker.balances_copy()

    # ---- trading -------------------------------------------------------------------------
    def _balance_event(self) -> dict[str, Any]:
        return {"exchange": self.name, "mode": "paper", "balances": self._broker.balances_copy()}

    async def _publish(self, order: Order, statuses: list[OrderStatus]) -> None:
        for st in statuses[:-1]:
            await self.bus.publish(events.ORDER_UPDATE, replace(order, status=st, fills=list(order.fills)))
        await self.bus.publish(events.ORDER_UPDATE, order)
        await self.bus.publish(events.BALANCE, self._balance_event())

    def _latency_s(self) -> float:
        base_ms = self.inner.health.order_latency_ms or self.settings.default_api_latency_ms
        return max(0.0, float(base_ms)) / 1000.0 * self._rng.uniform(0.8, 1.2)

    def _fresh_book(self, symbol: str) -> OrderBook | None:
        """The inner book for ``symbol`` when it is no older than
        ``settings.max_data_age_ms``; None when missing or stale."""
        book = self.inner.order_book(symbol)
        if book is None or book.age_ms(self.clock.mono()) > float(self.settings.max_data_age_ms) or book.is_broken:
            return None
        return book

    def _no_fresh_book(self, symbol: str) -> OrderRejected:
        cached = self.inner.order_book(symbol)
        age = f"{cached.age_ms(self.clock.mono()):.0f} ms old" if cached is not None else "missing"
        return OrderRejected(
            f"{self.name}: no fresh order book for {symbol} ({age}, max {float(self.settings.max_data_age_ms):.0f} ms)"
        )

    async def place_order(self, req: OrderRequest, *, reduce_only: bool = False) -> Order:
        """Simulate ``req`` against the latest fresh inner book after a
        simulated latency (see module doc for stale books).  ``reduce_only``
        (swap only) closes a position without flipping."""
        sent_mono, sent_ts = self.clock.mono(), self.clock.wall()
        latency = self._latency_s()
        if latency > 0:
            await asyncio.sleep(latency)
        self._mark_order_latency(latency * 1000.0)
        book = self._fresh_book(req.symbol)
        if book is None and not (req.type == OrderType.LIMIT and req.tif == TimeInForce.GTC):
            err = self._no_fresh_book(req.symbol)
            log.warning("%s paper %s %s %s rejected: %s", self.name, req.type.value, req.side.value, req.symbol, err)
            raise err
        fee = (await self.inner.fees(req.symbol)).taker
        order, statuses = self._broker.place(
            req, book, fee, reduce_only=reduce_only, sent_mono=sent_mono, sent_ts=sent_ts
        )
        if order.filled > 0:
            self._mark_fill_latency(latency * 1000.0)
        log.info(
            "%s paper %s %s %s %.8g @ %s -> %s filled=%.8g avg=%s",
            self.name, req.type.value, req.side.value, req.symbol, order.amount, req.price,
            order.status.value, order.filled, order.avg_price,
        )
        self._persist()
        await self._publish(order, statuses)
        return order

    async def fetch_order(self, order_id: str, symbol: str) -> Order:
        """Resting GTC orders are re-checked against the latest FRESH inner
        book (a stale book never fills anything)."""
        order, changed = self._broker.refresh(order_id, self._fresh_book(symbol))
        if changed:
            if order.first_fill_mono is not None and order.sent_mono is not None:
                self._mark_fill_latency(max(0.0, (order.first_fill_mono - order.sent_mono) * 1000.0))
            self._persist()
            await self._publish(order, [order.status])
        return order

    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        order = self._broker.orders.get(order_id)
        was_open = order is not None and not order.is_done
        order = self._broker.cancel(order_id)
        if was_open:
            self._persist()
            await self._publish(order, [order.status])
        return order

    async def cancel_all(self, symbol: str | None = None) -> int:
        count = 0
        for o in self._broker.open_orders(symbol):
            await self.cancel_order(o.id, o.symbol)
            count += 1
        return count

    async def open_orders(self, symbol: str | None = None) -> list[Order]:
        return self._broker.open_orders(symbol)
