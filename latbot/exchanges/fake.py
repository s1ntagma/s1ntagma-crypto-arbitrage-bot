"""Fake exchange adapter for tests and backtest replay.

Books are pushed in by the test (``push_book``), trading fills against the
stored book through the same ``SimBroker`` the paper adapter uses.  Knobs:

* ``fill_latency_s``  - sleep before filling (and mark order latency)
* ``reject_next``     - raise OrderRejected once
* ``partial_ratio``   - cap the fill at ``amount * ratio`` (IOC remainder is cancelled,
  GTC remainder rests, MARKET remainder is cancelled)
* ``balances``        - initial virtual balances (asset -> amount)
* ``volumes``         - 24h quote volume per asset reported by ``fetch_tickers``
* ``markets``         - raw market dicts per asset (``market(asset)``), e.g. for
  Bybit ``info.launchTime`` in the registry
* ``fail_fetch_tickers`` - make ``fetch_tickers`` raise ExchangeError
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import Any, Iterable

from .. import events
from ..clock import Clock
from ..config import ExchangeConfig
from ..events import EventBus
from ..links import split_symbol
from ..models import (
    Balance,
    ExchangeHealth,
    Fees,
    Order,
    OrderBook,
    OrderRequest,
    OrderStatus,
    Side,
    Ticker,
    Timestamps,
    Trade,
)
from ._fill import Ledger, PositionBook, SimBroker, levels_from_pairs
from .base import ExchangeAdapter, ExchangeError, OrderRejected

log = logging.getLogger(__name__)

_DEFAULT_BALANCE = 100_000.0
_DEFAULT_QUOTES = ("USDT", "USD", "USDC", "EUR")


class FakeAdapter(ExchangeAdapter):
    """In-memory adapter driven by the test (see module docstring)."""

    def __init__(
        self,
        cfg: ExchangeConfig,
        bus: EventBus,
        clock: Clock,
        symbols: dict[str, str] | None = None,
        *,
        balances: dict[str, float] | None = None,
        seed: int | None = None,
    ) -> None:
        super().__init__(cfg, bus, clock)
        self._symbols: dict[str, str] = {k.upper(): v for k, v in (symbols or {}).items()}
        self._assets: dict[str, str] = {v: k for k, v in self._symbols.items()}
        self._books: dict[str, OrderBook] = {}  # by asset
        self._started: list[str] = []
        self._seq = 0
        # knobs
        self.fill_latency_s: float = 0.0
        self.reject_next: bool = False
        self.partial_ratio: float | None = None
        self.volumes: dict[str, float] = {}
        self.markets: dict[str, dict[str, Any]] = {}
        self.fail_fetch_tickers: bool = False
        self.placed: list[OrderRequest] = []
        self.seed = seed
        ledger = Ledger(cfg.market_type)
        init = balances if balances is not None else {q: _DEFAULT_BALANCE for q in _DEFAULT_QUOTES}
        for asset, amount in init.items():
            ledger.credit(asset.upper(), float(amount))
        self._broker = SimBroker(cfg.name, cfg.market_type, clock, ledger, id_prefix="f")

    # ---- identity -----------------------------------------------------------------
    def symbol_for(self, asset: str) -> str | None:
        asset = asset.upper()
        sym = self._symbols.get(asset)
        if sym is None:
            sym = f"{asset}/USDT"
            self._symbols[asset] = sym
            self._assets[sym] = asset
        return sym

    def asset_for(self, symbol: str) -> str | None:
        return self._assets.get(symbol) or split_symbol(symbol)[0]

    def market(self, asset: str) -> dict[str, Any] | None:
        return self.markets.get(asset.upper())

    def _asset_of(self, symbol_or_asset: str) -> str:
        if symbol_or_asset in self._assets:
            return self._assets[symbol_or_asset]
        return split_symbol(symbol_or_asset)[0]

    # ---- lifecycle ------------------------------------------------------------------
    async def start(self, symbols: Iterable[str]) -> None:
        for item in symbols:
            asset = self._asset_of(item)
            self.symbol_for(asset)
            if asset not in self._started:
                self._started.append(asset)

    async def stop(self) -> None:
        return None

    def supported_symbols(self) -> set[str]:
        return {self._symbols[a] for a in self._started if a in self._symbols}

    # ---- test drivers -----------------------------------------------------------------
    def _ts(self, exchange_ts: float | None) -> Timestamps:
        return Timestamps(recv_ts=self.clock.wall(), recv_mono=self.clock.mono(), exchange_ts=exchange_ts)

    async def push_book(
        self,
        asset: str,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        exchange_ts: float | None = None,
    ) -> OrderBook:
        """Store a book for ``asset``, mark health and publish BOOK."""
        asset = asset.upper()
        symbol = self.symbol_for(asset) or asset
        self._seq += 1
        book = OrderBook(
            exchange=self.name,
            symbol=symbol,
            bids=levels_from_pairs(bids, descending=True),
            asks=levels_from_pairs(asks, descending=False),
            ts=self._ts(exchange_ts),
            asset=asset,
            seq=self._seq,
        )
        self._books[asset] = book
        self._mark_msg(book.ts.exchange_ts, book.ts.recv_ts, book.ts.recv_mono)
        await self.bus.publish(events.BOOK, book)
        return book

    async def push_trade(
        self, asset: str, price: float, amount: float, side: Side | None = None, exchange_ts: float | None = None
    ) -> Trade:
        asset = asset.upper()
        trade = Trade(
            exchange=self.name,
            symbol=self.symbol_for(asset) or asset,
            price=float(price),
            amount=float(amount),
            side=side,
            ts=self._ts(exchange_ts),
            asset=asset,
        )
        self._mark_msg(trade.ts.exchange_ts, trade.ts.recv_ts, trade.ts.recv_mono)
        await self.bus.publish(events.TRADE, trade)
        return trade

    async def push_ticker(
        self,
        asset: str,
        bid: float | None,
        ask: float | None,
        last: float | None = None,
        quote_volume: float | None = None,
        exchange_ts: float | None = None,
    ) -> Ticker:
        asset = asset.upper()
        if quote_volume is not None:
            self.volumes[asset] = float(quote_volume)
        ticker = Ticker(
            exchange=self.name,
            symbol=self.symbol_for(asset) or asset,
            bid=bid,
            ask=ask,
            last=last if last is not None else ((bid + ask) / 2.0 if bid is not None and ask is not None else None),
            ts=self._ts(exchange_ts),
            asset=asset,
            quote_volume=self.volumes.get(asset),
        )
        self._mark_msg(ticker.ts.exchange_ts, ticker.ts.recv_ts, ticker.ts.recv_mono)
        await self.bus.publish(events.TICKER, ticker)
        return ticker

    def set_health(self, **fields: Any) -> ExchangeHealth:
        """Set ExchangeHealth fields directly (unknown names raise ValueError)."""
        for k, v in fields.items():
            if not hasattr(self.health, k):
                raise ValueError(f"unknown health field {k!r}")
            setattr(self.health, k, v)
        return self.health

    # ---- market data -------------------------------------------------------------------------
    def order_book(self, symbol: str) -> OrderBook | None:
        return self._books.get(self._asset_of(symbol))

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> OrderBook:
        book = self.order_book(symbol)
        if book is None:
            raise ExchangeError(f"{self.name}: no order book for {symbol}")
        return replace(book, bids=book.bids[:limit], asks=book.asks[:limit], ts=self._ts(book.ts.exchange_ts))

    def _ticker_of(self, asset: str) -> Ticker:
        book = self._books.get(asset)
        bid = book.best_bid if book else None
        ask = book.best_ask if book else None
        return Ticker(
            exchange=self.name,
            symbol=self.symbol_for(asset) or asset,
            bid=bid,
            ask=ask,
            last=book.mid if book else None,
            ts=self._ts(book.ts.exchange_ts if book else None),
            asset=asset,
            quote_volume=self.volumes.get(asset),
        )

    async def fetch_ticker(self, symbol: str) -> Ticker:
        asset = self._asset_of(symbol)
        if asset not in self._books and asset not in self.volumes:
            raise ExchangeError(f"{self.name}: no ticker for {symbol}")
        return self._ticker_of(asset)

    async def fetch_tickers(self, assets: Iterable[str] | None = None) -> dict[str, Ticker]:
        if self.fail_fetch_tickers:
            raise ExchangeError(f"{self.name}: fetch_tickers failed (test knob)")
        keys = [a.upper() for a in assets] if assets is not None else list(dict.fromkeys([*self._books, *self.volumes]))
        return {a: self._ticker_of(a) for a in keys if a in self._books or a in self.volumes}

    # ---- account ----------------------------------------------------------------------------------
    async def fetch_balances(self) -> dict[str, Balance]:
        return self._broker.balances_copy()

    async def fees(self, symbol: str) -> Fees:
        return Fees(maker=self.cfg.maker_fee, taker=self.cfg.taker_fee)

    @property
    def positions(self) -> PositionBook:
        return self._broker.ledger.positions

    @property
    def realized_pnl_quote(self) -> float:
        return self._broker.ledger.realized_pnl_quote

    def credit(self, asset: str, amount: float) -> None:
        """Adjust a virtual balance (test helper)."""
        self._broker.ledger.credit(asset.upper(), float(amount))

    # ---- trading -------------------------------------------------------------------------------------
    def _balance_event(self) -> dict[str, Any]:
        return {"exchange": self.name, "mode": "paper", "balances": self._broker.balances_copy()}

    async def _publish(self, order: Order, statuses: list[OrderStatus]) -> None:
        for st in statuses[:-1]:
            await self.bus.publish(events.ORDER_UPDATE, replace(order, status=st, fills=list(order.fills)))
        await self.bus.publish(events.ORDER_UPDATE, order)
        await self.bus.publish(events.BALANCE, self._balance_event())

    async def place_order(self, req: OrderRequest, *, reduce_only: bool = False) -> Order:
        self.placed.append(req)
        if self.reject_next:
            self.reject_next = False
            raise OrderRejected(f"{self.name}: rejected (test knob)")
        sent_mono, sent_ts = self.clock.mono(), self.clock.wall()
        if self.fill_latency_s > 0:
            await asyncio.sleep(self.fill_latency_s)
            self._mark_order_latency(self.fill_latency_s * 1000.0)
        max_fill = req.amount * float(self.partial_ratio) if self.partial_ratio is not None else None
        order, statuses = self._broker.place(
            req,
            self.order_book(req.symbol),
            self.cfg.taker_fee,
            max_fill=max_fill,
            reduce_only=reduce_only,
            sent_mono=sent_mono,
            sent_ts=sent_ts,
        )
        if order.filled > 0 and self.fill_latency_s > 0:
            self._mark_fill_latency(self.fill_latency_s * 1000.0)
        await self._publish(order, statuses)
        return order

    async def fetch_order(self, order_id: str, symbol: str) -> Order:
        order, changed = self._broker.refresh(order_id, self.order_book(symbol))
        if changed:
            await self._publish(order, [order.status])
        return order

    async def cancel_order(self, order_id: str, symbol: str) -> Order:
        existing = self._broker.orders.get(order_id)
        was_open = existing is not None and not existing.is_done
        order = self._broker.cancel(order_id)
        if was_open:
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
