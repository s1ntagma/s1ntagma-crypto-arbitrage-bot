"""Shared fill simulation for the paper and fake adapters (private module).

Three building blocks:

* ``_walk`` / ``_walk_pairs`` - consume order-book levels in order (optionally
  stopping at a limit price).  Same semantics as ``engine.economics.walk_book``
  but kept local so the exchange package has no dependency on the engine.
* ``Ledger`` - virtual balances and (for swap markets) positions with 1x margin.
* ``SimBroker`` - order lifecycle (IOC / FOK / GTC / MARKET) on top of a Ledger:
  immediate fills against a book, resting GTC remainders that are re-checked
  against later books, cancellation, reservations.

Everything here is synchronous and deterministic (no I/O, no randomness).
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, Sequence

from ..clock import Clock
from ..links import split_symbol
from ..models import (
    Balance,
    BookLevel,
    Fill,
    Order,
    OrderBook,
    OrderRequest,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
    WalkResult,
    new_id,
)
from .base import ExchangeError, InsufficientBalance, OrderRejected

log = logging.getLogger(__name__)

_EPS = 1e-12

LevelLike = Any  # BookLevel | (price, size) tuple/list


def _level(lv: LevelLike) -> tuple[float, float]:
    """Accept BookLevel or a (price, size) pair."""
    if isinstance(lv, BookLevel):
        return float(lv.price), float(lv.size)
    return float(lv[0]), float(lv[1])


def _infer_side(levels: Sequence[LevelLike]) -> Side:
    """Descending prices -> bids (we SELL into them); otherwise asks (BUY)."""
    if len(levels) >= 2:
        p0, _ = _level(levels[0])
        p1, _ = _level(levels[1])
        if p0 > p1:
            return Side.SELL
    return Side.BUY


def _limit_blocks(side: Side, price: float, limit_price: float | None) -> bool:
    if limit_price is None:
        return False
    return price > limit_price if side == Side.BUY else price < limit_price


def _walk_pairs(
    levels: Sequence[LevelLike],
    *,
    base_amount: float,
    limit_price: float | None = None,
    side: Side | None = None,
) -> list[tuple[float, float]]:
    """Return the (price, amount) pairs consumed to fill ``base_amount``.

    Levels are consumed in order; a level beyond ``limit_price`` stops the walk
    (BUY: price above the limit, SELL: price below).  Levels with size <= 0 are
    skipped.  The result may fill less than requested (thin book / limit hit).
    """
    if base_amount <= 0:
        return []
    side = side or _infer_side(levels)
    remaining = float(base_amount)
    pairs: list[tuple[float, float]] = []
    for lv in levels:
        price, size = _level(lv)
        if size <= 0 or price <= 0:
            continue
        if _limit_blocks(side, price, limit_price):
            break
        take = min(size, remaining)
        pairs.append((price, take))
        remaining -= take
        if remaining <= _EPS * max(1.0, base_amount):
            break
    return pairs


def _walk(
    levels: Sequence[LevelLike],
    *,
    quote_amount: float | None = None,
    base_amount: float | None = None,
    limit_price: float | None = None,
    side: Side | None = None,
) -> WalkResult:
    """Walk ``levels`` until ``quote_amount`` or ``base_amount`` is consumed.

    Exactly one of the two amounts must be given.  Never raises on an empty
    book: ``exhausted=True`` and zeros are returned instead.
    """
    if (quote_amount is None) == (base_amount is None):
        raise ValueError("give exactly one of quote_amount / base_amount")
    side = side or _infer_side(levels)
    filled_base = 0.0
    filled_quote = 0.0
    used = 0
    worst: float | None = None
    if base_amount is not None:
        for price, take in _walk_pairs(levels, base_amount=base_amount, limit_price=limit_price, side=side):
            filled_base += take
            filled_quote += take * price
            used += 1
            worst = price
        exhausted = filled_base < base_amount - _EPS * max(1.0, base_amount)
    else:
        target = float(quote_amount or 0.0)
        remaining_q = target
        if target > 0:
            for lv in levels:
                price, size = _level(lv)
                if size <= 0 or price <= 0:
                    continue
                if _limit_blocks(side, price, limit_price):
                    break
                take = min(size, remaining_q / price)
                if take <= 0:
                    break
                filled_base += take
                filled_quote += take * price
                remaining_q -= take * price
                used += 1
                worst = price
                if remaining_q <= _EPS * max(1.0, target):
                    break
        exhausted = remaining_q > _EPS * max(1.0, target)
    vwap = filled_quote / filled_base if filled_base > 0 else None
    return WalkResult(
        vwap=vwap,
        filled_base=filled_base,
        filled_quote=filled_quote,
        levels_used=used,
        exhausted=exhausted,
        worst_price=worst,
    )


class PositionBook(dict):
    """``dict[symbol, {"side", "amount", "entry"}]`` that is also callable
    (``adapter.positions[symbol]`` and ``adapter.positions()`` both work)."""

    def __call__(self) -> dict[str, dict[str, Any]]:
        return {k: dict(v) for k, v in self.items()}


class Ledger:
    """Virtual balances (+ swap positions at 1x, no leverage).

    * spot BUY : quote -= cost + fee, base += amount
    * spot SELL: base -= amount,      quote += cost - fee
    * swap     : an opposite position is reduced first (realized P&L =
      (price - entry) * amount * sign, margin ``entry * amount`` returned),
      the rest opens / averages a position and reserves ``price * amount``
      of quote as margin.  Fees are always paid in quote.
    ``prepaid`` lowers the *free* debit by what was reserved earlier for a
    resting order (``reserve`` / ``release``).
    """

    def __init__(self, market_type: str = "spot") -> None:
        self.market_type = market_type
        self.balances: dict[str, Balance] = {}
        self.positions: PositionBook = PositionBook()
        self.realized_pnl_quote: float = 0.0

    # ---- balances ----------------------------------------------------------
    def balance(self, asset: str) -> Balance:
        b = self.balances.get(asset)
        if b is None:
            b = Balance(asset=asset, free=0.0, total=0.0)
            self.balances[asset] = b
        return b

    def credit(self, asset: str, amount: float) -> None:
        b = self.balance(asset)
        b.free += amount
        b.total += amount

    def reserve(self, asset: str, amount: float) -> None:
        if amount <= 0:
            return
        b = self.balance(asset)
        if b.free + 1e-9 < amount:
            raise InsufficientBalance(f"need {amount:.8g} {asset}, free {b.free:.8g}")
        b.free -= amount

    def release(self, asset: str, amount: float) -> None:
        if amount > 0:
            self.balance(asset).free += amount

    # ---- fills ---------------------------------------------------------------
    def apply_fill(
        self,
        symbol: str,
        side: Side,
        price: float,
        amount: float,
        fee_quote: float,
        *,
        prepaid: float = 0.0,
    ) -> tuple[float, str, float]:
        """Apply one fill.  Returns ``(realized_pnl, debit_asset, debit_amount)``."""
        base, quote = split_symbol(symbol)
        cost = price * amount
        if self.market_type != "swap":
            if side == Side.BUY:
                q = self.balance(quote)
                debit = cost + fee_quote
                q.free -= debit - prepaid
                q.total -= debit
                self.credit(base, amount)
                return 0.0, quote, debit
            bq = self.balance(base)
            bq.free -= amount - prepaid
            bq.total -= amount
            self.credit(quote, cost - fee_quote)
            return 0.0, base, amount
        # ---- swap (linear, 1x) ---------------------------------------------
        q = self.balance(quote)
        realized = 0.0
        remaining = amount
        pos = self.positions.get(symbol)
        want = "long" if side == Side.BUY else "short"
        if pos is not None and pos["side"] != want and pos["amount"] > 0:
            reduce = min(pos["amount"], remaining)
            sign = 1.0 if pos["side"] == "long" else -1.0
            realized = (price - pos["entry"]) * reduce * sign
            q.free += pos["entry"] * reduce + realized
            q.total += realized
            pos["amount"] -= reduce
            remaining -= reduce
            if pos["amount"] <= _EPS:
                del self.positions[symbol]
                pos = None
        open_notional = 0.0
        if remaining > _EPS:
            open_notional = price * remaining
            if pos is None or pos["side"] != want:
                self.positions[symbol] = {"side": want, "amount": remaining, "entry": price}
            else:
                tot = pos["amount"] + remaining
                pos["entry"] = (pos["entry"] * pos["amount"] + price * remaining) / tot
                pos["amount"] = tot
        debit = open_notional + fee_quote
        q.free -= debit - prepaid
        q.total -= fee_quote
        self.realized_pnl_quote += realized
        return realized, quote, debit

    # ---- persistence -----------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "balances": {a: {"free": b.free, "total": b.total} for a, b in self.balances.items()},
            "positions": {s: dict(p) for s, p in self.positions.items()},
            "realized_pnl_quote": self.realized_pnl_quote,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], market_type: str = "spot") -> "Ledger":
        led = cls(market_type)
        for asset, b in (data.get("balances") or {}).items():
            led.balances[asset] = Balance(asset=asset, free=float(b.get("free", 0.0)), total=float(b.get("total", 0.0)))
        for sym, p in (data.get("positions") or {}).items():
            led.positions[sym] = {"side": p["side"], "amount": float(p["amount"]), "entry": float(p["entry"])}
        led.realized_pnl_quote = float(data.get("realized_pnl_quote", 0.0))
        return led


class SimBroker:
    """Order lifecycle for simulated venues (paper / fake adapters)."""

    def __init__(self, exchange: str, market_type: str, clock: Clock, ledger: Ledger, id_prefix: str = "p") -> None:
        self.exchange = exchange
        self.market_type = market_type
        self.clock = clock
        self.ledger = ledger
        self.id_prefix = id_prefix
        self.orders: dict[str, Order] = {}
        self._reservations: dict[str, tuple[str, float]] = {}  # order id -> (asset, reserved amount)
        self._fee_rate: dict[str, float] = {}

    # ---- helpers -----------------------------------------------------------------
    @staticmethod
    def _levels(book: OrderBook | None, side: Side) -> list[BookLevel]:
        if book is None:
            return []
        return list(book.asks if side == Side.BUY else book.bids)

    def _reducible(self, symbol: str, side: Side) -> tuple[float, float]:
        """(amount of an opposite position, its entry price) for swap orders."""
        if self.market_type != "swap":
            return 0.0, 0.0
        pos = self.ledger.positions.get(symbol)
        if not pos:
            return 0.0, 0.0
        want = "long" if side == Side.BUY else "short"
        if pos["side"] == want:
            return 0.0, 0.0
        return float(pos["amount"]), float(pos["entry"])

    def _affordability(
        self,
        req: OrderRequest,
        pairs: list[tuple[float, float]],
        remainder: float,
        fee_rate: float,
        rests: bool,
    ) -> tuple[str, float, float, float]:
        """Return ``(asset, need_now, reserve_for_rest, available)``."""
        base, quote = split_symbol(req.symbol)
        limit = float(req.price or 0.0)
        fill_base = sum(a for _, a in pairs)
        fill_cost = sum(p * a for p, a in pairs)
        rest = remainder if rests else 0.0
        if self.market_type != "swap":
            if req.side == Side.BUY:
                need = fill_cost * (1.0 + fee_rate)
                reserve = rest * limit * (1.0 + fee_rate)
                return quote, need, reserve, self.ledger.balance(quote).free
            return base, fill_base, rest, self.ledger.balance(base).free
        reducible, entry = self._reducible(req.symbol, req.side)
        left = reducible
        open_notional = 0.0
        fees = 0.0
        released = 0.0
        for p, a in pairs:
            r = min(left, a)
            left -= r
            released += entry * r
            open_notional += (a - r) * p
            fees += p * a * fee_rate
        open_rest = max(0.0, rest - left) * limit
        reserve = open_rest + rest * limit * fee_rate
        return quote, open_notional + fees, reserve, self.ledger.balance(quote).free + released

    def _apply_pairs(self, order: Order, pairs: list[tuple[float, float]], fee_rate: float, prepaid_total: float = 0.0) -> None:
        _, quote = split_symbol(order.symbol)
        total_amt = sum(a for _, a in pairs)
        for price, amt in pairs:
            fee = price * amt * fee_rate
            prepaid = prepaid_total * (amt / total_amt) if total_amt > 0 else 0.0
            self.ledger.apply_fill(order.symbol, order.side, price, amt, fee, prepaid=prepaid)
            order.fills.append(
                Fill(order_id=order.id, price=price, amount=amt, fee_cost=fee, fee_asset=quote, ts=self.clock.wall(), trade_id=new_id("t"))
            )
        if pairs:
            order.filled = sum(f.amount for f in order.fills)
            order.avg_price = sum(f.price * f.amount for f in order.fills) / order.filled if order.filled > 0 else None
            order.fee_cost_quote = sum(f.fee_cost for f in order.fills)
            if order.first_fill_mono is None:
                order.first_fill_mono = self.clock.mono()

    def _finish(self, order: Order, status: OrderStatus, error: str | None = None) -> None:
        order.status = status
        if error and not order.error:
            order.error = error
        order.done_mono = self.clock.mono()
        res = self._reservations.pop(order.id, None)
        if res:
            self.ledger.release(*res)
        self._fee_rate.pop(order.id, None)

    # ---- API -------------------------------------------------------------------
    def place(
        self,
        req: OrderRequest,
        book: OrderBook | None,
        fee_rate: float,
        *,
        max_fill: float | None = None,
        reduce_only: bool = False,
        sent_mono: float | None = None,
        sent_ts: float | None = None,
    ) -> tuple[Order, list[OrderStatus]]:
        """Fill ``req`` against ``book``.  Returns the order and the sequence
        of statuses it went through (for ORDER_UPDATE events).

        Raises OrderRejected (bad request / reduce-only without a position /
        market order without a book) and InsufficientBalance (nothing filled).
        """
        if req.amount <= 0:
            raise OrderRejected("amount must be > 0")
        if req.type == OrderType.LIMIT and (req.price is None or req.price <= 0):
            raise OrderRejected("limit order needs a price")
        amount = float(req.amount)
        if reduce_only:
            if self.market_type != "swap":
                raise OrderRejected("reduceOnly is only valid on swap markets")
            reducible, _ = self._reducible(req.symbol, req.side)
            if reducible <= 0:
                raise OrderRejected("reduceOnly: no opposite position")
            amount = min(amount, reducible)
        levels = self._levels(book, req.side)
        if req.type == OrderType.MARKET and not levels:
            raise OrderRejected(f"no order book for {req.symbol}")
        limit = float(req.price) if req.type == OrderType.LIMIT else None
        cap = amount if max_fill is None else max(0.0, min(amount, float(max_fill)))
        pairs = _walk_pairs(levels, base_amount=cap, limit_price=limit, side=req.side)
        fill_base = sum(a for _, a in pairs)
        rests = req.type == OrderType.LIMIT and req.tif == TimeInForce.GTC
        if req.type == OrderType.LIMIT and req.tif == TimeInForce.FOK and fill_base < amount - _EPS:
            pairs = []
            fill_base = 0.0
        remainder = max(0.0, amount - fill_base)
        asset, need, reserve, available = self._affordability(req, pairs, remainder, fee_rate, rests)
        if available + 1e-9 < need + reserve:
            raise InsufficientBalance(f"need {need + reserve:.8g} {asset}, free {available:.8g}")

        order = Order(
            id=new_id(self.id_prefix),
            client_id=req.client_id,
            exchange=self.exchange,
            symbol=req.symbol,
            side=req.side,
            type=req.type,
            amount=amount,
            price=req.price,
            status=OrderStatus.SENT,
            sent_ts=self.clock.wall() if sent_ts is None else sent_ts,
            sent_mono=self.clock.mono() if sent_mono is None else sent_mono,
        )
        order.ack_mono = self.clock.mono()
        self.orders[order.id] = order
        self._apply_pairs(order, pairs, fee_rate)
        statuses: list[OrderStatus] = []
        if remainder <= _EPS * max(1.0, amount):
            self._finish(order, OrderStatus.FILLED)
            statuses.append(OrderStatus.FILLED)
            return order, statuses
        if order.filled > 0:
            statuses.append(OrderStatus.PARTIALLY_FILLED)
        if req.type == OrderType.MARKET:
            self._finish(order, OrderStatus.CANCELLED, "book exhausted")
            statuses.append(OrderStatus.CANCELLED)
        elif req.tif == TimeInForce.GTC:
            order.status = OrderStatus.PARTIALLY_FILLED if order.filled > 0 else OrderStatus.ACKNOWLEDGED
            if reserve > 0:
                self.ledger.reserve(asset, reserve)
                self._reservations[order.id] = (asset, reserve)
            self._fee_rate[order.id] = fee_rate
            if not statuses:
                statuses.append(order.status)
        else:  # IOC / FOK remainder
            self._finish(order, OrderStatus.CANCELLED, None if order.filled > 0 else "no fill within limit")
            statuses.append(OrderStatus.CANCELLED)
        return order, statuses

    def refresh(self, order_id: str, book: OrderBook | None) -> tuple[Order, bool]:
        """Re-check a resting GTC order against ``book``.  Returns (order, changed)."""
        order = self.orders.get(order_id)
        if order is None:
            raise ExchangeError(f"unknown order {order_id}")
        if order.is_done:
            return order, False
        levels = self._levels(book, order.side)
        before = order.remaining
        pairs = _walk_pairs(levels, base_amount=before, limit_price=order.price, side=order.side)
        if not pairs:
            return order, False
        fill = sum(a for _, a in pairs)
        asset, reserved = self._reservations.get(order.id, ("", 0.0))
        prepaid = reserved * (fill / before) if before > 0 else 0.0
        self._apply_pairs(order, pairs, self._fee_rate.get(order.id, 0.0), prepaid_total=prepaid)
        if reserved:
            left = reserved - prepaid
            self._reservations[order.id] = (asset, max(0.0, left))
        if order.remaining <= _EPS * max(1.0, order.amount):
            self._finish(order, OrderStatus.FILLED)
        else:
            order.status = OrderStatus.PARTIALLY_FILLED
        return order, True

    def cancel(self, order_id: str) -> Order:
        order = self.orders.get(order_id)
        if order is None:
            raise ExchangeError(f"unknown order {order_id}")
        if order.is_done:
            return order
        self._finish(order, OrderStatus.CANCELLED)
        return order

    def open_orders(self, symbol: str | None = None) -> list[Order]:
        return [o for o in self.orders.values() if not o.is_done and (symbol is None or o.symbol == symbol)]

    def balances_copy(self) -> dict[str, Balance]:
        return {a: Balance(asset=a, free=b.free, total=b.total) for a, b in self.ledger.balances.items()}


def levels_from_pairs(pairs: Iterable[tuple[float, float]], *, descending: bool) -> list[BookLevel]:
    """Build sorted BookLevels (drops size <= 0)."""
    out = [BookLevel(price=float(p), size=float(s)) for p, s in pairs if float(s) > 0]
    out.sort(key=lambda lv: lv.price, reverse=descending)
    return out
