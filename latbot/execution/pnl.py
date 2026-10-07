"""Realized P&L and "expected vs actual" explanation for one round trip.

``finalize`` is pure: it reads the entry / exit orders of a ``TradeRecord``
(or the explicit numbers the manual flow passes) and fills the ``actual_*``
fields, ``realized_pnl_quote``, ``actual_net_pct``, ``actual_slippage_pct``,
``execution_time_s``, ``entry_latency_ms`` and ``diff_reason``.  It never
touches ``state`` or ``finished_ts``.

Sign conventions
----------------
* ``direction`` +1 LONG (buy then sell), -1 SHORT (sell then buy).
* ``realized_pnl_quote = direction * (exit - entry) * closed_base - fees``.
* Slippage is positive when adverse: paying more than the expected entry or
  receiving less than the expected exit.  ``actual_slippage_pct`` is the sum of
  the entry and the exit part; the exit part is reported as "catch-up short".
"""
from __future__ import annotations

import logging

from ..links import split_symbol
from ..models import Order, TradeRecord

log = logging.getLogger(__name__)

_EPS = 1e-12


def _order_filled(order: Order | None) -> float:
    if order is None:
        return 0.0
    if order.filled and order.filled > 0:
        return float(order.filled)
    return float(sum(f.amount for f in order.fills)) if order.fills else 0.0


def _order_price(order: Order | None) -> float | None:
    """avg_price if set, else the VWAP of the fills."""
    if order is None:
        return None
    if order.avg_price is not None and order.avg_price > 0:
        return float(order.avg_price)
    if order.fills:
        amount = sum(f.amount for f in order.fills)
        if amount > _EPS:
            return float(sum(f.price * f.amount for f in order.fills) / amount)
    return None


def _order_fees_quote(order: Order | None, base: str, quote: str) -> float:
    """fee_cost_quote when the adapter filled it, else converted from fills."""
    if order is None:
        return 0.0
    if order.fee_cost_quote and order.fee_cost_quote > 0:
        return float(order.fee_cost_quote)
    total = 0.0
    for f in order.fills:
        asset = (f.fee_asset or "").upper()
        if not asset or asset == quote:
            total += float(f.fee_cost)
        elif asset == base:
            total += float(f.fee_cost) * float(f.price)
        else:
            log.debug("fee in %s not converted to %s (order %s)", asset, quote, order.id)
    return total


def _pct(a: float | None, b: float | None) -> float | None:
    """(a / b - 1) * 100 when both are known and b > 0."""
    if a is None or b is None or b <= 0:
        return None
    return (a / b - 1.0) * 100.0


def finalize(
    trade: TradeRecord,
    fees_quote: float | None = None,
    *,
    entry_price: float | None = None,
    exit_price: float | None = None,
    base_amount: float | None = None,
    entry_sent_mono: float | None = None,
    exit_done_mono: float | None = None,
) -> TradeRecord:
    """Fill the actual_* fields of ``trade`` in place and return it.

    ``fees_quote`` overrides the fees derived from the orders.  The keyword
    arguments let the manual flow (no orders) pass the user-reported prices,
    the amount that was round-tripped and the timing of the two legs.
    """
    d = 1 if trade.direction >= 0 else -1
    base_ccy, quote_ccy = split_symbol(trade.symbol or "")
    entry, exit_ = trade.entry_order, trade.exit_order

    entry_filled = _order_filled(entry)
    exit_filled = _order_filled(exit_)
    if base_amount is not None:
        closed_base = max(0.0, float(base_amount))
        if entry_filled <= 0:
            entry_filled = closed_base
        if exit_filled <= 0:
            exit_filled = closed_base
    else:
        closed_base = min(entry_filled, exit_filled)

    actual_entry = entry_price if entry_price is not None else (_order_price(entry) if entry_filled > 0 else None)
    if actual_entry is None:
        actual_entry = trade.actual_entry_price
    actual_exit = exit_price if exit_price is not None else (_order_price(exit_) if exit_filled > 0 else None)
    if actual_exit is None:
        actual_exit = trade.actual_exit_price

    if fees_quote is not None:
        fees = float(fees_quote)
    else:
        fees = _order_fees_quote(entry, base_ccy, quote_ccy) + _order_fees_quote(exit_, base_ccy, quote_ccy)

    round_trip = closed_base > _EPS and actual_entry is not None and actual_exit is not None
    if round_trip:
        realized = d * (float(actual_exit) - float(actual_entry)) * closed_base - fees
        cost_basis = float(actual_entry) * closed_base
        net_pct = realized / cost_basis * 100.0 if cost_basis > _EPS else None
    else:
        realized = -fees if fees > 0 else 0.0
        net_pct = None

    entry_slip = _pct(actual_entry, trade.expected_entry_price)
    entry_slip = None if entry_slip is None else d * entry_slip
    exit_slip = _pct(actual_exit, trade.expected_exit_price)
    exit_slip = None if exit_slip is None else -d * exit_slip
    if entry_slip is None and exit_slip is None:
        slippage: float | None = None
    else:
        slippage = (entry_slip or 0.0) + (exit_slip or 0.0)

    sent = entry.sent_mono if entry is not None and entry.sent_mono is not None else entry_sent_mono
    done = exit_.done_mono if exit_ is not None and exit_.done_mono is not None else exit_done_mono
    exec_time = (done - sent) if sent is not None and done is not None and done >= sent else trade.execution_time_s
    latency = entry.latency_ms() if entry is not None else None
    if latency is None:
        latency = trade.entry_latency_ms

    trade.actual_entry_price = actual_entry
    trade.actual_exit_price = actual_exit
    trade.actual_fees_quote = fees
    trade.realized_pnl_quote = realized
    trade.actual_net_pct = net_pct
    trade.actual_slippage_pct = slippage
    trade.execution_time_s = exec_time
    trade.entry_latency_ms = latency
    trade.diff_reason = _diff_reason(
        trade,
        entry_slip=entry_slip,
        exit_slip=exit_slip,
        fees=fees,
        cost_basis=(float(actual_entry) * closed_base) if round_trip else 0.0,
        latency_ms=latency,
        entry_filled=entry_filled,
        exit_filled=exit_filled,
        closed_base=closed_base,
    )
    return trade


def _diff_reason(
    trade: TradeRecord,
    *,
    entry_slip: float | None,
    exit_slip: float | None,
    fees: float,
    cost_basis: float,
    latency_ms: float | None,
    entry_filled: float,
    exit_filled: float,
    closed_base: float,
) -> str:
    """'expected +4.80% -> actual +3.95%: slippage +0.31%, fees 0.11%, latency 820 ms, catch-up short by 0.40%'."""
    if trade.actual_net_pct is not None:
        head = f"expected {trade.expected_net_pct:+.2f}% -> actual {trade.actual_net_pct:+.2f}%"
    else:
        head = f"expected {trade.expected_net_pct:+.2f}% -> no round trip"
    parts: list[str] = []
    if entry_slip is not None:
        parts.append(f"slippage {entry_slip:+.2f}%")
    if fees > 0:
        if cost_basis > _EPS:
            parts.append(f"fees {fees / cost_basis * 100.0:.2f}%")
        else:
            parts.append(f"fees {fees:.2f} {split_symbol(trade.symbol or '')[1] or 'quote'}")
    if latency_ms is not None:
        parts.append(f"latency {latency_ms:.0f} ms")
    if exit_slip is not None and abs(exit_slip) >= 0.005:
        if exit_slip > 0:
            parts.append(f"catch-up short by {exit_slip:.2f}%")
        else:
            parts.append(f"catch-up better by {-exit_slip:.2f}%")
    entry = trade.entry_order
    if entry is not None and entry.amount > _EPS:
        if entry_filled <= _EPS:
            parts.append("no fill")
        elif entry_filled < entry.amount * (1.0 - 1e-6):
            parts.append(f"partial fill {entry_filled / entry.amount * 100.0:.0f}%")
    exit_ = trade.exit_order
    if exit_ is not None and exit_filled > _EPS:
        market = 0.0
        if exit_.type.value == "market":
            market = exit_filled
        elif isinstance(exit_.raw, dict):
            try:
                market = float(exit_.raw.get("market_filled") or 0.0)
            except (TypeError, ValueError):
                market = 0.0
        if market > _EPS:
            parts.append("exit at market" if market >= exit_filled * (1.0 - 1e-6) else f"exit {market / exit_filled * 100.0:.0f}% at market")
    if entry_filled > _EPS and closed_base + _EPS < entry_filled:
        parts.append(f"position open {entry_filled - closed_base:.8g} {split_symbol(trade.symbol or '')[0] or 'base'}")
    if trade.error:
        parts.append(str(trade.error))
    return head + (": " + ", ".join(parts) if parts else "")
