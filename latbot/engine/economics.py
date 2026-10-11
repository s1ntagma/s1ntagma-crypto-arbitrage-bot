"""Executable economics of one round trip on the target exchange.

Everything is computed on the REAL order book: VWAP by walking levels, depth
within ``max_slippage_pct``, executable size, fees for both taker legs and the
execution-loss allowance.  ``net_pct`` is the only number shown as profit.
Spread is already inside the VWAPs and is reported, never added again.
Never raises on an empty side.
"""
from __future__ import annotations

import logging

from ..config import Settings
from ..models import BookLevel, ExecutionEconomics, Fees, OrderBook, Side, WalkResult

log = logging.getLogger(__name__)

_REL_EPS = 1e-9


def _side_of(levels: list[BookLevel], side: Side | str | None) -> Side | None:
    """Resolve the side a level list belongs to.  Explicit ``side`` wins;
    otherwise ascending prices = asks (buy), descending = bids (sell);
    a single level (or flat prices) cannot reveal its side -> None."""
    if side is not None:
        s = side.value if isinstance(side, Side) else str(side).lower()
        if s in ("buy", "ask", "asks"):
            return Side.BUY
        if s in ("sell", "bid", "bids"):
            return Side.SELL
        raise ValueError(f"unknown side {side!r}")
    if len(levels) >= 2:
        if levels[1].price > levels[0].price:
            return Side.BUY
        if levels[1].price < levels[0].price:
            return Side.SELL
    return None


def walk_book(
    levels: list[BookLevel],
    *,
    quote_amount: float | None = None,
    base_amount: float | None = None,
    limit_price: float | None = None,
    side: Side | str | None = None,
) -> WalkResult:
    """Consume ``levels`` in order until ``quote_amount`` (or ``base_amount``,
    whichever is given; both act as caps when both are given) is filled.

    ``limit_price`` stops the walk at levels beyond the limit (asks above it
    when buying, bids below it when selling).  The side is inferred from the
    price ordering; pass ``side`` for one-level books.  With no amount the
    whole (limit-bounded) side is consumed.  ``exhausted`` is True when a
    requested amount was not reached.
    """
    want_quote = quote_amount if quote_amount is not None and quote_amount > 0 else None
    want_base = base_amount if base_amount is not None and base_amount > 0 else None
    resolved = _side_of(levels, side)
    if limit_price is not None and resolved is None and len(levels) > 1:
        log.debug("walk_book: limit_price given but side unknown; limit ignored")

    filled_base = 0.0
    filled_quote = 0.0
    used = 0
    worst: float | None = None
    rem_quote = want_quote
    rem_base = want_base
    for lvl in levels:
        if lvl.size <= 0 or lvl.price <= 0:
            continue
        if limit_price is not None and resolved is not None:
            if resolved is Side.BUY and lvl.price > limit_price:
                break
            if resolved is Side.SELL and lvl.price < limit_price:
                break
        take = lvl.size
        if rem_base is not None:
            take = min(take, rem_base)
        if rem_quote is not None:
            take = min(take, rem_quote / lvl.price)
        if take <= 0:
            break
        filled_base += take
        filled_quote += take * lvl.price
        used += 1
        worst = lvl.price
        if rem_base is not None:
            rem_base -= take
        if rem_quote is not None:
            rem_quote -= take * lvl.price
        if (rem_base is not None and rem_base <= _REL_EPS * (want_base or 1.0)) or (
            rem_quote is not None and rem_quote <= _REL_EPS * (want_quote or 1.0)
        ):
            break

    reached_quote = want_quote is not None and filled_quote >= want_quote * (1.0 - _REL_EPS)
    reached_base = want_base is not None and filled_base >= want_base * (1.0 - _REL_EPS)
    if want_quote is None and want_base is None:
        exhausted = False
    else:
        exhausted = not (reached_quote or reached_base)
    vwap = filled_quote / filled_base if filled_base > 0 else None
    return WalkResult(
        vwap=vwap,
        filled_base=filled_base,
        filled_quote=filled_quote,
        levels_used=used,
        exhausted=exhausted,
        worst_price=worst,
    )


def depth_within(levels: list[BookLevel], max_slippage_pct: float) -> float:
    """Quote depth (sum price*size) of the levels within ``best * (1 +- slip)``."""
    if not levels:
        return 0.0
    best = levels[0].price
    if best <= 0:
        return 0.0
    slip = max(0.0, max_slippage_pct) / 100.0
    side = _side_of(levels, None)
    total = 0.0
    for lvl in levels:
        if lvl.size <= 0 or lvl.price <= 0:
            continue
        if side is Side.BUY and lvl.price > best * (1.0 + slip) * (1.0 + _REL_EPS):
            break
        if side is Side.SELL and lvl.price < best * (1.0 - slip) * (1.0 - _REL_EPS):
            break
        total += lvl.price * lvl.size
    return total


def _money(x: float) -> str:
    return f"${x:,.2f}"


def entry_cap_price(exit_target: float, direction: int, total_cost_pct: float, min_net_pct: float) -> float:
    """Worst entry price at which the round trip still nets ``min_net_pct``
    against ``exit_target`` after ``total_cost_pct`` (fees + execution loss).

    LONG (+1): the highest BUY price  = exit_target / (1 + x)
    SHORT (-1): the lowest SELL price = exit_target / (1 - x)
    with x = (total_cost_pct + min_net_pct) / 100 (net = gross - total_cost,
    gross = (exit_target / entry - 1) * 100 * direction).  0.0 when there is
    no usable exit price or the costs alone exceed 100 %.
    """
    if exit_target is None or exit_target <= 0:
        return 0.0
    x = (max(0.0, float(total_cost_pct)) + max(0.0, float(min_net_pct))) / 100.0
    if direction > 0:
        cap = exit_target / (1.0 + x)
    else:
        if x >= 1.0:
            return 0.0
        cap = exit_target / (1.0 - x)
    return max(0.0, cap)


def compute_economics(
    book: OrderBook,
    direction: int,
    fees: Fees,
    capital_quote: float,
    expected_catchup_pct: float,
    settings: Settings,
    now_mono: float,
) -> ExecutionEconomics:
    """Economics of one round trip of ``capital_quote`` on ``book``.

    direction +1 (LONG): entry = BUY walking asks, exit = SELL walking bids.
    direction -1 (SHORT): entry = SELL walking bids, exit = BUY walking asks.
    """
    d = 1 if direction >= 0 else -1
    capital = max(0.0, float(capital_quote))
    slip = settings.max_slippage_pct
    taker_pct = fees.taker_pct
    exec_loss = settings.execution_loss_pct
    total_cost = taker_pct * 2.0 + exec_loss
    bid_depth = depth_within(book.bids, slip)
    ask_depth = depth_within(book.asks, slip)
    age_ms = book.age_ms(now_mono)
    entry_levels = book.asks if d > 0 else book.bids
    exit_levels = book.bids if d > 0 else book.asks
    entry_side = Side.BUY if d > 0 else Side.SELL
    exit_side = Side.SELL if d > 0 else Side.BUY
    spread = book.spread_pct or 0.0

    crossed = book.is_broken  # a broken book (bid >= ask, stale levels) is a data error, never a price (hard rule 1)
    if not entry_levels or not exit_levels or crossed:
        px_in = entry_levels[0].price if entry_levels and not crossed else 0.0
        px_out = exit_levels[0].price if exit_levels and not crossed else 0.0
        return ExecutionEconomics(
            direction=d,
            capital_quote=capital,
            executable_quote=0.0,
            base_amount=0.0,
            entry_best=px_in,
            entry_vwap=px_in,
            exit_best=px_out,
            exit_vwap_now=px_out,
            exit_target=px_out,
            spread_pct=spread,
            entry_fee_pct=taker_pct,
            exit_fee_pct=taker_pct,
            entry_slippage_pct=0.0,
            exit_slippage_pct=0.0,
            execution_loss_pct=exec_loss,
            gross_pct=0.0,
            total_cost_pct=total_cost,
            net_pct=-total_cost,
            net_quote=0.0,
            liquidity_ok=False,
            liquidity_reason=(f"broken book ({book.broken or 'bid >= ask'}): data error, no price" if crossed
                              else "empty book side"),
            book_age_ms=age_ms,
            bid_depth_quote=bid_depth,
            ask_depth_quote=ask_depth,
        )

    entry_depth = ask_depth if d > 0 else bid_depth
    exit_depth = bid_depth if d > 0 else ask_depth
    executable = max(0.0, min(capital, entry_depth, exit_depth))
    ok = True
    reason = ""
    if executable < settings.min_trade_quote:
        ok = False
        reason = (
            f"book supports only {_money(executable)} within {slip:g}% slippage "
            f"(min trade {_money(settings.min_trade_quote)})"
        )
    elif entry_depth < settings.min_liquidity_quote or exit_depth < settings.min_liquidity_quote:
        ok = False
        thin = "entry" if entry_depth < settings.min_liquidity_quote else "exit"
        depth = entry_depth if thin == "entry" else exit_depth
        reason = (
            f"{thin} side has only {_money(depth)} within {slip:g}% slippage "
            f"(min liquidity {_money(settings.min_liquidity_quote)})"
        )
    elif capital > 0 and executable < capital * settings.min_fill_ratio:
        ok = False
        reason = f"book supports only {_money(executable)} of {_money(capital)} requested"

    entry_best = entry_levels[0].price
    exit_best = exit_levels[0].price
    base = 0.0
    entry_vwap = entry_best
    exit_vwap_now = exit_best
    if executable > 0:
        entry_walk = walk_book(entry_levels, quote_amount=executable, side=entry_side)
        if entry_walk.filled_base > 0 and entry_walk.vwap:
            base = entry_walk.filled_base
            entry_vwap = entry_walk.vwap
            exit_walk = walk_book(exit_levels, base_amount=base, side=exit_side)
            if exit_walk.exhausted:
                # the whole exit side is thinner than our position: use what is there
                ok = False
                reason = reason or (
                    f"exit side supports only {exit_walk.filled_base:.6g} of {base:.6g} base"
                )
                if exit_walk.filled_base > 0 and exit_walk.vwap:
                    base = exit_walk.filled_base
                    executable = min(executable, base * entry_vwap)
                    exit_vwap_now = exit_walk.vwap
            elif exit_walk.vwap:
                exit_vwap_now = exit_walk.vwap
        else:
            ok = False
            reason = reason or "entry side empty within request"

    exit_target = exit_vwap_now * (1.0 + d * expected_catchup_pct / 100.0)
    gross = (exit_target / entry_vwap - 1.0) * 100.0 * d if entry_vwap > 0 else 0.0
    net = gross - total_cost
    net_quote = executable * net / 100.0
    entry_slip = abs(entry_vwap - entry_best) / entry_best * 100.0 if entry_best > 0 else 0.0
    exit_slip = abs(exit_best - exit_vwap_now) / exit_best * 100.0 if exit_best > 0 else 0.0
    cap = entry_cap_price(exit_target, d, total_cost, settings.min_net_pct_show)

    return ExecutionEconomics(
        direction=d,
        capital_quote=capital,
        executable_quote=executable,
        base_amount=base,
        entry_best=entry_best,
        entry_vwap=entry_vwap,
        exit_best=exit_best,
        exit_vwap_now=exit_vwap_now,
        exit_target=exit_target,
        spread_pct=spread,
        entry_fee_pct=taker_pct,
        exit_fee_pct=taker_pct,
        entry_slippage_pct=entry_slip,
        exit_slippage_pct=exit_slip,
        execution_loss_pct=exec_loss,
        gross_pct=gross,
        total_cost_pct=total_cost,
        net_pct=net,
        net_quote=net_quote,
        liquidity_ok=ok,
        liquidity_reason=reason,
        book_age_ms=age_ms,
        bid_depth_quote=bid_depth,
        ask_depth_quote=ask_depth,
        entry_cap_price=cap,
    )
