"""Tests for the shared fill simulation (latbot/exchanges/_fill.py)."""
from __future__ import annotations

import pytest

from latbot.clock import SimClock
from latbot.exchanges._fill import Ledger, PositionBook, SimBroker, _walk, _walk_pairs, levels_from_pairs
from latbot.exchanges.base import ExchangeError, InsufficientBalance, OrderRejected
from latbot.models import BookLevel, OrderBook, OrderRequest, OrderStatus, OrderType, Side, TimeInForce, Timestamps


def asks(*pairs: tuple[float, float]) -> list[BookLevel]:
    return levels_from_pairs(pairs, descending=False)


def bids(*pairs: tuple[float, float]) -> list[BookLevel]:
    return levels_from_pairs(pairs, descending=True)


def book(bid_pairs, ask_pairs, symbol: str = "BTC/USDT") -> OrderBook:
    return OrderBook(
        exchange="x",
        symbol=symbol,
        bids=bids(*bid_pairs),
        asks=asks(*ask_pairs),
        ts=Timestamps(recv_ts=1.0, recv_mono=1.0),
        asset="BTC",
    )


# --------------------------------------------------------------------------- #
# _walk / _walk_pairs
# --------------------------------------------------------------------------- #
def test_walk_base_amount_across_levels():
    r = _walk(asks((100, 1), (101, 2)), base_amount=2)
    assert r.filled_base == pytest.approx(2)
    assert r.filled_quote == pytest.approx(201)
    assert r.vwap == pytest.approx(100.5)
    assert r.levels_used == 2
    assert r.worst_price == 101
    assert r.exhausted is False


def test_walk_quote_amount():
    r = _walk(asks((100, 1), (101, 2)), quote_amount=150)
    assert r.filled_quote == pytest.approx(150)
    assert r.filled_base == pytest.approx(1 + 50 / 101)
    assert r.exhausted is False
    assert r.worst_price == 101


def test_walk_limit_price_stops_buy_and_sell():
    r = _walk(asks((100, 1), (101, 2)), base_amount=2, limit_price=100, side=Side.BUY)
    assert r.filled_base == pytest.approx(1)
    assert r.exhausted is True
    r2 = _walk(bids((100, 1), (99, 1)), base_amount=2, limit_price=99.5, side=Side.SELL)
    assert r2.filled_base == pytest.approx(1)
    assert r2.worst_price == 100


def test_walk_side_inferred_from_ordering():
    # descending prices -> bids -> SELL semantics: a bid below the limit stops the walk
    r = _walk([(100, 1), (99, 1)], base_amount=2, limit_price=99.5)
    assert r.filled_base == pytest.approx(1)


def test_walk_empty_and_thin_book_never_raises():
    r = _walk([], base_amount=1)
    assert r.filled_base == 0 and r.vwap is None and r.exhausted is True and r.worst_price is None
    r2 = _walk(asks((100, 0.5)), quote_amount=1000)
    assert r2.filled_base == pytest.approx(0.5) and r2.exhausted is True


def test_walk_requires_exactly_one_amount():
    with pytest.raises(ValueError):
        _walk(asks((100, 1)), base_amount=1, quote_amount=1)
    with pytest.raises(ValueError):
        _walk(asks((100, 1)))


def test_walk_pairs_skips_empty_levels_and_accepts_tuples():
    pairs = _walk_pairs([(100, 0), (101, 1), (102, 3)], base_amount=2, side=Side.BUY)
    assert pairs == [(101, 1), (102, 1)]
    assert _walk_pairs(asks((100, 1)), base_amount=0) == []


# --------------------------------------------------------------------------- #
# Ledger
# --------------------------------------------------------------------------- #
def test_ledger_spot_buy_sell():
    led = Ledger("spot")
    led.credit("USDT", 1000)
    realized, asset, debit = led.apply_fill("BTC/USDT", Side.BUY, 100, 2, 0.2)
    assert realized == 0 and asset == "USDT" and debit == pytest.approx(200.2)
    assert led.balance("USDT").free == pytest.approx(799.8)
    assert led.balance("USDT").total == pytest.approx(799.8)
    assert led.balance("BTC").free == pytest.approx(2)
    led.apply_fill("BTC/USDT", Side.SELL, 110, 1, 0.11)
    assert led.balance("BTC").total == pytest.approx(1)
    assert led.balance("USDT").free == pytest.approx(799.8 + 109.89)


def test_ledger_spot_prepaid_reduces_free_debit_only():
    led = Ledger("spot")
    led.credit("USDT", 1000)
    led.reserve("USDT", 300)
    assert led.balance("USDT").free == pytest.approx(700)
    led.apply_fill("BTC/USDT", Side.BUY, 100, 2, 0.0, prepaid=300)
    # 300 were already taken from free; the fill cost 200 -> 100 returned
    assert led.balance("USDT").free == pytest.approx(800)
    assert led.balance("USDT").total == pytest.approx(800)


def test_ledger_reserve_release():
    led = Ledger("spot")
    led.credit("EUR", 10)
    with pytest.raises(InsufficientBalance):
        led.reserve("EUR", 11)
    led.reserve("EUR", 4)
    assert led.balance("EUR").free == pytest.approx(6)
    led.release("EUR", 4)
    assert led.balance("EUR").free == pytest.approx(10)


def test_ledger_swap_short_open_reduce_flip():
    led = Ledger("swap")
    led.credit("USDT", 1000)
    sym = "BTC/USDT:USDT"
    realized, _, _ = led.apply_fill(sym, Side.SELL, 100, 1, 0.1)
    assert realized == 0
    assert led.positions[sym] == {"side": "short", "amount": 1, "entry": 100}
    assert led.balance("USDT").free == pytest.approx(1000 - 100 - 0.1)
    assert led.balance("USDT").total == pytest.approx(999.9)
    # reduce half at 90 -> +5 realized
    realized, _, _ = led.apply_fill(sym, Side.BUY, 90, 0.5, 0.045)
    assert realized == pytest.approx(5)
    assert led.positions[sym]["amount"] == pytest.approx(0.5)
    assert led.balance("USDT").free == pytest.approx(899.9 + 50 + 5 - 0.045)
    assert led.balance("USDT").total == pytest.approx(999.9 + 5 - 0.045)
    assert led.realized_pnl_quote == pytest.approx(5)
    # flip: buy 1.0 at 95 -> closes 0.5 (+2.5) and opens 0.5 long
    realized, _, _ = led.apply_fill(sym, Side.BUY, 95, 1.0, 0.0)
    assert realized == pytest.approx(2.5)
    assert led.positions[sym] == {"side": "long", "amount": pytest.approx(0.5), "entry": 95}
    assert led.realized_pnl_quote == pytest.approx(7.5)


def test_ledger_swap_averages_same_side():
    led = Ledger("swap")
    led.credit("USDT", 1000)
    led.apply_fill("BTC/USDT:USDT", Side.BUY, 100, 1, 0)
    led.apply_fill("BTC/USDT:USDT", Side.BUY, 110, 1, 0)
    pos = led.positions["BTC/USDT:USDT"]
    assert pos["side"] == "long" and pos["amount"] == 2 and pos["entry"] == pytest.approx(105)


def test_ledger_round_trip_and_position_book_callable():
    led = Ledger("swap")
    led.credit("USDT", 10)
    led.apply_fill("ETH/USDT:USDT", Side.SELL, 2, 1, 0)
    data = led.to_dict()
    back = Ledger.from_dict(data, "swap")
    assert back.balance("USDT").free == pytest.approx(led.balance("USDT").free)
    assert back.positions["ETH/USDT:USDT"] == led.positions["ETH/USDT:USDT"]
    assert isinstance(back.positions, PositionBook)
    assert back.positions() == {"ETH/USDT:USDT": {"side": "short", "amount": 1.0, "entry": 2.0}}
    assert back.positions() is not back.positions


# --------------------------------------------------------------------------- #
# SimBroker
# --------------------------------------------------------------------------- #
def make_broker(market_type: str = "spot", quote: float = 10_000.0) -> tuple[SimBroker, Ledger, SimClock]:
    clock = SimClock()
    led = Ledger(market_type)
    led.credit("USDT", quote)
    return SimBroker("x", market_type, clock, led, id_prefix="t"), led, clock


def req(side: Side, amount: float, *, type_: OrderType = OrderType.LIMIT, price=None, tif=TimeInForce.IOC, symbol="BTC/USDT"):
    return OrderRequest(exchange="x", symbol=symbol, side=side, amount=amount, type=type_, price=price, tif=tif)


def test_broker_ioc_full_fill():
    br, led, clock = make_broker()
    o, st = br.place(req(Side.BUY, 1, price=101), book([(99, 1)], [(100, 1), (101, 1)]), 0.001)
    assert st == [OrderStatus.FILLED]
    assert o.status == OrderStatus.FILLED and o.filled == 1 and o.avg_price == 100
    assert o.fee_cost_quote == pytest.approx(0.1)
    assert o.fills[0].fee_asset == "USDT" and o.fills[0].ts == clock.wall()
    assert o.sent_mono is not None and o.ack_mono is not None and o.done_mono is not None
    assert o.first_fill_mono is not None
    assert led.balance("USDT").free == pytest.approx(10_000 - 100.1)
    assert led.balance("BTC").free == 1
    assert o.id.startswith("t") and br.orders[o.id] is o


def test_broker_ioc_partial_then_cancelled_keeps_fills():
    br, led, _ = make_broker()
    o, st = br.place(req(Side.BUY, 1, price=101), book([], [(100, 0.4), (100.5, 0.3), (102, 5)]), 0.001)
    assert st == [OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED]
    assert o.status == OrderStatus.CANCELLED
    assert o.filled == pytest.approx(0.7) and len(o.fills) == 2
    assert o.remaining == pytest.approx(0.3)
    assert led.balance("BTC").free == pytest.approx(0.7)
    assert not br.open_orders()


def test_broker_ioc_no_fill_within_limit():
    br, led, _ = make_broker()
    o, st = br.place(req(Side.BUY, 1, price=99), book([], [(100, 5)]), 0.001)
    assert st == [OrderStatus.CANCELLED] and o.filled == 0 and o.error
    assert led.balance("USDT").free == 10_000


def test_broker_fok_all_or_nothing():
    br, _, _ = make_broker()
    o, st = br.place(req(Side.BUY, 1, price=101, tif=TimeInForce.FOK), book([], [(100, 0.5)]), 0.0)
    assert st == [OrderStatus.CANCELLED] and o.filled == 0 and not o.fills
    o2, _ = br.place(req(Side.BUY, 1, price=101, tif=TimeInForce.FOK), book([], [(100, 2)]), 0.0)
    assert o2.status == OrderStatus.FILLED


def test_broker_market_walks_fully_and_cancels_remainder_on_thin_book():
    br, _, _ = make_broker()
    o, st = br.place(req(Side.BUY, 3, type_=OrderType.MARKET), book([], [(100, 1), (110, 1)]), 0.0)
    assert st == [OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED]
    assert o.filled == 2 and o.avg_price == pytest.approx(105) and o.error == "book exhausted"
    with pytest.raises(OrderRejected):
        br.place(req(Side.BUY, 1, type_=OrderType.MARKET), None, 0.0)
    with pytest.raises(OrderRejected):
        br.place(req(Side.SELL, 1, type_=OrderType.MARKET), book([], [(100, 1)]), 0.0)


def test_broker_rejects_bad_requests():
    br, _, _ = make_broker()
    with pytest.raises(OrderRejected):
        br.place(req(Side.BUY, 0, price=100), book([], [(100, 1)]), 0.0)
    with pytest.raises(OrderRejected):
        br.place(req(Side.BUY, 1), book([], [(100, 1)]), 0.0)  # limit without price
    with pytest.raises(OrderRejected):
        br.place(req(Side.SELL, 1, price=100), book([(100, 1)], []), 0.0, reduce_only=True)  # spot


def test_broker_insufficient_balance_fills_nothing():
    br, led, _ = make_broker(quote=50)
    with pytest.raises(InsufficientBalance):
        br.place(req(Side.BUY, 1, price=100), book([], [(100, 1)]), 0.001)
    assert led.balance("USDT").free == 50 and not br.orders
    # spot sell needs the base asset
    with pytest.raises(InsufficientBalance):
        br.place(req(Side.SELL, 1, price=100), book([(100, 1)], []), 0.001)


def test_broker_gtc_rests_reserves_and_refreshes():
    br, led, clock = make_broker()
    o, st = br.place(req(Side.BUY, 2, price=101, tif=TimeInForce.GTC), book([], [(100, 0.5), (105, 5)]), 0.001)
    assert st == [OrderStatus.PARTIALLY_FILLED]
    assert o.status == OrderStatus.PARTIALLY_FILLED and o.filled == 0.5
    assert br.open_orders() == [o] and br.open_orders("ETH/USDT") == []
    # 0.5 @ 100 paid (+fee), 1.5 @ 101 (+fee) reserved
    expect_free = 10_000 - 50 * 1.001 - 1.5 * 101 * 1.001
    assert led.balance("USDT").free == pytest.approx(expect_free)
    assert led.balance("USDT").total == pytest.approx(10_000 - 50 * 1.001)
    same, changed = br.refresh(o.id, book([], [(105, 5)]))
    assert same is o and changed is False
    assert br.refresh(o.id, None) == (o, False)
    clock.advance(1)
    o2, changed = br.refresh(o.id, book([], [(100.5, 1), (101, 3)]))
    assert changed is True and o2.status == OrderStatus.FILLED
    assert o2.filled == pytest.approx(2) and len(o2.fills) == 3
    cost = 50 + 100.5 + 0.5 * 101
    assert led.balance("USDT").free == pytest.approx(10_000 - cost * 1.001)
    assert led.balance("USDT").total == pytest.approx(led.balance("USDT").free)
    assert led.balance("BTC").free == pytest.approx(2)
    assert o2.done_mono == clock.mono() and not br.open_orders()


def test_broker_gtc_no_immediate_fill_is_acknowledged():
    br, led, _ = make_broker()
    o, st = br.place(req(Side.BUY, 1, price=90, tif=TimeInForce.GTC), book([], [(100, 1)]), 0.0)
    assert st == [OrderStatus.ACKNOWLEDGED] and o.status == OrderStatus.ACKNOWLEDGED
    assert led.balance("USDT").free == pytest.approx(10_000 - 90)
    cancelled = br.cancel(o.id)
    assert cancelled.status == OrderStatus.CANCELLED and led.balance("USDT").free == 10_000
    assert br.cancel(o.id) is cancelled  # no-op on done
    with pytest.raises(ExchangeError):
        br.cancel("nope")
    with pytest.raises(ExchangeError):
        br.refresh("nope", None)


def test_broker_max_fill_cap():
    br, _, _ = make_broker()
    o, st = br.place(req(Side.BUY, 1, price=101), book([], [(100, 5)]), 0.0, max_fill=0.25)
    assert o.filled == pytest.approx(0.25) and st == [OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED]


def test_broker_swap_reduce_only_and_positions():
    br, led, _ = make_broker("swap")
    sym = "BTC/USDT:USDT"
    with pytest.raises(OrderRejected):
        br.place(req(Side.BUY, 1, price=100, symbol=sym), book([], [(100, 1)], sym), 0.0, reduce_only=True)
    o, _ = br.place(req(Side.SELL, 2, type_=OrderType.MARKET, symbol=sym), book([(100, 5)], [(101, 5)], sym), 0.001)
    assert o.status == OrderStatus.FILLED
    assert led.positions[sym] == {"side": "short", "amount": 2, "entry": 100}
    assert led.balance("USDT").free == pytest.approx(10_000 - 200 - 0.2)
    # reduce-only buy for more than the position is capped to the position
    o2, _ = br.place(req(Side.BUY, 5, price=95, symbol=sym), book([(90, 5)], [(90.5, 5)], sym), 0.0, reduce_only=True)
    assert o2.amount == 2 and o2.status == OrderStatus.FILLED and o2.filled == 2
    assert sym not in led.positions
    assert led.realized_pnl_quote == pytest.approx((100 - 90.5) * 2)
    assert led.balance("USDT").total == pytest.approx(10_000 - 0.2 + 19)
    assert led.balance("USDT").free == pytest.approx(led.balance("USDT").total)


def test_broker_swap_affordability_counts_released_margin():
    br, led, _ = make_broker("swap", quote=100)
    sym = "BTC/USDT:USDT"
    br.place(req(Side.BUY, 1, type_=OrderType.MARKET, symbol=sym), book([(99, 5)], [(100, 5)], sym), 0.0)
    assert led.balance("USDT").free == pytest.approx(0)
    # closing needs only the fee which the released margin covers
    o, _ = br.place(req(Side.SELL, 1, type_=OrderType.MARKET, symbol=sym), book([(99, 5)], [(100, 5)], sym), 0.001)
    assert o.status == OrderStatus.FILLED
    assert led.balance("USDT").free == pytest.approx(100 - 1 - 0.099)
    with pytest.raises(InsufficientBalance):
        br.place(req(Side.BUY, 5, type_=OrderType.MARKET, symbol=sym), book([(99, 5)], [(100, 5)], sym), 0.0)
