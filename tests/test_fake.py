"""Tests for FakeAdapter (test/backtest adapter)."""
from __future__ import annotations

from typing import Any

import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import ExchangeConfig
from latbot.events import EventBus
from latbot.exchanges.base import ExchangeError, OrderRejected
from latbot.exchanges.fake import FakeAdapter
from latbot.models import ExchangeRole, OrderRequest, OrderStatus, OrderType, Side, TimeInForce


def make(market_type: str = "spot", symbols: dict[str, str] | None = None, **kw: Any):
    bus = EventBus()
    clock = SimClock()
    cfg = ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, market_type=market_type,
                         allow_short=market_type == "swap", taker_fee=0.002, maker_fee=0.001)
    seen: list[tuple[str, Any]] = []
    bus.subscribe_all(lambda t, p: seen.append((t, p)))
    return FakeAdapter(cfg, bus, clock, symbols=symbols, **kw), clock, seen


def of(seen, topic):
    return [p for t, p in seen if t == topic]


def req(symbol: str, side: Side, amount: float, *, type_=OrderType.LIMIT, price=None, tif=TimeInForce.IOC):
    return OrderRequest(exchange="kraken", symbol=symbol, side=side, amount=amount, type=type_, price=price, tif=tif)


async def test_push_book_stores_sorts_publishes_and_marks_health():
    fake, clock, seen = make(symbols={"BTC": "BTC/EUR"})
    assert fake.health.ws_connected is False
    book = await fake.push_book("btc", [(99, 1), (100, 2), (98, 0)], [(103, 1), (101, 1)], exchange_ts=clock.wall() - 0.05)
    assert book.exchange == "kraken" and book.symbol == "BTC/EUR" and book.asset == "BTC" and book.seq == 1
    assert [lv.price for lv in book.bids] == [100, 99] and [lv.price for lv in book.asks] == [101, 103]
    assert book.ts.recv_ts == clock.wall() and book.ts.recv_mono == clock.mono()
    assert of(seen, events.BOOK) == [book]
    assert fake.order_book("BTC") is book and fake.order_book("BTC/EUR") is book and fake.order_book("ETH") is None
    assert fake.health.ws_connected is True and fake.health.last_msg_mono == clock.mono()
    assert fake.health.ws_latency_ms == pytest.approx(50.0)
    assert fake.health.is_usable(clock.mono())
    b2 = await fake.push_book("BTC", [(100, 1)], [(101, 1)])
    assert b2.seq == 2 and b2.ts.exchange_ts is None


async def test_start_symbols_and_mapping():
    fake, _, _ = make(symbols={"BTC": "BTC/EUR"})
    await fake.start(["BTC", "eth", "SOL/USDC"])
    assert fake.supported_symbols() == {"BTC/EUR", "ETH/USDT", "SOL/USDT"}
    assert fake.symbol_for("BTC") == "BTC/EUR" and fake.symbol_for("doge") == "DOGE/USDT"
    assert fake.asset_for("BTC/EUR") == "BTC" and fake.asset_for("XRP/USDT:USDT") == "XRP"
    await fake.stop()
    assert fake.market("BTC") is None
    fake.markets["BTC"] = {"info": {"launchTime": "1"}}
    assert fake.market("btc") == {"info": {"launchTime": "1"}}


async def test_default_and_custom_balances_and_fees():
    fake, _, _ = make()
    bal = await fake.fetch_balances()
    assert bal["USDT"].free == 100_000 and bal["EUR"].total == 100_000
    fake.credit("BTC", 2)
    assert (await fake.fetch_balances())["BTC"].free == 2
    f = await fake.fees("BTC/USDT")
    assert f.taker == 0.002 and f.maker == 0.001
    fake2, _, _ = make(balances={"usdt": 5})
    assert set(await fake2.fetch_balances()) == {"USDT"} and (await fake2.fetch_balances())["USDT"].free == 5


async def test_place_order_fills_and_publishes():
    fake, clock, seen = make()
    await fake.push_book("BTC", [(99, 5)], [(100, 1), (101, 1)])
    seen.clear()
    o = await fake.place_order(req("BTC/USDT", Side.BUY, 1.5, type_=OrderType.MARKET))
    assert o.status is OrderStatus.FILLED and o.filled == 1.5 and o.id.startswith("f")
    assert o.fee_cost_quote == pytest.approx(150.5 * 0.002)
    assert fake.placed and fake.placed[0].amount == 1.5
    assert [u.status for u in of(seen, events.ORDER_UPDATE)] == [OrderStatus.FILLED]
    assert of(seen, events.BALANCE)[0]["mode"] == "paper" and of(seen, events.BALANCE)[0]["balances"]["BTC"].free == 1.5
    assert fake.health.order_latency_ms is None  # no latency knob -> nothing marked


async def test_reject_next_knob_is_one_shot():
    fake, _, _ = make()
    await fake.push_book("BTC", [(99, 5)], [(100, 5)])
    fake.reject_next = True
    with pytest.raises(OrderRejected):
        await fake.place_order(req("BTC/USDT", Side.BUY, 1, price=100))
    assert fake.reject_next is False and len(fake.placed) == 1
    o = await fake.place_order(req("BTC/USDT", Side.BUY, 1, price=100))
    assert o.status is OrderStatus.FILLED


async def test_partial_ratio_knob():
    fake, _, seen = make()
    await fake.push_book("BTC", [(99, 5)], [(100, 5)])
    fake.partial_ratio = 0.5
    o = await fake.place_order(req("BTC/USDT", Side.BUY, 2, price=100))
    assert o.status is OrderStatus.CANCELLED and o.filled == pytest.approx(1)
    assert [u.status for u in of(seen, events.ORDER_UPDATE)][-2:] == [OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED]
    g = await fake.place_order(req("BTC/USDT", Side.BUY, 2, price=100, tif=TimeInForce.GTC))
    assert g.status is OrderStatus.PARTIALLY_FILLED and g.filled == pytest.approx(1)
    assert await fake.open_orders() == [g]
    fake.partial_ratio = None
    # the remainder fills on refresh against the same book
    g2 = await fake.fetch_order(g.id, "BTC/USDT")
    assert g2 is g and g2.status is OrderStatus.FILLED and g2.filled == pytest.approx(2)


async def test_fill_latency_knob_marks_latency():
    fake, _, _ = make()
    await fake.push_book("BTC", [(99, 5)], [(100, 5)])
    fake.fill_latency_s = 0.005
    o = await fake.place_order(req("BTC/USDT", Side.BUY, 1, price=100))
    assert o.status is OrderStatus.FILLED
    assert fake.health.order_latency_ms == pytest.approx(5.0)
    assert fake.health.fill_latency_ms == pytest.approx(5.0)


async def test_set_health():
    fake, clock, _ = make()
    h = fake.set_health(ws_connected=True, last_msg_mono=clock.mono(), order_latency_ms=120.0)
    assert h is fake.health and fake.health.is_usable(clock.mono()) and fake.health.order_latency_ms == 120.0
    fake.set_health(stale=True)
    assert not fake.health.is_usable(clock.mono())
    with pytest.raises(ValueError):
        fake.set_health(nonsense=1)


async def test_fetch_tickers_and_ticker():
    fake, _, _ = make()
    await fake.push_book("BTC", [(99, 5)], [(101, 5)])
    await fake.push_book("ETH", [(9, 5)], [(11, 5)])
    fake.volumes.update({"BTC": 1e6, "SOL": 5e5})
    tk = await fake.fetch_tickers()
    assert set(tk) == {"BTC", "ETH", "SOL"}
    assert tk["BTC"].bid == 99 and tk["BTC"].ask == 101 and tk["BTC"].last == 100 and tk["BTC"].quote_volume == 1e6
    assert tk["ETH"].quote_volume is None and tk["SOL"].bid is None and tk["SOL"].symbol == "SOL/USDT"
    assert (await fake.fetch_ticker("BTC/USDT")).asset == "BTC"
    assert set(await fake.fetch_tickers(["eth"])) == {"ETH"}
    with pytest.raises(ExchangeError):
        await fake.fetch_ticker("XRP/USDT")
    fake.fail_fetch_tickers = True
    with pytest.raises(ExchangeError):
        await fake.fetch_tickers()


async def test_fetch_order_book_rest():
    fake, clock, _ = make()
    with pytest.raises(ExchangeError):
        await fake.fetch_order_book("BTC/USDT")
    await fake.push_book("BTC", [(99, 1), (98, 1), (97, 1)], [(100, 1), (101, 1)])
    clock.advance(1)
    snap = await fake.fetch_order_book("BTC/USDT", limit=2)
    assert len(snap.bids) == 2 and len(snap.asks) == 2 and snap.ts.recv_mono == clock.mono()
    assert len(fake.order_book("BTC").bids) == 3  # stored book untouched


async def test_push_trade_and_ticker_events():
    fake, _, seen = make()
    t = await fake.push_trade("BTC", 100.0, 0.5, Side.SELL)
    assert of(seen, events.TRADE) == [t] and t.symbol == "BTC/USDT" and t.side is Side.SELL
    tk = await fake.push_ticker("BTC", 99.0, 101.0, quote_volume=42.0)
    assert of(seen, events.TICKER) == [tk] and tk.last == 100.0 and tk.quote_volume == 42.0 and fake.volumes["BTC"] == 42.0


async def test_swap_positions_reduce_only_and_cancel_all():
    fake, _, _ = make("swap", symbols={"BTC": "BTC/USDT:USDT"})
    sym = "BTC/USDT:USDT"
    await fake.push_book("BTC", [(100, 5)], [(101, 5)])
    o = await fake.place_order(req(sym, Side.SELL, 1, type_=OrderType.MARKET))
    assert o.status is OrderStatus.FILLED and fake.positions[sym]["side"] == "short"
    with pytest.raises(OrderRejected):
        await fake.place_order(req(sym, Side.SELL, 1, type_=OrderType.MARKET), reduce_only=True)
    await fake.push_book("BTC", [(95, 5)], [(96, 5)])
    c = await fake.place_order(req(sym, Side.BUY, 1, price=96), reduce_only=True)
    assert c.status is OrderStatus.FILLED and sym not in fake.positions and fake.realized_pnl_quote == pytest.approx(4)
    g1 = await fake.place_order(req(sym, Side.BUY, 1, price=90, tif=TimeInForce.GTC))
    g2 = await fake.place_order(req(sym, Side.SELL, 1, price=120, tif=TimeInForce.GTC))
    assert {o.id for o in await fake.open_orders(sym)} == {g1.id, g2.id}
    assert await fake.cancel_all() == 2 and await fake.open_orders() == []
    assert (await fake.cancel_order(g1.id, sym)) is g1 and g1.status is OrderStatus.CANCELLED
