"""Tests for PaperAdapter (simulated execution over a FakeAdapter feed)."""
from __future__ import annotations

from typing import Any

import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Settings
from latbot.events import EventBus
from latbot.exchanges.base import ExchangeError, InsufficientBalance, OrderRejected
from latbot.exchanges.fake import FakeAdapter
from latbot.exchanges.paper import PaperAdapter
from latbot.models import ExchangeRole, OrderRequest, OrderStatus, OrderType, Side, TimeInForce

SPOT = "BTC/EUR"
SWAP = "BTC/USDT:USDT"


class KV:
    """db stub with the kv_get/kv_set API."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def kv_get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def kv_set(self, key: str, value: Any) -> None:
        self.data[key] = value


class Env:
    def __init__(self, market_type: str = "spot", *, db: Any = None, initial_quote: float | None = None,
                 settings: Settings | None = None, seed: int | None = 1) -> None:
        self.bus = EventBus()
        self.clock = SimClock()
        self.cfg = ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, market_type=market_type,
                                  allow_short=market_type == "swap", taker_fee=0.001, maker_fee=0.0005)
        self.symbol = SWAP if market_type == "swap" else SPOT
        self.inner = FakeAdapter(self.cfg, self.bus, self.clock, symbols={"BTC": self.symbol})
        self.settings = settings or Settings(default_api_latency_ms=1.0, paper_balance_quote=10_000.0)
        self.events: list[tuple[str, Any]] = []
        self.bus.subscribe_all(lambda t, p: self.events.append((t, p)))
        self.paper = PaperAdapter(self.inner, self.settings, self.bus, self.clock, db=db, initial_quote=initial_quote,
                                  seed=seed, quote_assets=["EUR", "USDC", "USD", "USDT"])

    async def book(self, bids, asks):
        return await self.inner.push_book("BTC", bids, asks)

    def of(self, topic: str) -> list[Any]:
        return [p for t, p in self.events if t == topic]

    def req(self, side: Side, amount: float, *, type_: OrderType = OrderType.LIMIT, price=None,
            tif=TimeInForce.IOC) -> OrderRequest:
        return OrderRequest(exchange="kraken", symbol=self.symbol, side=side, amount=amount, type=type_, price=price, tif=tif)

    async def balance(self, asset: str):
        return (await self.paper.fetch_balances())[asset]


async def test_identity_and_initial_balances():
    e = Env()
    assert e.paper.name == "kraken" and e.paper.cfg is e.cfg and e.paper.can_execute
    bal = await e.paper.fetch_balances()
    assert bal["EUR"].free == 10_000 and bal["USDT"].total == 10_000 and bal["USD"].free == 10_000
    assert e.paper.positions == {} and e.paper.positions() == {} and e.paper.realized_pnl_quote == 0
    assert await e.paper.open_orders() == []


async def test_market_order_fills_fees_and_events():
    e = Env()
    await e.book([(99, 5)], [(100, 1), (101, 1)])
    e.events.clear()
    order = await e.paper.place_order(e.req(Side.BUY, 1.5, type_=OrderType.MARKET))
    assert order.status is OrderStatus.FILLED and order.filled == pytest.approx(1.5)
    assert order.avg_price == pytest.approx((100 + 50.5) / 1.5)
    assert order.fee_cost_quote == pytest.approx(150.5 * 0.001)
    assert all(f.fee_asset == "EUR" for f in order.fills) and len(order.fills) == 2
    assert order.exchange == "kraken" and order.symbol == SPOT and order.id.startswith("p")
    assert order.sent_mono is not None and order.ack_mono is not None and order.first_fill_mono is not None
    assert (await e.balance("EUR")).free == pytest.approx(10_000 - 150.5 * 1.001)
    assert (await e.balance("BTC")).total == pytest.approx(1.5)
    updates = e.of(events.ORDER_UPDATE)
    assert [u.status for u in updates] == [OrderStatus.FILLED] and updates[0] is order
    bal_events = e.of(events.BALANCE)
    assert len(bal_events) == 1 and bal_events[0]["exchange"] == "kraken" and bal_events[0]["mode"] == "paper"
    assert bal_events[0]["balances"]["BTC"].free == pytest.approx(1.5)
    # simulated latency: 1 ms +- 20 %
    assert 0.8 <= e.paper.health.order_latency_ms <= 1.2
    assert 0.8 <= e.paper.health.fill_latency_ms <= 1.2
    assert e.inner.health.order_latency_ms is None


async def test_ioc_partial_fill_against_thin_book():
    e = Env()
    await e.book([(99, 5)], [(100, 0.4), (100.5, 0.3), (102, 5)])
    e.events.clear()
    order = await e.paper.place_order(e.req(Side.BUY, 1, price=101))
    assert order.status is OrderStatus.CANCELLED and order.filled == pytest.approx(0.7)
    assert len(order.fills) == 2 and order.remaining == pytest.approx(0.3)
    assert [u.status for u in e.of(events.ORDER_UPDATE)] == [OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED]
    assert e.of(events.ORDER_UPDATE)[0].filled == pytest.approx(0.7)  # partial update carries the fill
    assert (await e.balance("BTC")).free == pytest.approx(0.7)
    assert (await e.balance("EUR")).free == pytest.approx(10_000 - (40 + 30.15) * 1.001)
    assert await e.paper.open_orders() == []
    # cancel of a done order is a no-op returning it
    assert await e.paper.cancel_order(order.id, SPOT) is order
    assert len(e.of(events.ORDER_UPDATE)) == 2


async def test_ioc_no_fill_when_limit_below_book():
    e = Env()
    await e.book([(99, 5)], [(100, 5)])
    order = await e.paper.place_order(e.req(Side.BUY, 1, price=99.5))
    assert order.status is OrderStatus.CANCELLED and order.filled == 0 and not order.fills
    assert (await e.balance("EUR")).free == 10_000
    assert e.paper.health.fill_latency_ms is None


async def test_market_order_without_book_is_rejected():
    e = Env()
    with pytest.raises(OrderRejected):
        await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET))
    assert e.of(events.ORDER_UPDATE) == []


async def test_gtc_exit_fills_later_when_book_moves():
    e = Env()
    await e.book([(99, 5)], [(100, 5)])
    entry = await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET))
    assert entry.status is OrderStatus.FILLED
    e.events.clear()
    exit_order = await e.paper.place_order(e.req(Side.SELL, 1, price=105, tif=TimeInForce.GTC))
    assert exit_order.status is OrderStatus.ACKNOWLEDGED and exit_order.filled == 0
    assert [u.status for u in e.of(events.ORDER_UPDATE)] == [OrderStatus.ACKNOWLEDGED]
    assert await e.paper.open_orders(SPOT) == [exit_order]
    btc = await e.balance("BTC")
    assert btc.free == 0 and btc.total == pytest.approx(1)  # reserved while resting
    # book unchanged -> still resting, nothing published
    e.events.clear()
    same = await e.paper.fetch_order(exit_order.id, SPOT)
    assert same is exit_order and same.status is OrderStatus.ACKNOWLEDGED and e.events == []
    # the market reaches the target
    e.clock.advance(2.0)
    await e.book([(106, 0.5), (105, 1), (104, 5)], [(107, 5)])
    e.events.clear()
    prev_fill_latency = e.paper.health.fill_latency_ms  # ~1 ms sample from the entry order
    done = await e.paper.fetch_order(exit_order.id, SPOT)
    assert done is exit_order and done.status is OrderStatus.FILLED
    assert [f.price for f in done.fills] == [106, 105] and done.filled == pytest.approx(1)
    assert done.avg_price == pytest.approx(105.5) and done.first_fill_mono == e.clock.mono()
    assert [u.status for u in e.of(events.ORDER_UPDATE)] == [OrderStatus.FILLED] and len(e.of(events.BALANCE)) == 1
    btc = await e.balance("BTC")
    assert btc.free == 0 and btc.total == 0
    eur = await e.balance("EUR")
    assert eur.free == pytest.approx(10_000 - 100 * 1.001 + 105.5 * 0.999)
    assert eur.free == pytest.approx(eur.total)
    assert await e.paper.open_orders() == []
    # fill latency = sent -> first fill (2 s), blended into the EWMA (alpha 0.2)
    assert e.paper.health.fill_latency_ms == pytest.approx(0.2 * 2000.0 + 0.8 * prev_fill_latency)


async def test_gtc_partial_rest_then_cancel_releases_reservation():
    e = Env()
    await e.book([(99, 5)], [(100, 0.5), (103, 5)])
    order = await e.paper.place_order(e.req(Side.BUY, 2, price=101, tif=TimeInForce.GTC))
    assert order.status is OrderStatus.PARTIALLY_FILLED and order.filled == 0.5
    eur = await e.balance("EUR")
    assert eur.total == pytest.approx(10_000 - 50 * 1.001)
    assert eur.free == pytest.approx(eur.total - 1.5 * 101 * 1.001)
    e.events.clear()
    cancelled = await e.paper.cancel_order(order.id, SPOT)
    assert cancelled is order and order.status is OrderStatus.CANCELLED and order.filled == 0.5
    eur = await e.balance("EUR")
    assert eur.free == pytest.approx(eur.total)
    assert [u.status for u in e.of(events.ORDER_UPDATE)] == [OrderStatus.CANCELLED]
    assert await e.paper.cancel_all() == 0
    o2 = await e.paper.place_order(e.req(Side.BUY, 1, price=90, tif=TimeInForce.GTC))
    o3 = await e.paper.place_order(e.req(Side.BUY, 1, price=91, tif=TimeInForce.GTC))
    assert await e.paper.cancel_all(SPOT) == 2 and o2.is_done and o3.is_done


async def test_swap_short_positions_and_realized_pnl():
    e = Env("swap")
    await e.book([(100, 5)], [(100.5, 5)])
    entry = await e.paper.place_order(e.req(Side.SELL, 2, type_=OrderType.MARKET))
    assert entry.status is OrderStatus.FILLED
    assert e.paper.positions[SWAP] == {"side": "short", "amount": 2, "entry": 100}
    assert e.paper.positions() == {SWAP: {"side": "short", "amount": 2, "entry": 100}}
    usdt = await e.balance("USDT")
    assert usdt.free == pytest.approx(10_000 - 200 - 0.2) and usdt.total == pytest.approx(10_000 - 0.2)
    # the market drops; close half with a reduce-only IOC limit
    await e.book([(90, 5)], [(90.5, 5)])
    close = await e.paper.place_order(e.req(Side.BUY, 1, price=91), reduce_only=True)
    assert close.status is OrderStatus.FILLED and close.fills[0].price == 90.5
    assert e.paper.positions[SWAP]["amount"] == pytest.approx(1)
    assert e.paper.realized_pnl_quote == pytest.approx(9.5)
    usdt = await e.balance("USDT")
    assert usdt.total == pytest.approx(10_000 - 0.2 - 0.0905 + 9.5)
    assert usdt.free == pytest.approx(usdt.total - 100)  # margin of the remaining short
    # close the rest at market
    rest = await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET), reduce_only=True)
    assert rest.status is OrderStatus.FILLED and SWAP not in e.paper.positions
    assert e.paper.realized_pnl_quote == pytest.approx(19.0)
    usdt = await e.balance("USDT")
    assert usdt.free == pytest.approx(usdt.total)
    with pytest.raises(OrderRejected):
        await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET), reduce_only=True)


async def test_swap_long_then_short_flip_without_reduce_only():
    e = Env("swap")
    await e.book([(100, 5)], [(101, 5)])
    await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET))
    assert e.paper.positions[SWAP]["side"] == "long"
    await e.paper.place_order(e.req(Side.SELL, 2, type_=OrderType.MARKET))
    assert e.paper.positions[SWAP] == {"side": "short", "amount": pytest.approx(1), "entry": 100}
    assert e.paper.realized_pnl_quote == pytest.approx(-1)


async def test_insufficient_balance_raises_and_changes_nothing():
    e = Env(initial_quote=50.0)
    await e.book([(99, 5)], [(100, 5)])
    e.events.clear()
    with pytest.raises(InsufficientBalance):
        await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET))
    assert (await e.balance("EUR")).free == 50 and e.of(events.ORDER_UPDATE) == [] and await e.paper.open_orders() == []
    with pytest.raises(InsufficientBalance):
        await e.paper.place_order(e.req(Side.SELL, 1, price=99))  # no BTC to sell
    assert e.paper.health.order_latency_ms is not None  # latency was still simulated


async def test_persistence_via_kv_and_reset():
    db = KV()
    e = Env(db=db)
    await e.book([(99, 5)], [(100, 5)])
    await e.paper.place_order(e.req(Side.BUY, 1.5, type_=OrderType.MARKET))
    saved = db.data["paper:kraken"]
    assert saved["balances"]["BTC"]["total"] == pytest.approx(1.5)
    # a new adapter on the same db restores the state
    e2 = Env(db=db)
    assert (await e2.balance("BTC")).free == pytest.approx(1.5)
    assert (await e2.balance("EUR")).free == pytest.approx((await e.balance("EUR")).free)
    # an explicit initial_quote ignores the stored state
    e3 = Env(db=db, initial_quote=123.0)
    assert (await e3.balance("EUR")).free == 123.0 and "BTC" not in await e3.balance_dict()
    # reset persists and publishes BALANCE
    e2.events.clear()
    bal = e2.paper.reset(5000.0)
    assert bal["EUR"].free == 5000.0 and "BTC" not in bal
    assert db.data["paper:kraken"]["balances"]["EUR"]["free"] == 5000.0
    import asyncio
    await asyncio.sleep(0)
    assert len(e2.of(events.BALANCE)) == 1
    e2.paper.reset()
    assert (await e2.balance("EUR")).free == e2.settings.paper_balance_quote


async def test_broken_db_is_tolerated():
    class Bad:
        def kv_get(self, key, default=None):
            raise RuntimeError("db down")

        def kv_set(self, key, value):
            raise RuntimeError("db down")

    e = Env(db=Bad())
    await e.book([(99, 5)], [(100, 5)])
    order = await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET))
    assert order.status is OrderStatus.FILLED


async def test_health_overlay_and_delegation():
    e = Env()
    e.inner.set_health(ws_connected=True, ws_latency_ms=12.0, order_latency_ms=None)
    h = e.paper.health
    assert h.ws_connected is True and h.ws_latency_ms == 12.0 and h.order_latency_ms is None and h.exchange == "kraken"
    await e.book([(99, 5)], [(100, 5)])
    await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET))
    assert e.paper.health.order_latency_ms is not None and e.inner.health.order_latency_ms is None
    assert e.paper.health.ws_connected is True
    # staleness is the inner's
    assert e.paper.check_staleness(e.clock.mono() + 100, 10) is True and e.inner.health.stale is True
    assert e.paper.health.stale is True
    assert e.paper.order_book(SPOT) is e.inner.order_book("BTC")
    assert (await e.paper.fetch_order_book(SPOT)).best_bid == 99
    assert (await e.paper.fetch_ticker(SPOT)).bid == 99
    assert e.paper.symbol_for("BTC") == SPOT and e.paper.asset_for(SPOT) == "BTC" and e.paper.market("BTC") is None
    assert (await e.paper.fees(SPOT)).taker == 0.001
    await e.inner.start(["BTC"])
    assert e.paper.supported_symbols() == {SPOT}
    assert (await e.paper.fetch_tickers())["BTC"].asset == "BTC"
    await e.paper.start(["BTC"])
    await e.paper.stop()


async def test_latency_follows_inner_order_latency_ewma():
    e = Env()
    e.inner.set_health(order_latency_ms=2.0)
    await e.book([(99, 5)], [(100, 5)])
    await e.paper.place_order(e.req(Side.BUY, 1, type_=OrderType.MARKET))
    assert 1.6 <= e.paper.health.order_latency_ms <= 2.4


async def test_fetch_order_unknown_id_raises():
    e = Env()
    with pytest.raises(ExchangeError):
        await e.paper.fetch_order("nope", SPOT)
    with pytest.raises(ExchangeError):
        await e.paper.cancel_order("nope", SPOT)


async def test_fills_use_current_book_not_detection_book():
    e = Env()
    await e.book([(99, 5)], [(100, 5)])
    await e.book([(99, 5)], [(120, 5)])  # moved before the order arrives
    order = await e.paper.place_order(e.req(Side.BUY, 1, price=101))
    assert order.status is OrderStatus.CANCELLED and order.filled == 0


# helper used by the persistence test
async def _balance_dict(self: Env) -> dict[str, Any]:
    return await self.paper.fetch_balances()


Env.balance_dict = _balance_dict  # type: ignore[attr-defined]
