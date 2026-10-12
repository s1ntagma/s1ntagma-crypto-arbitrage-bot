"""Offline tests for CcxtAdapter: a fake ccxt instance is injected through
``exchange_factory`` so nothing touches the network."""
from __future__ import annotations

import asyncio
from typing import Any

import ccxt
import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, ExchangeCredentials, Settings
from latbot.events import EventBus
from latbot.exchanges.base import ExchangeError, InsufficientBalance, OrderRejected, RateLimited
from latbot.exchanges.ccxt_adapter import CcxtAdapter, _map_status, _rest_base
from latbot.models import ExchangeRole, OrderRequest, OrderStatus, OrderType, Side, Ticker, TimeInForce

MARKETS: dict[str, dict[str, Any]] = {
    "BTC/USDT": {"symbol": "BTC/USDT", "base": "BTC", "quote": "USDT", "spot": True, "swap": False, "active": True,
                 "taker": 0.00075, "maker": 0.0005},
    "BTC/EUR": {"symbol": "BTC/EUR", "base": "BTC", "quote": "EUR", "spot": True, "swap": False, "active": True},
    "ETH/USDT": {"symbol": "ETH/USDT", "base": "ETH", "quote": "USDT", "spot": True, "active": False},
    "ETH/USDC": {"symbol": "ETH/USDC", "base": "ETH", "quote": "USDC", "spot": True, "active": True,
                 "taker": "0.002", "maker": None},
    "SOL/BTC": {"symbol": "SOL/BTC", "base": "SOL", "quote": "BTC", "spot": True, "active": True},
    "BTC/USDT:USDT": {"symbol": "BTC/USDT:USDT", "base": "BTC", "quote": "USDT", "spot": False, "swap": True,
                      "linear": True, "active": True, "info": {"launchTime": "1600000000000"}},
    "ETH/USD:ETH": {"symbol": "ETH/USD:ETH", "base": "ETH", "quote": "USD", "spot": False, "swap": True,
                    "linear": False, "inverse": True, "active": True},
}

BOOK1 = {"bids": [[100.0, 1.0], [101.0, 2.0], [99.0, 0.0]], "asks": [[103.0, 1.0], [102.0, 2.0]],
         "timestamp": 1_700_000_000_123, "nonce": 42, "symbol": "BTC/EUR"}
BOOK2 = {"bids": [[110.0, 1.0]], "asks": [[111.0, 1.0]], "timestamp": None}


class FakeCcxt:
    """Minimal stand-in for a ccxt.pro exchange instance."""

    demo_rewrites_url = True

    def __init__(self, name: str, opts: dict[str, Any]) -> None:
        self.ccxt_name = name
        self.opts = opts
        self.markets: dict[str, dict[str, Any]] = dict(MARKETS)
        self.currencies: dict[str, Any] = {}
        self.has = {"watchOrderBook": True, "watchTrades": True, "watchBidsAsks": False, "fetchTradingFee": True}
        self.urls: dict[str, Any] = {"api": {"public": "https://api.bybit.com", "private": "https://api.bybit.com"}}
        self.books: list[Any] = []
        self.trades: list[Any] = []
        self.tickers_ws: list[Any] = []
        self.rest_book: dict[str, Any] = BOOK1
        self.tickers: dict[str, dict[str, Any]] = {}
        self.balance: dict[str, Any] = {"free": {"USDT": 10.0, "BTC": 0.0}, "total": {"USDT": 12.0, "BTC": 0.0}}
        self.trading_fee: Any = None
        self.trading_fee_calls = 0
        self.create_result: Any = None
        self.create_calls: list[tuple] = []
        self.fetch_result: Any = None
        self.fetch_calls: list[tuple] = []
        self.cancel_result: Any = None
        self.cancel_all_result: Any = None
        self.open_result: list[dict[str, Any]] = []
        self.demo = False
        self.loaded = 0
        self.closed = False
        self.markets_set = False

    # -- lifecycle
    async def load_markets(self, reload: bool = False) -> dict[str, Any]:
        self.loaded += 1
        return self.markets

    def set_markets(self, markets: dict[str, Any], currencies: Any = None) -> None:
        self.markets = markets
        self.markets_set = True

    async def close(self) -> None:
        self.closed = True

    def enable_demo_trading(self, flag: bool) -> None:
        self.demo = flag
        if flag and self.demo_rewrites_url:
            self.urls["api"] = {"public": "https://api-demo.bybit.com", "private": "https://api-demo.bybit.com"}

    # -- streams
    async def _next(self, script: list[Any]) -> Any:
        if not script:
            await asyncio.sleep(3600)
        item = script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    async def watch_order_book(self, symbol: str, limit: int | None = None, params: Any = None) -> Any:
        return await self._next(self.books)

    async def watch_trades(self, symbol: str, since: Any = None, limit: Any = None, params: Any = None) -> Any:
        return await self._next(self.trades)

    async def watch_ticker(self, symbol: str, params: Any = None) -> Any:
        return await self._next(self.tickers_ws)

    # -- REST public
    async def fetch_order_book(self, symbol: str, limit: int | None = None, params: Any = None) -> Any:
        return self.rest_book

    async def fetch_ticker(self, symbol: str, params: Any = None) -> Any:
        return self.tickers[symbol]

    async def fetch_tickers(self, symbols: Any = None, params: Any = None) -> Any:
        if symbols is None:
            return self.tickers
        return {s: t for s, t in self.tickers.items() if s in symbols}

    # -- REST private
    async def fetch_balance(self, params: Any = None) -> Any:
        return self.balance

    async def fetch_trading_fee(self, symbol: str, params: Any = None) -> Any:
        self.trading_fee_calls += 1
        if isinstance(self.trading_fee, BaseException):
            raise self.trading_fee
        return self.trading_fee

    async def create_order(self, symbol, type_, side, amount, price=None, params=None):
        self.create_calls.append((symbol, type_, side, amount, price, dict(params or {})))
        if isinstance(self.create_result, BaseException):
            raise self.create_result
        return self.create_result or {"id": "ex1", "status": "open", "filled": 0, "amount": amount, "price": price}

    async def fetch_order(self, id_, symbol=None, params=None):
        self.fetch_calls.append((id_, symbol, dict(params or {})))
        if isinstance(self.fetch_result, BaseException):
            raise self.fetch_result
        return self.fetch_result

    async def cancel_order(self, id_, symbol=None, params=None):
        if isinstance(self.cancel_result, BaseException):
            raise self.cancel_result
        return self.cancel_result if self.cancel_result is not None else {"id": id_, "status": "canceled"}

    async def cancel_all_orders(self, symbol=None, params=None):
        if isinstance(self.cancel_all_result, BaseException):
            raise self.cancel_all_result
        return self.cancel_all_result

    async def fetch_open_orders(self, symbol=None, since=None, limit=None, params=None):
        return self.open_result


class Harness:
    def __init__(self, cfg: ExchangeConfig, creds: ExchangeCredentials | None = None, *, settings: Settings | None = None,
                 fake_cls: type = FakeCcxt, **kw: Any) -> None:
        self.bus = EventBus()
        self.clock = SimClock()
        self.settings = settings or Settings()
        self.instances: list[FakeCcxt] = []
        self.events: list[tuple[str, Any]] = []
        self.bus.subscribe_all(lambda topic, payload: self.events.append((topic, payload)))

        def factory(name: str, opts: dict[str, Any]) -> FakeCcxt:
            inst = fake_cls(name, opts)
            self.instances.append(inst)
            return inst

        self.adapter = CcxtAdapter(cfg, self.bus, self.clock, self.settings, creds, exchange_factory=factory, **kw)

    @property
    def public(self) -> FakeCcxt:
        return self.instances[0]

    @property
    def private(self) -> FakeCcxt:
        return self.instances[1]

    def of(self, topic: str) -> list[Any]:
        return [p for t, p in self.events if t == topic]


CREDS = ExchangeCredentials(name="bybit", api_key="k" * 16, api_secret="s" * 16)


def spot_cfg(name: str = "kraken", **kw: Any) -> ExchangeConfig:
    kw.setdefault("book_depth", 10)
    kw.setdefault("taker_fee", 0.004)
    kw.setdefault("maker_fee", 0.0025)
    return ExchangeConfig(name=name, role=ExchangeRole.EXECUTION, **kw)


# --------------------------------------------------------------------------- #
# construction / credentials / demo
# --------------------------------------------------------------------------- #
def test_public_instance_has_no_credentials_and_options():
    h = Harness(spot_cfg(hostname="kraken.eu"), CREDS)
    assert len(h.instances) == 2
    assert "apiKey" not in h.public.opts and "secret" not in h.public.opts
    assert h.public.opts["options"]["defaultType"] == "spot" and h.public.opts["enableRateLimit"] is True
    assert h.public.opts["hostname"] == "kraken.eu"
    assert h.private.opts["apiKey"] == CREDS.api_key and h.private.opts["secret"] == CREDS.api_secret
    assert "password" not in h.private.opts
    assert h.adapter.health.ws_connected is False


def test_no_private_without_creds_or_for_signal_role():
    h = Harness(spot_cfg())
    assert h.adapter.private is None
    h2 = Harness(ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL), CREDS)
    assert h2.adapter.private is None and len(h2.instances) == 1
    h3 = Harness(spot_cfg(), ExchangeCredentials(name="kraken", api_key="only-key"))
    assert h3.adapter.private is None


async def test_place_order_and_balances_need_private():
    h = Harness(spot_cfg())
    with pytest.raises(ExchangeError):
        await h.adapter.place_order(OrderRequest(exchange="kraken", symbol="BTC/EUR", side=Side.BUY, amount=1, price=1))
    assert await h.adapter.fetch_balances() == {}
    assert await h.adapter.open_orders() == [] and await h.adapter.cancel_all() == 0


def test_demo_enables_demo_trading_and_checks_host():
    h = Harness(ExchangeConfig(name="bybit", demo=True), CREDS)
    assert h.private.demo is True and h.public.demo is False
    assert "api-demo" in _rest_base(h.private)


def test_demo_guard_raises_when_host_is_not_demo():
    class NoRewrite(FakeCcxt):
        demo_rewrites_url = False

    with pytest.raises(ExchangeError):
        Harness(ExchangeConfig(name="bybit", demo=True), CREDS, fake_cls=NoRewrite)


async def test_demo_adapter_refuses_non_demo_host_at_order_time():
    h = Harness(ExchangeConfig(name="bybit", demo=True), CREDS)
    h.private.urls["api"] = {"private": "https://api.bybit.com"}  # something reverted the host
    with pytest.raises(ExchangeError):
        await h.adapter.place_order(OrderRequest(exchange="bybit", symbol="BTC/USDT", side=Side.BUY, amount=1, price=1))
    assert h.private.create_calls == []


def test_rest_base_handles_dict_and_str():
    class U:
        urls = {"api": "https://x"}

    assert _rest_base(U()) == "https://x"
    assert _rest_base(object()) == ""


# --------------------------------------------------------------------------- #
# market resolution
# --------------------------------------------------------------------------- #
async def test_market_resolution_quote_preference_and_skips(caplog):
    h = Harness(spot_cfg())
    await h.adapter.start(["BTC", "eth", "SOL", "XXX", "BTC"])
    try:
        assert h.adapter.symbol_for("BTC") == "BTC/EUR"  # EUR preferred over USDT
        assert h.adapter.symbol_for("ETH") == "ETH/USDC"  # USDT market inactive
        assert h.adapter.symbol_for("SOL") is None  # quote BTC not allowed
        assert h.adapter.symbol_for("XXX") is None
        assert h.adapter.asset_for("ETH/USDC") == "ETH"
        assert h.adapter.supported_symbols() == {"BTC/EUR", "ETH/USDC"}
        assert h.adapter.market("BTC")["quote"] == "EUR" and h.adapter.market("XXX") is None
        assert h.public.loaded == 1 and h.adapter.private is None
    finally:
        await h.adapter.stop()


async def test_market_resolution_swap_requires_linear():
    h = Harness(ExchangeConfig(name="bybit", market_type="swap", allow_short=True), CREDS)
    await h.adapter.start(["BTC", "ETH"])
    try:
        assert h.adapter.symbol_for("BTC") == "BTC/USDT:USDT"
        assert h.adapter.symbol_for("ETH") is None
        assert h.private.markets_set is True
    finally:
        await h.adapter.stop()


async def test_ws_subscription_limits():
    h = Harness(spot_cfg(max_ws_subscriptions=1), settings=Settings(ws_assets=5))
    await h.adapter.start(["ETH", "BTC"])
    try:
        assert h.adapter.supported_symbols() == {"ETH/USDC"}
    finally:
        await h.adapter.stop()
    h2 = Harness(spot_cfg(), settings=Settings(ws_assets=1))
    await h2.adapter.start(["BTC", "ETH"])
    try:
        assert h2.adapter.supported_symbols() == {"BTC/EUR"}
    finally:
        await h2.adapter.stop()
    h3 = Harness(spot_cfg())
    await h3.adapter.start(["BTC", "ETH"], ws_assets=1)
    try:
        assert h3.adapter.supported_symbols() == {"BTC/EUR"}
    finally:
        await h3.adapter.stop()


def test_resolve_assets_universe_without_start():
    h = Harness(spot_cfg())
    h.adapter.public.markets = dict(MARKETS)
    res = h.adapter.resolve_assets(None)
    assert res == {"BTC": "BTC/EUR", "ETH": "ETH/USDC"}


# --------------------------------------------------------------------------- #
# streaming
# --------------------------------------------------------------------------- #
async def test_book_normalisation_and_timestamps():
    h = Harness(spot_cfg())
    h.public.books = [dict(BOOK1)]
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.05)
    finally:
        await h.adapter.stop()
    books = h.of(events.BOOK)
    assert len(books) == 1
    b = books[0]
    assert b.exchange == "kraken" and b.symbol == "BTC/EUR" and b.asset == "BTC" and b.seq == 42
    assert [lv.price for lv in b.bids] == [101.0, 100.0]  # sorted desc, zero size dropped
    assert [lv.price for lv in b.asks] == [102.0, 103.0]
    assert b.bids[0].size == 2.0
    assert b.ts.exchange_ts == pytest.approx(1_700_000_000.123)
    assert b.ts.recv_ts == h.clock.wall() and b.ts.recv_mono == h.clock.mono()
    assert h.adapter.order_book("BTC") is b and h.adapter.order_book("BTC/EUR") is b
    assert h.adapter.health.ws_connected is False  # stop() flips it off
    assert h.adapter.health.last_msg_mono == h.clock.mono()


async def test_book_depth_truncation_and_missing_timestamp():
    h = Harness(spot_cfg(book_depth=1))
    h.public.books = [dict(BOOK1), dict(BOOK2)]
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.05)
    finally:
        await h.adapter.stop()
    b1, b2 = h.of(events.BOOK)
    assert len(b1.bids) == 1 and len(b1.asks) == 1 and b1.bids[0].price == 101.0
    assert b2.ts.exchange_ts is None and b2.seq is None


async def test_reconnect_with_backoff_after_stream_error():
    h = Harness(spot_cfg(), backoff_min_s=0.01, backoff_max_s=0.02, health_interval_s=0.005)
    h.public.books = [dict(BOOK1), RuntimeError("socket closed " + "x" * 300), dict(BOOK2)]
    seen: list[bool] = []
    h.bus.subscribe(events.HEALTH, lambda hh: seen.append(hh.ws_connected))
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.15)
        assert len(h.of(events.BOOK)) == 2
        assert h.adapter.health.ws_connected is True
    finally:
        await h.adapter.stop()
    assert h.adapter.health.error_count == 1
    assert h.adapter.health.reason == "RuntimeError"
    assert False in seen and True in seen  # flipped off on error, on again after reconnect
    assert seen.index(False) < len(seen) - 1 - seen[::-1].index(True)


async def test_trades_stream_best_effort():
    h = Harness(spot_cfg(), backoff_min_s=0.001)
    h.public.trades = [[{"id": "t1", "price": 100.5, "amount": 0.2, "side": "buy", "timestamp": 1_700_000_000_000}],
                       RuntimeError("a"), RuntimeError("b"), RuntimeError("c"),
                       [{"id": "t2", "price": 1, "amount": 1, "side": "sell"}]]
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.1)
    finally:
        await h.adapter.stop()
    trades = h.of(events.TRADE)
    assert len(trades) == 1  # disabled after 3 consecutive failures
    t = trades[0]
    assert t.price == 100.5 and t.amount == 0.2 and t.side == Side.BUY and t.asset == "BTC" and t.trade_id == "t1"
    assert t.ts.exchange_ts == pytest.approx(1_700_000_000.0)
    assert len(h.public.trades) == 1  # the loop stopped consuming the script after being disabled
    assert h.adapter.health.error_count == 0  # trades are best effort: no health error, book stream untouched


async def test_bbo_stream_publishes_tickers():
    h = Harness(ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo"))
    h.public.tickers_ws = [{"bid": 99.0, "ask": 101.0, "last": 100.0, "timestamp": 1_700_000_000_000, "quoteVolume": 5e6}]
    await h.adapter.start(["BTC"])
    try:
        await asyncio.sleep(0.05)
    finally:
        await h.adapter.stop()
    tk = h.of(events.TICKER)
    assert len(tk) == 1 and isinstance(tk[0], Ticker)
    assert tk[0].bid == 99.0 and tk[0].ask == 101.0 and tk[0].asset == "BTC" and tk[0].quote_volume == 5e6
    assert h.of(events.BOOK) == []


async def test_stop_cancels_tasks_and_closes_instances():
    h = Harness(spot_cfg(), CREDS)
    await h.adapter.start(["BTC", "ETH"])
    assert h.adapter._tasks
    await h.adapter.stop()
    assert h.adapter._tasks == []
    assert h.public.closed and h.private.closed
    await h.adapter.stop()  # idempotent


# --------------------------------------------------------------------------- #
# REST market data / fees / balances
# --------------------------------------------------------------------------- #
async def test_fetch_order_book_and_ticker_rest():
    h = Harness(spot_cfg())
    h.public.markets = dict(MARKETS)
    h.adapter.resolve_assets(["BTC"])
    h.public.tickers = {"BTC/EUR": {"bid": 1.0, "ask": 2.0, "last": 1.5, "timestamp": None, "baseVolume": 3.0, "quoteVolume": 4.0}}
    book = await h.adapter.fetch_order_book("BTC", limit=5)
    assert book.symbol == "BTC/EUR" and book.asset == "BTC" and book.best_bid == 101.0
    assert h.adapter.health.rest_latency_ms is not None
    t = await h.adapter.fetch_ticker("BTC/EUR")
    assert t.asset == "BTC" and t.bid == 1.0 and t.quote_volume == 4.0 and t.ts.exchange_ts is None
    assert h.adapter.order_book("BTC") is None  # REST snapshots do not replace the WS cache


async def test_fetch_tickers_keyed_by_asset():
    h = Harness(spot_cfg())
    h.public.tickers = {
        "BTC/EUR": {"symbol": "BTC/EUR", "bid": 1, "ask": 2, "last": 1.5, "quoteVolume": 100.0},
        "ETH/USDC": {"symbol": "ETH/USDC", "bid": 1, "ask": 2, "last": 1.5, "quoteVolume": 50.0},
        "SOL/BTC": {"symbol": "SOL/BTC", "bid": 1, "ask": 2, "last": 1.5, "quoteVolume": 1.0},
    }
    res = await h.adapter.fetch_tickers()
    assert set(res) == {"BTC", "ETH"}
    assert res["BTC"].quote_volume == 100.0 and res["BTC"].symbol == "BTC/EUR"
    only = await h.adapter.fetch_tickers(["ETH"])
    assert set(only) == {"ETH"}
    assert h.public.loaded == 1


async def test_fetch_tickers_falls_back_when_the_venue_takes_one_symbol_only():
    """Crypto.com: fetchTickers(symbols) refuses more than one symbol -> one call for all markets,
    filtered; later rounds skip the refused form (no error every poll)."""
    h = Harness(spot_cfg())
    h.public.tickers = {
        "BTC/EUR": {"symbol": "BTC/EUR", "bid": 1, "ask": 2, "last": 1.5, "quoteVolume": 100.0},
        "ETH/USDC": {"symbol": "ETH/USDC", "bid": 1, "ask": 2, "last": 1.5, "quoteVolume": 50.0},
    }
    calls: list[Any] = []
    plain = h.public.fetch_tickers

    async def one_symbol_only(symbols: Any = None, params: Any = None) -> Any:
        calls.append(symbols)
        if symbols is not None and len(symbols) > 1:
            raise ccxt.BadRequest("cryptocom fetchTickers() symbols argument cannot contain more than 1 symbol")
        return await plain(symbols, params)

    h.public.fetch_tickers = one_symbol_only
    assert set(await h.adapter.fetch_tickers(["BTC", "ETH"])) == {"BTC", "ETH"}
    assert set(await h.adapter.fetch_tickers(["BTC", "ETH"])) == {"BTC", "ETH"}
    assert calls == [["BTC/EUR", "ETH/USDC"], None, None]


async def test_fetch_tickers_returns_the_whole_universe_after_start():
    """The app starts the adapter with the watchlist first; a later
    ``fetch_tickers()`` (registry.resolve_universe) must still see every
    listed market, not just the watchlist (ticker breadth, ARCHITECTURE 2.3)."""
    h = Harness(spot_cfg())
    h.public.tickers = {
        "BTC/EUR": {"symbol": "BTC/EUR", "bid": 1, "ask": 2, "last": 1.5, "quoteVolume": 100.0},
        "ETH/USDC": {"symbol": "ETH/USDC", "bid": 1, "ask": 2, "last": 1.5, "quoteVolume": 50.0},
    }
    await h.adapter.start(["BTC"])
    try:
        assert h.adapter.supported_symbols() == {"BTC/EUR"}
        assert set(await h.adapter.fetch_tickers(["BTC"])) == {"BTC"}  # the poller's watchlist round
        assert set(await h.adapter.fetch_tickers()) == {"BTC", "ETH"}
        assert h.adapter.symbol_for("ETH") == "ETH/USDC"
        assert set(await h.adapter.fetch_tickers(["BTC"])) == {"BTC"}
        assert set(await h.adapter.fetch_tickers()) == {"BTC", "ETH"}  # still the universe
        assert h.adapter.supported_symbols() == {"BTC/EUR"}  # streaming untouched
        assert h.public.loaded == 1
    finally:
        await h.adapter.stop()


async def test_rest_errors_are_translated():
    h = Harness(spot_cfg())

    async def boom(*a, **k):
        raise ccxt.ExchangeNotAvailable("down")

    h.public.fetch_order_book = boom  # type: ignore[assignment]
    with pytest.raises(ExchangeError):
        await h.adapter.fetch_order_book("BTC/EUR")
    # a connection blip is not maintenance: the flag would hide the venue's data until restart
    assert h.adapter.health.maintenance is False and h.adapter.health.error_count == 1


async def test_maintenance_is_set_only_by_on_maintenance_and_clears_on_the_next_success():
    h = Harness(spot_cfg())
    real = h.public.fetch_order_book

    async def maint(*a, **k):
        raise ccxt.OnMaintenance("system upgrade")

    h.public.fetch_order_book = maint  # type: ignore[assignment]
    with pytest.raises(ExchangeError):
        await h.adapter.fetch_order_book("BTC/EUR")
    assert h.adapter.health.maintenance is True
    h.public.fetch_order_book = real  # type: ignore[assignment]
    await h.adapter.fetch_order_book("BTC/EUR")
    assert h.adapter.health.maintenance is False  # the venue answers again: no longer in maintenance


async def test_maintenance_clears_when_stream_data_arrives():
    h = Harness(spot_cfg())
    h.adapter.health.maintenance = True
    h.adapter._mark_msg(None, h.clock.wall(), h.clock.mono())
    assert h.adapter.health.maintenance is False


async def test_fees_from_market_cfg_and_private_fetch():
    h = Harness(spot_cfg(), CREDS)
    h.public.markets = dict(MARKETS)
    h.private.trading_fee = {"taker": 0.001, "maker": 0.0001}
    f = await h.adapter.fees("BTC/USDT")
    assert f.taker == 0.001 and f.maker == 0.0001  # fetch_trading_fee wins
    assert h.private.trading_fee_calls == 1
    await h.adapter.fees("BTC/USDT")
    assert h.private.trading_fee_calls == 1  # cached
    h.private.trading_fee = ccxt.ExchangeError("nope")
    f2 = await h.adapter.fees("ETH/USDC")
    # without an account fee the higher of config and the library's market fee (often outdated) is
    # charged: NET is never shown on a fee lower than the venue's published one (hard rule 2)
    assert f2.taker == 0.004 and f2.maker == 0.0025
    f3 = await h.adapter.fees("BTC/EUR")
    assert f3.taker == 0.004 and f3.maker == 0.0025  # cfg fallback
    hp = Harness(spot_cfg(taker_fee=0.0005, maker_fee=0.0001))
    hp.public.markets = dict(MARKETS)
    f4 = await hp.adapter.fees("BTC/USDT")
    assert f4.taker == 0.00075 and f4.maker == 0.0005  # the market fee is higher than the config here


async def test_fetch_balances():
    h = Harness(spot_cfg(), CREDS)
    bal = await h.adapter.fetch_balances()
    assert set(bal) == {"USDT"} and bal["USDT"].free == 10.0 and bal["USDT"].total == 12.0


# --------------------------------------------------------------------------- #
# orders
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "status,filled,amount,expected",
    [
        ("open", 0.0, 1.0, OrderStatus.ACKNOWLEDGED),
        ("open", 0.5, 1.0, OrderStatus.PARTIALLY_FILLED),
        ("closed", 1.0, 1.0, OrderStatus.FILLED),
        ("canceled", 0.3, 1.0, OrderStatus.CANCELLED),
        ("cancelled", 0.0, 1.0, OrderStatus.CANCELLED),
        ("rejected", 0.0, 1.0, OrderStatus.REJECTED),
        ("expired", 0.0, 1.0, OrderStatus.EXPIRED),
        (None, 1.0, 1.0, OrderStatus.FILLED),
        (None, 0.2, 1.0, OrderStatus.PARTIALLY_FILLED),
        ("weird", 0.0, 1.0, OrderStatus.ACKNOWLEDGED),
    ],
)
def test_status_mapping(status, filled, amount, expected):
    assert _map_status(status, filled, amount) is expected


def limit_req(**kw: Any) -> OrderRequest:
    base = dict(exchange="kraken", symbol="BTC/EUR", side=Side.BUY, amount=1.0, price=100.0, tif=TimeInForce.IOC,
                client_id="cid-1")
    base.update(kw)
    return OrderRequest(**base)


async def test_place_order_params_and_result_mapping():
    h = Harness(spot_cfg(), CREDS)
    h.private.create_result = {
        "id": "ex1", "clientOrderId": "cid-1", "status": "closed", "filled": 1.0, "amount": 1.0, "price": 100.0,
        "average": 99.5, "timestamp": 1_700_000_000_000,
        "trades": [
            {"id": "t1", "price": 99.0, "amount": 0.5, "fee": {"cost": 0.0005, "currency": "BTC"}, "timestamp": 1_700_000_000_500},
            {"id": "t2", "price": 100.0, "amount": 0.5, "fee": {"cost": 0.05, "currency": "EUR"}},
        ],
    }
    h.clock.set(mono=50.0)
    order = await h.adapter.place_order(limit_req(post_only=True))
    symbol, type_, side, amount, price, params = h.private.create_calls[0]
    assert (symbol, type_, side, amount, price) == ("BTC/EUR", "limit", "buy", 1.0, 100.0)
    assert params == {"timeInForce": "IOC", "postOnly": True, "clientOrderId": "cid-1"}
    assert order.id == "ex1" and order.client_id == "cid-1" and order.status is OrderStatus.FILLED
    assert order.sent_mono == 50.0 and order.ack_mono == 50.0 and order.sent_ts == h.clock.wall()
    assert order.first_fill_mono == 50.0 and order.done_mono == 50.0
    assert h.adapter.health.order_latency_ms == 0.0
    assert len(order.fills) == 2 and order.fills[0].trade_id == "t1" and order.fills[0].ts == pytest.approx(1_700_000_000.5)
    assert order.avg_price == 99.5 and order.filled == 1.0
    assert order.fee_cost_quote == pytest.approx(0.0005 * 99.0 + 0.05)  # base fee converted at fill price
    assert order.raw is h.private.create_result


async def test_place_order_synthetic_fill_from_filled_average_fee():
    h = Harness(spot_cfg(), CREDS)
    h.private.create_result = {"id": "ex2", "status": "open", "filled": 0.4, "amount": 1.0, "average": 101.0,
                               "fee": {"cost": 0.04, "currency": "EUR"}}
    order = await h.adapter.place_order(limit_req(tif=TimeInForce.GTC))
    assert order.status is OrderStatus.PARTIALLY_FILLED and order.filled == 0.4
    assert len(order.fills) == 1 and order.fills[0].price == 101.0 and order.fills[0].amount == 0.4
    assert order.fee_cost_quote == pytest.approx(0.04) and order.avg_price == 101.0
    assert "timeInForce" not in h.private.create_calls[0][5]


async def test_place_order_market_and_fok_params():
    h = Harness(spot_cfg(), CREDS)
    await h.adapter.place_order(limit_req(type=OrderType.MARKET, price=None, tif=TimeInForce.GTC))
    assert h.private.create_calls[-1][1] == "market" and h.private.create_calls[-1][5] == {"clientOrderId": "cid-1"}
    await h.adapter.place_order(limit_req(tif=TimeInForce.FOK))
    assert h.private.create_calls[-1][5]["timeInForce"] == "FOK"


async def test_bybit_client_id_and_reduce_only_on_swap_only():
    h = Harness(ExchangeConfig(name="bybit", market_type="swap", allow_short=True, demo=True), CREDS)
    req = limit_req(exchange="bybit", symbol="BTC/USDT:USDT", side=Side.SELL)
    await h.adapter.place_order(req, reduce_only=True)
    params = h.private.create_calls[-1][5]
    assert params["orderLinkId"] == "cid-1" and params["reduceOnly"] is True and "clientOrderId" not in params
    await h.adapter.place_order(req)
    assert "reduceOnly" not in h.private.create_calls[-1][5]
    hs = Harness(spot_cfg(), CREDS)
    await hs.adapter.place_order(limit_req(), reduce_only=True)
    assert "reduceOnly" not in hs.private.create_calls[-1][5]


@pytest.mark.parametrize(
    "exc,expected",
    [
        (ccxt.InsufficientFunds("poor"), InsufficientBalance),
        (ccxt.RateLimitExceeded("slow"), RateLimited),
        (ccxt.DDoSProtection("ddos"), RateLimited),
        (ccxt.InvalidOrder("bad"), OrderRejected),
        (ccxt.OrderNotFound("gone"), OrderRejected),
        (ccxt.OnMaintenance("maint"), ExchangeError),
        (ccxt.ExchangeNotAvailable("down"), ExchangeError),
        (ccxt.AuthenticationError("auth"), ExchangeError),
        (ccxt.BadSymbol("sym"), ExchangeError),
        (ccxt.NetworkError("net"), ExchangeError),
    ],
)
async def test_place_order_error_mapping(exc, expected):
    h = Harness(spot_cfg(), CREDS)
    h.private.create_result = exc
    with pytest.raises(expected) as info:
        await h.adapter.place_order(limit_req())
    assert type(info.value) is expected
    assert h.adapter.health.error_count == 1
    if expected is RateLimited:
        assert h.adapter.health.rate_limited_until_mono is not None and h.adapter.health.rate_limited_until_mono > h.clock.mono()
        assert info.value.retry_after_s > 0
    if isinstance(exc, ccxt.OnMaintenance):
        assert h.adapter.health.maintenance is True
    else:
        assert h.adapter.health.maintenance is False
    cached = h.adapter._orders_by_client["cid-1"]
    assert cached.is_done and cached.error


class StatusCcxt(FakeCcxt):
    """A ccxt instance with the ``on_rest_response`` hook (as ccxt.pro has)."""

    def on_rest_response(self, code, reason, url, method, response_headers, response_body, request_headers, request_body):
        return response_body.strip()


async def test_rate_limit_retry_after_header_and_418_floor():
    h = Harness(spot_cfg(), CREDS)
    h.clock.set(mono=100.0)
    h.private.create_result = ccxt.RateLimitExceeded("binance {\"code\":-1003,\"msg\":\"Way too much request weight\"}")
    h.private.last_response_headers = {"Retry-After": "45"}
    with pytest.raises(RateLimited) as info:
        await h.adapter.place_order(limit_req())
    assert info.value.retry_after_s >= 45.0
    assert h.adapter.health.rate_limited_until_mono >= 100.0 + 45.0
    assert "rate limit" in h.adapter.health.reason.lower()
    # an IP ban (HTTP 418) is never retried before 2 minutes, header or not
    h2 = Harness(spot_cfg(), CREDS)
    h2.clock.set(mono=100.0)
    h2.private.create_result = ccxt.DDoSProtection("banned")
    h2.private.last_http_status_code = 418
    h2.private.last_response_headers = {}
    with pytest.raises(RateLimited) as info2:
        await h2.adapter.place_order(limit_req())
    assert info2.value.retry_after_s >= 120.0
    assert h2.adapter.health.rate_limited_until_mono >= 100.0 + 120.0
    h3 = Harness(spot_cfg(), CREDS)
    h3.private.create_result = ccxt.DDoSProtection("banned")
    h3.private.last_http_status_code = 418
    h3.private.last_response_headers = {"Retry-After": "600"}
    with pytest.raises(RateLimited) as info3:
        await h3.adapter.place_order(limit_req())
    assert info3.value.retry_after_s >= 600.0


async def test_rate_limit_escalates_on_repeated_hits_and_resets_after_a_quiet_period():
    h = Harness(spot_cfg(), CREDS)
    h.clock.set(mono=0.0)
    h.private.create_result = ccxt.RateLimitExceeded("slow")
    retries: list[float] = []
    for i in range(4):
        h.clock.set(mono=float(i))  # one hit per second
        with pytest.raises(RateLimited) as info:
            await h.adapter.place_order(limit_req(client_id=f"cid-{i}"))
        retries.append(info.value.retry_after_s)
    assert retries[0] >= 1.0 and retries[1] > retries[0] and retries[2] > retries[1] and retries[3] > retries[2]
    assert h.adapter.health.rate_limited_until_mono == pytest.approx(3.0 + retries[3])
    # the public instance shares the penalty (same IP budget)
    # (market data is not even requested during the ban: the shared deadline is returned)
    h.public.fetch_order_book = _raise(AssertionError("must not be called during a ban"))  # type: ignore[assignment]
    h.clock.set(mono=4.0)
    with pytest.raises(RateLimited) as pub:
        await h.adapter.fetch_order_book("BTC/EUR")
    assert pub.value.retry_after_s == pytest.approx(h.adapter.health.rate_limited_until_mono - 4.0)
    # a quiet period resets the escalation
    h.clock.set(mono=4.0 + 600.0)
    with pytest.raises(RateLimited) as fresh:
        await h.adapter.place_order(limit_req(client_id="cid-x"))
    assert fresh.value.retry_after_s == pytest.approx(retries[0])
    # the penalty is capped
    for i in range(20):
        h.clock.set(mono=700.0 + i)
        with pytest.raises(RateLimited) as capped:
            await h.adapter.place_order(limit_req(client_id=f"cid-c{i}"))
    assert capped.value.retry_after_s <= 300.0
    # the window never shrinks: a later, smaller penalty keeps the earlier deadline
    until = h.adapter.health.rate_limited_until_mono
    assert until >= 719.0 + 300.0 - 1e-9
    h.clock.set(mono=720.0)
    with pytest.raises(RateLimited) as again:
        await h.adapter.place_order(limit_req(client_id="cid-z"))
    assert h.adapter.health.rate_limited_until_mono >= until
    assert again.value.retry_after_s == pytest.approx(h.adapter.health.rate_limited_until_mono - 720.0)


def _raise(exc: BaseException):
    async def boom(*a, **k):
        raise exc

    return boom


async def test_http_status_is_recorded_through_the_ccxt_hook():
    h = Harness(spot_cfg(), CREDS, fake_cls=StatusCcxt)
    assert h.private.on_rest_response(418, "I'm a teapot", "https://x", "GET", {}, "  body ", {}, None) == "body"
    assert h.private.last_http_status_code == 418
    h.private.create_result = ccxt.RateLimitExceeded("binance {\"code\":-1003}")  # Binance maps -1003 before the 418
    with pytest.raises(RateLimited) as info:
        await h.adapter.place_order(limit_req())
    assert info.value.retry_after_s >= 120.0
    assert h.public.on_rest_response(200, "OK", "https://x", "GET", {}, "ok", {}, None) == "ok"
    assert h.public.last_http_status_code == 200


async def test_place_order_timeout_returns_unknown_status_order():
    h = Harness(spot_cfg(), CREDS)
    h.private.create_result = ccxt.RequestTimeout("slow")
    order = await h.adapter.place_order(limit_req())
    assert order.status is OrderStatus.SENT and order.id == "" and "unknown" in (order.error or "")
    assert order.ack_mono is None and h.adapter.health.error_count == 1
    # reconcile by client id: fetch_order with empty id uses the clientOrderId param
    h.private.fetch_result = {"id": "ex9", "clientOrderId": "cid-1", "status": "closed", "filled": 1.0, "amount": 1.0,
                              "average": 100.0}
    fetched = await h.adapter.fetch_order("", "BTC/EUR", client_id="cid-1")
    assert h.private.fetch_calls[-1] == (None, "BTC/EUR", {"clientOrderId": "cid-1"})
    assert fetched is order and fetched.id == "ex9" and fetched.status is OrderStatus.FILLED
    assert fetched.sent_mono == order.sent_mono


async def test_fetch_order_keeps_timestamps_and_maps_status():
    h = Harness(spot_cfg(), CREDS)
    h.clock.set(mono=10.0)
    placed = await h.adapter.place_order(limit_req(tif=TimeInForce.GTC))
    assert placed.status is OrderStatus.ACKNOWLEDGED
    h.clock.set(mono=11.0)
    h.private.fetch_result = {"id": "ex1", "status": "canceled", "filled": 0.25, "amount": 1.0, "average": 100.0}
    o = await h.adapter.fetch_order("ex1", "BTC/EUR")
    assert o is placed and o.status is OrderStatus.CANCELLED and o.filled == 0.25
    assert o.sent_mono == 10.0 and o.first_fill_mono == 11.0 and o.done_mono == 11.0
    with pytest.raises(ExchangeError):
        await h.adapter.fetch_order("", "BTC/EUR")
    h.private.fetch_result = ccxt.OrderNotFound("x")
    with pytest.raises(OrderRejected):
        await h.adapter.fetch_order("zzz", "BTC/EUR")


async def test_cancel_order_paths():
    h = Harness(spot_cfg(), CREDS)
    placed = await h.adapter.place_order(limit_req(tif=TimeInForce.GTC))
    o = await h.adapter.cancel_order("ex1", "BTC/EUR")
    assert o is placed and o.status is OrderStatus.CANCELLED and o.done_mono is not None
    # minimal cancel response without status -> CANCELLED
    h.private.cancel_result = {"id": "ex1"}
    o2 = await h.adapter.cancel_order("ex1", "BTC/EUR")
    assert o2.status is OrderStatus.CANCELLED
    # OrderNotFound -> fetch fallback (already filled meanwhile)
    h.private.cancel_result = ccxt.OrderNotFound("gone")
    h.private.fetch_result = {"id": "ex1", "status": "closed", "filled": 1.0, "amount": 1.0, "average": 100.0}
    o3 = await h.adapter.cancel_order("ex1", "BTC/EUR")
    assert o3.status is OrderStatus.FILLED
    h.private.fetch_result = ccxt.OrderNotFound("gone")
    with pytest.raises(OrderRejected):
        await h.adapter.cancel_order("ex1", "BTC/EUR")
    h.private.cancel_result = ccxt.RateLimitExceeded("slow")
    with pytest.raises(RateLimited):
        await h.adapter.cancel_order("ex1", "BTC/EUR")


async def test_open_orders_and_cancel_all():
    h = Harness(spot_cfg(), CREDS)
    h.private.open_result = [
        {"id": "a", "symbol": "BTC/EUR", "status": "open", "filled": 0, "amount": 2.0, "price": 90.0, "side": "sell", "type": "limit"},
        {"id": "b", "symbol": "BTC/EUR", "status": "open", "filled": 1, "amount": 2.0, "price": 91.0, "side": "buy", "type": "market"},
    ]
    orders = await h.adapter.open_orders("BTC/EUR")
    assert [o.id for o in orders] == ["a", "b"]
    assert orders[0].side is Side.SELL and orders[0].status is OrderStatus.ACKNOWLEDGED and orders[0].price == 90.0
    assert orders[1].type is OrderType.MARKET and orders[1].status is OrderStatus.PARTIALLY_FILLED
    h.private.cancel_all_result = [{"id": "a", "status": "canceled"}, {"id": "b", "status": "canceled"}]
    assert await h.adapter.cancel_all("BTC/EUR") == 2
    assert h.adapter._orders["a"].status is OrderStatus.CANCELLED
    # fallback: cancel_all_orders unsupported -> one by one
    h.private.cancel_all_result = ccxt.NotSupported("no")
    h.private.cancel_result = None
    assert await h.adapter.cancel_all("BTC/EUR") == 2


# --------------------------------------------------------------------------- #
# execution-only adapters (LIVE / Bybit demo) share the market adapter's data
# --------------------------------------------------------------------------- #
async def test_exec_only_start_resolves_markets_without_streams():
    """``ws_assets=0``: load markets + resolve symbols, but no book/trade/health
    tasks - the market adapter of the same exchange already streams them."""
    h = Harness(spot_cfg(), CREDS)
    h.public.books = [dict(BOOK1)]
    await h.adapter.start(["BTC", "ETH"], ws_assets=0)
    try:
        await asyncio.sleep(0.05)
        assert h.adapter._tasks == []
        assert h.of(events.BOOK) == [] and h.of(events.HEALTH) == []
        assert h.adapter.symbol_for("BTC") == "BTC/EUR" and h.adapter.symbol_for("ETH") == "ETH/USDC"
        assert h.adapter.supported_symbols() == set()
        assert h.public.loaded == 1 and h.private.markets_set
        book = await h.adapter.fetch_order_book("BTC")  # REST still works for revalidation
        assert book.symbol == "BTC/EUR" and book.best_bid == 101.0
        assert h.adapter.order_book("BTC") is None  # nothing cached without a market source
    finally:
        await h.adapter.stop()


async def test_exec_only_adapter_shares_the_market_adapters_books_and_health():
    market = Harness(spot_cfg())
    instances: list[FakeCcxt] = []

    def factory(name: str, opts: dict[str, Any]) -> FakeCcxt:
        inst = FakeCcxt(name, opts)
        instances.append(inst)
        return inst

    live = CcxtAdapter(spot_cfg(), market.bus, market.clock, market.settings, CREDS, exchange_factory=factory)
    live.market_source = market.adapter
    assert live.health is market.adapter.health
    assert live.market_source is market.adapter
    market.public.books = [dict(BOOK1)]
    await market.adapter.start(["BTC"])
    await live.start(["BTC"], ws_assets=0)
    try:
        await asyncio.sleep(0.05)
        books = market.of(events.BOOK)
        assert len(books) == 1  # one stream per exchange, not one per adapter
        assert live.order_book("BTC/EUR") is books[0] and live.order_book("BTC") is books[0]
        assert live.health.ws_connected is True and live.health.last_msg_mono is not None
        healths = market.of(events.HEALTH)
        assert healths and all(hh is market.adapter.health for hh in healths)
        assert [t.get_name() for t in live._tasks] == []
        assert instances[0].books == []  # the live public instance never opened a stream
        await live.stop()
        assert market.adapter.health.ws_connected is True  # stopping the exec adapter leaves the shared health alone
    finally:
        await market.adapter.stop()
    assert market.adapter.health.ws_connected is False


async def test_exec_only_demo_adapter_asserts_the_demo_host_at_start():
    """A demo adapter whose private REST base stopped being a demo host (e.g. a
    hostname override) is refused at start - before any order could be sent."""
    h = Harness(spot_cfg(name="bybit", demo=True), CREDS)
    assert h.private.demo  # construction enabled demo trading on the private instance
    h.private.urls["api"] = {"private": "https://api.bybit.com"}
    with pytest.raises(ExchangeError, match="demo"):
        await h.adapter.start(["BTC"], ws_assets=0)
    assert h.adapter._tasks == [] and h.public.loaded == 0
    await h.adapter.stop()


def test_htx_ticker_stream_uses_the_bbo_ticker_channel():
    """HTX's default watchTicker channel ('detail') is 24h statistics without
    bid/ask; the 'ticker' channel carries bid/ask/last."""
    h = Harness(ExchangeConfig(name="htx", role=ExchangeRole.SIGNAL, stream="bbo"), None)
    assert h.public.opts["options"]["watchTicker"] == {"name": "market.{marketId}.ticker"}
    h2 = Harness(spot_cfg(), CREDS)
    assert "watchTicker" not in h2.public.opts["options"]


# --------------------------------------------------------------------------- #
# Revolut X: Retry-After in milliseconds, no market-data requests during a ban
# --------------------------------------------------------------------------- #
def revolutx_cfg(**kw: Any) -> ExchangeConfig:
    from latbot.config import DEFAULT_EXCHANGES
    base = next(e for e in DEFAULT_EXCHANGES if e.name == "revolutx")
    return ExchangeConfig(**{**{f: getattr(base, f) for f in base.__dataclass_fields__}, **kw})


async def test_revolutx_retry_after_is_milliseconds():
    """developer.revolut.com: 'The Retry-After header specifies the delay in
    milliseconds' - 850 means 0.85 s, never a 14-minute ban."""
    cfg = revolutx_cfg()
    assert cfg.retry_after_ms is True
    h = Harness(cfg, None)
    h.clock.set(mono=100.0)
    h.public.fetch_tickers = _raise(ccxt.RateLimitExceeded("revolutx 429"))  # type: ignore[assignment]
    h.public.last_response_headers = {"Retry-After": "850"}
    with pytest.raises(RateLimited) as info:
        await h.adapter.fetch_tickers(["BTC"])
    assert 0.8 <= info.value.retry_after_s <= 1.5  # the 1 s floor of a plain 429, not 850 s
    assert h.adapter.health.rate_limited_until_mono <= 101.5
    # other venues keep the HTTP standard (seconds)
    assert spot_cfg().retry_after_ms is False


async def test_no_market_data_requests_while_banned():
    h = Harness(spot_cfg(), CREDS)
    h.clock.set(mono=10.0)
    h.public.fetch_order_book = _raise(ccxt.RateLimitExceeded("slow"))  # type: ignore[assignment]
    h.public.last_response_headers = {"Retry-After": "30"}
    with pytest.raises(RateLimited):
        await h.adapter.fetch_order_book("BTC/EUR")
    until = h.adapter.health.rate_limited_until_mono
    assert until == pytest.approx(40.0)
    calls: list[str] = []

    async def counting(*a, **k):
        calls.append("x")
        return h.public.rest_book

    h.public.fetch_order_book = counting  # type: ignore[assignment]
    h.clock.set(mono=20.0)
    with pytest.raises(RateLimited) as info:
        await h.adapter.fetch_order_book("BTC/EUR")
    assert calls == [] and info.value.retry_after_s == pytest.approx(20.0)
    assert h.adapter.health.rate_limited_until_mono == pytest.approx(until)  # refusing does not escalate
    h.clock.set(mono=41.0)  # ban over -> requests flow again
    await h.adapter.fetch_order_book("BTC/EUR")
    assert calls == ["x"]
