"""REST-only venues (no WebSocket, e.g. Revolut X): no stream tasks, REST calls count as liveness."""

from latbot.clock import SimClock
from latbot.config import ExchangeConfig, ExchangeCredentials, Settings
from latbot.events import EventBus
from latbot.exchanges.ccxt_adapter import CcxtAdapter, is_rest_only
from latbot.models import ExchangeRole


class _RestOnlyExchange:
    """Minimal ccxt.async_support-like instance: markets + REST, no watch_*."""

    def __init__(self, opts):
        self.opts = opts
        self.has = {"fetchTickers": True, "fetchOrderBook": True}
        self.urls = {"api": {"public": "https://revx.revolut.com/api", "private": "https://revx.revolut.com/api"}}
        self.markets = {
            "BTC/USD": {"symbol": "BTC/USD", "base": "BTC", "quote": "USD", "spot": True, "active": True, "taker": 0.0009, "maker": 0.0},
            "ETH/EUR": {"symbol": "ETH/EUR", "base": "ETH", "quote": "EUR", "spot": True, "active": True, "taker": 0.0009, "maker": 0.0},
        }
        self.rateLimit = 1000

    async def load_markets(self, reload=False):
        return self.markets

    async def fetch_order_book(self, symbol, limit=None):
        return {"bids": [[100.0, 1.0]], "asks": [[101.0, 1.0]], "timestamp": 1_700_000_000_000}

    async def fetch_tickers(self, symbols=None):
        return {s: {"symbol": s, "bid": 100.0, "ask": 101.0, "last": 100.5, "timestamp": 1_700_000_000_000, "quoteVolume": 5e6}
                for s in (symbols or list(self.markets))}

    async def close(self):
        return None


def _factory(name, opts):
    return _RestOnlyExchange(opts)


def _adapter(creds=None):
    cfg = ExchangeConfig(name="revolutx", role=ExchangeRole.EXECUTION, stream="rest", label="Revolut X")
    settings = Settings()
    return CcxtAdapter(cfg, EventBus(), SimClock(), settings, creds, exchange_factory=_factory)


def test_is_rest_only_detects_missing_watch_methods():
    assert is_rest_only(_RestOnlyExchange({}))


async def test_start_creates_no_stream_tasks_and_rest_marks_liveness():
    a = _adapter()
    assert a._rest_only
    await a.start(["BTC", "ETH", "DOGE"])
    assert a.supported_symbols() == set()
    assert not [t for t in a._tasks if "book" in t.get_name() or "bbo" in t.get_name()]
    assert a.symbol_for("BTC") == "BTC/USD" and a.symbol_for("DOGE") is None
    assert a.health.ws_connected is False
    book = await a.fetch_order_book("BTC/USD")
    assert book.asset == "BTC" and book.best_bid == 100.0
    assert a.health.ws_connected is True and a.health.last_msg_mono is not None
    tickers = await a.fetch_tickers()
    assert set(tickers) == {"BTC", "ETH"}
    await a.stop()


def test_private_key_credential_is_passed_for_private_instance():
    a = _adapter(ExchangeCredentials(name="revolutx", api_key="k", api_secret="pem-private-key"))
    assert a.private is not None
    assert a.private.opts.get("privateKey") == "pem-private-key"
    assert a.private.opts.get("apiKey") == "k"


def test_default_settings_include_revolutx_as_rest_target():
    s = Settings()
    cfg = s.exchange("revolutx")
    assert cfg is not None and cfg.role == ExchangeRole.EXECUTION and cfg.stream == "rest"
    assert "revolutx" in s.execution_exchanges()
