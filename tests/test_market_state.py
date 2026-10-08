"""Offline tests for latbot.market.state.MarketState."""
from __future__ import annotations

from latbot.clock import SimClock
from latbot.config import Settings
from latbot.market.state import MarketState
from latbot.models import BookLevel, ExchangeHealth, OrderBook, Side, Ticker, Timestamps, Trade


def ts(mono: float) -> Timestamps:
    return Timestamps(recv_ts=1_700_000_000.0 + (mono - 1000.0), recv_mono=mono)


def book(ex: str, mid: float, mono: float, asset: str = "BTC", symbol: str = "BTC/USDT") -> OrderBook:
    bids = [BookLevel(mid * (1 - 0.0001 * k), 50.0) for k in (1, 2, 3)]
    asks = [BookLevel(mid * (1 + 0.0001 * k), 50.0) for k in (1, 2, 3)]
    return OrderBook(exchange=ex, symbol=symbol, bids=bids, asks=asks, ts=ts(mono), asset=asset)


def ticker(ex: str, mono: float, bid=100.0, ask=100.2, last=None, base_volume=None, quote_volume=None,
           asset="BTC", symbol="BTC/USDT") -> Ticker:
    return Ticker(exchange=ex, symbol=symbol, bid=bid, ask=ask, last=last, ts=ts(mono), asset=asset,
                  base_volume=base_volume, quote_volume=quote_volume)


def make_state(**over) -> tuple[MarketState, SimClock]:
    settings = Settings(**over)
    clock = SimClock()
    return MarketState(settings, clock), clock


def test_store_latest_book_and_lookups():
    st, clock = make_state(price_sample_min_interval_ms=0)
    b1 = book("binance", 100.0, 1000.0)
    b2 = book("binance", 101.0, 1001.0)
    st.on_book(b1)
    st.on_book(b2)
    st.on_book(book("kraken", 100.0, 1001.0, symbol="BTC/EUR"))
    assert st.book("binance", "BTC") is b2
    assert st.book("kraken", "ETH") is None
    assert set(st.books_for("BTC")) == {"binance", "kraken"}
    assert st.assets() == {"BTC"}
    assert st.exchanges_for("BTC") == ["binance", "kraken"]
    assert st.exchanges_for("ETH") == []
    assert st.symbol("kraken", "BTC") == "BTC/EUR"
    assert st.symbol("kraken", "ETH") is None
    # asset derived from the symbol when the adapter did not fill it
    st.on_book(book("okx", 10.0, 1001.0, asset="", symbol="SOL/USDT"))
    assert "SOL" in st.assets()
    assert st.book("okx", "SOL") is not None


def test_series_append_and_trim():
    st, _ = make_state(price_history_s=10.0, price_sample_min_interval_ms=0)
    for i in range(20):
        st.on_book(book("binance", 100.0 + i, 1000.0 + i))
    series = st.mid_series("binance", "BTC")
    monos = [m for m, _ in series]
    assert monos == sorted(monos)
    assert monos[0] >= 1019.0 - 10.0
    assert monos[-1] == 1019.0
    assert series[-1][1] == 119.0
    assert st.mid_series("nope", "BTC") == ()


def test_series_throttle_replaces_newest_point():
    st, _ = make_state(price_sample_min_interval_ms=100.0)
    st.on_book(book("binance", 100.0, 1000.0))
    st.on_book(book("binance", 101.0, 1000.05))  # lone first point is never replaced
    assert [p[1] for p in st.mid_series("binance", "BTC")] == [100.0, 101.0]
    st.on_book(book("binance", 102.0, 1000.10))  # within interval of the last accepted sample -> replace
    assert [p for p in st.mid_series("binance", "BTC")] == [(1000.0, 100.0), (1000.10, 102.0)]
    st.on_book(book("binance", 103.0, 1000.20))  # interval elapsed -> append
    assert [p[1] for p in st.mid_series("binance", "BTC")] == [100.0, 102.0, 103.0]
    # out-of-order sample is ignored
    st.on_book(book("binance", 50.0, 999.0))
    assert [p[1] for p in st.mid_series("binance", "BTC")] == [100.0, 102.0, 103.0]


def test_mid_at():
    st, _ = make_state(price_sample_min_interval_ms=0)
    for i in range(5):
        st.on_book(book("binance", 100.0 + i, 1000.0 + i))
    assert st.mid_at("binance", "BTC", 999.0) is None
    assert st.mid_at("binance", "BTC", 1000.0) == 100.0
    assert st.mid_at("binance", "BTC", 1002.5) == 102.0
    assert st.mid_at("binance", "BTC", 2000.0) == 104.0
    assert st.mid_at("kraken", "BTC", 1000.0) is None


def test_is_fresh_age_and_health():
    st, _ = make_state()
    assert st.is_fresh("binance", "BTC", 1000.0, 1500.0) is False
    st.on_book(book("binance", 100.0, 1000.0))
    assert st.is_fresh("binance", "BTC", 1001.0, 1500.0) is True
    assert st.is_fresh("binance", "BTC", 1002.0, 1500.0) is False
    h = ExchangeHealth(exchange="binance", ws_connected=True, stale=True)
    st.on_health(h)
    assert st.health("binance") is h
    assert st.is_fresh("binance", "BTC", 1001.0, 1500.0) is False
    h.stale = False  # same object -> live flag
    assert st.is_fresh("binance", "BTC", 1001.0, 1500.0) is True
    h.maintenance = True
    assert st.is_fresh("binance", "BTC", 1001.0, 1500.0) is False


def test_recent_quote_volume_window():
    st, _ = make_state()
    for i in range(10):
        st.on_trade(Trade(exchange="binance", symbol="BTC/USDT", price=100.0, amount=1.0, side=Side.BUY,
                          ts=ts(1000.0 + i), asset="BTC"))
    assert st.recent_quote_volume("binance", "BTC", 5.0, 1009.0) == 600.0  # monos 1004..1009
    assert st.recent_quote_volume("binance", "BTC", 100.0, 1009.0) == 1000.0
    assert st.recent_quote_volume("binance", "BTC", 5.0, 2000.0) == 0.0
    assert st.recent_quote_volume("kraken", "BTC", 5.0, 1009.0) == 0.0


def test_ticker_mid_volume_and_series():
    st, _ = make_state(price_sample_min_interval_ms=0, tick_outlier_pct=0)  # artificial 2x jumps
    st.on_ticker(ticker("kraken", 1000.0, bid=100.0, ask=100.2, quote_volume=5_000_000.0, symbol="BTC/EUR"))
    assert st.volume24h("kraken", "BTC") == 5_000_000.0
    assert st.mid_series("kraken", "BTC")[-1] == (1000.0, 100.1)
    assert st.symbol("kraken", "BTC") == "BTC/EUR"
    assert st.ticker("kraken", "BTC") is not None
    assert st.is_fresh("kraken", "BTC", 1001.0, 1500.0) is True
    # base volume * mid when no quote volume; last price when bid/ask missing
    st.on_ticker(ticker("kraken", 1001.0, bid=None, ask=None, last=200.0, base_volume=10.0, symbol="BTC/EUR"))
    assert st.volume24h("kraken", "BTC") == 2000.0
    assert st.mid_series("kraken", "BTC")[-1] == (1001.0, 200.0)
    assert st.volume24h("kraken", "ETH") is None
    assert "kraken" in st.exchanges_for("BTC")


def test_on_ticker_volume_only_does_not_touch_series():
    """Polled tickers for WebSocket-streamed assets update volume / ticker /
    liveness only: their (older) price never enters the live mid series."""
    st, _ = make_state(price_sample_min_interval_ms=0, tick_outlier_pct=0)
    st.on_book(book("binance", 102.0, 1000.0))
    st.on_ticker(ticker("binance", 1000.2, bid=99.99, ask=100.01, quote_volume=7_000_000.0), append_mid=False)
    assert list(st.mid_series("binance", "BTC")) == [(1000.0, 102.0)]
    assert st.volume24h("binance", "BTC") == 7_000_000.0
    assert st.ticker("binance", "BTC") is not None and st.symbol("binance", "BTC") == "BTC/USDT"
    assert st.last_data_mono("binance", "BTC") == 1000.2  # the feed is alive
    assert st.is_fresh("binance", "BTC", 1001.0, 1500.0) is True
    # default keeps appending (poll-only assets, WS bbo tickers)
    st.on_ticker(ticker("binance", 1000.4, bid=99.99, ask=100.01))
    assert list(st.mid_series("binance", "BTC"))[-1] == (1000.4, 100.0)


def test_ticker_outlier_tick_is_held_until_a_second_tick_confirms_it():
    """A lone ticker tick 0.8 % away from the series is a data glitch until the
    next tick agrees (HTX 'detail' flapping produced fake +-0.8 % moves)."""
    st, _ = make_state(tick_outlier_pct=0.5)
    st.on_ticker(ticker("htx", 1000.0, bid=100.0, ask=100.0))
    st.on_ticker(ticker("htx", 1001.0, bid=100.0, ask=100.0))
    st.on_ticker(ticker("htx", 1002.0, bid=100.8, ask=100.8))  # outlier: held
    assert st.mid_at("htx", "BTC", 1002.0) == 100.0
    st.on_ticker(ticker("htx", 1003.0, bid=100.0, ask=100.0))  # reverted: glitch dropped
    assert st.mid_at("htx", "BTC", 1003.0) == 100.0
    assert len(st._series[("htx", "BTC")]) == 3
    st.on_ticker(ticker("htx", 1004.0, bid=100.8, ask=100.8))  # held again
    st.on_ticker(ticker("htx", 1005.0, bid=100.85, ask=100.85))  # confirmed: both points land
    series = st._series[("htx", "BTC")]
    assert [m for _, m in series[-2:]] == [100.8, 100.85]
    assert st.last_data_mono("htx", "BTC") == 1005.0  # liveness never suffers


def test_ticker_outlier_guard_off_and_books_untouched():
    st, _ = make_state(tick_outlier_pct=0.0)
    st.on_ticker(ticker("htx", 1000.0, bid=100.0, ask=100.0))
    st.on_ticker(ticker("htx", 1001.0, bid=102.0, ask=102.0))
    assert st.mid_at("htx", "BTC", 1001.0) == 102.0
    st2, _ = make_state(tick_outlier_pct=0.5)
    st2.on_book(book("kraken", 100.0, 1000.0))
    st2.on_book(book("kraken", 102.0, 1001.0))
    assert abs(st2.mid_at("kraken", "BTC", 1001.0) - 102.0) < 0.01


def test_health_is_polling_window():
    h = ExchangeHealth(exchange="mexc")
    assert h.is_polling(100.0) is False
    h.last_poll_mono = 80.0  # 20 s ago, one failed round in between: still covered
    assert h.is_polling(100.0, 2.0) is True
    assert h.is_polling(111.0, 2.0) is False  # 31 s: no longer
