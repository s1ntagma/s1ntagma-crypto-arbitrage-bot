"""Order-book depth snapping and error parsing for exchange WebSocket limits."""
from latbot.exchanges.ccxt_adapter import _WS_BOOK_LIMITS, allowed_depths_from_error, snap_depth


def test_snap_depth_picks_smallest_allowed_at_or_above_request():
    assert snap_depth(10, [1, 50, 200, 1000]) == 50
    assert snap_depth(10, [5, 10, 20]) == 10
    assert snap_depth(2000, [1, 50]) == 50
    assert snap_depth(None, [5, 50]) == 5


def test_snap_depth_none_when_venue_takes_no_limit():
    assert snap_depth(10, [None]) is None
    assert snap_depth(10, []) is None


def test_allowed_depths_from_bybit_error():
    msg = "bybit watchOrderBookForSymbols(): for spot markets limit can be one of: [1,50,200,1000]"
    assert allowed_depths_from_error(msg) == [1, 50, 200, 1000]
    assert allowed_depths_from_error("something else") is None


def test_known_limits_cover_default_targets():
    for name in ("bybit", "kraken", "coinbase", "okx", "myokx", "gate", "bitvavo", "bitstamp", "cryptocom"):
        assert name in _WS_BOOK_LIMITS
