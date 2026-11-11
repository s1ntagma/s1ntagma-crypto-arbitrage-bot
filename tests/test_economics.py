"""Offline tests for latbot.engine.economics (book walk, depth, NET)."""
from __future__ import annotations

import pytest

from latbot.config import Settings
from latbot.engine.economics import compute_economics, depth_within, walk_book
from latbot.models import BookLevel, Fees, OrderBook, Timestamps


def ts(mono: float = 1000.0) -> Timestamps:
    return Timestamps(recv_ts=1_700_000_000.0, recv_mono=mono)


def lv(pairs):
    return [BookLevel(p, s) for p, s in pairs]


def mk_book(bids, asks, mono: float = 1000.0) -> OrderBook:
    return OrderBook(exchange="bybit", symbol="BTC/USDT", bids=lv(bids), asks=lv(asks), ts=ts(mono), asset="BTC")


FEES = Fees(maker=0.001, taker=0.001)
NOFEES = Fees(maker=0.0, taker=0.0)


# ---------------------------------------------------------------- walk_book
def test_walk_book_quote_amount_partial_level():
    r = walk_book(lv([(100.0, 1.0), (101.0, 1.0)]), quote_amount=150.0)
    assert r.filled_base == pytest.approx(1.0 + 50.0 / 101.0)
    assert r.filled_quote == pytest.approx(150.0)
    assert r.vwap == pytest.approx(150.0 / (1.0 + 50.0 / 101.0))
    assert r.levels_used == 2 and r.worst_price == 101.0 and r.exhausted is False


def test_walk_book_base_amount_and_exhausted():
    r = walk_book(lv([(100.0, 1.0), (101.0, 1.0)]), base_amount=1.5)
    assert r.filled_base == pytest.approx(1.5)
    assert r.vwap == pytest.approx((100.0 + 50.5) / 1.5)
    assert r.exhausted is False
    r2 = walk_book(lv([(100.0, 1.0), (101.0, 1.0)]), base_amount=5.0)
    assert r2.filled_base == pytest.approx(2.0) and r2.exhausted is True and r2.levels_used == 2
    r3 = walk_book(lv([(100.0, 1.0)]), quote_amount=1000.0)
    assert r3.exhausted is True and r3.filled_quote == pytest.approx(100.0)


def test_walk_book_limit_price_both_sides():
    asks = lv([(100.0, 1.0), (101.0, 1.0), (102.0, 1.0)])
    r = walk_book(asks, quote_amount=500.0, limit_price=101.0)
    assert r.levels_used == 2 and r.filled_quote == pytest.approx(201.0) and r.exhausted is True
    bids = lv([(100.0, 1.0), (99.0, 1.0), (98.0, 1.0)])
    r2 = walk_book(bids, base_amount=3.0, limit_price=99.5)
    assert r2.levels_used == 1 and r2.filled_base == pytest.approx(1.0) and r2.worst_price == 100.0
    # single level: side must be given for the limit to apply
    r3 = walk_book(lv([(100.0, 1.0)]), base_amount=1.0, limit_price=99.0, side="buy")
    assert r3.filled_base == 0.0 and r3.vwap is None and r3.exhausted is True
    r4 = walk_book(lv([(100.0, 1.0)]), base_amount=1.0, limit_price=99.0, side="sell")
    assert r4.filled_base == pytest.approx(1.0)
    with pytest.raises(ValueError):
        walk_book(lv([(100.0, 1.0)]), base_amount=1.0, side="sideways")


def test_walk_book_no_amount_and_empty():
    r = walk_book(lv([(100.0, 1.0), (101.0, 2.0)]))
    assert r.filled_base == pytest.approx(3.0) and r.exhausted is False
    e = walk_book([], quote_amount=10.0)
    assert e.vwap is None and e.filled_base == 0.0 and e.exhausted is True and e.levels_used == 0
    z = walk_book(lv([(100.0, 0.0), (0.0, 5.0)]), quote_amount=10.0)
    assert z.filled_base == 0.0 and z.exhausted is True


# ------------------------------------------------------------- depth_within
def test_depth_within():
    assert depth_within(lv([(100.0, 1.0), (100.2, 1.0), (101.0, 1.0)]), 0.3) == pytest.approx(200.2)
    assert depth_within(lv([(100.0, 1.0), (99.8, 1.0), (99.0, 1.0)]), 0.3) == pytest.approx(199.8)
    assert depth_within([], 0.3) == 0.0
    assert depth_within(lv([(100.0, 2.0)]), 0.0) == pytest.approx(200.0)
    assert depth_within(lv([(100.0, 1.0), (100.3, 1.0)]), 0.3) == pytest.approx(200.3)  # boundary included


# ------------------------------------------------------- compute_economics
def test_thin_book_executable_is_what_the_book_supports():
    s = Settings()
    book = mk_book(bids=[(99.9, 1.0)], asks=[(100.0, 0.03), (100.1, 0.04), (101.0, 10.0)])
    e = compute_economics(book, +1, FEES, 100.0, 1.0, s, 1000.0)
    assert e.capital_quote == 100.0
    assert e.executable_quote == pytest.approx(7.004)
    assert e.liquidity_ok is False
    assert "$7.00" in e.liquidity_reason and "0.3% slippage" in e.liquidity_reason
    assert e.ask_depth_quote == pytest.approx(7.004) and e.bid_depth_quote == pytest.approx(99.9)
    assert e.base_amount == pytest.approx(0.07)
    assert e.net_quote == pytest.approx(e.executable_quote * e.net_pct / 100)


def test_partial_fill_and_min_liquidity_reasons():
    s = Settings(min_liquidity_quote=50.0, min_fill_ratio=0.5, min_trade_quote=10.0)
    book = mk_book(bids=[(99.9, 0.3)], asks=[(100.0, 0.3)])  # ~30 per side
    e = compute_economics(book, +1, FEES, 100.0, 1.0, s, 1000.0)
    assert e.liquidity_ok is False and "min liquidity" in e.liquidity_reason
    s2 = Settings(min_liquidity_quote=10.0, min_fill_ratio=0.5, min_trade_quote=10.0)
    e2 = compute_economics(book, +1, FEES, 100.0, 1.0, s2, 1000.0)
    assert e2.liquidity_ok is False and "of $100.00 requested" in e2.liquidity_reason
    e3 = compute_economics(book, +1, FEES, 40.0, 1.0, s2, 1000.0)
    assert e3.liquidity_ok is True and e3.executable_quote == pytest.approx(min(40.0, 30.0, 29.97))


def test_vwap_slippage_vs_best():
    s = Settings(max_slippage_pct=2.0, execution_loss_pct=0.0, min_liquidity_quote=10.0, min_fill_ratio=0.1)
    book = mk_book(bids=[(99.9, 5.0)], asks=[(100.0, 1.0), (101.0, 1.0)])
    e = compute_economics(book, +1, NOFEES, 150.0, 0.0, s, 1000.0)
    base = 1.0 + 50.0 / 101.0
    assert e.executable_quote == pytest.approx(150.0)
    assert e.entry_best == 100.0 and e.entry_vwap == pytest.approx(150.0 / base)
    assert e.entry_slippage_pct == pytest.approx((150.0 / base - 100.0) / 100.0 * 100)
    assert e.exit_best == 99.9 and e.exit_vwap_now == pytest.approx(99.9) and e.exit_slippage_pct == 0.0
    assert e.gross_pct == pytest.approx((99.9 / (150.0 / base) - 1) * 100)
    assert e.net_pct == pytest.approx(e.gross_pct)
    assert e.spread_pct == pytest.approx(book.spread_pct)


def test_net_is_gross_minus_fees_and_execution_loss():
    s = Settings(execution_loss_pct=0.05)
    book = mk_book(bids=[(99.99, 50.0)], asks=[(100.01, 50.0)])
    e = compute_economics(book, +1, FEES, 100.0, 1.2, s, 1000.0)
    assert e.entry_fee_pct == pytest.approx(0.1) and e.exit_fee_pct == pytest.approx(0.1)
    assert e.execution_loss_pct == 0.05
    assert e.total_cost_pct == pytest.approx(0.25)
    assert e.exit_target == pytest.approx(99.99 * 1.012)
    assert e.gross_pct == pytest.approx((99.99 * 1.012 / 100.01 - 1) * 100)
    assert e.net_pct == pytest.approx(e.gross_pct - 0.25)
    assert e.net_quote == pytest.approx(100.0 * e.net_pct / 100)
    assert e.liquidity_ok is True and e.liquidity_reason == ""
    assert e.book_age_ms == 0.0
    assert e.buy_price == e.entry_vwap and e.sell_price == e.exit_target
    e_old = compute_economics(book, +1, FEES, 100.0, 1.2, s, 1002.0)
    assert e_old.book_age_ms == pytest.approx(2000.0)


def test_short_direction_symmetry():
    s = Settings()
    book = mk_book(bids=[(99.9, 10.0)], asks=[(100.1, 10.0)])
    long_ = compute_economics(book, +1, FEES, 100.0, 1.0, s, 1000.0)
    short = compute_economics(book, -1, FEES, 100.0, 1.0, s, 1000.0)
    assert long_.direction == 1 and short.direction == -1
    assert short.entry_best == 99.9 and short.exit_best == 100.1
    assert short.exit_target == pytest.approx(100.1 * 0.99)
    assert short.gross_pct == pytest.approx((100.1 * 0.99 / 99.9 - 1) * 100 * -1)
    assert long_.gross_pct > 0 and short.gross_pct > 0
    assert abs(long_.gross_pct - short.gross_pct) < 0.01
    assert short.buy_price == short.exit_target and short.sell_price == short.entry_vwap
    assert short.base_amount == pytest.approx(100.0 / 99.9)


def test_empty_side_never_raises():
    s = Settings()
    for bids, asks in (([], [(100.0, 1.0)]), ([(99.0, 1.0)], []), ([], [])):
        e = compute_economics(mk_book(bids, asks), +1, FEES, 100.0, 1.0, s, 1000.0)
        assert e.liquidity_ok is False and e.liquidity_reason == "empty book side"
        assert e.executable_quote == 0.0 and e.base_amount == 0.0 and e.net_quote == 0.0
        assert e.net_pct < 0
        e2 = compute_economics(mk_book(bids, asks), -1, FEES, 100.0, 1.0, s, 1000.0)
        assert e2.liquidity_ok is False


def test_exit_side_thinner_than_position():
    s = Settings(min_liquidity_quote=1.0, min_fill_ratio=0.0, min_trade_quote=1.0, max_slippage_pct=5.0)
    book = mk_book(bids=[(99.0, 0.5)], asks=[(100.0, 10.0)])
    e = compute_economics(book, +1, FEES, 100.0, 1.0, s, 1000.0)
    # executable capped by the exit depth (49.5): the full round trip fits
    assert e.executable_quote == pytest.approx(49.5) and e.liquidity_ok is True
    # SHORT: entry sells into bids (49.5 -> 0.5 base), exit buys back on asks: fine
    e2 = compute_economics(book, -1, FEES, 100.0, 1.0, s, 1000.0)
    assert e2.base_amount == pytest.approx(0.5) and e2.exit_vwap_now == 100.0


# ---------------------------------------------------------- entry_cap_price
def test_entry_cap_price_is_the_worst_entry_that_still_nets_min_show():
    from latbot.engine.economics import entry_cap_price

    s = Settings(min_net_pct_show=0.1)
    book = mk_book(bids=[(99.99, 5.0), (99.98, 5.0)], asks=[(100.01, 5.0), (100.02, 5.0)])
    e = compute_economics(book, 1, FEES, 100.0, 1.2, s, 1000.0)
    x = (e.total_cost_pct + s.min_net_pct_show) / 100.0
    assert e.entry_cap_price == pytest.approx(e.exit_target / (1 + x))
    assert 0 < e.entry_cap_price < e.exit_target
    # buying exactly at the cap nets exactly min_net_pct_show against the same exit target
    net_at_cap = (e.exit_target / e.entry_cap_price - 1) * 100 - e.total_cost_pct
    assert net_at_cap == pytest.approx(s.min_net_pct_show)
    # a higher show threshold lowers the cap (the limit gets stricter)
    strict = compute_economics(book, 1, FEES, 100.0, 1.2, Settings(min_net_pct_show=0.5), 1000.0)
    assert strict.entry_cap_price < e.entry_cap_price
    assert strict.exit_target == pytest.approx(e.exit_target)

    # SHORT: the lowest sell price; selling at the cap nets min_net_pct_show when bought back at the target
    sh = compute_economics(book, -1, FEES, 100.0, 1.2, s, 1000.0)
    assert sh.entry_cap_price == pytest.approx(sh.exit_target / (1 - x))
    assert sh.entry_cap_price > sh.exit_target
    net_short = (1 - sh.exit_target / sh.entry_cap_price) * 100 - sh.total_cost_pct
    assert net_short == pytest.approx(s.min_net_pct_show)

    # pure helper: direction, guards
    assert entry_cap_price(100.0, 1, 0.25, 0.1) == pytest.approx(100.0 / 1.0035)
    assert entry_cap_price(100.0, -1, 0.25, 0.1) == pytest.approx(100.0 / 0.9965)
    assert entry_cap_price(0.0, 1, 0.25, 0.1) == 0.0
    assert entry_cap_price(100.0, -1, 60.0, 45.0) == 0.0  # costs >= 100 %: no sell price works
    assert entry_cap_price(100.0, 1, -5.0, -5.0) == pytest.approx(100.0)  # negative inputs never raise the cap


def test_entry_cap_price_zero_on_empty_book_side():
    s = Settings()
    empty = compute_economics(mk_book(bids=[], asks=[(100.0, 1.0)]), 1, FEES, 100.0, 1.0, s, 1000.0)
    assert empty.liquidity_ok is False and empty.entry_cap_price == 0.0
    empty2 = compute_economics(mk_book(bids=[(100.0, 1.0)], asks=[]), -1, FEES, 100.0, 1.0, s, 1000.0)
    assert empty2.entry_cap_price == 0.0
