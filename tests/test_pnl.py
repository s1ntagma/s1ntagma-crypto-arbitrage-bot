"""Offline tests for latbot.execution.pnl.finalize."""
from __future__ import annotations

import pytest

from latbot.execution.pnl import finalize
from latbot.models import Fill, Mode, OppState, Order, OrderStatus, OrderType, Side, TradeRecord

T0 = 1_700_000_000.0


def make_trade(direction: int = 1, expected_entry: float = 100.0, expected_exit: float = 105.05,
               expected_net: float = 4.8, symbol: str = "BTC/USDT") -> TradeRecord:
    return TradeRecord(id="tr_1", opportunity_id="opp_1", mode=Mode.PAPER, exchange="bybit", asset="BTC",
                       symbol=symbol, direction=direction, capital_quote=100.0, expected_net_pct=expected_net,
                       expected_profit_quote=4.8, expected_entry_price=expected_entry,
                       expected_exit_price=expected_exit, state=OppState.EXECUTING, started_ts=T0)


def make_order(side: Side, amount: float, filled: float, avg: float | None, fee_quote: float = 0.0,
               fills: list[Fill] | None = None, sent_mono: float | None = 1000.0, ack_mono: float | None = 1000.82,
               done_mono: float | None = 1001.0, status: OrderStatus = OrderStatus.FILLED,
               otype: OrderType = OrderType.LIMIT, raw=None) -> Order:
    return Order(id="o", client_id="c", exchange="bybit", symbol="BTC/USDT", side=side, type=otype, amount=amount,
                 price=avg, status=status, filled=filled, avg_price=avg, fills=fills or [], fee_cost_quote=fee_quote,
                 sent_mono=sent_mono, ack_mono=ack_mono, done_mono=done_mono, raw=raw)


def test_long_round_trip_numbers() -> None:
    t = make_trade()
    t.entry_order = make_order(Side.BUY, 1.0, 1.0, 100.1, fee_quote=0.1001, sent_mono=1000.0, ack_mono=1000.82, done_mono=1000.9)
    t.exit_order = make_order(Side.SELL, 1.0, 1.0, 104.63, fee_quote=0.10463, sent_mono=1001.0, ack_mono=1001.1, done_mono=1003.5)
    out = finalize(t)
    assert out is t
    assert t.actual_entry_price == 100.1
    assert t.actual_exit_price == 104.63
    assert t.actual_fees_quote == pytest.approx(0.20473)
    assert t.realized_pnl_quote == pytest.approx((104.63 - 100.1) * 1.0 - 0.20473)
    assert t.actual_net_pct == pytest.approx(t.realized_pnl_quote / 100.1 * 100.0)
    entry_slip = (100.1 / 100.0 - 1) * 100  # +0.1 % adverse
    exit_slip = -(104.63 / 105.05 - 1) * 100  # +0.3998 % short of the target
    assert t.actual_slippage_pct == pytest.approx(entry_slip + exit_slip)
    assert t.execution_time_s == pytest.approx(3.5)  # exit done - entry sent
    assert t.entry_latency_ms == pytest.approx(820.0)
    assert t.state == OppState.EXECUTING  # untouched
    assert t.finished_ts is None
    reason = t.diff_reason
    assert reason.startswith("expected +4.80% -> actual +4.32%")
    assert "slippage +0.10%" in reason
    assert "fees 0.20%" in reason
    assert "latency 820 ms" in reason
    assert "catch-up short by 0.40%" in reason
    assert "partial" not in reason and "no fill" not in reason


def test_short_direction_sign() -> None:
    t = make_trade(direction=-1, expected_entry=100.0, expected_exit=95.0, expected_net=4.75)
    t.entry_order = make_order(Side.SELL, 2.0, 2.0, 99.9, fee_quote=0.2)
    t.exit_order = make_order(Side.BUY, 2.0, 2.0, 95.5, fee_quote=0.2)
    finalize(t)
    assert t.realized_pnl_quote == pytest.approx(-1 * (95.5 - 99.9) * 2.0 - 0.4)  # +8.4
    assert t.realized_pnl_quote > 0
    assert t.actual_net_pct == pytest.approx(8.4 / (99.9 * 2.0) * 100.0)
    # selling lower than expected and buying back higher than target are both adverse (positive)
    entry_slip = -1 * (99.9 / 100.0 - 1) * 100  # +0.1
    exit_slip = 1 * (95.5 / 95.0 - 1) * 100  # +0.526
    assert t.actual_slippage_pct == pytest.approx(entry_slip + exit_slip)
    assert "slippage +0.10%" in t.diff_reason
    assert "catch-up short by 0.53%" in t.diff_reason


def test_short_losing_trade_negative_pnl() -> None:
    t = make_trade(direction=-1, expected_entry=100.0, expected_exit=95.0)
    t.entry_order = make_order(Side.SELL, 1.0, 1.0, 100.0)
    t.exit_order = make_order(Side.BUY, 1.0, 1.0, 102.0)
    finalize(t)
    assert t.realized_pnl_quote == pytest.approx(-2.0)
    assert t.actual_net_pct == pytest.approx(-2.0)


def test_vwap_from_fills_when_no_avg_price_and_fee_fallback() -> None:
    t = make_trade()
    fills_in = [Fill("o", 100.0, 0.5, 0.05, "USDT", T0), Fill("o", 100.4, 0.5, 0.0005, "BTC", T0)]  # fee in base
    fills_out = [Fill("o", 105.0, 1.0, 0.105, "usdt", T0)]
    t.entry_order = make_order(Side.BUY, 1.0, 0.0, None, fills=fills_in)  # filled field not set -> from fills
    t.exit_order = make_order(Side.SELL, 1.0, 1.0, None, fills=fills_out)
    finalize(t)
    assert t.actual_entry_price == pytest.approx(100.2)
    assert t.actual_exit_price == pytest.approx(105.0)
    # fees: 0.05 USDT + 0.0005 BTC * 100.4 + 0.105 USDT
    assert t.actual_fees_quote == pytest.approx(0.05 + 0.0005 * 100.4 + 0.105)
    assert t.realized_pnl_quote == pytest.approx((105.0 - 100.2) * 1.0 - t.actual_fees_quote)


def test_fees_quote_override() -> None:
    t = make_trade()
    t.entry_order = make_order(Side.BUY, 1.0, 1.0, 100.0, fee_quote=0.1)
    t.exit_order = make_order(Side.SELL, 1.0, 1.0, 105.0, fee_quote=0.1)
    finalize(t, fees_quote=1.0)
    assert t.actual_fees_quote == 1.0
    assert t.realized_pnl_quote == pytest.approx(4.0)
    assert "fees 1.00%" in t.diff_reason


def test_partial_fill_uses_closed_base() -> None:
    t = make_trade()
    t.entry_order = make_order(Side.BUY, 1.0, 0.4, 100.0, fee_quote=0.04, status=OrderStatus.CANCELLED)
    t.exit_order = make_order(Side.SELL, 0.4, 0.4, 101.0, fee_quote=0.0404, otype=OrderType.MARKET)
    finalize(t)
    assert t.realized_pnl_quote == pytest.approx(0.4 * 1.0 - 0.0804)
    assert t.actual_net_pct == pytest.approx(t.realized_pnl_quote / 40.0 * 100.0)
    assert "partial fill 40%" in t.diff_reason
    assert "exit at market" in t.diff_reason


def test_no_fill_realized_zero_and_state_untouched() -> None:
    t = make_trade()
    t.state = OppState.CANCELLED
    t.error = "entry not filled (CANCELLED)"
    t.entry_order = make_order(Side.BUY, 1.0, 0.0, None, status=OrderStatus.CANCELLED)
    finalize(t)
    assert t.realized_pnl_quote == 0.0
    assert t.actual_net_pct is None
    assert t.actual_entry_price is None and t.actual_exit_price is None
    assert t.actual_slippage_pct is None
    assert t.state == OppState.CANCELLED
    assert t.diff_reason.startswith("expected +4.80% -> no round trip")
    assert "no fill" in t.diff_reason
    assert "entry not filled" in t.diff_reason


def test_entry_filled_but_no_exit_counts_fees_and_open_position() -> None:
    t = make_trade()
    t.state = OppState.FAILED
    t.entry_order = make_order(Side.BUY, 1.0, 1.0, 100.0, fee_quote=0.1)
    finalize(t)
    assert t.realized_pnl_quote == pytest.approx(-0.1)
    assert t.actual_net_pct is None
    assert t.actual_entry_price == 100.0 and t.actual_exit_price is None
    assert t.actual_slippage_pct == pytest.approx(0.0)  # entry exactly as expected
    assert "position open 1 BTC" in t.diff_reason
    assert t.state == OppState.FAILED


def test_no_orders_at_all() -> None:
    t = make_trade()
    finalize(t)
    assert t.realized_pnl_quote == 0.0
    assert t.actual_net_pct is None
    assert t.execution_time_s is None and t.entry_latency_ms is None
    assert t.diff_reason == "expected +4.80% -> no round trip"


def test_manual_kwargs_without_orders() -> None:
    t = make_trade()
    finalize(t, fees_quote=0.2, entry_price=100.2, exit_price=105.0, base_amount=0.5, entry_sent_mono=1000.0, exit_done_mono=1012.5)
    assert t.actual_entry_price == 100.2 and t.actual_exit_price == 105.0
    assert t.realized_pnl_quote == pytest.approx((105.0 - 100.2) * 0.5 - 0.2)
    assert t.actual_net_pct == pytest.approx(t.realized_pnl_quote / (100.2 * 0.5) * 100.0)
    assert t.execution_time_s == pytest.approx(12.5)
    assert t.entry_latency_ms is None
    assert "latency" not in t.diff_reason
    assert "slippage +0.20%" in t.diff_reason


def test_combined_exit_partially_at_market() -> None:
    t = make_trade()
    t.entry_order = make_order(Side.BUY, 1.0, 1.0, 100.0)
    t.exit_order = make_order(Side.SELL, 1.0, 1.0, 104.0, raw={"orders": ["a", "b"], "market_filled": 0.6})
    finalize(t)
    assert "exit 60% at market" in t.diff_reason


def test_catch_up_better_than_expected() -> None:
    t = make_trade()
    t.entry_order = make_order(Side.BUY, 1.0, 1.0, 100.0)
    t.exit_order = make_order(Side.SELL, 1.0, 1.0, 106.0)
    finalize(t)
    assert "catch-up better by 0.90%" in t.diff_reason
    assert t.actual_slippage_pct < 0


def test_swap_symbol_quote_detection() -> None:
    t = make_trade(symbol="BTC/USDT:USDT")
    fills = [Fill("o", 100.0, 1.0, 0.06, "USDT", T0)]
    t.entry_order = make_order(Side.BUY, 1.0, 1.0, 100.0, fills=fills)
    t.exit_order = make_order(Side.SELL, 1.0, 1.0, 101.0, fills=[Fill("o", 101.0, 1.0, 0.0606, "USDT", T0)])
    finalize(t)
    assert t.actual_fees_quote == pytest.approx(0.1206)
