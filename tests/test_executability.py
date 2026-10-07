"""Offline tests for latbot.engine.executability (HUMAN / BOT lights)."""
from __future__ import annotations

import pytest

from latbot.config import Settings
from latbot.engine.executability import assess_bot, assess_human
from latbot.models import ExchangeHealth, ExecutionEconomics, Light, ReactionStats


def econ(net: float = 1.0, ok: bool = True, age_ms: float = 0.0, reason: str = "") -> ExecutionEconomics:
    return ExecutionEconomics(direction=1, capital_quote=100.0, executable_quote=100.0, base_amount=1.0,
                              entry_best=100.0, entry_vwap=100.0, exit_best=99.9, exit_vwap_now=99.9,
                              exit_target=101.0, spread_pct=0.1, entry_fee_pct=0.1, exit_fee_pct=0.1,
                              entry_slippage_pct=0.0, exit_slippage_pct=0.0, execution_loss_pct=0.05,
                              gross_pct=net + 0.25, total_cost_pct=0.25, net_pct=net, net_quote=net,
                              liquidity_ok=ok, liquidity_reason=reason, book_age_ms=age_ms)


def health(order_ms: float | None = 200.0, ws_ms: float | None = 50.0, **over) -> ExchangeHealth:
    d = dict(exchange="bybit", ws_connected=True, last_msg_mono=1000.0, ws_latency_ms=ws_ms, order_latency_ms=order_ms)
    d.update(over)
    return ExchangeHealth(**d)


S = Settings(human_execution_time_s=7.0, human_green_margin=1.5, bot_green_margin=3.0, bot_yellow_margin=1.2,
             default_api_latency_ms=250.0, max_api_latency_ms=1500.0, bot_fixed_overhead_ms=150.0,
             max_data_age_ms=1500.0, min_net_pct_bot=0.3)


# ------------------------------------------------------------------ human
def test_human_green_yellow_red():
    g = assess_human(20.0, 1.0, S)
    assert g.light is Light.GREEN and g.label == "HIGH" and g.probability == pytest.approx(0.8)
    assert g.est_execution_s == 7.0 and g.margin_s == pytest.approx(13.0) and g.reasons
    y = assess_human(9.0, 1.0, S)
    assert y.light is Light.YELLOW and y.label == "MEDIUM" and y.probability == pytest.approx(0.5)
    low = assess_human(7.5, 1.0, S)
    assert low.light is Light.YELLOW and low.label == "LOW"
    r = assess_human(5.0, 1.0, S)
    assert r.light is Light.RED and r.label == "NOT EXECUTABLE" and r.probability == pytest.approx(0.1)
    assert any("too fast for human" in x for x in r.reasons)
    assert r.margin_s == pytest.approx(-2.0)
    assert any("net +1.00%" in x for x in r.reasons)
    assert assess_human(-3.0, 1.0, S).light is Light.RED


def test_human_stats_blend():
    rs = ReactionStats(asset="BTC", exchange="bybit", human_window_total=10, human_window_ok=7)
    a = assess_human(20.0, 1.0, S, stats=rs)
    assert a.stats_rate == pytest.approx(0.7) and a.stats_n == 10
    assert a.probability == pytest.approx(0.5 * 0.8 + 0.5 * 0.7)
    assert any("by stats: 7/10 made it" in x for x in a.reasons)
    few = ReactionStats(asset="BTC", exchange="bybit", human_window_total=3, human_window_ok=0)
    b = assess_human(20.0, 1.0, S, stats=few)
    assert b.probability == pytest.approx(0.8) and b.stats_n == 3 and b.stats_rate == 0.0
    c = assess_human(20.0, 1.0, S, stats=ReactionStats(asset="BTC", exchange="bybit"))
    assert c.stats_rate is None and c.stats_n == 0


# -------------------------------------------------------------------- bot
def test_bot_green_with_margin():
    a = assess_bot(10.0, econ(1.0), health(), None, S, 1000.0)
    assert a.light is Light.GREEN and a.label == "HIGH"
    assert a.est_execution_s == pytest.approx(0.4)  # 50 + 200 + 0 + 150 ms
    assert a.margin_s == pytest.approx(9.6) and a.probability == pytest.approx(0.8) and a.reasons
    # unknown order latency -> default; fill latency added when known
    b = assess_bot(10.0, econ(1.0), health(order_ms=None, ws_ms=None, fill_latency_ms=100.0), None, S, 1000.0)
    assert b.est_execution_s == pytest.approx(0.5)


@pytest.mark.parametrize(
    "remaining, e, h, needle",
    [
        (10.0, econ(1.0, age_ms=2000.0), health(), "stale book"),
        (10.0, econ(1.0), None, "no exchange health"),
        (10.0, econ(1.0), health(ws_connected=False), "websocket disconnected"),
        (10.0, econ(1.0), health(stale=True), "stale"),
        (10.0, econ(1.0), health(maintenance=True), "maintenance"),
        (10.0, econ(1.0), health(rate_limited_until_mono=1005.0), "rate limited"),
        (10.0, econ(0.2), health(), "bot minimum"),
        (10.0, econ(1.0, ok=False, reason="book supports only $7.00"), health(), "insufficient liquidity: book supports only $7.00"),
        (10.0, econ(1.0), health(order_ms=2000.0), "order latency"),
        (0.3, econ(1.0), health(), "needed"),
    ],
)
def test_bot_red_cases(remaining, e, h, needle):
    a = assess_bot(remaining, e, h, None, S, 1000.0)
    assert a.light is Light.RED and a.label == "NOT EXECUTABLE" and a.probability == pytest.approx(0.1)
    assert any(needle in x for x in a.reasons), a.reasons


def test_bot_yellow_thin_margin_and_low_success_rate():
    a = assess_bot(1.0, econ(1.0), health(), None, S, 1000.0)  # 1.0 < 0.4*3 but >= 0.4*1.2
    assert a.light is Light.YELLOW and a.label in ("MEDIUM", "LOW") and a.probability == pytest.approx(0.5)
    assert any("thin margin" in x for x in a.reasons)
    rs = ReactionStats(asset="BTC", exchange="bybit", success_count=1, fail_count=4)
    b = assess_bot(10.0, econ(1.0), health(), rs, S, 1000.0)
    assert b.light is Light.YELLOW and any("success rate" in x for x in b.reasons)
    assert b.stats_rate == pytest.approx(0.2) and b.stats_n == 5
    assert b.probability == pytest.approx(0.6 * 0.5 + 0.4 * 0.2)
    few = ReactionStats(asset="BTC", exchange="bybit", success_count=0, fail_count=2)
    c = assess_bot(10.0, econ(1.0), health(), few, S, 1000.0)
    assert c.light is Light.GREEN and c.probability == pytest.approx(0.8) and c.stats_n == 2
    good = ReactionStats(asset="BTC", exchange="bybit", success_count=9, fail_count=1)
    d = assess_bot(10.0, econ(1.0), health(), good, S, 1000.0)
    assert d.light is Light.GREEN and d.probability == pytest.approx(0.6 * 0.8 + 0.4 * 0.9)
