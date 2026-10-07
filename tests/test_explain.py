"""Offline tests for latbot.engine.explain.build_why."""
from __future__ import annotations

from latbot.engine.explain import build_why
from latbot.models import ExecAssessment, ExecutionEconomics, LagSignal, Light


def signal(**over) -> LagSignal:
    d = dict(asset="BTC", direction=1, first_mover="binance", first_move_pct=4.1, first_move_start_mono=998.2,
             reacted={"binance": 4.1, "bybit": 1.2}, target="bybit", target_symbol="BTC/USDT", target_move_pct=1.2,
             lag_pct=2.9, expected_catchup_pct=1.74, catchup_ratio=0.6, detected_mono=1000.0,
             detected_ts=1_700_000_000.0, movers_count=2, median_reaction_delay_s=3.4, target_quote="USDT")
    d.update(over)
    return LagSignal(**d)


def econ(ok: bool = True) -> ExecutionEconomics:
    return ExecutionEconomics(direction=1, capital_quote=250.0, executable_quote=250.0, base_amount=0.0025,
                              entry_best=100_000.0, entry_vwap=100_050.0, exit_best=99_990.0, exit_vwap_now=99_980.0,
                              exit_target=101_700.0, spread_pct=0.01, entry_fee_pct=0.055, exit_fee_pct=0.055,
                              entry_slippage_pct=0.05, exit_slippage_pct=0.01, execution_loss_pct=0.05,
                              gross_pct=1.70, total_cost_pct=0.16, net_pct=1.54, net_quote=3.85, liquidity_ok=ok,
                              liquidity_reason="" if ok else "book supports only $7.00 within 0.3% slippage")


def assess(light: Light, est: float, margin: float, reasons=None) -> ExecAssessment:
    label = {Light.GREEN: "HIGH", Light.YELLOW: "MEDIUM", Light.RED: "NOT EXECUTABLE"}[light]
    return ExecAssessment(light=light, label=label, est_execution_s=est, margin_s=margin, probability=0.5,
                          reasons=reasons or [])


def test_why_shape_and_numbers():
    human = assess(Light.YELLOW, 7.0, 1.0)
    bot = assess(Light.GREEN, 0.4, 7.6)
    why = build_why(signal(), econ(), human, bot, 8.0, max_slippage_pct=0.3)
    lines = why.splitlines()
    assert 1 <= len(lines) <= 6
    assert lines[0].startswith("Binance moved +4.1% 1.8 s ago")
    assert "Bybit moved only +1.2% (lag +2.9%)" in lines[0]
    assert "1 other exchange followed" in lines[0]
    assert "median reaction delay on Bybit: 3.4 s" in lines[1]
    assert "Expected catch-up: +1.7% (ratio 0.60)" in lines[1]
    assert "Target book supports $250 within 0.3% slippage" in lines[2]
    assert "Costs: fees 0.11% + execution 0.05%. Net after costs: +1.54%." == lines[3]
    assert "HUMAN 🟡 (7 s needed, ~8 s window). BOT 🟢 (est. 0.4 s)." == lines[4]


def test_why_variants():
    human = assess(Light.GREEN, 7.0, 20.0)
    bot = assess(Light.RED, 0.4, -1.0, reasons=["stale book (2000 ms > 1500 ms)"])
    why = build_why(signal(median_reaction_delay_s=None, movers_count=1, target_quote="EUR"), econ(ok=False),
                    human, bot, 8.0, names={"bybit": "Bybit EU", "binance": "Binance"}, quote="EUR")
    lines = why.splitlines()
    assert len(lines) <= 6
    assert "No reaction history on Bybit EU yet." in lines[1]
    assert "followed" not in lines[0]
    assert lines[2] == "Liquidity: book supports only $7.00 within 0.3% slippage."
    assert "BOT 🔴" in lines[4]
    assert lines[5].startswith("Bot: stale book")
    ok_why = build_why(signal(), econ(), human, assess(Light.GREEN, 0.4, 5.0), 8.0, quote="EUR")
    assert "€250" in ok_why and "slippage 0.05%" in ok_why


def test_why_with_human_stats_line():
    human = ExecAssessment(light=Light.GREEN, label="HIGH", est_execution_s=7.0, margin_s=5.0, probability=0.75,
                           stats_rate=0.7, stats_n=10)
    why = build_why(signal(), econ(), human, assess(Light.GREEN, 0.4, 5.0), 8.0)
    assert len(why.splitlines()) == 6
    assert "70% of 10 such windows" in why
