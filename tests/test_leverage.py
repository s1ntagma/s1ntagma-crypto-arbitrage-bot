"""Offline tests for latbot.engine.leverage (plan_leverage, quality_cap, liquidation).

Every expected number is computed by hand in the comment next to it.
Reference econ: entry 100, fees 0.1 % + 0.1 %, execution loss 0.05 %, no
slippage -> costs 0.25 %; NET 1.0 %; margin = executable = 100; depth 10 000.
"""
from __future__ import annotations

import json
import math
from types import SimpleNamespace as NS

import pytest

from latbot.config import ExchangeConfig, Settings
from latbot.engine.leverage import (
    LeveragePlan,
    funding_events,
    liquidation_price,
    plan_leverage,
    quality_cap,
    venue_kind,
)
from latbot.models import ExecutionEconomics


def econ(net: float = 1.0, direction: int = 1, executable: float = 100.0, bid: float = 10_000.0,
         ask: float = 10_000.0, entry: float = 100.0, entry_slip: float = 0.0, exit_slip: float = 0.0,
         capital: float = 100.0) -> ExecutionEconomics:
    return ExecutionEconomics(
        direction=direction, capital_quote=capital, executable_quote=executable, base_amount=executable / entry,
        entry_best=entry, entry_vwap=entry, exit_best=entry, exit_vwap_now=entry, exit_target=entry * 1.0125,
        spread_pct=0.1, entry_fee_pct=0.1, exit_fee_pct=0.1, entry_slippage_pct=entry_slip,
        exit_slippage_pct=exit_slip, execution_loss_pct=0.05, gross_pct=net + 0.25, total_cost_pct=0.25,
        net_pct=net, net_quote=executable * net / 100.0, liquidity_ok=True, bid_depth_quote=bid, ask_depth_quote=ask,
    )


def perp(**over) -> NS:
    d = dict(max_leverage=10.0, margin_kind="perp", maintenance_margin_rate=0.005, taker_fee=0.001)
    d.update(over)
    return NS(**d)


def margin_venue(**over) -> NS:
    d = dict(max_leverage=5.0, margin_kind="margin", maintenance_margin_rate=0.005, taker_fee=0.001,
             borrow_rate_daily_pct=0.12, margin_open_fee_pct=0.02)
    d.update(over)
    return NS(**d)


def plan(e: ExecutionEconomics | None = None, q: float = 85.0, settings=None, venue=None, **kw) -> LeveragePlan:
    args = dict(sigma_1m_pct=0.1, tail_1m_pct=0.2, hold_s=60.0, funding_rate_8h=None)
    args.update(kw)
    return plan_leverage(e or econ(), q, settings if settings is not None else NS(),
                         venue if venue is not None else perp(), **args)


# --------------------------------------------------------------------------- quality tiers
def test_quality_cap_tiers_default():
    s = NS()
    assert quality_cap(0, s) == 1.0
    assert quality_cap(69.99, s) == 1.0
    assert quality_cap(70, s) == 2.0
    assert quality_cap(79.9, s) == 2.0
    assert quality_cap(80, s) == 3.0
    assert quality_cap(89, s) == 3.0
    assert quality_cap(90, s) == 5.0
    assert quality_cap(100, s) == 5.0
    assert quality_cap(float("nan"), s) == 1.0


def test_quality_cap_custom_and_unsorted_tiers():
    s = NS(leverage_tiers=[[90, 4], [60, 2]])
    assert quality_cap(59, s) == 1.0
    assert quality_cap(60, s) == 2.0
    assert quality_cap(95, s) == 4.0
    assert quality_cap(95, NS(leverage_tiers=[])) == 1.0  # owner disabled leverage tiers


# --------------------------------------------------------------------------- stop
def test_stop_min_floor_sigma_and_tail():
    # 2.5 * 0.1 * sqrt(60/60) = 0.25 < 0.3 floor; tail 0.2 -> stop 0.3
    p = plan(sigma_1m_pct=0.1, tail_1m_pct=0.2, hold_s=60)
    assert p.stop_pct == pytest.approx(0.3)
    assert p.sigma_hold_pct == pytest.approx(0.1)
    # 2.5 * 0.2 * sqrt(240/60) = 2.5 * 0.4 = 1.0
    p = plan(sigma_1m_pct=0.2, tail_1m_pct=0.2, hold_s=240)
    assert p.sigma_hold_pct == pytest.approx(0.4)
    assert p.stop_pct == pytest.approx(1.0)
    # tail dominates: max(0.3, 0.25, 0.8) = 0.8
    p = plan(sigma_1m_pct=0.1, tail_1m_pct=0.8, hold_s=60)
    assert p.stop_pct == pytest.approx(0.8)


# --------------------------------------------------------------------------- reference plan
def test_reference_plan_quality_binds_all_numbers():
    p = plan(q=85.0)
    # caps: quality 85 -> 3; global 5; venue 10;
    # liquidation 1/(3*0.003 + 0.005 + 0.001) = 1/0.015 = 66.667;
    # risk: 1000 * 0.5 % = 5 / 0.0055 = 909.09 notional / 100 margin = 9.0909;
    # depth 10000/100 = 100; move: NET 1.0 >= 0.4, R:R 1/0.55 = 1.818 >= 1.5 -> no cap
    assert p.caps["quality"] == 3.0
    assert p.caps["global"] == 5.0
    assert p.caps["venue"] == 10.0
    assert p.caps["liquidation"] == pytest.approx(1 / 0.015)
    assert p.caps["risk"] == pytest.approx(5.0 / 0.0055 / 100.0)
    assert p.caps["depth"] == pytest.approx(100.0)
    assert p.caps["move"] is None
    assert p.recommended == 3.0 and p.max_by_signal == 3.0
    assert p.binding == "quality" and p.binding_by_signal == "quality"
    assert p.venue_kind == "perp" and p.venue_max == 10.0 and p.quality_cap == 3.0
    # money at 3x on margin 100
    assert p.margin_quote == pytest.approx(100.0)
    assert p.notional_quote == pytest.approx(300.0)
    assert p.fees_quote == pytest.approx(0.6)  # 300 * 0.2 %
    assert p.slippage_quote == pytest.approx(0.15)  # 300 * (0 + 0 + 0.05) %
    assert p.funding_quote == 0.0 and p.borrow_quote == 0.0
    assert p.expected_profit_quote == pytest.approx(3.0)  # 300 * 1 %
    assert p.expected_roi_pct == pytest.approx(3.0)  # 3 / 100 margin
    assert p.stop_price == pytest.approx(99.7)
    assert p.loss_at_stop_quote == pytest.approx(1.65)  # 300 * 0.55 %
    assert p.risk_pct_of_capital == pytest.approx(0.165)  # 1.65 / 1000
    assert p.risk_budget_quote == pytest.approx(5.0)
    # isolated long liq at 3x: 100 * (1 - 1/3 + 0.005) = 67.1667
    assert p.liq_price == pytest.approx(100 * (1 - 1 / 3 + 0.005))
    assert p.liq_distance_pct == pytest.approx(100 * (1 / 3 - 0.005))
    assert p.liq_buffer == pytest.approx(100 * (1 / 3 - 0.005) / 0.3)
    assert p.rr == pytest.approx(3.0 / 1.65)
    assert "3×" in p.note and "качество" in p.note
    assert all(isinstance(r, str) for r in p.reasons)


def test_global_cap_binds():
    p = plan(q=95.0, settings=NS(max_leverage=4))
    assert p.caps["quality"] == 5.0 and p.caps["global"] == 4.0
    assert p.recommended == 4.0 and p.binding == "global"


def test_venue_cap_binds_and_max_by_signal_ignores_venue():
    p = plan(q=95.0, venue=perp(max_leverage=2.0))
    assert p.recommended == 2.0 and p.binding == "venue"
    # quality 5 and global 5 tie -> the more specific name (quality) is reported
    assert p.max_by_signal == 5.0 and p.binding_by_signal == "quality"
    assert p.notional_quote == pytest.approx(200.0)


def test_liquidation_cap_binds_with_high_mmr():
    # mmr 20 %: 1/(3*0.003 + 0.2 + 0.001) = 1/0.21 = 4.76 -> 4x
    p = plan(q=95.0, venue=perp(maintenance_margin_rate=0.2), settings=NS(max_leverage=10))
    assert p.caps["liquidation"] == pytest.approx(1 / 0.21)
    assert p.recommended == 4.0 and p.binding == "liquidation"
    # liq long at 4x: 100 * (1 - 0.25 + 0.2) = 95 -> 5 % away = 16.7 stops
    assert p.liq_price == pytest.approx(95.0)
    assert p.liq_distance_pct == pytest.approx(5.0)
    assert p.liq_buffer == pytest.approx(5.0 / 0.3)
    assert p.liq_distance_pct >= 3 * p.stop_pct


def test_liquidation_cap_uses_buffer_k():
    # k = 100: 1/(100*0.003 + 0.005 + 0.001) = 1/0.306 = 3.27 -> 3x
    p = plan(q=95.0, settings=NS(leverage_liq_buffer=100))
    assert p.caps["liquidation"] == pytest.approx(1 / 0.306)
    assert p.recommended == 3.0 and p.binding == "liquidation"


def test_risk_cap_binds():
    # equity 300: budget 1.5 / 0.0055 = 272.73 notional -> 2.727x -> 2x
    p = plan(q=95.0, total_capital_quote=300.0)
    assert p.caps["risk"] == pytest.approx(1.5 / 0.0055 / 100.0)
    assert p.recommended == 2.0 and p.binding == "risk"
    assert p.loss_at_stop_quote == pytest.approx(200 * 0.0055)  # 1.1 <= 1.5 budget
    assert p.loss_at_stop_quote <= p.risk_budget_quote
    assert p.risk_pct_of_capital == pytest.approx(1.1 / 300 * 100)


def test_risk_cap_respects_hard_max_risk_per_trade():
    # frac = min(0.5, 0.25) = 0.25 %: 2.5 / 0.0055 = 454.5 -> 4.545x -> 4x
    p = plan(q=95.0, settings=NS(max_risk_per_trade_pct=0.25, max_leverage=10))
    assert p.caps["risk"] == pytest.approx(2.5 / 0.0055 / 100.0)
    assert p.recommended == 4.0 and p.binding == "risk"


def test_risk_below_one_x_keeps_one_x_and_says_so():
    # equity 100: budget 0.5 / 0.0055 = 90.9 notional < 100 margin
    p = plan(q=95.0, total_capital_quote=100.0)
    assert p.caps["risk"] == pytest.approx(0.5 / 0.0055 / 100.0)
    assert p.recommended == 1.0 and p.binding == "risk"
    assert any("уменьши размер" in r for r in p.reasons)


def test_depth_cap_binds_on_thinner_side():
    # min(250, 10000) / 100 = 2.5 -> 2x
    p = plan(q=95.0, e=econ(bid=250.0))
    assert p.caps["depth"] == pytest.approx(2.5)
    assert p.recommended == 2.0 and p.binding == "depth"
    p = plan(q=95.0, e=econ(ask=350.0))
    assert p.caps["depth"] == pytest.approx(3.5)
    assert p.recommended == 3.0 and p.binding == "depth"


def test_move_cap_net_and_rr():
    p = plan(q=95.0, e=econ(net=0.35))  # NET below 0.4
    assert p.caps["move"] == 1.0 and p.recommended == 1.0 and p.binding == "move"
    p = plan(q=95.0, e=econ(net=0.8))  # R:R 0.8 / 0.55 = 1.4545 < 1.5
    assert p.caps["move"] == 1.0 and p.recommended == 1.0 and p.binding == "move"
    p = plan(q=95.0, e=econ(net=0.85))  # R:R 0.85 / 0.55 = 1.545 >= 1.5
    assert p.caps["move"] is None and p.recommended == 5.0 and p.binding == "quality"


def test_kelly_cap_from_p10():
    # b = 1.0 / 0.55 = 1.818; f* = 0.36 - 0.64 * 0.55 = 0.008; * 0.25 = 0.002
    # notional = 0.002 * 1000 / 0.0055 = 363.6 -> 3.636x -> 3x
    p = plan(q=95.0, p10=0.36, n=25)
    assert p.caps["kelly"] == pytest.approx(0.002 * 1000 / 0.0055 / 100.0)
    assert p.recommended == 3.0 and p.binding == "kelly"
    # too few outcomes -> no Kelly cap
    p = plan(q=95.0, p10=0.36, n=10)
    assert "kelly" not in p.caps and p.recommended == 5.0
    assert any("n=10 < 20" in r for r in p.reasons)
    # no edge at the lower bound -> f* < 0 -> cap 0 -> 1x
    p = plan(q=95.0, p10=0.30, n=40)
    assert p.caps["kelly"] == 0.0 and p.recommended == 1.0 and p.binding == "kelly"


def test_unknown_volatility_is_one_x():
    p = plan(q=95.0, sigma_1m_pct=None, tail_1m_pct=None)
    assert p.recommended == 1.0 and p.max_by_signal == 1.0
    assert p.binding == "volatility" and p.sigma_hold_pct is None
    assert p.stop_pct == pytest.approx(0.3)  # min stop only
    assert "нет данных о волатильности" in p.note
    p = plan(q=95.0, sigma_1m_pct=float("nan"))
    assert p.recommended == 1.0 and p.binding == "volatility"


def test_spot_venue_recommends_one_x_but_computes_max_by_signal():
    p = plan(q=85.0, venue=NS(max_leverage=5.0, margin_kind="spot"))
    assert p.venue_kind == "spot" and p.venue_max == 1.0
    assert p.recommended == 1.0 and p.binding == "venue"
    assert p.max_by_signal == 3.0 and p.binding_by_signal == "quality"
    assert p.liq_price is None and p.liq_distance_pct is None and p.liq_buffer is None
    assert p.notional_quote == pytest.approx(100.0)
    assert p.expected_profit_quote == pytest.approx(1.0)
    assert "спот" in p.note
    # no venue at all -> spot
    p = plan_leverage(econ(), 85.0, NS(), None, sigma_1m_pct=0.1, tail_1m_pct=None, hold_s=60, funding_rate_8h=None)
    assert p.venue_kind == "spot" and p.recommended == 1.0


def test_exchange_config_defaults_and_swap_inference():
    assert venue_kind(ExchangeConfig(name="kraken")) == "spot"
    assert venue_kind(ExchangeConfig(name="bybit", market_type="swap")) == "perp"
    assert venue_kind(NS(margin_kind="weird")) == "spot"
    # a 1x linear swap position still has a liquidation price: long 100 * (1 - 1 + 0.005) = 0.5
    p = plan(venue=ExchangeConfig(name="bybit", market_type="swap"))
    assert p.venue_kind == "perp" and p.recommended == 1.0 and p.binding == "venue"
    assert p.liq_price == pytest.approx(0.5)
    assert p.liq_distance_pct == pytest.approx(99.5)


def test_liquidation_price_formulas():
    assert liquidation_price(100.0, 3.0, 0.005, 1) == pytest.approx(67.1666667)
    assert liquidation_price(100.0, 3.0, 0.005, -1) == pytest.approx(132.8333333)
    assert liquidation_price(100.0, 10.0, 0.005, 1) == pytest.approx(90.5)
    assert liquidation_price(100.0, 10.0, 0.005, -1) == pytest.approx(109.5)
    assert liquidation_price(40_000.0, 50.0, 0.005, 1) == pytest.approx(39_400.0)  # 40000*(1-0.02+0.005)
    assert liquidation_price(0.0, 3.0, 0.005, 1) is None
    assert liquidation_price(100.0, float("nan"), 0.005, 1) is None


def test_short_perp_plan():
    p = plan(q=85.0, e=econ(direction=-1))
    assert p.direction == -1 and p.recommended == 3.0
    assert p.stop_price == pytest.approx(100.3)
    assert p.liq_price == pytest.approx(100 * (1 + 1 / 3 - 0.005))


def test_funding_only_when_hold_crosses_settlement():
    assert funding_events(60, 1000.0, 1030.0, 8.0) == (1, True)
    assert funding_events(60, 1000.0, 2000.0, 8.0) == (0, True)
    assert funding_events(60, None, 1030.0, 8.0) == (0, False)
    assert funding_events(9 * 3600, None, None, 8.0) == (1, False)  # certain crossing
    assert funding_events(3 * 3600, 0.0, 1800.0, 1.0) == (3, True)  # 1800, 5400, 9000 <= 10800
    # crossing, 8 h interval: 300 notional * 0.0001 = 0.03 cost for a long
    p = plan(funding_rate_8h=0.0001, now_wall=1000.0, next_funding_ts=1030.0)
    assert p.funding_events == 1
    assert p.funding_quote == pytest.approx(0.03)
    assert p.expected_profit_quote == pytest.approx(3.0 - 0.03)
    assert p.loss_at_stop_quote == pytest.approx(1.65 + 0.03)
    # 4 h interval: the 8 h-normalised rate is paid at half per settlement
    p = plan(funding_rate_8h=0.0001, funding_interval_h=4.0, now_wall=1000.0, next_funding_ts=1030.0)
    assert p.funding_quote == pytest.approx(0.015)
    # not crossing -> 0
    p = plan(funding_rate_8h=0.0001, now_wall=1000.0, next_funding_ts=2000.0)
    assert p.funding_quote == 0.0 and p.funding_events == 0
    # a short receives a positive rate: income is never credited
    p = plan(e=econ(direction=-1), funding_rate_8h=0.0001, now_wall=1000.0, next_funding_ts=1030.0)
    assert p.funding_quote == 0.0
    assert p.expected_profit_quote == pytest.approx(3.0)
    assert any("не учитывается" in r for r in p.reasons)
    # unknown rate on a crossing hold: not invented, flagged
    p = plan(funding_rate_8h=None, now_wall=1000.0, next_funding_ts=1030.0)
    assert p.funding_quote == 0.0 and any("ставка неизвестна" in r for r in p.reasons)


def test_funding_certain_crossing_without_timestamp():
    # 9 h hold crosses one 8 h settlement for sure; sigma unknown -> 1x -> 100 * 0.0001 = 0.01
    p = plan(sigma_1m_pct=None, hold_s=9 * 3600.0, funding_rate_8h=0.0001)
    assert p.recommended == 1.0 and p.funding_events == 1
    assert p.funding_quote == pytest.approx(0.01)


def test_spot_and_margin_venues_pay_no_funding():
    p = plan(venue=NS(max_leverage=1.0, margin_kind="spot"), funding_rate_8h=0.01,
             now_wall=1000.0, next_funding_ts=1030.0)
    assert p.funding_quote == 0.0


def test_margin_borrow_open_fee_and_rollover():
    # hold 4 h: sigma_hold = 0.01 * sqrt(240) = 0.1549; stop = max(0.3, 2.5 * 0.1549 = 0.3873)
    sig_hold = 0.01 * math.sqrt(240.0)
    stop = 2.5 * sig_hold
    p = plan(q=85.0, e=econ(net=2.0), venue=margin_venue(), sigma_1m_pct=0.01, tail_1m_pct=None, hold_s=4 * 3600.0)
    assert p.venue_kind == "margin" and p.stop_pct == pytest.approx(stop)
    assert p.recommended == 3.0 and p.binding == "quality"
    # borrowed = 300 - 100 = 200; 0.02 % open + 1 rollover * 0.12/6 = 0.02 % -> 200 * 0.04 % = 0.08
    assert p.borrow_quote == pytest.approx(0.08)
    assert p.expected_profit_quote == pytest.approx(300 * 0.02 - 0.08)
    assert p.loss_at_stop_quote == pytest.approx(300 * (stop + 0.25) / 100 + 0.08)
    assert p.liq_price == pytest.approx(100 * (1 - 1 / 3 + 0.005))
    # short hold: no rollover, opening fee only: 200 * 0.02 % = 0.04
    p = plan(q=85.0, e=econ(net=2.0), venue=margin_venue())
    assert p.borrow_quote == pytest.approx(0.04)


def test_margin_one_x_long_has_no_borrow_short_borrows_base():
    p = plan(q=50.0, venue=margin_venue())
    assert p.recommended == 1.0 and p.borrow_quote == 0.0 and p.liq_price is None
    # a spot-margin short borrows the whole position: 100 * 0.02 % = 0.02; liq 100 * (1 + 1 - 0.005)
    p = plan(q=50.0, e=econ(direction=-1), venue=margin_venue())
    assert p.borrow_quote == pytest.approx(0.02)
    assert p.liq_price == pytest.approx(199.5)


def test_event_window_lowers_quality_tier():
    assert plan(q=85.0).recommended == 3.0
    p = plan(q=85.0, event_window=True)  # 85 - 10 = 75 -> 2x
    assert p.caps["quality"] == 2.0 and p.recommended == 2.0
    assert any("окно макро-события" in r for r in p.reasons)


def test_margin_quote_override():
    p = plan(q=85.0, margin_quote=50.0)
    assert p.caps["risk"] == pytest.approx(5.0 / 0.0055 / 50.0)
    assert p.caps["depth"] == pytest.approx(200.0)
    assert p.notional_quote == pytest.approx(150.0)
    assert p.expected_roi_pct == pytest.approx(3.0)  # 1.5 / 50


def test_slippage_counts_in_risk_and_costs():
    # entry 0.05 + exit 0.05 + exec loss 0.05 = 0.15 %: risk 0.3 + 0.2 + 0.15 = 0.65 %
    p = plan(q=85.0, e=econ(entry_slip=0.05, exit_slip=0.05))
    assert p.caps["risk"] == pytest.approx(5.0 / 0.0065 / 100.0)
    assert p.slippage_quote == pytest.approx(300 * 0.15 / 100)
    assert p.loss_at_stop_quote == pytest.approx(300 * 0.65 / 100)


def test_non_finite_and_empty_inputs_stay_finite():
    bad = econ(net=float("nan"), bid=float("inf"))
    p = plan(e=bad, q=float("nan"), hold_s=float("nan"), funding_rate_8h=float("inf"),
             total_capital_quote=float("nan"), sigma_1m_pct=float("inf"))
    d = p.to_dict()
    json.dumps(d, allow_nan=False)
    assert p.recommended == 1.0
    assert any("время удержания неизвестно" in r for r in p.reasons)
    empty = plan(e=econ(executable=0.0, capital=0.0))
    assert empty.recommended == 1.0 and empty.binding == "margin"
    assert empty.notional_quote == 0.0 and "нет исполнимого объёма" in empty.note
    json.dumps(empty.to_dict(), allow_nan=False)


def test_to_dict_json_safe_and_complete():
    p = plan(q=85.0, p10=0.6, n=30, funding_rate_8h=0.0001, now_wall=1000.0, next_funding_ts=1030.0)
    d = p.to_dict()
    text = json.dumps(d, allow_nan=False, ensure_ascii=False)
    assert "recommended" in text
    for key in ("venue_kind", "venue_max", "quality_cap", "max_by_signal", "recommended", "binding", "reasons",
                "margin_quote", "notional_quote", "fees_quote", "slippage_quote", "funding_quote", "borrow_quote",
                "expected_profit_quote", "expected_roi_pct", "stop_pct", "stop_price", "loss_at_stop_quote",
                "risk_pct_of_capital", "liq_price", "liq_distance_pct", "liq_buffer", "rr", "sigma_hold_pct", "note"):
        assert key in d
    assert d["caps"]["move"] is None  # unbounded cap serialised as null


def test_real_settings_defaults_match_reference():
    p = plan_leverage(econ(), 85.0, Settings(), perp(), sigma_1m_pct=0.1, tail_1m_pct=0.2, hold_s=60.0,
                      funding_rate_8h=None)
    assert p.recommended == 3.0 and p.binding == "quality"
    assert p.caps["risk"] == pytest.approx(5.0 / 0.0055 / 100.0)


def test_deterministic():
    a = plan(q=88.0, p10=0.5, n=50).to_dict()
    b = plan(q=88.0, p10=0.5, n=50).to_dict()
    assert a == b
