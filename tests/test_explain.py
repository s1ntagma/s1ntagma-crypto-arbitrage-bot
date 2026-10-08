"""Offline tests for latbot.engine.explain: build_why (English WHY) and build_plan (Russian plan)."""
from __future__ import annotations

import pytest

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


# --------------------------------------------------------------------------- #
# build_plan - the Russian trading plan for the manual trader
# --------------------------------------------------------------------------- #
import time  # noqa: E402

from latbot.clock import SimClock  # noqa: E402
from latbot.config import Settings  # noqa: E402
from latbot.engine.explain import (  # noqa: E402
    PLAN_KEYS,
    build_plan,
    cap_price,
    fmt_duration,
    fmt_money,
    fmt_price,
    plan_lines,
    price_str,
)
from latbot.models import Mode, Opportunity, OppState, new_id  # noqa: E402

NAMES = {"revolutx": "Revolut X", "kraken": "Kraken", "binance": "Binance", "bybit": "Bybit EU"}
WALL = 1_700_000_000.0


def zk_signal(**over) -> LagSignal:
    d = dict(asset="ZK", direction=1, first_mover="revolutx", first_move_pct=2.2, first_move_start_mono=990.0,
             reacted={"revolutx": 2.2, "kraken": 0.3}, target="kraken", target_symbol="ZK/EUR", target_move_pct=0.3,
             lag_pct=1.9, expected_catchup_pct=1.14, catchup_ratio=0.6, detected_mono=1000.0, detected_ts=WALL,
             movers_count=2, source="poll", target_quote="EUR", mover_volumes_quote={"revolutx": 5_000_000.0})
    d.update(over)
    return LagSignal(**d)


def zk_econ(**over) -> ExecutionEconomics:
    d = dict(direction=1, capital_quote=100.0, executable_quote=100.0, base_amount=100.0 / 0.01097,
             entry_best=0.0109631, entry_vwap=0.01097, exit_best=0.010955, exit_vwap_now=0.01095,
             exit_target=0.0114109, spread_pct=0.07, entry_fee_pct=0.4, exit_fee_pct=0.4, entry_slippage_pct=0.06,
             exit_slippage_pct=0.05, execution_loss_pct=0.05, gross_pct=3.28, total_cost_pct=0.85, net_pct=2.43,
             net_quote=2.43, liquidity_ok=True, bid_depth_quote=900.0, ask_depth_quote=840.0,
             entry_cap_price=0.0112291)
    d.update(over)
    return ExecutionEconomics(**d)


def make_opp(clock: SimClock, sig: LagSignal | None = None, e: ExecutionEconomics | None = None, **over) -> Opportunity:
    sig = sig or zk_signal()
    e = e or zk_econ()
    now = clock.mono()
    base = dict(id=new_id("opp_"), asset=sig.asset, target_exchange=sig.target, target_symbol=sig.target_symbol,
                first_mover=sig.first_mover, direction=sig.direction, mode=Mode.PAPER, state=OppState.VALID, signal=sig,
                econ=e, human=assess(Light.YELLOW, 7.0, 1.0), bot=assess(Light.GREEN, 0.4, 7.6), why="why",
                detected_ts=clock.wall(), first_seen_mono=now, last_valid_mono=now, last_update_mono=now,
                estimated_duration_s=8.0, quote=sig.target_quote, last_update_ts=clock.wall())
    base.update(over)
    return Opportunity(**base)


def settings(**over) -> Settings:
    base = dict(min_mover_volume_quote=1_000_000.0, max_hold_s=600.0, min_net_pct_show=0.1)
    base.update(over)
    return Settings(**base)


def test_plan_long_matches_previous_bot_style():
    clock = SimClock(wall=WALL, mono=1000.0)
    opp = make_opp(clock, first_seen_mono=1000.0 - (24 * 60 + 13), typical_window_s=168.0,
                   caught_up_ok=0, caught_up_total=32, human_ok=3, human_total=10)
    plan = build_plan(opp, settings(), NAMES, clock.mono(), clock.wall())
    assert tuple(plan) == PLAN_KEYS
    assert plan["header"] == "🏃 ZK · догоняющая без перевода · NET +2.43% (+€2.43 на €100) · держится 24 мин 13 с"
    assert plan["movers"] == "Revolut X двинулась первой: +2.2% (ещё 1 биржа следом); Kraken пока +0.3% · разрыв 1.9%"
    assert plan["buy"] == ("1. Купить на Kraken ZK/EUR ≤ 0.0112291 EUR (сейчас 0.0109631 EUR) "
                           "· сумма до €100 (стакан держит €840)")
    assert plan["sell"] == ("2. Не переводить. Продать там же на Kraken, когда догонит: ≥ 0.0114109 EUR "
                            "(цель с учётом издержек)")
    assert plan["timing"] == "Окно по этой монете: ~2 мин 48 с · догнала 0 из 32 · человек успевал 3 из 10"
    # caught up 0 of 32 (< 30 % with >= 5 samples) is a concrete reason; volume 5M >= 2 x 1M is not
    assert plan["caveat"] == ("! Kraken может не догнать: догоняла редко (0 из 32). "
                              "Не догнала за ~10 мин — продай по рынку.")
    assert plan["updated"] == f"обновлено {time.strftime('%H:%M:%S', time.localtime(WALL))}"
    assert plan_lines(plan) == [plan[k] for k in PLAN_KEYS]
    # the same plan is what the opportunity's to_dict carries once the pipeline stored it
    opp.plan = plan
    assert opp.to_dict(clock.mono())["plan"] == plan


def test_plan_short_direction_aware_lines():
    clock = SimClock(wall=WALL, mono=1000.0)
    sig = zk_signal(direction=-1, first_move_pct=-2.2, target_move_pct=-0.3, asset="ETH", target="bybit",
                    target_symbol="ETH/USDT:USDT", target_quote="USDT", mover_volumes_quote={"revolutx": 9e6})
    e = zk_econ(direction=-1, entry_best=2000.5, entry_vwap=2000.0, exit_vwap_now=1999.0, exit_target=1975.0,
                entry_cap_price=1981.9, bid_depth_quote=5000.0, ask_depth_quote=4000.0)
    opp = make_opp(clock, sig, e, quote="USDT")
    plan = build_plan(opp, settings(), NAMES, clock.mono() + 48.0, clock.wall())
    assert plan["header"].startswith("🏃 ETH · шорт · догоняющая без перевода · NET +2.43% (+$2.43 на $100) · держится 48 с")
    assert plan["movers"] == "Revolut X двинулась первой: -2.2% (ещё 1 биржа следом); Bybit EU пока -0.3% · разрыв 1.9%"
    assert plan["buy"] == ("1. Продать (шорт) на Bybit EU ETH/USDT ≥ 1981.90 USDT (сейчас 2000.50 USDT) "
                           "· сумма до $100 (стакан держит $4,000)")
    assert plan["sell"] == ("2. Не переводить. Откупить там же на Bybit EU, когда догонит: ≤ 1975.00 USDT "
                            "(цель с учётом издержек)")
    assert plan["caveat"] == "Не догнала за ~10 мин — продай по рынку."  # nothing to warn about: only the exit rule


def test_plan_without_history_says_so():
    clock = SimClock(wall=WALL, mono=1000.0)
    opp = make_opp(clock, last_update_ts=0.0)  # no stats, no update stamp yet
    plan = build_plan(opp, settings(), NAMES, clock.mono(), clock.wall() + 5.0)
    assert plan["timing"] == "Окно по этой монете: ~8 с (оценка) · догоняла: нет истории"
    assert "человек" not in plan["timing"]
    # a plan with an unknown update time uses "now"
    assert plan["updated"] == f"обновлено {time.strftime('%H:%M:%S', time.localtime(WALL + 5.0))}"
    # 4 of 4 caught up is history but not yet a statistic for the caveat (< 5 samples)
    opp = make_opp(clock, caught_up_ok=1, caught_up_total=4, human_ok=0, human_total=4)
    plan = build_plan(opp, settings(), NAMES, clock.mono(), clock.wall())
    assert plan["timing"].endswith("догнала 1 из 4 · человек успевал 0 из 4")
    assert "догоняла редко" not in plan["caveat"]


def test_plan_low_or_unknown_mover_volume_caveat():
    clock = SimClock(wall=WALL, mono=1000.0)
    low = make_opp(clock, zk_signal(mover_volumes_quote={"revolutx": 1_500_000.0}))  # < 2 x min_mover_volume_quote
    plan = build_plan(low, settings(), NAMES, clock.mono(), clock.wall())
    assert plan["caveat"] == "! Kraken может не догнать: объём на Revolut X мал. Не догнала за ~10 мин — продай по рынку."
    unknown = make_opp(clock, zk_signal(mover_volumes_quote={}))
    plan = build_plan(unknown, settings(max_hold_s=60.0), NAMES, clock.mono(), clock.wall())
    assert plan["caveat"] == ("! Kraken может не догнать: объём на Revolut X неизвестен. "
                              "Не догнала за ~1 мин — продай по рынку.")
    # names fall back to the capitalised exchange id
    assert build_plan(unknown, settings(), None, clock.mono(), clock.wall())["movers"].startswith("Revolutx двинулась первой")


def test_plan_thin_book_shows_what_the_book_holds():
    clock = SimClock(wall=WALL, mono=1000.0)
    e = zk_econ(executable_quote=60.0, net_quote=1.46, liquidity_ok=False, ask_depth_quote=60.0, bid_depth_quote=75.0,
                liquidity_reason="book supports only $60.00 of $100.00 requested")
    opp = make_opp(clock, e=e, caught_up_ok=1, caught_up_total=5)
    plan = build_plan(opp, settings(), NAMES, clock.mono(), clock.wall())
    assert "(+€1.46 на €60)" in plan["header"]
    assert plan["buy"].endswith("· сумма до €60 (стакан держит €60)")
    assert plan["caveat"] == ("! Kraken может не догнать: догоняла редко (1 из 5) / стакан тонкий (держит €60). "
                              "Не догнала за ~10 мин — продай по рынку.")
    # a book that holds exactly the requested capital does not repeat the number
    full = make_opp(clock, e=zk_econ(ask_depth_quote=100.0, bid_depth_quote=100.0))
    assert build_plan(full, settings(), NAMES, clock.mono(), clock.wall())["buy"].endswith("· сумма до €100")


def test_plan_cap_price_falls_back_to_the_formula():
    clock = SimClock(wall=WALL, mono=1000.0)
    s = settings(min_net_pct_show=0.1)
    e = zk_econ(entry_cap_price=0.0)  # economics built without the field (older rows, test doubles)
    assert cap_price(e, s) == pytest.approx(0.0114109 / (1 + (0.85 + 0.1) / 100))
    plan = build_plan(make_opp(clock, e=e), s, NAMES, clock.mono(), clock.wall())
    assert f"≤ {fmt_price(cap_price(e, s))} EUR" in plan["buy"]
    assert cap_price(zk_econ(entry_cap_price=0.0112291), s) == 0.0112291


def test_plan_formatters():
    assert fmt_price(0.0112291) == "0.0112291"
    assert fmt_price(0.5) == "0.5"
    assert fmt_price(0.00001234567) == "0.0000123457"
    assert fmt_price(2.4) == "2.40" and fmt_price(2.40843) == "2.4084" and fmt_price(99.99999) == "100.00"
    assert fmt_price(102140.0) == "102140.00" and fmt_price(0.0) == "—" and fmt_price(None) == "—"
    assert price_str(0.0112291, "EUR") == "0.0112291 EUR" and price_str(0.0, "EUR") == "—" and price_str(2.5, "") == "2.50"
    assert fmt_duration(48) == "48 с" and fmt_duration(24 * 60 + 13) == "24 мин 13 с" and fmt_duration(168) == "2 мин 48 с"
    assert fmt_duration(120) == "2 мин" and fmt_duration(3900) == "1 ч 5 мин" and fmt_duration(7200) == "2 ч"
    assert fmt_duration(0.4) == "0 с" and fmt_duration(None) == "0 с"
    assert fmt_money(102_140) == "$102,140" and fmt_money(2.43, "EUR", signed=True) == "+€2.43"
    assert fmt_money(840.0, "EUR") == "€840" and fmt_money(-1.234, "EUR", signed=True) == "-€1.23"
