"""Offline tests for latbot.engine.quality (assess_quality, grades, context S_eff).

Reference inputs (hand-computed component values):
* edge     NET 1.0 %, total cost 0.25 % + spread 0.10 % + slippage 0 -> 1 / 1.35 = 0.7407
* breadth  3 movers -> (3-1)/(4-1) = 0.6667; volumes binance 1e8 (log10(100)/2 = 1.0),
           okx 1e7 (0.5), third mover unknown (0) -> 1.5 / 3 = 0.5; value 0.5*0.6667 + 0.5*0.5 = 0.5833
* depth    100 of 100 (fill 1.0), book 1000 / 100 = 10x >= 5x (1.0) -> 1.0
* history  caught up 8/10, k = 10 -> (8 + 5) / 20 = 0.65
* timing   14 s window / 7 s human = 2x -> (2-1)/(3-1) = 0.5; delays [1,2,2,3,4]: median 2,
           IQR 3-2 = 1 -> 1/(1+0.5) = 0.6667; value 0.5*0.5 + 0.5*0.6667 = 0.5833
* context  news +0.5 (w 1.0), btc_regime -0.2 (w 0.8), funding unavailable (w 0.6):
           S = (0.5 - 0.16) / 1.8 = 0.18889, C = 1.8 / 2.4 = 0.75, S_eff = 0.14167 -> 0.57083
score = 100 * (0.2*0.7407 + 0.1*0.5833 + 0.1*1 + 0.2*0.65 + 0.1*0.5833 + 0.3*0.57083) = 66.61 -> 67 (B)
"""
from __future__ import annotations

import json
from types import SimpleNamespace as NS

import pytest

from latbot.config import Settings
from latbot.context.base import Factor
from latbot.engine.quality import (
    DEFAULT_QUALITY_WEIGHTS,
    QualityReport,
    assess_quality,
    grade_for,
    quality_weights,
    required_score,
)
from latbot.models import ExecAssessment, ExecutionEconomics, LagSignal, Light, ReactionStats

EDGE = 1.0 / 1.35
BREADTH = 0.5 * (2.0 / 3.0) + 0.5 * 0.5
DEPTH = 1.0
HISTORY = 0.65
TIMING = 0.5 * 0.5 + 0.5 * (1.0 / 1.5)
S_MEAN = (0.5 * 1.0 - 0.2 * 0.8) / 1.8
COVERAGE = 1.8 / 2.4
CONTEXT = 0.5 + 0.5 * S_MEAN * COVERAGE
RAW = 100 * (0.2 * EDGE + 0.1 * BREADTH + 0.1 * DEPTH + 0.2 * HISTORY + 0.1 * TIMING + 0.3 * CONTEXT)


def signal(movers: int = 3, volumes: dict | None = None) -> LagSignal:
    return LagSignal(
        asset="BTC", direction=1, first_mover="binance", first_move_pct=1.5, first_move_start_mono=0.0,
        reacted={}, target="kraken", target_symbol="BTC/EUR", target_move_pct=0.1, lag_pct=1.4,
        expected_catchup_pct=1.35, catchup_ratio=1.0, detected_mono=0.0, detected_ts=0.0, movers_count=movers,
        mover_volumes_quote={"binance": 1e8, "okx": 1e7} if volumes is None else volumes,
    )


def econ(net: float = 1.0, capital: float = 100.0, executable: float = 100.0, depth: float = 1000.0,
         spread: float = 0.1, slip: float = 0.0) -> ExecutionEconomics:
    return ExecutionEconomics(
        direction=1, capital_quote=capital, executable_quote=executable, base_amount=1.0, entry_best=100.0,
        entry_vwap=100.0, exit_best=99.9, exit_vwap_now=99.9, exit_target=101.35, spread_pct=spread,
        entry_fee_pct=0.1, exit_fee_pct=0.1, entry_slippage_pct=slip, exit_slippage_pct=slip,
        execution_loss_pct=0.05, gross_pct=net + 0.25, total_cost_pct=0.25, net_pct=net, net_quote=net,
        liquidity_ok=True, bid_depth_quote=depth, ask_depth_quote=depth,
    )


def assess(light: Light = Light.GREEN, est: float = 7.0) -> ExecAssessment:
    return ExecAssessment(light=light, label="HIGH", est_execution_s=est, margin_s=7.0, probability=0.8)


def stats(ok: int = 8, total: int = 10, delays: list[float] | None = None) -> ReactionStats:
    return ReactionStats(asset="BTC", exchange="kraken", caught_up_ok=ok, caught_up_total=total,
                         reaction_delays_s=[1.0, 2.0, 2.0, 3.0, 4.0] if delays is None else delays)


def factors() -> list[Factor]:
    return [
        Factor(key="news", label="News", available=True, score=0.5, weight=1.0, detail="ETF приток +1.2 млрд"),
        Factor(key="funding", label="Funding", available=False, score=0.0, weight=0.6,
               detail="история копится 12/288"),
        Factor(key="btc_regime", label="BTC regime", available=True, score=-0.2, weight=0.8,
               detail="BTC 15м −0.1%"),
    ]


S = NS()  # every quality setting at its documented default


def run(**kw) -> QualityReport:
    args = dict(signal=signal(), econ=econ(), human=assess(), bot=assess(est=0.5), stats=stats(),
                factors=factors(), settings=S, remaining_s=14.0)
    args.update(kw)
    remaining = args.pop("remaining_s")
    return assess_quality(args["signal"], args["econ"], args["human"], args["bot"], args["stats"],
                          args["factors"], args["settings"], remaining_s=remaining,
                          **{k: v for k, v in args.items() if k in ("metalabel", "event_window")})


def comp(report: QualityReport, key: str):
    return next(c for c in report.components if c.key == key)


# --------------------------------------------------------------------------- reference
def test_reference_score_and_components():
    r = run()
    assert comp(r, "edge").value == pytest.approx(EDGE)
    assert comp(r, "breadth").value == pytest.approx(BREADTH)
    assert comp(r, "depth").value == pytest.approx(DEPTH)
    assert comp(r, "history").value == pytest.approx(HISTORY)
    assert comp(r, "timing").value == pytest.approx(TIMING)
    assert comp(r, "context").value == pytest.approx(CONTEXT)
    assert r.coverage == pytest.approx(COVERAGE)
    assert r.context_s == pytest.approx(S_MEAN)
    assert r.context_s_eff == pytest.approx(S_MEAN * COVERAGE)
    assert r.raw_score == pytest.approx(RAW)
    assert r.score == 67 and r.grade == "B" and r.passed and r.vetoes == []
    assert r.min_score == 60
    assert [c.key for c in r.components] == ["edge", "breadth", "depth", "history", "timing", "context"]
    assert [c.weight for c in r.components] == [0.2, 0.1, 0.1, 0.2, 0.1, 0.3]
    assert [c.label for c in r.components] == ["Edge", "Breadth", "Depth", "History", "Timing", "Context"]
    assert r.summary.startswith("Качество 67/100 (B)")


def test_with_real_settings_defaults():
    r = run(settings=Settings())
    assert r.score == 67 and r.passed


# --------------------------------------------------------------------------- components
def test_edge_component():
    # NET 0.25 vs friction 0.35 -> 0.25 / 0.6
    r = run(econ=econ(net=0.25))
    assert comp(r, "edge").value == pytest.approx(0.25 / 0.6)
    # slippage counts as friction: 0.25 + 0.1 + 0.1 + 0.1 = 0.55 -> 1 / 1.55
    r = run(econ=econ(slip=0.1))
    assert comp(r, "edge").value == pytest.approx(1.0 / 1.55)
    assert "NET +1.00%" in comp(r, "edge").detail


def test_net_not_positive_never_passes():
    r = run(econ=econ(net=-0.1))
    assert comp(r, "edge").value == 0.0
    assert not r.passed and r.score <= 59
    assert any("NET" in v for v in r.vetoes)


def test_breadth_component():
    r = run(signal=signal(movers=1, volumes={}))
    assert comp(r, "breadth").value == 0.0
    assert "нет данных" in comp(r, "breadth").detail
    r = run(signal=signal(movers=4, volumes={"a": 1e8, "b": 2e8, "c": 1e9, "d": 1e8}))
    assert comp(r, "breadth").value == pytest.approx(1.0)
    # the unknown third mover is not credited and is said so
    assert "без данных об объёме: 1" in comp(run(), "breadth").detail


def test_breadth_volume_reference_follows_min_mover_volume():
    # ref 1e7: binance 1e8 -> 0.5, okx 1e7 -> 0 -> 0.5 / 3
    r = run(settings=NS(min_mover_volume_quote=1e7))
    assert comp(r, "breadth").value == pytest.approx(0.5 * (2 / 3) + 0.5 * (0.5 / 3))


def test_depth_component():
    # fill 50/100 = 0.5; headroom 150/50 = 3 -> (3-1)/(5-1) = 0.5
    r = run(econ=econ(executable=50.0, depth=150.0))
    assert comp(r, "depth").value == pytest.approx(0.5)
    r = run(econ=econ(executable=5.0, depth=5.0))  # below min_trade_quote 10
    assert comp(r, "depth").value == 0.0
    assert "минимальной сделки" in comp(r, "depth").detail


def test_history_beta_shrinkage_and_missing_stats():
    r = run(stats=None)
    h = comp(r, "history")
    assert h.value == pytest.approx(0.5) and not h.available
    assert "без данных: история" in r.summary
    r = run(stats=stats(ok=0, total=10))  # (0 + 5) / 20
    assert comp(r, "history").value == pytest.approx(0.25)
    r = run(stats=stats(ok=30, total=30), settings=NS(quality_history_k=20))  # (30 + 10) / 50
    assert comp(r, "history").value == pytest.approx(0.8)


def test_history_uses_metalabel_p10_only_with_enough_outcomes():
    ml = {"n": 25, "mean": 0.7, "p10": 0.58, "key": ("BTC", "kraken", "binance", "1-2", "mid"), "level": "full"}
    r = run(metalabel=ml)
    h = comp(r, "history")
    assert h.value == pytest.approx(0.58) and h.available
    assert "p10 58%" in h.detail and "n=25" in h.detail and "BTC/kraken" in h.detail
    small = dict(ml, n=7)
    r = run(metalabel=small)
    assert comp(r, "history").value == pytest.approx(HISTORY)
    assert "n=7 < 20" in comp(r, "history").detail
    r = run(metalabel={"n": 25, "mean": None, "p10": None, "key": ()})  # no estimate -> catch-up
    assert comp(r, "history").value == pytest.approx(HISTORY)
    assert "нет оценки p10" in comp(r, "history").detail
    r = run(metalabel=dict(ml, n=12), settings=NS(metalabel_min_n=10))
    assert comp(r, "history").value == pytest.approx(0.58)


def test_timing_component():
    r = run(stats=stats(delays=[]))  # no delay history -> neutral stability 0.5
    assert comp(r, "timing").value == pytest.approx(0.5 * 0.5 + 0.5 * 0.5)
    assert "мало данных" in comp(r, "timing").detail
    r = run(remaining_s=5.0)  # window shorter than the human needs
    assert comp(r, "timing").value == pytest.approx(0.5 * 0.0 + 0.5 * (1 / 1.5))
    r = run(remaining_s=30.0, stats=stats(delays=[2.0] * 6))  # 30/7 >= 3x; constant delays
    assert comp(r, "timing").value == pytest.approx(1.0)


# --------------------------------------------------------------------------- context
def test_context_all_unavailable_is_exactly_neutral():
    fs = [Factor(key="news", label="News", available=False, weight=1.0, detail="источник недоступен"),
          Factor(key="tradfi", label="TradFi", available=False, weight=0.3, detail="рынок закрыт")]
    r = run(factors=fs)
    c = comp(r, "context")
    assert c.value == pytest.approx(0.5) and not c.available and r.coverage == 0.0
    assert r.unavailable == ["news", "tradfi"]
    assert "нет данных: News (источник недоступен); TradFi (рынок закрыт)" in r.summary
    r = run(factors=[])
    assert comp(r, "context").value == pytest.approx(0.5) and r.coverage == 0.0


def test_missing_factor_never_creates_support():
    base = [Factor(key="news", label="News", available=True, score=0.8, weight=1.0, detail="x")]
    with_missing = base + [Factor(key="whale", label="Whale", available=False, weight=1.0, detail="нет ключа")]
    with_zero = base + [Factor(key="whale", label="Whale", available=True, score=0.0, weight=1.0, detail="0")]
    v_base = comp(run(factors=base), "context").value
    v_missing = comp(run(factors=with_missing), "context").value
    v_zero = comp(run(factors=with_zero), "context").value
    # 0.5 + 0.5 * 0.8 * 1.0 = 0.9; with a missing equal-weight factor C = 0.5 -> 0.7
    assert v_base == pytest.approx(0.9)
    assert v_missing == pytest.approx(0.7)
    assert v_missing < v_base
    assert v_missing == pytest.approx(v_zero)  # unknown counts as neutral, never as support


def test_veto_blocks_and_caps_score():
    fs = factors() + [Factor(key="macro_event", label="Macro", available=True, score=1.0, weight=1.0,
                             detail="CPI через 120 с", veto=True)]
    r = run(factors=fs, stats=stats(ok=10, total=10), signal=signal(movers=5))
    assert not r.passed
    assert r.raw_score > 70  # would pass without the veto ...
    assert r.score == 59  # ... but is capped below min_quality_score
    assert r.vetoes == ["Macro: CPI через 120 с"]
    assert r.grade == "X"  # blocked: kept out of the A-D statistics
    assert "вето: Macro: CPI через 120 с" in r.summary


def test_veto_from_unavailable_factor_still_blocks():
    fs = [Factor(key="erratic", label="Erratic", available=False, weight=1.0, detail="2 бара ≥1% за час",
                 veto=True)]
    r = run(factors=fs)
    assert not r.passed and r.vetoes == ["Erratic: 2 бара ≥1% за час"]


def test_event_window_raises_minimum():
    r = run(event_window=True)
    assert r.score == 67 and r.min_score == 70
    assert not r.passed and r.vetoes == []
    assert "окно макро-события" in r.summary
    assert required_score(S) == 60 and required_score(S, True) == 70
    assert required_score(NS(min_quality_score=50, quality_event_window_bonus=5), True) == 55


def test_grades():
    assert grade_for(100) == "A" and grade_for(80) == "A"
    assert grade_for(79) == "B" and grade_for(65) == "B"
    assert grade_for(64) == "C" and grade_for(50) == "C"
    assert grade_for(49) == "D" and grade_for(0) == "D"
    assert grade_for(float("nan")) == "D"


def test_weights_override_and_fallback():
    w = quality_weights(NS(quality_weights={"context": 0.0, "bogus": 5.0, "edge": float("nan")}))
    assert w["context"] == 0.0 and w["edge"] == 0.0 and "bogus" not in w
    assert quality_weights(NS(quality_weights={k: 0.0 for k in DEFAULT_QUALITY_WEIGHTS})) == DEFAULT_QUALITY_WEIGHTS
    r = run(settings=NS(quality_weights={"context": 0.0}))
    expected = 100 * (0.2 * EDGE + 0.1 * BREADTH + 0.1 * DEPTH + 0.2 * HISTORY + 0.1 * TIMING) / 0.7
    assert r.raw_score == pytest.approx(expected)


def test_dict_factors_and_none_inputs():
    fs = [{"key": "news", "label": "News", "available": True, "score": 1.0, "weight": 1.0, "detail": "d"}]
    r = assess_quality(signal(), econ(), None, None, None, fs, S, remaining_s=14.0)
    assert comp(r, "context").value == pytest.approx(1.0)
    assert 0 <= r.score <= 100


def test_to_dict_json_safe():
    fs = factors() + [Factor(key="liquidations", label="Liquidations", available=True, score=0.3, weight=0.5,
                             detail="ликвидации шортов 3.1M",
                             data={"nan": float("nan"), "pair": ("a", 1), "light": Light.RED, "nested": {"x": float("inf")}})]
    r = run(factors=fs, metalabel={"n": 30, "mean": 0.6, "p10": 0.52, "key": ("BTC", "kraken")})
    d = r.to_dict()
    json.dumps(d, allow_nan=False, ensure_ascii=False)
    assert isinstance(d["score"], int) and d["grade"] in ("A", "B", "C", "D")
    assert d["factors"][-1]["data"] == {"nan": None, "pair": ["a", 1], "light": "red", "nested": {"x": None}}
    assert [c["key"] for c in d["components"]] == ["edge", "breadth", "depth", "history", "timing", "context"]
    for key in ("score", "grade", "passed", "vetoes", "components", "factors", "coverage", "summary"):
        assert key in d


def test_deterministic():
    assert run().to_dict() == run().to_dict()
