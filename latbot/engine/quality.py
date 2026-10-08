"""Signal quality score (Phase 3, ARCHITECTURE P3.9 + P3.12).

A transparent 0..100 score for ONE lag signal on ONE target exchange, built
from six components with fixed, documented weights (``quality_weights``).
Every component value is 0..1 and carries a Russian one-liner with the
numbers behind it:

* ``edge``    - share of the expected catch-up that survives as NET:
                NET / (NET + friction), friction = fees + execution loss +
                spread + entry/exit slippage (all already inside NET).
* ``breadth`` - how many venues moved (full marks at
                ``quality_breadth_full_movers``) and their 24 h volume
                (log10 of volume / ``min_mover_volume_quote``, full at 100x);
                a mover with unknown volume contributes 0 (no data, no support).
* ``depth``   - executable / requested capital and book headroom
                (min(bid, ask) depth within max slippage / executable size,
                full at ``quality_depth_full_ratio``).
* ``history`` - measured outcome rate: the meta-label lower bound p10 when
                ``n >= metalabel_min_n``, otherwise the catch-up rate of
                (asset, target) shrunk to 0.5 with a Beta prior of strength
                ``quality_history_k`` - n = 0 gives exactly 0.5.
* ``timing``  - window vs human execution time (full at
                ``quality_timing_full_ratio`` x human time) and stability of
                the target's reaction delays (1 / (1 + IQR / median); fewer
                than ``quality_timing_min_n`` samples -> neutral 0.5).
* ``context`` - ``0.5 + 0.5 * S_eff`` with S = sum(w*s) / sum(w) over the
                AVAILABLE factors, coverage C = sum(w available) / sum(w all)
                and S_eff = S * C: missing data shrinks towards neutral and can
                never create support.

Score = round(100 * sum(w*v) / sum(w)).  A factor veto (or NET <= 0) sets
``passed=False`` and caps the score below ``min_quality_score``.  Inside a
macro event window the required minimum rises by
``quality_event_window_bonus``.  Grades: A >= 80, B >= 65, C >= 50, D < 50.

Pure function: no I/O, no clock, deterministic.  Weights and thresholds are
owner settings - never fitted to past trades (CLAUDE.md rule 10).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

from ..models import percentile

log = logging.getLogger(__name__)

COMPONENT_KEYS: tuple[str, ...] = ("edge", "breadth", "depth", "history", "timing", "context")

DEFAULT_QUALITY_WEIGHTS: dict[str, float] = {
    "edge": 0.20,
    "breadth": 0.10,
    "depth": 0.10,
    "history": 0.20,
    "timing": 0.10,
    "context": 0.30,
}

# English UI labels (the card shows them next to the bar); details are Russian.
COMPONENT_LABELS: dict[str, str] = {
    "edge": "Edge",
    "breadth": "Breadth",
    "depth": "Depth",
    "history": "History",
    "timing": "Timing",
    "context": "Context",
}

# Russian names used inside the human-facing summary.
_COMPONENT_RU: dict[str, str] = {
    "edge": "запас NET",
    "breadth": "ширина движения",
    "depth": "глубина",
    "history": "история",
    "timing": "тайминг",
    "context": "контекст",
}

GRADE_THRESHOLDS: tuple[tuple[str, int], ...] = (("A", 80), ("B", 65), ("C", 50))

# Defaults of the settings read through getattr (the integration adds the same
# fields to Settings).
DEFAULT_MIN_QUALITY_SCORE = 60
DEFAULT_EVENT_WINDOW_BONUS = 10
DEFAULT_HISTORY_K = 10.0
DEFAULT_METALABEL_MIN_N = 20
DEFAULT_BREADTH_FULL_MOVERS = 4
DEFAULT_BREADTH_FULL_VOLUME_X = 100.0  # mover volume / min_mover_volume_quote for full marks
DEFAULT_DEPTH_FULL_RATIO = 5.0
DEFAULT_TIMING_FULL_RATIO = 3.0
DEFAULT_TIMING_MIN_N = 5
_DEFAULT_MIN_MOVER_VOLUME = 1_000_000.0
_DELAY_MEDIAN_FLOOR_S = 0.1  # IQR / median would explode for sub-100 ms medians


# --------------------------------------------------------------------------- #
# small numeric helpers
# --------------------------------------------------------------------------- #
def _num(v: Any) -> float | None:
    """Finite float or None (bools are not numbers here)."""
    if isinstance(v, bool) or v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _f(v: Any, default: float = 0.0) -> float:
    x = _num(v)
    return default if x is None else x


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return lo if x < lo else hi if x > hi else x


def _setting(settings: Any, name: str, default: float) -> float:
    x = _num(getattr(settings, name, default))
    return float(default) if x is None else x


def _round_half_up(x: float) -> int:
    return int(math.floor(x + 0.5))


def _fmt_vol(v: float) -> str:
    if v >= 1e9:
        return f"{v / 1e9:.1f}B"
    if v >= 1e6:
        return f"{v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{v / 1e3:.0f}k"
    return f"{v:.0f}"


def _json_safe(v: Any) -> Any:
    """Recursively convert to JSON-serialisable values; non-finite floats -> None."""
    if v is None or isinstance(v, bool):
        return v
    if isinstance(v, str):
        inner = getattr(v, "value", v)  # str-mixin Enums -> their plain value
        return str(inner) if isinstance(inner, str) else str.__str__(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, dict):
        return {str(k): _json_safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_json_safe(x) for x in v]
    value = getattr(v, "value", None)  # Enums
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        return _json_safe(value)
    x = _num(v)
    if x is not None:
        return x
    return str(v)


def grade_for(score: float) -> str:
    """A >= 80, B >= 65, C >= 50, D below."""
    s = _f(score, 0.0)
    for grade, threshold in GRADE_THRESHOLDS:
        if s >= threshold:
            return grade
    return "D"


def quality_weights(settings: Any) -> dict[str, float]:
    """Fixed component weights: defaults overlaid with ``settings.quality_weights``.

    Unknown keys are ignored, negative / non-finite weights become 0; an
    all-zero result falls back to the documented defaults."""
    w = dict(DEFAULT_QUALITY_WEIGHTS)
    given = getattr(settings, "quality_weights", None)
    if isinstance(given, dict):
        for k, v in given.items():
            if k in w:
                x = _num(v)
                w[k] = max(0.0, x) if x is not None else 0.0
    if sum(w.values()) <= 0.0:
        w = dict(DEFAULT_QUALITY_WEIGHTS)
    return w


def required_score(settings: Any, event_window: bool = False) -> int:
    """Minimum score for ``passed`` (raised inside macro event windows)."""
    base = _setting(settings, "min_quality_score", DEFAULT_MIN_QUALITY_SCORE)
    bonus = max(0.0, _setting(settings, "quality_event_window_bonus", DEFAULT_EVENT_WINDOW_BONUS))
    return _round_half_up(base + (bonus if event_window else 0.0))


# --------------------------------------------------------------------------- #
# report
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class QualityComponent:
    key: str
    label: str
    value: float  # 0..1
    weight: float
    detail: str  # Russian, with the numbers
    available: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "value": _json_safe(round(self.value, 4)),
            "weight": _json_safe(self.weight),
            "detail": self.detail,
            "available": bool(self.available),
        }


@dataclass(slots=True)
class QualityReport:
    score: int
    grade: str
    passed: bool
    vetoes: list[str]
    components: list[QualityComponent]
    factors: list[dict]
    coverage: float  # context coverage C (0..1)
    summary: str  # Russian
    min_score: int = DEFAULT_MIN_QUALITY_SCORE  # the minimum that applied (event window included)
    raw_score: float = 0.0  # before rounding / veto cap
    event_window: bool = False
    context_s: float = 0.0  # S: weighted mean over available factors (-1..1)
    context_s_eff: float = 0.0  # S * C
    unavailable: list[str] = field(default_factory=list)  # keys of factors without data

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": int(self.score),
            "grade": self.grade,
            "passed": bool(self.passed),
            "vetoes": [str(v) for v in self.vetoes],
            "components": [c.to_dict() for c in self.components],
            "factors": _json_safe(self.factors),
            "coverage": _json_safe(round(self.coverage, 4)),
            "summary": self.summary,
            "min_score": int(self.min_score),
            "raw_score": _json_safe(round(self.raw_score, 2)),
            "event_window": bool(self.event_window),
            "context_s": _json_safe(round(self.context_s, 4)),
            "context_s_eff": _json_safe(round(self.context_s_eff, 4)),
            "unavailable": [str(u) for u in self.unavailable],
        }


# --------------------------------------------------------------------------- #
# components
# --------------------------------------------------------------------------- #
def _edge(econ: Any) -> tuple[float, str]:
    net = _f(getattr(econ, "net_pct", 0.0))
    fees = _f(getattr(econ, "entry_fee_pct", 0.0)) + _f(getattr(econ, "exit_fee_pct", 0.0))
    total_cost = max(0.0, _f(getattr(econ, "total_cost_pct", 0.0)))
    exec_loss = max(0.0, total_cost - fees)
    spread = max(0.0, _f(getattr(econ, "spread_pct", 0.0)))
    slip = max(0.0, _f(getattr(econ, "entry_slippage_pct", 0.0))) + max(0.0, _f(getattr(econ, "exit_slippage_pct", 0.0)))
    friction = total_cost + spread + slip
    if net <= 0.0:
        value = 0.0
    else:
        value = _clamp(net / (net + friction)) if net + friction > 0 else 0.0
    detail = (
        f"NET {net:+.2f}% при издержках {friction:.2f}% (комиссии {fees:.2f}%, исполнение {exec_loss:.2f}%, "
        f"спред {spread:.2f}%, проскальзывание {slip:.2f}%): в NET остаётся {value:.0%} движения"
    )
    return value, detail


def _breadth(signal: Any, settings: Any) -> tuple[float, str]:
    full = max(2.0, _setting(settings, "quality_breadth_full_movers", DEFAULT_BREADTH_FULL_MOVERS))
    movers = max(1, int(_f(getattr(signal, "movers_count", 1), 1.0)))
    count_part = _clamp((movers - 1) / (full - 1.0))
    ref = _f(getattr(settings, "min_mover_volume_quote", _DEFAULT_MIN_MOVER_VOLUME), _DEFAULT_MIN_MOVER_VOLUME)
    if ref <= 0:
        ref = _DEFAULT_MIN_MOVER_VOLUME
    full_x = max(1.0 + 1e-9, _setting(settings, "quality_breadth_full_volume_x", DEFAULT_BREADTH_FULL_VOLUME_X))
    raw_volumes = getattr(signal, "mover_volumes_quote", None) or {}
    volumes: dict[str, float] = {}
    if isinstance(raw_volumes, dict):
        for ex, v in raw_volumes.items():
            x = _num(v)
            if x is not None and x >= 0:
                volumes[str(ex)] = x
    slots = max(movers, len(volumes))
    parts = [_clamp(math.log10(v / ref) / math.log10(full_x)) if v > 0 else 0.0 for v in volumes.values()]
    vol_part = sum(parts) / slots if slots > 0 else 0.0
    value = 0.5 * count_part + 0.5 * vol_part
    vols_txt = ", ".join(f"{ex} {_fmt_vol(v)}" for ex, v in sorted(volumes.items(), key=lambda kv: -kv[1]))
    unknown = slots - len(volumes)
    detail = f"двинулось бирж: {movers} (полный балл с {int(full)}); объём 24ч: {vols_txt or 'нет данных'}"
    if unknown > 0:
        detail += f"; без данных об объёме: {unknown} (не засчитаны)"
    return value, detail


def _depth(econ: Any, settings: Any) -> tuple[float, str]:
    capital = max(0.0, _f(getattr(econ, "capital_quote", 0.0)))
    executable = max(0.0, _f(getattr(econ, "executable_quote", 0.0)))
    bid = max(0.0, _f(getattr(econ, "bid_depth_quote", 0.0)))
    ask = max(0.0, _f(getattr(econ, "ask_depth_quote", 0.0)))
    min_trade = max(0.0, _f(getattr(settings, "min_trade_quote", 10.0), 10.0))
    full = max(1.0 + 1e-9, _setting(settings, "quality_depth_full_ratio", DEFAULT_DEPTH_FULL_RATIO))
    fill = _clamp(executable / capital) if capital > 0 else 0.0
    book = min(bid, ask)
    headroom = book / executable if executable > 0 else 0.0
    head_part = _clamp((headroom - 1.0) / (full - 1.0))
    value = 0.5 * fill + 0.5 * head_part
    detail = (
        f"исполнимо {executable:,.2f} из {capital:,.2f} ({fill:.0%}); стакан в пределах проскальзывания "
        f"{book:,.2f} = {headroom:.1f}× размера (полный балл с {full:g}×)"
    )
    if executable < min_trade or executable <= 0:
        value = 0.0
        detail += f"; меньше минимальной сделки {min_trade:,.2f}"
    return value, detail


def _history(stats: Any, metalabel: dict | None, settings: Any) -> tuple[float, str, bool]:
    min_n = int(_setting(settings, "metalabel_min_n", DEFAULT_METALABEL_MIN_N))
    ml_note = ""
    if isinstance(metalabel, dict):
        n_ml = int(_f(metalabel.get("n"), 0.0))
        p10 = _num(metalabel.get("p10"))
        mean = _num(metalabel.get("mean"))
        key = metalabel.get("key")
        key_txt = "/".join(str(k) for k in key) if isinstance(key, (list, tuple)) else str(key or "")
        level = str(metalabel.get("level") or "")
        if level:
            key_txt = f"{level}:{key_txt}" if key_txt else level
        if n_ml >= min_n and p10 is not None:
            value = _clamp(p10)
            mean_txt = f"{mean:.0%}" if mean is not None else "н/д"
            detail = (
                f"мета-метка [{key_txt or 'global'}]: побед {mean_txt}, нижняя граница p10 {value:.0%}, "
                f"n={n_ml} (≥ {min_n})"
            )
            return value, detail, True
        if n_ml < min_n:
            ml_note = f"; мета-метка: n={n_ml} < {min_n} — используется догон"
        else:
            ml_note = f"; мета-метка: нет оценки p10 (n={n_ml}) — используется догон"
    k = max(0.0, _setting(settings, "quality_history_k", DEFAULT_HISTORY_K))
    ok = max(0, int(_f(getattr(stats, "caught_up_ok", 0), 0.0))) if stats is not None else 0
    total = max(0, int(_f(getattr(stats, "caught_up_total", 0), 0.0))) if stats is not None else 0
    ok = min(ok, total)
    if total + k > 0:
        value = (ok + 0.5 * k) / (total + k)
    else:
        value = 0.5
    if total > 0:
        detail = f"догоняла {ok} из {total} ({ok / total:.0%}); со сжатием к 50% (k={k:g}): {value:.0%}"
    else:
        detail = "нет истории догона по этой паре (нейтрально 50%)"
    return _clamp(value), detail + ml_note, total > 0


def _timing(human: Any, stats: Any, settings: Any, remaining_s: float) -> tuple[float, str]:
    est = _num(getattr(human, "est_execution_s", None)) if human is not None else None
    if est is None or est <= 0:
        est = max(0.0, _f(getattr(settings, "human_execution_time_s", 7.0), 7.0))
    remaining = max(0.0, _f(remaining_s, 0.0))
    full = max(1.0 + 1e-9, _setting(settings, "quality_timing_full_ratio", DEFAULT_TIMING_FULL_RATIO))
    if est > 0:
        ratio = remaining / est
        window_part = _clamp((ratio - 1.0) / (full - 1.0))
        window_txt = f"окно {remaining:.1f} с против ~{est:.0f} с у человека ({ratio:.1f}×, полный балл с {full:g}×)"
    else:
        window_part = 0.0
        window_txt = "время исполнения человеком неизвестно"
    min_n = max(1, int(_setting(settings, "quality_timing_min_n", DEFAULT_TIMING_MIN_N)))
    raw = getattr(stats, "reaction_delays_s", None) if stats is not None else None
    delays = [x for x in (_num(v) for v in (raw or [])) if x is not None and x >= 0]
    if len(delays) >= min_n:
        med = percentile(delays, 50) or 0.0
        p25 = percentile(delays, 25) or 0.0
        p75 = percentile(delays, 75) or 0.0
        iqr = max(0.0, p75 - p25)
        cv = iqr / max(med, _DELAY_MEDIAN_FLOOR_S)
        stability = 1.0 / (1.0 + cv)
        delay_txt = f"задержка реакции: медиана {med:.1f} с, IQR {iqr:.1f} с (n={len(delays)}) → стабильность {stability:.0%}"
    else:
        stability = 0.5
        delay_txt = f"задержки реакции: мало данных ({len(delays)}/{min_n}, нейтрально 50%)"
    value = 0.5 * window_part + 0.5 * stability
    return value, f"{window_txt}; {delay_txt}"


def _fget(f: Any, name: str, default: Any = None) -> Any:
    if isinstance(f, dict):
        return f.get(name, default)
    return getattr(f, name, default)


def _factor_dict(f: Any) -> dict:
    if isinstance(f, dict):
        return dict(f)
    to_dict = getattr(f, "to_dict", None)
    if callable(to_dict):
        try:
            d = to_dict()
            if isinstance(d, dict):
                return d
        except Exception:  # noqa: BLE001 - a broken factor must not break the score
            log.debug("factor to_dict failed", exc_info=True)
    return {k: _fget(f, k) for k in ("key", "label", "available", "score", "weight", "detail", "veto", "data")}


def _context(factors: list[Any]) -> tuple[float, str, bool, float, float, float, list[str], list[str], list[str]]:
    """-> value, detail, available, coverage, S, S_eff, vetoes, unavailable keys, unavailable lines."""
    sum_all = 0.0
    sum_avail = 0.0
    sum_ws = 0.0
    vetoes: list[str] = []
    unavailable: list[str] = []
    unavailable_txt: list[str] = []
    used: list[str] = []
    for f in factors or []:
        key = str(_fget(f, "key", "") or "")
        label = str(_fget(f, "label", "") or key)
        detail = str(_fget(f, "detail", "") or "")
        w = _num(_fget(f, "weight", 1.0))
        w = max(0.0, w) if w is not None else 0.0
        score = _num(_fget(f, "score", 0.0))
        if bool(_fget(f, "veto", False)):
            vetoes.append(f"{label}: {detail}" if detail else label)
        sum_all += w
        if bool(_fget(f, "available", False)) and score is not None:
            s = _clamp(score, -1.0, 1.0)
            sum_avail += w
            sum_ws += w * s
            used.append(f"{label} {s:+.2f}×{w:g}")
        else:
            unavailable.append(key or label)
            unavailable_txt.append(f"{label} ({detail})" if detail else label)
    s_mean = sum_ws / sum_avail if sum_avail > 0 else 0.0
    coverage = sum_avail / sum_all if sum_all > 0 else 0.0
    s_eff = s_mean * coverage
    value = _clamp(0.5 + 0.5 * s_eff)
    if not factors:
        detail = "нет факторов контекста (нейтрально 50%)"
    else:
        detail = f"S {s_mean:+.2f} × покрытие {coverage:.0%} = {s_eff:+.2f}"
        if used:
            detail += "; " + ", ".join(used)
        if unavailable_txt:
            detail += "; нет данных: " + "; ".join(unavailable_txt)
    return value, detail, sum_avail > 0, coverage, s_mean, s_eff, vetoes, unavailable, unavailable_txt


# --------------------------------------------------------------------------- #
# public
# --------------------------------------------------------------------------- #
def assess_quality(
    signal: Any,
    econ: Any,
    human: Any,
    bot: Any,
    stats: Any,
    factors: list[Any],
    settings: Any,
    *,
    remaining_s: float,
    metalabel: dict | None = None,
    event_window: bool = False,
) -> QualityReport:
    """Score one opportunity (see the module docstring for every formula).

    ``bot`` is accepted for the contract (the BOT light gates AUTO on its
    own); timing is judged against the human, as the owner trades by hand.
    """
    weights = quality_weights(settings)
    edge_v, edge_d = _edge(econ)
    breadth_v, breadth_d = _breadth(signal, settings)
    depth_v, depth_d = _depth(econ, settings)
    hist_v, hist_d, hist_avail = _history(stats, metalabel, settings)
    timing_v, timing_d = _timing(human, stats, settings, remaining_s)
    (ctx_v, ctx_d, ctx_avail, coverage, s_mean, s_eff, vetoes, unavailable, unavailable_txt) = _context(factors)

    values = {
        "edge": (edge_v, edge_d, True),
        "breadth": (breadth_v, breadth_d, True),
        "depth": (depth_v, depth_d, True),
        "history": (hist_v, hist_d, hist_avail),
        "timing": (timing_v, timing_d, True),
        "context": (ctx_v, ctx_d, ctx_avail),
    }
    components = [
        QualityComponent(key=k, label=COMPONENT_LABELS[k], value=_clamp(values[k][0]), weight=weights[k],
                         detail=values[k][1], available=values[k][2])
        for k in COMPONENT_KEYS
    ]
    total_w = sum(c.weight for c in components)
    raw = 100.0 * sum(c.weight * c.value for c in components) / total_w if total_w > 0 else 0.0
    raw = _clamp(raw, 0.0, 100.0)
    score = _round_half_up(raw)

    net = _f(getattr(econ, "net_pct", 0.0))
    if net <= 0.0:
        vetoes = vetoes + [f"NET после издержек {net:+.2f}% ≤ 0"]
    base_min = required_score(settings, False)
    min_score = required_score(settings, event_window)
    if vetoes:
        score = min(score, max(0, base_min - 1))
    score = max(0, min(100, score))
    passed = not vetoes and score >= min_score
    grade = grade_for(score)

    parts = [f"Качество {score}/100 ({grade})"]
    if passed:
        parts.append(f"проходит порог {min_score}")
    else:
        parts.append(f"ниже порога {min_score}" if not vetoes else f"заблокировано (порог {min_score})")
    if event_window:
        parts.append(f"окно макро-события: порог +{min_score - base_min}")
    weighted = [c for c in components if c.weight > 0]
    by_value = sorted(weighted, key=lambda c: (-c.value, COMPONENT_KEYS.index(c.key)))
    if by_value:
        best = by_value[0]
        parts.append(f"сильнее всего: {_COMPONENT_RU[best.key]} {best.value:.0%}")
        weakest = [c for c in reversed(by_value[1:])][:2]
    else:
        weakest = []
    if weakest:
        parts.append("слабее всего: " + ", ".join(f"{_COMPONENT_RU[c.key]} {c.value:.0%}" for c in weakest))
    parts.append(f"контекст: покрытие {coverage:.0%}, S_eff {s_eff:+.2f}")
    missing_components = [_COMPONENT_RU[c.key] for c in components if not c.available]
    if missing_components:
        parts.append("без данных: " + ", ".join(missing_components))
    if unavailable_txt:
        parts.append("нет данных: " + "; ".join(unavailable_txt))
    if vetoes:
        parts.append("вето: " + "; ".join(vetoes))

    return QualityReport(
        score=score,
        grade=grade,
        passed=passed,
        vetoes=list(vetoes),
        components=components,
        factors=[_json_safe(_factor_dict(f)) for f in (factors or [])],
        coverage=coverage,
        summary=" · ".join(parts),
        min_score=min_score,
        raw_score=raw,
        event_window=bool(event_window),
        context_s=s_mean,
        context_s_eff=s_eff,
        unavailable=unavailable,
    )
