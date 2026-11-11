"""Leverage / position plan for one opportunity (Phase 3, ARCHITECTURE P3.10 + P3.12).

Execution stays spot (no leveraged orders are sent); the plan is shown to the
manual trader and used for virtual P&L.  Every cap is a number with a Russian
explanation; the smallest one wins and ``binding`` names it:

* ``volatility``  - unknown 1-minute volatility -> 1x ("нет данных о волатильности").
* ``quality``     - tiers ``leverage_tiers=[[70, 2], [80, 3], [90, 5]]`` (below the
                    first tier 1x); inside a macro event window the score is reduced by
                    ``quality_event_window_bonus`` before the lookup.
* ``global``      - ``max_leverage=5``.
* ``venue``       - ``venue.max_leverage`` (a spot venue is always 1x).
* ``liquidation`` - ``L_liq = 1 / (k*stop + mmr + taker_fee)``, k = ``leverage_liq_buffer=3``:
                    the isolated liquidation distance stays >= k x stop distance.
* ``risk``        - risk-first sizing: notional <= equity * ``leverage_risk_frac_pct``% /
                    (stop + fees + slippage); leverage only raises margin efficiency,
                    never the per-trade risk budget (also capped by ``max_risk_per_trade_pct``).
* ``depth``       - notional <= min(bid depth, ask depth) within ``max_slippage_pct``.
* ``move``        - L > 1 only when NET >= ``leverage_min_net_pct=0.4`` and
                    R:R = NET / (stop + fees + slippage) >= ``leverage_min_rr=1.5``.
* ``kelly``       - optional: fractional Kelly (``leverage_kelly_fraction=0.25``) from the
                    meta-label lower bound p10 when n >= ``metalabel_min_n=20``.

Stop = max(``leverage_min_stop_pct=0.3``, ``leverage_stop_sigma=2.5`` * sigma_1m *
sqrt(hold/60), tail_1m).  ``recommended`` = min of all caps rounded DOWN to an
integer >= 1; ``max_by_signal`` = the same without the venue cap (what the
signal would justify on a margin venue).

Carry: perp funding only for the settlements the hold crosses (a positive
cost is counted, a funding income is never credited); margin venues pay the
opening fee on the borrowed amount plus a rollover every 4 h
(``borrow_rate_daily_pct / 6`` per rollover).  Isolated liquidation price:
long ``entry*(1 - 1/L + mmr)``, short ``entry*(1 + 1/L - mmr)``; None when no
position can be liquidated (spot, or an unborrowed 1x margin long).

Pure function: no I/O, no clock, deterministic, all money values finite.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_LEVERAGE_TIERS: tuple[tuple[float, float], ...] = ((70.0, 2.0), (80.0, 3.0), (90.0, 5.0))
DEFAULT_MAX_LEVERAGE = 5.0
DEFAULT_LIQ_BUFFER = 3.0
DEFAULT_RISK_FRAC_PCT = 0.5
DEFAULT_MIN_NET_PCT = 0.4
DEFAULT_MIN_RR = 1.5
DEFAULT_KELLY_FRACTION = 0.25
DEFAULT_METALABEL_MIN_N = 20
DEFAULT_MIN_STOP_PCT = 0.3
DEFAULT_STOP_SIGMA = 2.5
DEFAULT_EVENT_WINDOW_BONUS = 10.0

# venue defaults (ExchangeConfig fields added by the integration)
DEFAULT_VENUE_MAX_LEVERAGE = 1.0
DEFAULT_MARGIN_KIND = "spot"
DEFAULT_MMR = 0.005
DEFAULT_BORROW_RATE_DAILY_PCT = 0.0
DEFAULT_MARGIN_OPEN_FEE_PCT = 0.0

ROLLOVER_INTERVAL_S = 4 * 3600.0
VENUE_KINDS = ("spot", "margin", "perp")

# Tie-break order when several caps have the same value: the most specific
# reason is reported first.
_CAP_ORDER: tuple[str, ...] = (
    "volatility", "venue", "move", "quality", "kelly", "risk", "liquidation", "depth", "global",
)
CAP_LABELS_RU: dict[str, str] = {
    "volatility": "нет данных о волатильности",
    "venue": "лимит биржи",
    "move": "размер движения",
    "quality": "качество сигнала",
    "kelly": "Келли (нижняя граница побед)",
    "risk": "риск на сделку",
    "liquidation": "запас до ликвидации",
    "depth": "глубина стакана",
    "global": "общий лимит плеча",
    "margin": "нет исполнимого объёма",
}
_EPS = 1e-9


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _num(v: Any) -> float | None:
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


def _setting(settings: Any, name: str, default: float) -> float:
    x = _num(getattr(settings, name, default))
    return float(default) if x is None else x


def _floor_lev(x: float) -> float:
    """Round a leverage cap DOWN to an integer >= 1 (tolerant to 2.9999999)."""
    if not math.isfinite(x):
        return 1.0 if x < 0 else float("inf")
    return float(max(1, int(math.floor(x + _EPS))))


def _fin(x: float | None, digits: int = 8) -> float | None:
    if x is None or not math.isfinite(x):
        return None
    return round(x, digits)


def _fmt_lev(x: float) -> str:
    if not math.isfinite(x):
        return "∞"
    if abs(x - round(x)) < 1e-9:
        return f"{int(round(x))}×"
    return f"{x:.2f}×"


def _fmt_money(x: float) -> str:
    return f"{x:,.2f}".replace(",", " ")


def quality_cap(score: float, settings: Any) -> float:
    """Leverage allowed by the quality score: the highest tier whose threshold
    the score reaches (``leverage_tiers=[[70, 2], [80, 3], [90, 5]]``), else 1x."""
    s = _num(score)
    if s is None:
        return 1.0
    raw = getattr(settings, "leverage_tiers", None)
    tiers: list[tuple[float, float]] = []
    if isinstance(raw, (list, tuple)):
        for t in raw:
            if isinstance(t, (list, tuple)) and len(t) >= 2:
                th, lev = _num(t[0]), _num(t[1])
                if th is not None and lev is not None and lev >= 1.0:
                    tiers.append((th, lev))
    if raw is None:
        tiers = list(DEFAULT_LEVERAGE_TIERS)
    cap = 1.0
    for th, lev in sorted(tiers):
        if s >= th:
            cap = lev
    return cap


def venue_kind(venue: Any) -> str:
    """"spot" | "margin" | "perp".  A swap market whose ``margin_kind`` was left at
    the "spot" default is a perpetual (it pays funding and can be liquidated)."""
    if venue is None:
        return "spot"
    kind = str(getattr(venue, "margin_kind", DEFAULT_MARGIN_KIND) or DEFAULT_MARGIN_KIND).strip().lower()
    if kind not in VENUE_KINDS:
        kind = "spot"
    if kind == "spot" and str(getattr(venue, "market_type", "spot") or "spot").lower() == "swap":
        kind = "perp"
    return kind


def liquidation_price(entry: float, leverage: float, mmr: float, direction: int) -> float | None:
    """Isolated-margin liquidation estimate: long entry*(1 - 1/L + mmr),
    short entry*(1 + 1/L - mmr)."""
    e, lev, m = _num(entry), _num(leverage), _num(mmr)
    if e is None or lev is None or m is None or e <= 0 or lev <= 0:
        return None
    if direction >= 0:
        return max(0.0, e * (1.0 - 1.0 / lev + m))
    return e * (1.0 + 1.0 / lev - m)


def funding_events(hold_s: float, now_wall: float | None, next_funding_ts: float | None,
                   interval_h: float) -> tuple[int, bool]:
    """How many funding settlements a hold of ``hold_s`` started at ``now_wall``
    crosses -> (count, timing_known).  Without a (future) next settlement time
    only the crossings that are certain (floor(hold / interval)) are counted."""
    interval_s = max(1.0, interval_h * 3600.0)
    hold = max(0.0, hold_s)
    now = _num(now_wall)
    nxt = _num(next_funding_ts)
    if now is not None and nxt is not None and nxt > now:
        end = now + hold
        if nxt > end:
            return 0, True
        return 1 + int(math.floor((end - nxt) / interval_s + _EPS)), True
    return int(math.floor(hold / interval_s + _EPS)), False


# --------------------------------------------------------------------------- #
# plan
# --------------------------------------------------------------------------- #
@dataclass(slots=True)
class LeveragePlan:
    venue_kind: str
    venue_max: float
    quality_cap: float
    max_by_signal: float
    recommended: float
    binding: str
    reasons: list[str]
    margin_quote: float
    notional_quote: float
    fees_quote: float
    slippage_quote: float  # entry + exit slippage + execution loss allowance
    funding_quote: float  # cost >= 0 (income is never credited)
    borrow_quote: float
    expected_profit_quote: float  # NET on notional - funding - borrow
    expected_roi_pct: float  # on margin
    stop_pct: float
    stop_price: float
    loss_at_stop_quote: float  # stop + fees + slippage on notional, plus carry
    risk_pct_of_capital: float
    liq_price: float | None
    liq_distance_pct: float | None
    liq_buffer: float | None  # liquidation distance / stop distance
    rr: float | None  # expected profit / loss at stop
    sigma_hold_pct: float | None
    note: str  # Russian one-liner
    # --- transparency extras ---------------------------------------------
    binding_by_signal: str = ""
    caps: dict[str, float | None] = field(default_factory=dict)  # None = no cap (unbounded)
    direction: int = 1
    entry_price: float = 0.0
    hold_s: float = 0.0
    risk_budget_quote: float = 0.0
    equity_quote: float = 0.0
    funding_events: int = 0

    def to_dict(self) -> dict[str, Any]:
        def r(x: float | None, digits: int = 8) -> float | None:
            return _fin(x, digits)

        return {
            "venue_kind": self.venue_kind,
            "venue_max": r(self.venue_max, 4),
            "quality_cap": r(self.quality_cap, 4),
            "max_by_signal": r(self.max_by_signal, 4),
            "recommended": r(self.recommended, 4),
            "binding": self.binding,
            "reasons": [str(x) for x in self.reasons],
            "margin_quote": r(self.margin_quote),
            "notional_quote": r(self.notional_quote),
            "fees_quote": r(self.fees_quote),
            "slippage_quote": r(self.slippage_quote),
            "funding_quote": r(self.funding_quote),
            "borrow_quote": r(self.borrow_quote),
            "expected_profit_quote": r(self.expected_profit_quote),
            "expected_roi_pct": r(self.expected_roi_pct, 6),
            "stop_pct": r(self.stop_pct, 6),
            "stop_price": r(self.stop_price),
            "loss_at_stop_quote": r(self.loss_at_stop_quote),
            "risk_pct_of_capital": r(self.risk_pct_of_capital, 6),
            "liq_price": r(self.liq_price),
            "liq_distance_pct": r(self.liq_distance_pct, 6),
            "liq_buffer": r(self.liq_buffer, 4),
            "rr": r(self.rr, 4),
            "sigma_hold_pct": r(self.sigma_hold_pct, 6),
            "note": self.note,
            "binding_by_signal": self.binding_by_signal,
            "caps": {str(k): r(v, 4) for k, v in self.caps.items()},
            "direction": int(self.direction),
            "entry_price": r(self.entry_price),
            "hold_s": r(self.hold_s, 3),
            "risk_budget_quote": r(self.risk_budget_quote),
            "equity_quote": r(self.equity_quote),
            "funding_events": int(self.funding_events),
        }


def _argmin(caps: dict[str, float], exclude: tuple[str, ...] = ()) -> tuple[str, float]:
    best_key, best = "global", float("inf")
    for key in _CAP_ORDER:
        if key in exclude or key not in caps:
            continue
        v = caps[key]
        if v < best - _EPS:
            best_key, best = key, v
    return best_key, best


def plan_leverage(
    econ: Any,
    quality_score: float,
    settings: Any,
    venue: Any,
    *,
    sigma_1m_pct: float | None,
    tail_1m_pct: float | None,
    hold_s: float,
    funding_rate_8h: float | None,
    funding_interval_h: float | None = None,
    next_funding_ts: float | None = None,
    now_wall: float | None = None,
    margin_quote: float | None = None,
    p10: float | None = None,
    n: int | None = None,
    total_capital_quote: float | None = None,
    event_window: bool = False,
) -> LeveragePlan:
    """Leverage plan for ``econ`` (see the module docstring for every cap).

    ``sigma_1m_pct`` / ``tail_1m_pct`` are percents of 1-minute returns (sigma
    and a tail quantile); ``funding_rate_8h`` is a fraction per 8 h (positive:
    longs pay); ``margin_quote`` defaults to the executable size of ``econ``;
    ``p10`` / ``n`` come from the meta-label table.
    """
    s = settings
    d = 1 if _f(getattr(econ, "direction", 1), 1.0) >= 0 else -1
    reasons: list[str] = []

    # ---- inputs (sanitised) ---------------------------------------------
    entry = _f(getattr(econ, "entry_vwap", 0.0))
    if entry <= 0:
        entry = _f(getattr(econ, "entry_best", 0.0))
    net = _f(getattr(econ, "net_pct", 0.0))
    entry_fee = max(0.0, _f(getattr(econ, "entry_fee_pct", 0.0)))
    exit_fee = max(0.0, _f(getattr(econ, "exit_fee_pct", 0.0)))
    fees_pct = entry_fee + exit_fee
    slip_pct = (
        max(0.0, _f(getattr(econ, "entry_slippage_pct", 0.0)))
        + max(0.0, _f(getattr(econ, "exit_slippage_pct", 0.0)))
        + max(0.0, _f(getattr(econ, "execution_loss_pct", 0.0)))
    )
    bid_depth = max(0.0, _f(getattr(econ, "bid_depth_quote", 0.0)))
    ask_depth = max(0.0, _f(getattr(econ, "ask_depth_quote", 0.0)))

    kind = venue_kind(venue)
    venue_cfg_max = max(1.0, _f(getattr(venue, "max_leverage", DEFAULT_VENUE_MAX_LEVERAGE), DEFAULT_VENUE_MAX_LEVERAGE))
    venue_cap = 1.0 if kind == "spot" else venue_cfg_max
    mmr = min(0.5, max(0.0, _f(getattr(venue, "maintenance_margin_rate", DEFAULT_MMR), DEFAULT_MMR)))
    taker = _num(getattr(venue, "taker_fee", None)) if venue is not None else None
    if taker is None or taker < 0:
        taker = entry_fee / 100.0
    borrow_daily = max(0.0, _f(getattr(venue, "borrow_rate_daily_pct", DEFAULT_BORROW_RATE_DAILY_PCT)))
    open_fee = max(0.0, _f(getattr(venue, "margin_open_fee_pct", DEFAULT_MARGIN_OPEN_FEE_PCT)))

    hold = _num(hold_s)
    if hold is None or hold <= 0:
        hold = max(1.0, _setting(s, "max_hold_s", 60.0))
        reasons.append(f"время удержания неизвестно — взят лимит {hold:.0f} с")

    equity = _num(total_capital_quote)
    if equity is None or equity <= 0:
        equity = max(0.0, _setting(s, "total_capital_quote", 1000.0))
    margin = _num(margin_quote)
    if margin is None or margin <= 0:
        margin = max(0.0, _f(getattr(econ, "executable_quote", 0.0)))
        if margin <= 0:
            margin = max(0.0, _f(getattr(econ, "capital_quote", 0.0)))

    # ---- stop -------------------------------------------------------------
    min_stop = max(0.0, _setting(s, "leverage_min_stop_pct", DEFAULT_MIN_STOP_PCT))
    k_sigma = max(0.0, _setting(s, "leverage_stop_sigma", DEFAULT_STOP_SIGMA))
    sigma = _num(sigma_1m_pct)
    if sigma is not None and sigma <= 0:
        sigma = None
    tail = _num(tail_1m_pct)
    if tail is not None and tail <= 0:
        tail = None
    sigma_hold = sigma * math.sqrt(hold / 60.0) if sigma is not None else None
    stop = max(min_stop, k_sigma * sigma_hold if sigma_hold is not None else 0.0, tail if tail is not None else 0.0)
    if stop <= 0:
        stop = DEFAULT_MIN_STOP_PCT
    risk_pct = stop + fees_pct + slip_pct  # loss at stop, % of notional
    stop_txt = f"стоп {stop:.2f}% = max(мин {min_stop:.2f}%"
    stop_txt += f", {k_sigma:g}·σ1м {sigma:.3f}%·√({hold:.0f}/60) = {k_sigma * sigma_hold:.2f}%" if sigma_hold is not None else ", σ: нет данных"
    stop_txt += f", хвост 1м {tail:.2f}%)" if tail is not None else ", хвост 1м: нет данных)"
    reasons.append(stop_txt)

    # ---- caps -------------------------------------------------------------
    caps: dict[str, float] = {}
    inf = float("inf")
    if sigma is None:
        caps["volatility"] = 1.0
        reasons.append("нет данных о волатильности — плечо не рассчитывается (1×)")

    score = _f(quality_score, 0.0)
    bonus = max(0.0, _setting(s, "quality_event_window_bonus", DEFAULT_EVENT_WINDOW_BONUS)) if event_window else 0.0
    q_cap = quality_cap(score - bonus, s)
    caps["quality"] = q_cap
    if event_window:
        reasons.append(f"качество {score:.0f} − {bonus:g} (окно макро-события) → до {_fmt_lev(q_cap)}")
    else:
        reasons.append(f"качество {score:.0f} → до {_fmt_lev(q_cap)}")

    global_max = max(1.0, _setting(s, "max_leverage", DEFAULT_MAX_LEVERAGE))
    caps["global"] = global_max
    reasons.append(f"общий лимит {_fmt_lev(global_max)}")

    caps["venue"] = venue_cap
    if kind == "spot":
        reasons.append("биржа: спот — только 1×")
    else:
        reasons.append(f"биржа ({'маржа' if kind == 'margin' else 'perp'}): до {_fmt_lev(venue_cap)}")

    k_liq = max(0.0, _setting(s, "leverage_liq_buffer", DEFAULT_LIQ_BUFFER))
    liq_den = k_liq * stop / 100.0 + mmr + taker
    caps["liquidation"] = 1.0 / liq_den if liq_den > 0 else inf
    reasons.append(
        f"ликвидация: 1/({k_liq:g}·{stop:.2f}% + mmr {mmr * 100:.2f}% + taker {taker * 100:.3f}%) = "
        f"{_fmt_lev(caps['liquidation'])}"
    )

    risk_frac = max(0.0, _setting(s, "leverage_risk_frac_pct", DEFAULT_RISK_FRAC_PCT))
    hard_risk = _num(getattr(s, "max_risk_per_trade_pct", None))
    if hard_risk is not None and hard_risk >= 0:
        risk_frac = min(risk_frac, hard_risk)
    budget = equity * risk_frac / 100.0
    notional_risk_max = budget / (risk_pct / 100.0) if risk_pct > 0 else inf
    if margin > 0:
        caps["risk"] = notional_risk_max / margin
        caps["depth"] = min(bid_depth, ask_depth) / margin
    else:
        caps["risk"] = 0.0
        caps["depth"] = 0.0
    reasons.append(
        f"риск: {risk_frac:g}% капитала {_fmt_money(equity)} = {_fmt_money(budget)} / (стоп {stop:.2f}% + "
        f"комиссии {fees_pct:.2f}% + проскальзывание {slip_pct:.2f}%) → номинал до {_fmt_money(notional_risk_max) if math.isfinite(notional_risk_max) else '∞'}"
        f" = {_fmt_lev(caps['risk'])} от маржи {_fmt_money(margin)}"
    )
    reasons.append(
        f"глубина: min(bid {_fmt_money(bid_depth)}, ask {_fmt_money(ask_depth)}) / маржа {_fmt_money(margin)} = "
        f"{_fmt_lev(caps['depth'])}"
    )

    min_net = _setting(s, "leverage_min_net_pct", DEFAULT_MIN_NET_PCT)
    min_rr = _setting(s, "leverage_min_rr", DEFAULT_MIN_RR)
    rr_signal = net / risk_pct if risk_pct > 0 else 0.0
    if net >= min_net and rr_signal >= min_rr:
        caps["move"] = inf
        reasons.append(f"движение: NET {net:.2f}% ≥ {min_net:.2f}%, R:R {rr_signal:.2f} ≥ {min_rr:g} — плечо допустимо")
    else:
        caps["move"] = 1.0
        reasons.append(
            f"движение: плечо только при NET ≥ {min_net:.2f}% и R:R ≥ {min_rr:g} (сейчас NET {net:.2f}%, R:R {rr_signal:.2f})"
        )

    min_n = int(_setting(s, "metalabel_min_n", DEFAULT_METALABEL_MIN_N))
    p = _num(p10)
    n_obs = _num(n)
    if p is not None and n_obs is not None and n_obs >= min_n and margin > 0:
        p = min(1.0, max(0.0, p))
        k_frac = max(0.0, _setting(s, "leverage_kelly_fraction", DEFAULT_KELLY_FRACTION))
        b = rr_signal
        f_star = p - (1.0 - p) / b if b > 0 else -1.0
        f_used = k_frac * f_star
        kelly_notional = f_used * equity / (risk_pct / 100.0) if risk_pct > 0 else inf
        caps["kelly"] = max(0.0, kelly_notional / margin)
        reasons.append(
            f"Келли: p10 {p:.0%} (n={int(n_obs)}), b {b:.2f} → f* {f_star:+.3f} × {k_frac:g} = {f_used:+.4f} капитала → "
            f"до {_fmt_lev(caps['kelly'])}"
        )
    elif p is not None and n_obs is not None:
        reasons.append(f"Келли: n={int(n_obs)} < {min_n} — не применяется")

    if margin <= 0 or entry <= 0:
        caps["margin"] = 1.0

    # ---- recommendation -----------------------------------------------------
    order_caps = {k: v for k, v in caps.items()}
    binding_sig, sig_raw = _argmin(order_caps, exclude=("venue",))
    binding, rec_raw = _argmin(order_caps)
    if "margin" in caps:
        binding = binding_sig = "margin"
    max_by_signal = _floor_lev(sig_raw)
    recommended = _floor_lev(rec_raw)
    if not math.isfinite(max_by_signal):
        max_by_signal = 1.0
    if not math.isfinite(recommended):
        recommended = 1.0
    recommended = min(recommended, max_by_signal)

    # ---- money at the recommended leverage ----------------------------------
    lev = recommended
    notional = margin * lev
    fees_quote = notional * fees_pct / 100.0
    slippage_quote = notional * slip_pct / 100.0

    funding_quote = 0.0
    events = 0
    if kind == "perp":
        interval_h = _num(funding_interval_h)
        if interval_h is None or interval_h <= 0:
            interval_h = 8.0
        events, timing_known = funding_events(hold, now_wall, next_funding_ts, interval_h)
        rate = _num(funding_rate_8h)
        if not timing_known:
            reasons.append(
                f"фандинг: время расчёта неизвестно — учтены только гарантированные расчёты "
                f"(удержание {hold:.0f} с, интервал {interval_h:g} ч: {events})"
            )
        if events > 0:
            if rate is None:
                reasons.append(f"фандинг: удержание пересекает {events} расчёт(а), ставка неизвестна — не учтён")
            else:
                per_event = rate * interval_h / 8.0
                cost = notional * per_event * events * d
                if cost >= 0:
                    funding_quote = cost
                    reasons.append(
                        f"фандинг: {events} расчёт × {per_event * 100:.4f}% × {_fmt_money(notional)} = {_fmt_money(cost)}"
                    )
                else:
                    reasons.append(f"фандинг в нашу пользу ({_fmt_money(-cost)}) не учитывается")
        elif timing_known:
            reasons.append("фандинг: удержание не пересекает время расчёта — 0")

    borrow_quote = 0.0
    if kind == "margin":
        borrowed = notional if d < 0 else max(0.0, notional - margin)
        rollovers = int(math.floor(hold / ROLLOVER_INTERVAL_S + _EPS))
        if borrowed > 0:
            rate_pct = open_fee + rollovers * borrow_daily / 6.0
            borrow_quote = borrowed * rate_pct / 100.0
            reasons.append(
                f"заём {_fmt_money(borrowed)}: открытие {open_fee:.3f}% + {rollovers} ролловер(а) × "
                f"{borrow_daily / 6.0:.3f}% = {_fmt_money(borrow_quote)}"
            )

    carry = funding_quote + borrow_quote
    expected_profit = notional * net / 100.0 - carry
    roi = expected_profit / margin * 100.0 if margin > 0 else 0.0
    stop_price = entry * (1.0 - d * stop / 100.0) if entry > 0 else 0.0
    loss_at_stop = notional * risk_pct / 100.0 + carry
    risk_of_capital = loss_at_stop / equity * 100.0 if equity > 0 else 0.0
    rr = expected_profit / loss_at_stop if loss_at_stop > 0 else None

    liq_price: float | None = None
    if entry > 0 and (kind == "perp" or (kind == "margin" and (lev > 1.0 or d < 0))):
        liq_price = liquidation_price(entry, lev, mmr, d)
    liq_distance = abs(entry - liq_price) / entry * 100.0 if liq_price is not None and entry > 0 else None
    liq_buffer = liq_distance / stop if liq_distance is not None and stop > 0 else None

    if margin > 0 and caps["risk"] < 1.0:
        reasons.append(
            f"даже на 1× риск на стопе {margin * risk_pct / 100.0 / equity * 100.0 if equity > 0 else 0.0:.2f}% капитала "
            f"> бюджета {risk_frac:g}% — уменьши размер до {_fmt_money(notional_risk_max)}"
        )

    # ---- note -------------------------------------------------------------
    if "margin" in caps:
        note = "нет исполнимого объёма — плечо не рассчитывается (1×)"
    elif "volatility" in caps:
        note = "нет данных о волатильности — только 1×"
    elif kind == "spot":
        note = f"биржа спотовая — 1×; по сигналу до {_fmt_lev(max_by_signal)} ({CAP_LABELS_RU.get(binding_sig, binding_sig)})"
    else:
        note = f"плечо {_fmt_lev(recommended)} (ограничение: {CAP_LABELS_RU.get(binding, binding)})"
        if max_by_signal > recommended:
            note += f"; по сигналу до {_fmt_lev(max_by_signal)}"
    if liq_price is not None and liq_distance is not None:
        note += f"; ликвидация ~{_fmt_money(liq_price)} ({liq_distance:.1f}% от входа)"

    def fin(x: float) -> float:
        return x if math.isfinite(x) else 0.0

    return LeveragePlan(
        venue_kind=kind,
        venue_max=fin(venue_cap),
        quality_cap=fin(q_cap),
        max_by_signal=fin(max_by_signal),
        recommended=fin(recommended),
        binding=binding,
        reasons=reasons,
        margin_quote=fin(margin),
        notional_quote=fin(notional),
        fees_quote=fin(fees_quote),
        slippage_quote=fin(slippage_quote),
        funding_quote=fin(funding_quote),
        borrow_quote=fin(borrow_quote),
        expected_profit_quote=fin(expected_profit),
        expected_roi_pct=fin(roi),
        stop_pct=fin(stop),
        stop_price=fin(stop_price),
        loss_at_stop_quote=fin(loss_at_stop),
        risk_pct_of_capital=fin(risk_of_capital),
        liq_price=_fin(liq_price, 12),
        liq_distance_pct=_fin(liq_distance, 12),
        liq_buffer=_fin(liq_buffer, 12),
        rr=_fin(rr, 12),
        sigma_hold_pct=_fin(sigma_hold, 12),
        note=note,
        binding_by_signal=binding_sig,
        caps={k: (v if math.isfinite(v) else None) for k, v in caps.items()},
        direction=d,
        entry_price=fin(entry),
        hold_s=fin(hold),
        risk_budget_quote=fin(budget),
        equity_quote=fin(equity),
        funding_events=events,
    )
