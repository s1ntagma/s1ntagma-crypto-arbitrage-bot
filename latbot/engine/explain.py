"""build_why - the measurable WHY behind an opportunity (<= 6 short lines,
English, for the WHY? expander); build_plan - the Russian trading PLAN the
owner acts on by hand (what to buy, at which limit, where to sell, how long
such windows last, what can go wrong)."""
from __future__ import annotations

import math
import time

from ..config import Settings
from ..links import split_symbol
from ..models import ExecAssessment, ExecutionEconomics, LagSignal, Light, Opportunity
from .economics import entry_cap_price

_EMOJI = {Light.GREEN: "🟢", Light.YELLOW: "🟡", Light.RED: "🔴"}

PLAN_KEYS: tuple[str, ...] = ("header", "movers", "buy", "sell", "timing", "caveat", "updated")


def light_emoji(light: Light) -> str:
    return _EMOJI.get(light, "⚪")


def currency_symbol(quote: str) -> str:
    return "€" if (quote or "").upper() == "EUR" else "$"


# --------------------------------------------------------------------------- #
# formatting helpers (shared by the plan, Telegram and the API)
# --------------------------------------------------------------------------- #
def fmt_money(value: float | None, quote: str = "", signed: bool = False) -> str:
    """``$102,140`` / ``$4.82`` / ``$100`` / ``€0.5234`` / ``+$4.82`` (signed)."""
    sym = currency_symbol(quote)
    v = float(value or 0.0)
    if signed:
        sign = "+" if v >= 0 else "-"
    else:
        sign = "-" if v < 0 else ""
    a = abs(v)
    if a >= 1000:
        body = f"{a:,.0f}"
    elif abs(a - round(a)) < 1e-9:
        body = f"{a:,.0f}"
    elif a >= 1:
        body = f"{a:,.2f}"
    else:
        decimals = max(2, -int(math.floor(math.log10(a))) + 3) if a > 0 else 2
        body = f"{a:.{min(decimals, 10)}f}".rstrip("0").rstrip(".")
    return f"{sign}{sym}{body}"


def fmt_price(price: float | None) -> str:
    """A price with as many digits as it needs: 2 decimals from 100 up,
    up to 4 decimals (trailing zeros trimmed, at least 2 kept) between 1 and
    100, 6 significant digits below 1 (``0.0112291``).  ``—`` when unknown."""
    p = float(price or 0.0)
    if not math.isfinite(p) or p <= 0:
        return "—"
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        s = f"{p:.4f}".rstrip("0")
        whole, _, frac = s.partition(".")
        return f"{whole}.{frac.ljust(2, '0')}"
    decimals = 5 - int(math.floor(math.log10(p)))  # 6 significant digits
    return f"{p:.{min(decimals, 14)}f}".rstrip("0").rstrip(".")


def price_str(price: float | None, quote: str = "") -> str:
    """``0.0112291 EUR`` (the quote the owner sees on the exchange page)."""
    q = (quote or "").upper()
    s = fmt_price(price)
    return f"{s} {q}" if q and s != "—" else s


def fmt_duration(seconds: float | None) -> str:
    """``48 с`` / ``24 мин 13 с`` / ``2 мин`` / ``1 ч 5 мин``."""
    s = max(0, int(round(float(seconds or 0.0))))
    if s < 60:
        return f"{s} с"
    if s < 3600:
        m, r = divmod(s, 60)
        return f"{m} мин {r} с" if r else f"{m} мин"
    h, r = divmod(s, 3600)
    m = r // 60
    return f"{h} ч {m} мин" if m else f"{h} ч"


def _plural(n: int, one: str, few: str, many: str) -> str:
    n10, n100 = n % 10, n % 100
    if n10 == 1 and n100 != 11:
        return one
    if 2 <= n10 <= 4 and not 12 <= n100 <= 14:
        return few
    return many


def cap_price(econ: ExecutionEconomics, settings: Settings) -> float:
    """``econ.entry_cap_price`` or, for economics built without it, the same
    number derived from exit_target / costs / ``min_net_pct_show``."""
    cap = float(getattr(econ, "entry_cap_price", 0.0) or 0.0)
    if cap > 0:
        return cap
    return entry_cap_price(econ.exit_target, econ.direction, econ.total_cost_pct, settings.min_net_pct_show)


def plan_lines(plan: dict[str, str]) -> list[str]:
    """The plan's lines in display order (empty lines dropped)."""
    return [str(plan[k]) for k in PLAN_KEYS if plan.get(k)]


# --------------------------------------------------------------------------- #
# WHY (English, the expander)
# --------------------------------------------------------------------------- #
def build_why(
    signal: LagSignal,
    econ: ExecutionEconomics,
    human: ExecAssessment,
    bot: ExecAssessment,
    est_duration_s: float,
    *,
    names: dict[str, str] | None = None,
    quote: str = "",
    max_slippage_pct: float | None = None,
) -> str:
    """Explain the opportunity in at most six lines."""
    names = names or {}

    def nm(ex: str) -> str:
        return names.get(ex) or ex.capitalize()

    cur = currency_symbol(quote or signal.target_quote)
    age = max(0.0, signal.detected_mono - signal.first_move_start_mono)
    others = ""
    if signal.movers_count >= 2:
        n = signal.movers_count - 1
        others = f" ({n} other exchange{'s' if n > 1 else ''} followed)"
    lines = [
        f"{nm(signal.first_mover)} moved {signal.first_move_pct:+.1f}% {age:.1f} s ago{others}. "
        f"{nm(signal.target)} moved only {signal.target_move_pct:+.1f}% (lag {signal.lag_pct:+.1f}%)."
    ]
    if signal.median_reaction_delay_s is not None:
        hist = f"Historical median reaction delay on {nm(signal.target)}: {signal.median_reaction_delay_s:.1f} s."
    else:
        hist = f"No reaction history on {nm(signal.target)} yet."
    lines.append(f"{hist} Expected catch-up: {signal.expected_catchup_pct:+.1f}% (ratio {signal.catchup_ratio:.2f}).")
    if econ.liquidity_ok:
        if max_slippage_pct is not None:
            lines.append(
                f"Target book supports {cur}{econ.executable_quote:,.0f} within {max_slippage_pct:g}% slippage "
                f"(entry slippage {econ.entry_slippage_pct:.2f}%)."
            )
        else:
            worst = max(econ.entry_slippage_pct, econ.exit_slippage_pct)
            lines.append(f"Target book supports {cur}{econ.executable_quote:,.0f} (slippage {worst:.2f}%).")
    else:
        lines.append(f"Liquidity: {econ.liquidity_reason or 'book too thin'}.")
    fees = econ.entry_fee_pct + econ.exit_fee_pct
    lines.append(
        f"Costs: fees {fees:.2f}% + execution {econ.execution_loss_pct:.2f}%. Net after costs: {econ.net_pct:+.2f}%."
    )
    window = max(0.0, human.margin_s + human.est_execution_s)
    lines.append(
        f"HUMAN {light_emoji(human.light)} ({human.est_execution_s:.0f} s needed, ~{window:.0f} s window). "
        f"BOT {light_emoji(bot.light)} (est. {bot.est_execution_s:.1f} s)."
    )
    extra: list[str] = []
    if bot.light is Light.RED and bot.reasons:
        extra.append(f"Bot: {bot.reasons[0]}.")
    if human.stats_rate is not None and human.stats_n >= 5:
        extra.append(f"By stats {human.stats_rate:.0%} of {human.stats_n} such windows were made by hand.")
    if extra:
        lines.append(" ".join(extra))
    return "\n".join(lines[:6])


# --------------------------------------------------------------------------- #
# PLAN (Russian, what the owner does by hand)
# --------------------------------------------------------------------------- #
def build_plan(
    opp: Opportunity,
    settings: Settings,
    names: dict[str, str] | None,
    now_mono: float,
    now_ts: float,
) -> dict[str, str]:
    """The trading plan for one opportunity, one Russian line per key
    (``PLAN_KEYS``), in the style of the previous bot's message but with the
    lag semantics of latbot: both legs on the target exchange, no transfer.

    header  🏃 ZK · догоняющая без перевода · NET +2.43% (+$2.43 на $100) · держится 24 мин 13 с
    movers  Revolut X двинулась первой: +2.2%; Kraken пока +0.3% · разрыв 1.9%
    buy     1. Купить на Kraken ZK/EUR ≤ 0.0112291 EUR (сейчас 0.0109631 EUR) · сумма до $100 (стакан держит $840)
    sell    2. Не переводить. Продать там же на Kraken, когда догонит: ≥ 0.0114109 EUR (цель с учётом издержек)
    timing  Окно по этой монете: ~2 мин 48 с · догнала 0 из 32 · человек успевал 3 из 10
    caveat  ! Kraken может не догнать: объём на Revolut X мал / стакан тонкий. Не догнала за ~10 мин — продай по рынку.
    updated обновлено 12:41:10

    Every number comes from the opportunity itself (signal, economics on the
    real book, learned stats copied onto the opp) - nothing is invented.  The
    ``≤`` limit is ``econ.entry_cap_price`` (the worst entry at which NET is
    still >= ``min_net_pct_show``); ``сейчас`` is the current best entry.
    SHORT plans say "Продать (шорт) ... ≥" and "Откупить ... ≤".  The caveat
    names the concrete risk (first mover's volume small / unknown, the target
    rarely caught up, thin book) and always ends with the exit discipline:
    not caught up within ``max_hold_s`` -> sell at market.
    """
    names = names or {}

    def nm(ex: str) -> str:
        return names.get(ex) or str(ex).capitalize()

    sig = opp.signal
    e = opp.econ
    long = opp.direction > 0
    quote = opp.quote or getattr(sig, "target_quote", "") or split_symbol(opp.target_symbol or "")[1]
    target = nm(opp.target_exchange)
    first = nm(sig.first_mover)
    pair = (opp.target_symbol or "").split(":")[0] or opp.asset

    # --- header ------------------------------------------------------------
    side = "" if long else " · шорт"
    header = (
        f"🏃 {opp.asset}{side} · догоняющая без перевода · NET {e.net_pct:+.2f}% "
        f"({fmt_money(e.net_quote, quote, signed=True)} на {fmt_money(e.executable_quote, quote)}) "
        f"· держится {fmt_duration(opp.age_s(now_mono))}"
    )

    # --- movers ------------------------------------------------------------
    followers = max(0, int(getattr(sig, "movers_count", 1) or 1) - 1)
    extra = f" (ещё {followers} {_plural(followers, 'биржа', 'биржи', 'бирж')} следом)" if followers else ""
    movers = (
        f"{first} двинулась первой: {sig.first_move_pct:+.1f}%{extra}; "
        f"{target} пока {sig.target_move_pct:+.1f}% · разрыв {sig.lag_pct:.1f}%"
    )

    # --- entry / exit ------------------------------------------------------
    cap = cap_price(e, settings)
    entry_depth = e.ask_depth_quote if long else e.bid_depth_quote
    exit_depth = e.bid_depth_quote if long else e.ask_depth_quote
    held = min(float(entry_depth or 0.0), float(exit_depth or 0.0))
    thin = e.executable_quote < e.capital_quote - 1e-9
    holds = ""
    if held > 0 and (thin or held > e.executable_quote * 1.001):
        holds = f" (стакан держит {fmt_money(held, quote)})"
    amount = f"сумма до {fmt_money(e.executable_quote, quote)}{holds}"
    if long:
        buy = (
            f"1. Купить на {target} {pair} ≤ {price_str(cap, quote)} "
            f"(сейчас {price_str(e.entry_best, quote)}) · {amount}"
        )
        sell = (
            f"2. Не переводить. Продать там же на {target}, когда догонит: "
            f"≥ {price_str(e.exit_target, quote)} (цель с учётом издержек)"
        )
    else:
        buy = (
            f"1. Продать (шорт) на {target} {pair} ≥ {price_str(cap, quote)} "
            f"(сейчас {price_str(e.entry_best, quote)}) · {amount}"
        )
        sell = (
            f"2. Не переводить. Откупить там же на {target}, когда догонит: "
            f"≤ {price_str(e.exit_target, quote)} (цель с учётом издержек)"
        )

    # --- timing (learned history of this asset on this target) -------------
    typical = opp.typical_window_s
    if typical is not None and typical > 0:
        window = f"Окно по этой монете: ~{fmt_duration(typical)}"
    else:
        window = f"Окно по этой монете: ~{fmt_duration(opp.estimated_duration_s)} (оценка)"
    parts = [window]
    if opp.caught_up_total > 0:
        parts.append(f"догнала {opp.caught_up_ok} из {opp.caught_up_total}")
    else:
        parts.append("догоняла: нет истории")
    if opp.human_total > 0:
        parts.append(f"человек успевал {opp.human_ok} из {opp.human_total}")
    timing = " · ".join(parts)

    # --- caveat ------------------------------------------------------------
    reasons: list[str] = []
    volumes = getattr(sig, "mover_volumes_quote", None) or {}
    vol = volumes.get(sig.first_mover)
    if vol is None:
        reasons.append(f"объём на {first} неизвестен")
    elif float(vol) < 2.0 * float(settings.min_mover_volume_quote):
        reasons.append(f"объём на {first} мал")
    if opp.caught_up_total >= 5 and opp.caught_up_ok / opp.caught_up_total < 0.3:
        reasons.append(f"догоняла редко ({opp.caught_up_ok} из {opp.caught_up_total})")
    if not e.liquidity_ok:
        reasons.append(f"стакан тонкий (держит {fmt_money(e.executable_quote, quote)})")
    exit_rule = f"Не догнала за ~{fmt_duration(settings.max_hold_s)} — продай по рынку."
    if reasons:
        caveat = f"! {target} может не догнать: {' / '.join(reasons)}. {exit_rule}"
    else:
        caveat = exit_rule

    # --- updated -----------------------------------------------------------
    ts = float(opp.last_update_ts or 0.0) or float(now_ts)
    updated = f"обновлено {time.strftime('%H:%M:%S', time.localtime(ts))}"

    return {
        "header": header,
        "movers": movers,
        "buy": buy,
        "sell": sell,
        "timing": timing,
        "caveat": caveat,
        "updated": updated,
    }
