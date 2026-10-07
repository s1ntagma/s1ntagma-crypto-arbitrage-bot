"""build_why - the measurable WHY behind an opportunity (<= 6 short lines)."""
from __future__ import annotations

from ..models import ExecAssessment, ExecutionEconomics, LagSignal, Light

_EMOJI = {Light.GREEN: "🟢", Light.YELLOW: "🟡", Light.RED: "🔴"}


def light_emoji(light: Light) -> str:
    return _EMOJI.get(light, "⚪")


def currency_symbol(quote: str) -> str:
    return "€" if (quote or "").upper() == "EUR" else "$"


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
