"""Human and bot executability (traffic lights) - evaluated separately.

Human: pure time budget (``human_execution_time_s`` vs the remaining window),
Phase 2.4 blends the probability with the learned "made it" rate.
Bot: latency budget (ws + order ack + fill + fixed overhead) plus hard checks
on health, book age, liquidity and the bot NET minimum.
"""
from __future__ import annotations

import logging

from ..config import Settings
from ..models import ExchangeHealth, ExecAssessment, ExecutionEconomics, Light, ReactionStats

log = logging.getLogger(__name__)

_P_GREEN = 0.8
_P_YELLOW = 0.5
_P_RED = 0.1
_MIN_STATS_N = 5


def _fmt_margin(margin: float) -> str:
    return f"{margin:+.1f} s margin"


def assess_human(
    remaining_s: float, net_pct: float, settings: Settings, stats: ReactionStats | None = None
) -> ExecAssessment:
    """Can a human do BUY + SELL by hand inside the remaining window?"""
    est = float(settings.human_execution_time_s)
    remaining = max(0.0, float(remaining_s))
    margin = remaining - est
    reasons: list[str] = []
    if remaining >= est * settings.human_green_margin:
        light, label, prob = Light.GREEN, "HIGH", _P_GREEN
        reasons.append(f"{remaining:.1f} s window vs ~{est:.0f} s needed ({_fmt_margin(margin)})")
    elif remaining >= est:
        light = Light.YELLOW
        label = "MEDIUM" if remaining >= est * 1.2 else "LOW"
        prob = _P_YELLOW
        reasons.append(f"tight: {remaining:.1f} s window vs ~{est:.0f} s needed ({_fmt_margin(margin)})")
    else:
        light, label, prob = Light.RED, "NOT EXECUTABLE", _P_RED
        reasons.append(f"too fast for human: {remaining:.1f} s window < {est:.0f} s needed")
    reasons.append(f"net {net_pct:+.2f}%")

    stats_rate: float | None = None
    stats_n = 0
    if stats is not None:
        stats_rate = stats.human_success_rate
        stats_n = int(stats.human_window_total)
        if stats_rate is not None and stats_n >= _MIN_STATS_N:
            prob = 0.5 * prob + 0.5 * stats_rate
            reasons.append(f"by stats: {stats.human_window_ok}/{stats_n} made it")
    return ExecAssessment(
        light=light,
        label=label,
        est_execution_s=est,
        margin_s=margin,
        probability=max(0.0, min(1.0, prob)),
        reasons=reasons,
        stats_rate=stats_rate,
        stats_n=stats_n,
    )


def _unusable_reason(health: ExchangeHealth, now_mono: float) -> str:
    if health.maintenance:
        return "exchange in maintenance"
    if health.stale:
        return "exchange feed stale"
    if not health.ws_connected:
        return "websocket disconnected"
    if health.rate_limited_until_mono and now_mono < health.rate_limited_until_mono:
        return f"rate limited for {health.rate_limited_until_mono - now_mono:.1f} s"
    return health.reason or "exchange not usable"


def assess_bot(
    remaining_s: float,
    econ: ExecutionEconomics,
    health: ExchangeHealth | None,
    stats: ReactionStats | None,
    settings: Settings,
    now_mono: float,
) -> ExecAssessment:
    """Can the bot execute entry + exit inside the window with the real book?"""
    s = settings
    remaining = max(0.0, float(remaining_s))
    ws = float(health.ws_latency_ms or 0.0) if health is not None else 0.0
    if health is not None and health.order_latency_ms is not None:
        order = float(health.order_latency_ms)
    else:
        order = float(s.default_api_latency_ms)
    fill = float(health.fill_latency_ms) if health is not None and health.fill_latency_ms is not None else 0.0
    est = (ws + order + fill + s.bot_fixed_overhead_ms) / 1000.0
    margin = remaining - est

    red: list[str] = []
    if health is None:
        red.append("no exchange health information")
    elif not health.is_usable(now_mono):
        red.append(_unusable_reason(health, now_mono))
    if econ.book_age_ms > s.max_data_age_ms:
        red.append(f"stale book ({econ.book_age_ms:.0f} ms > {s.max_data_age_ms:.0f} ms)")
    if order > s.max_api_latency_ms:
        red.append(f"order latency {order:.0f} ms > {s.max_api_latency_ms:.0f} ms")
    if not econ.liquidity_ok:
        red.append(f"insufficient liquidity: {econ.liquidity_reason or 'book too thin'}")
    if econ.net_pct < s.min_net_pct_bot:
        red.append(f"net {econ.net_pct:+.2f}% < {s.min_net_pct_bot:.2f}% bot minimum")
    if remaining < est * s.bot_yellow_margin:
        red.append(f"window {remaining:.1f} s < {est * s.bot_yellow_margin:.1f} s needed")

    n_trades = (stats.success_count + stats.fail_count) if stats is not None else 0
    success_rate = stats.success_rate if stats is not None else None
    budget = f"est. {est:.2f} s (ws {ws:.0f} + order {order:.0f} + fill {fill:.0f} + overhead {s.bot_fixed_overhead_ms:.0f} ms)"

    if red:
        light, label, prob = Light.RED, "NOT EXECUTABLE", _P_RED
        reasons = red + [budget]
    else:
        yellow: list[str] = []
        if remaining < est * s.bot_green_margin:
            yellow.append(f"thin margin: {remaining:.1f} s window vs {est:.2f} s estimated")
        if success_rate is not None and n_trades >= _MIN_STATS_N and success_rate < 0.5:
            yellow.append(f"success rate {success_rate:.0%} over {n_trades} trades")
        if yellow:
            light, prob = Light.YELLOW, _P_YELLOW
            mid_margin = est * (s.bot_yellow_margin + s.bot_green_margin) / 2.0
            label = "LOW" if (remaining < mid_margin or len(yellow) > 1) else "MEDIUM"
            reasons = yellow + [budget]
        else:
            light, label, prob = Light.GREEN, "HIGH", _P_GREEN
            reasons = [f"{remaining:.1f} s window, {budget}"]
        if success_rate is not None and n_trades >= _MIN_STATS_N:
            prob = 0.6 * prob + 0.4 * success_rate
    reasons.append(f"net {econ.net_pct:+.2f}% on {econ.executable_quote:,.2f}")
    return ExecAssessment(
        light=light,
        label=label,
        est_execution_s=est,
        margin_s=margin,
        probability=max(0.0, min(1.0, prob)),
        reasons=reasons,
        stats_rate=success_rate,
        stats_n=n_trades,
    )
