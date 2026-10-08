"""Pipeline diagnostics: the funnel from raw ticks to approved signals and an
honest answer to "why are there no signals?".

Everything that silently drops a candidate (MoveDetector steps 5/6, Pipeline
evaluate) reports here.  Counters are process-lifetime; ``snapshot`` adds the
live view (exchanges, scanned assets) and ``why_no_signals`` turns the numbers
into an ordered Russian explanation.  Hard rule 1 stays: the diagnostics only
describe what happened, they never create a signal.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any

from ..config import ExchangeRole, Settings

log = logging.getLogger(__name__)

# drop reasons in funnel order (keys are stable: UI / Telegram / tests rely on them)
REASONS: tuple[tuple[str, str], ...] = (
    ("min_movers", "двинулась только одна биржа (нужно >= min_movers)"),
    ("leader_unknown", "простой режим: лидер монеты неизвестен - биржи-источники ещё не дали объём 24ч"),
    ("leader_not_moved", "простой режим: двинулась не самая ликвидная биржа, лидер стоит"),
    ("late_entry", "простой режим: лидер двинулся давнее simple_entry_deadline_s - вход уже поздний"),
    ("mover_volume", "у двинувшихся бирж объём 24ч ниже min_mover_volume_quote"),
    ("mover_volume_unknown", "объём 24ч двинувшейся биржи неизвестен (биржа не отдала объём в тикере)"),
    ("target_no_data", "по твоей бирже нет свежих данных по этой монете"),
    ("direction", "направление не разрешено на твоей бирже (SHORT без allow_short)"),
    ("target_volume", "объём 24ч на твоей бирже ниже min_target_volume_quote"),
    ("min_lag", "твоя биржа двинулась вместе с рынком: лаг меньше min_lag_pct"),
    ("no_fresh_book", "нет свежего стакана на твоей бирже (старше max_data_age_ms)"),
    ("fees", "edge был, но комиссии и исполнение съели его: NET ниже min_net_pct_show"),
    ("below_min_net", "ожидаемый догон слишком мал даже до комиссий (gross ниже min_net_pct_show)"),
    ("liquidity", "стакан не держит минимальную сделку (min_trade_quote)"),
)
REASON_KEYS = tuple(k for k, _ in REASONS)
_RECENT = 10
_DEDUPE_S = 30.0  # the same (asset, target, direction) rejected again within this window counts once


class Diagnostics:
    """Counters for the signal funnel.  Cheap to call from hot paths."""

    def __init__(self, settings: Settings, clock: Any | None = None) -> None:
        self._settings = settings
        self._clock = clock
        self.started_mono = self._mono()
        self.ticks: dict[str, dict[str, int]] = {}  # exchange -> {"book": n, "ticker": n, "trade": n}
        self.episodes = 0
        self.first_movers: dict[str, int] = {}
        self.rejected: dict[str, int] = {k: 0 for k in REASON_KEYS}
        self.recent: dict[str, deque[str]] = {k: deque(maxlen=_RECENT) for k in REASON_KEYS}
        self.signals = 0
        self.opportunities = 0
        self.alerts = 0
        self._last_reject: dict[tuple[str, str, str, int], float] = {}
        # Phase 3 quality gate (counted once per new opportunity)
        self.quality_checked = 0
        self.quality_passed = 0
        self.quality_fail_reasons: dict[str, int] = {}

    # ---- recording --------------------------------------------------------
    def _mono(self) -> float:
        if self._clock is not None and hasattr(self._clock, "mono"):
            return float(self._clock.mono())
        return time.monotonic()

    def tick(self, exchange: str, kind: str) -> None:
        d = self.ticks.get(exchange)
        if d is None:
            d = self.ticks[exchange] = {"book": 0, "ticker": 0, "trade": 0, "poll": 0}
        d[kind] = d.get(kind, 0) + 1

    def episode(self, asset: str, first_mover: str) -> None:
        self.episodes += 1
        self.first_movers[first_mover] = self.first_movers.get(first_mover, 0) + 1

    def reject(self, reason: str, asset: str = "", target: str = "", direction: int = 0, detail: str = "") -> None:
        """Count one rejected candidate.  Repeats of the same (reason, asset,
        target, direction) within ``_DEDUPE_S`` are one event, so a hot book
        stream does not inflate the funnel."""
        if reason not in self.rejected:
            log.debug("unknown diagnostics reason %s", reason)
            return
        now = self._mono()
        key = (reason, asset, target, direction)
        last = self._last_reject.get(key)
        if last is not None and now - last < _DEDUPE_S:
            return
        self._last_reject[key] = now
        if len(self._last_reject) > 5000:
            cutoff = now - _DEDUPE_S
            self._last_reject = {k: v for k, v in self._last_reject.items() if v >= cutoff}
        self.rejected[reason] += 1
        label = asset or "?"
        if target:
            label += f"@{target}"
        if detail:
            label += f" ({detail})"
        self.recent[reason].append(label)

    def signal(self) -> None:
        self.signals += 1

    def opportunity(self) -> None:
        self.opportunities += 1

    def alert(self) -> None:
        self.alerts += 1

    def quality(self, passed: bool, reason: str = "") -> None:
        """One new opportunity went through the quality check (engine.quality)."""
        self.quality_checked += 1
        if passed:
            self.quality_passed += 1
            return
        key = (reason or "качество ниже порога").strip()[:120]
        self.quality_fail_reasons[key] = self.quality_fail_reasons.get(key, 0) + 1
        if len(self.quality_fail_reasons) > 200:
            keep = sorted(self.quality_fail_reasons.items(), key=lambda kv: -kv[1])[:100]
            self.quality_fail_reasons = dict(keep)

    # ---- reporting --------------------------------------------------------
    def snapshot(self, state: Any | None = None, adapters: dict[str, Any] | None = None,
                 opp_counters: dict[str, Any] | None = None, pool: dict[str, Any] | None = None) -> dict[str, Any]:
        """Funnel + live view.  ``state`` is the MarketState (scanned assets,
        health), ``adapters`` the market adapters (health), ``opp_counters``
        OpportunityManager.counters()."""
        s = self._settings
        now = self._mono()
        exchanges: dict[str, dict[str, Any]] = {}
        coverage: dict[str, int] = {}
        if state is not None and hasattr(state, "volume_coverage"):
            try:
                coverage = state.volume_coverage()
            except Exception:  # noqa: BLE001
                coverage = {}
        for e in s.exchanges:
            if not e.enabled:
                continue
            t = self.ticks.get(e.name, {})
            health = None
            adapter = (adapters or {}).get(e.name)
            if adapter is not None:
                health = getattr(adapter, "health", None)
            if health is None and state is not None and hasattr(state, "health"):
                health = state.health(e.name)
            connected = False
            last_age = None
            status = ""
            rl_left = None
            if health is not None:
                until = getattr(health, "rate_limited_until_mono", None)
                if until is not None and float(until) > now:
                    rl_left = round(float(until) - now, 1)
                    status = str(getattr(health, "reason", "") or "rate limited")
                elif getattr(health, "maintenance", False):
                    status = "maintenance"
                elif getattr(health, "stale", False):
                    status = str(getattr(health, "reason", "") or "stale")
                last_poll = getattr(health, "last_poll_mono", None)
                interval = float(getattr(s, "ticker_poll_interval_s", 2.0) or 2.0)
                polling = health.is_polling(now, interval) if hasattr(health, "is_polling") else (
                    last_poll is not None and now - float(last_poll) <= max(30.0, 10.0 * interval))
                connected = bool(getattr(health, "ws_connected", False)) or polling
                last = max((float(x) for x in (getattr(health, "last_msg_mono", None), last_poll) if x), default=None)
                if last is not None:
                    last_age = max(0.0, now - last)
            exchanges[e.name] = {
                "role": e.role.value if isinstance(e.role, ExchangeRole) else str(e.role),
                "connected": connected,
                "books": int(t.get("book", 0)),
                "tickers": int(t.get("ticker", 0)),
                "trades": int(t.get("trade", 0)),
                "polled": int(t.get("poll", 0)),
                "volumes": int(coverage.get(e.name, 0)),
                "status": status,
                "rate_limited_s": rl_left,
                "last_data_s": round(last_age, 1) if last_age is not None else None,
                "stale": bool(getattr(health, "stale", False)) if health is not None else None,
            }
        assets: list[str] = []
        if state is not None and hasattr(state, "assets"):
            try:
                assets = sorted(state.assets())
            except Exception:  # noqa: BLE001
                assets = []
        total_ticks = sum(sum(v.values()) for v in self.ticks.values())
        funnel = {
            "assets_scanned": len(assets),
            "exchanges_connected": sum(1 for x in exchanges.values() if x["connected"]),
            "exchanges_enabled": len(exchanges),
            "ticks_received": total_ticks,
            "episodes_detected": self.episodes,
            "candidates": self.signals + sum(self.rejected.values()),
            "rejected": dict(self.rejected),
            "rejected_total": sum(self.rejected.values()),
            "signals": self.signals,
            "opportunities": self.opportunities,
            "alerts": self.alerts,
            "quality_checked": self.quality_checked,
            "quality_passed": self.quality_passed,
            "quality_failed": self.quality_checked - self.quality_passed,
        }
        oc = opp_counters or {}
        funnel["expired_today"] = int(oc.get("expired", 0) or 0)
        funnel["active_now"] = int(oc.get("active", oc.get("total_active", 0)) or 0)
        snap = {
            "uptime_s": round(now - self.started_mono, 1),
            "exchanges": exchanges,
            "assets": assets[:200],
            "funnel": funnel,
            "recent_rejections": {k: list(v) for k, v in self.recent.items() if v},
            "first_movers": dict(sorted(self.first_movers.items(), key=lambda kv: -kv[1])),
            "pool": dict(pool) if pool is not None else {"enabled": False, "size": 0, "assets": [], "skipped_n": 0, "skipped": []},
            "quality_fail_reasons": [[k, n] for k, n in sorted(self.quality_fail_reasons.items(), key=lambda kv: -kv[1])[:10]],
            "thresholds": {
                "move_threshold_pct": s.move_threshold_pct,
                "move_window_s": s.move_window_s,
                "min_movers": s.min_movers,
                "min_mover_volume_quote": s.min_mover_volume_quote,
                "min_target_volume_quote": s.min_target_volume_quote,
                "min_lag_pct": s.min_lag_pct,
                "max_data_age_ms": s.max_data_age_ms,
                "min_net_pct_show": s.min_net_pct_show,
                "min_trade_quote": s.min_trade_quote,
                "telegram_profit_threshold_pct": s.telegram_profit_threshold_pct,
            },
        }
        snap["why"] = self.why_no_signals(snap)
        return snap

    def why_no_signals(self, snap: dict[str, Any]) -> list[str]:
        """Ordered explanation (Russian) of where the funnel empties.  Each line
        names the stage, the numbers and the setting behind it."""
        s = self._settings
        f = snap["funnel"]
        ex = snap["exchanges"]
        out: list[str] = []
        exec_names = {n for n, x in ex.items() if x["role"] == ExchangeRole.EXECUTION.value}
        sig_names = {n for n, x in ex.items() if x["role"] == ExchangeRole.SIGNAL.value}
        connected = {n for n, x in ex.items() if x["connected"]}
        with_data = {n for n, x in ex.items() if x["books"] + x["tickers"] + x.get("polled", 0) > 0}
        uptime = float(snap.get("uptime_s", 0.0))

        if f["opportunities"] > 0:
            out.append(f"Сигналы были: возможностей {f['opportunities']}, алертов в Telegram {f['alerts']}.")
        pool = snap.get("pool") or {}
        if pool.get("enabled"):
            line = (f"Пул: {pool.get('size', 0)} предсказуемых монет (без мем/хайп/скачущих)"
                    f"; вне пула пропущено монет: {pool.get('skipped_n', 0)}.")
            out.append(line)
        if f.get("quality_checked"):
            reasons = snap.get("quality_fail_reasons") or []
            line = f"Проверку качества прошли {f['quality_passed']} из {f['quality_checked']} возможностей"
            if reasons:
                line += "; чаще всего мешает: " + "; ".join(f"{k} ({n})" for k, n in reasons[:3])
            out.append(line + ". Остальные видны по фильтру «показать все» и торгуются виртуально (обучение).")
        if not connected:
            out.append("Ни одна биржа не подключена: нет данных вообще. Проверь интернет, логи "
                       "(logs/console.log) и что биржи не заблокированы для твоей сети.")
            return out
        if f["ticks_received"] == 0:
            out.append(f"Подключено бирж: {len(connected)}, но ни одного тика ещё не пришло"
                       + (" (бот только запустился, подожди 10-20 с)." if uptime < 30 else
                          " - потоки не отдают данные; смотри предупреждения в логе."))
            return out
        dead_exec = sorted(exec_names - with_data)
        dead_sig = sorted(sig_names - with_data)
        if exec_names and not (exec_names & with_data):
            out.append("Данные идут только с бирж-источников; ни одна из ТВОИХ бирж "
                       f"({', '.join(sorted(exec_names))}) не отдаёт цены - без них сигнал невозможен.")
            return out
        for n, x in ex.items():
            if x.get("rate_limited_s"):
                out.append(f"{n}: биржа включила лимит запросов, бот ждёт {x['rate_limited_s']:.0f} с и не шлёт запросы "
                           "(данные по ней в это время не обновляются).")
        if dead_exec:
            out.append(f"Без данных твои биржи: {', '.join(dead_exec)}.")
        if dead_sig:
            out.append(f"Без данных источники: {', '.join(dead_sig)}.")
        if len(with_data) < 2:
            out.append("Данные есть только с одной биржи - сравнивать не с чем.")
            return out
        if f["assets_scanned"] == 0:
            out.append("Ни одной монеты в наблюдении: watchlist пуст или биржи ещё не отдали рынки.")
            return out
        if f["episodes_detected"] == 0:
            out.append(f"Рынок спокоен: ни одна биржа не двигалась на >= {s.move_threshold_pct:g}% "
                       f"за {s.move_window_s:g} с (move_threshold_pct / move_window_s) по {f['assets_scanned']} монетам.")
            return out
        rej = f["rejected"]
        labels = dict(REASONS)
        out.append(f"Движений (эпизодов) замечено: {f['episodes_detected']}; кандидатов отброшено: {f['rejected_total']}, "
                   f"сигналов прошло: {f['signals']}.")
        ranked = [(k, rej.get(k, 0)) for k in REASON_KEYS if rej.get(k, 0) > 0]
        ranked.sort(key=lambda kv: -kv[1])
        for k, n in ranked[:4]:
            recent = ", ".join(list(snap.get("recent_rejections", {}).get(k, []))[-3:])
            line = f"- {n} x {labels[k]}"
            if recent:
                line += f" (последние: {recent})"
            out.append(line)
        if f["signals"] > 0 and f["opportunities"] == 0:
            out.append("Сигналы доходили до оценки, но ни один не стал возможностью: смотри причины выше "
                       "(стакан, NET после издержек, ликвидность).")
        if f["opportunities"] > 0 and f["alerts"] == 0:
            out.append(f"Возможности были, но NET < {s.telegram_profit_threshold_pct:g}% или человек не успевал - "
                       "в Telegram не ушло (порог: telegram_profit_threshold_pct).")
        if not ranked and f["signals"] == 0:
            out.append("Движения были только на одной бирже за раз и закончились до того, как кто-то отстал.")
        return out


__all__ = ["Diagnostics", "REASONS", "REASON_KEYS"]
