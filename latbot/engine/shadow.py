"""Shadow trading: every approved opportunity is traded virtually, on the real
book, without sending a single order.

* entry  = the opportunity's executable economics at detection (entry VWAP
  walked on the real target book, executable size, real fees);
* exit   = the latest real exit VWAP from the opportunity's refreshes - when it
  reaches ``exit_target`` (target caught up) or when the opportunity ends
  (window over -> sold at the last measured market price);
* result = price move after spread/slippage (already in the VWAPs) minus entry
  fee, exit fee and execution loss.  No book data -> no shadow trade.

Phase 3: shadow trading is the TRAINING of the system.  Every opportunity with
NET >= ``shadow_min_net_pct`` (0.1 %) is traded virtually regardless of its
quality score; each trade has three barriers - target reached (win), stop
(loss, the leverage plan's stop or ``shadow_stop_pct``) and the window end
(time: win iff NET > 0) - and its outcome, quality score/grade and leveraged
virtual P&L are recorded; closed trades feed ``engine.metalabel.MetaLabelStore``.

Results go to SQLite (``shadow_trades``) and feed the strategy statistics:
expected NET vs what the market actually gave, per target exchange and asset.
Shadow trades never touch balances, risk limits or the exchange.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from .. import events
from .exit_policy import RideConfig, RidePosition, decide
from ..models import TERMINAL_STATES, Opportunity, OppState

PERIOD_KEY = "stats_period_start_ts"  # kv: Stats count virtual trades opened from here on (history stays)

# expiry reasons that mean the last seen price is old (pipeline / OpportunityManager.sweep)
_STALE_REASONS = frozenset({"target book stale", "no updates"})

log = logging.getLogger(__name__)


@dataclass(slots=True)
class ShadowTrade:
    id: str  # = opportunity id (one shadow trade per opportunity)
    asset: str
    target: str
    symbol: str
    quote: str
    first_mover: str
    direction: int
    opened_ts: float
    entry_price: float
    base_amount: float
    capital_quote: float
    entry_fee_pct: float
    exit_fee_pct: float
    execution_loss_pct: float
    expected_net_pct: float
    exit_target: float
    last_exit_price: float
    last_update_ts: float
    closed_ts: float | None = None
    exit_price: float | None = None
    exit_reason: str = ""
    actual_net_pct: float | None = None
    pnl_quote: float | None = None
    max_net_pct: float = field(default=float("-inf"))
    # Phase 3 (training)
    lag_pct: float = 0.0
    quality_score: float | None = None
    grade: str = ""
    btc_vol_pctile: float | None = None
    leverage_max: float = 1.0  # LeveragePlan.max_by_signal at entry (what the signal justified)
    stop_pct: float = 1.0
    stop_price: float = 0.0
    barrier: str = ""  # "target" | "stop" | "time" | "no_data" (closed without a fresh price: not learned)
    exit_kind: str = ""  # ride the leader: stop / leader_reversed / caught_up / trailing / max_hold
    pnl_leveraged_quote: float | None = None

    def net_at(self, exit_price: float) -> float:
        move = (exit_price / self.entry_price - 1.0) * 100.0 * self.direction
        return move - self.entry_fee_pct - self.exit_fee_pct - self.execution_loss_pct

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d["max_net_pct"] == float("-inf"):
            d["max_net_pct"] = None
        d["open"] = self.closed_ts is None
        return d


class ShadowTrader:
    """Subscribes to OPP_NEW / OPP_UPDATE / OPP_STATE; no network, no orders."""

    def __init__(self, settings: Any, bus: events.EventBus, clock: Any, db: Any | None = None, diag: Any | None = None,
                 metalabel: Any | None = None, market: Any | None = None) -> None:
        self._settings = settings
        self._bus = bus
        self._clock = clock
        self._db = db
        self._diag = diag
        self._metalabel = metalabel
        self._market = market  # Pipeline.market_view: prices for positions that outlive their opportunity
        self._ride: dict[str, RidePosition] = {}
        self._open: dict[str, ShadowTrade] = {}
        self._closed: list[ShadowTrade] = []
        self.opened = 0
        self.closed = 0
        self.no_data = 0  # closed without a fresh price (outcome unknown, not learned)
        self.skipped: dict[str, int] = {}
        self._period_start: float | None = self._load_period()
        self._period_pnl = self._load_period_pnl()  # virtual balance = start + this
        self._unsubs = [
            bus.subscribe(events.OPP_NEW, self._on_new),
            bus.subscribe(events.OPP_UPDATE, self._on_update),
            bus.subscribe(events.OPP_STATE, self._on_state),
        ]

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._settings, "shadow_trading", True))

    def stop(self) -> None:
        for u in self._unsubs:
            try:
                u()
            except Exception:  # noqa: BLE001
                pass
        self._unsubs = []

    # ---- events ------------------------------------------------------------
    def _on_new(self, opp: Opportunity) -> None:
        if not self.enabled or opp.id in self._open:
            return
        e = opp.econ
        # A thin book (liquidity flag: less than the requested capital) is still a real
        # trade at the size the book supports; only "no executable book" is skipped.
        min_trade = float(getattr(self._settings, "min_trade_quote", 0.0) or 0.0)
        if not (e.entry_vwap > 0 and e.exit_vwap_now > 0 and e.base_amount > 0):
            self._skip("no executable book")
            return
        if e.executable_quote < min_trade:
            self._skip("book below min trade size")
            return
        if float(e.net_pct) < float(getattr(self._settings, "shadow_min_net_pct", 0.1)):
            self._skip("NET below training threshold")
            return
        if float(e.executable_quote) > self._free_balance() + 1e-9:
            self._skip("no free virtual balance")  # like a real account: open stakes are not available
            return
        now = self._clock.wall()
        quality = opp.quality if isinstance(getattr(opp, "quality", None), dict) else {}
        lev = opp.leverage if isinstance(getattr(opp, "leverage", None), dict) else {}
        stop_pct = _positive(lev.get("stop_pct")) or float(getattr(self._settings, "shadow_stop_pct", 1.0))
        t = ShadowTrade(
            id=opp.id, asset=opp.asset, target=opp.target_exchange, symbol=opp.target_symbol,
            quote=getattr(opp, "quote", "") or opp.signal.target_quote, first_mover=opp.first_mover,
            direction=int(opp.direction), opened_ts=now, entry_price=float(e.entry_vwap),
            base_amount=float(e.base_amount), capital_quote=float(e.executable_quote),
            entry_fee_pct=float(e.entry_fee_pct), exit_fee_pct=float(e.exit_fee_pct),
            execution_loss_pct=float(e.execution_loss_pct), expected_net_pct=float(e.net_pct),
            exit_target=float(e.exit_target), last_exit_price=float(e.exit_vwap_now), last_update_ts=now,
            lag_pct=float(getattr(opp.signal, "lag_pct", 0.0) or 0.0),
            quality_score=_number(quality.get("score")), grade=str(quality.get("grade") or ""),
            btc_vol_pctile=_number(quality.get("btc_vol_pctile")),
            leverage_max=_positive(lev.get("max_by_signal")) or 1.0,
            stop_pct=stop_pct,
            # measured on the exit side (what the trade would be sold at), so the spread does not eat the stop
            stop_price=float(e.exit_vwap_now) * (1.0 - int(opp.direction) * stop_pct / 100.0),
        )
        t.max_net_pct = t.net_at(t.last_exit_price)
        self._open[opp.id] = t
        if self._ride_on():
            self._ride[opp.id] = self._ride_position(t, opp)
        self.opened += 1
        self._save(t)
        log.info("shadow open %s %s@%s entry %.8g expected net %+.2f%%", t.id, t.asset, t.target, t.entry_price, t.expected_net_pct)

    def _on_update(self, opp: Opportunity) -> None:
        t = self._open.get(opp.id)
        if t is None:
            return
        price = float(opp.econ.exit_vwap_now or 0.0)
        if price <= 0:
            return
        t.last_exit_price = price
        t.last_update_ts = self._clock.wall()
        t.max_net_pct = max(t.max_net_pct, t.net_at(price))
        if t.id in self._ride:
            return  # ride the leader: tick() decides with the leader's price too
        reached = (price - t.exit_target) * t.direction >= 0
        if reached:
            self._close(t, price, "target reached", "target")
        elif t.stop_price > 0 and (price - t.stop_price) * t.direction <= 0:
            self._close(t, price, f"stop {t.stop_pct:.2f}% hit", "stop")

    def _on_state(self, payload: Any) -> None:
        opp = payload.get("opportunity") if isinstance(payload, dict) else None
        if opp is None:
            return
        t = self._open.get(opp.id)
        if t is None:
            return
        to = payload.get("to")
        try:
            state = OppState(to)
        except ValueError:
            return
        if state in TERMINAL_STATES and t.id in self._ride:
            return  # the entry offer ended; the position rides on until the exit policy sells it
        if state in TERMINAL_STATES:
            reason = str(payload.get("reason") or "")
            age_s = self._clock.wall() - t.last_update_ts
            if reason in _STALE_REASONS or age_s > 2.0 * float(self._settings.move_window_s):
                # the last price is old: the outcome is unknown - not a win, not a loss, never learned
                self._close(t, t.last_exit_price, f"opportunity {state.value.lower()} ({reason or 'no fresh price'}): "
                            "no fresh price, outcome unknown", "no_data")
            else:
                why = f" ({reason})" if reason else ""
                self._close(t, t.last_exit_price, f"opportunity {state.value.lower()}{why}: sold at last market price", "time")

    # ---- ride the leader -----------------------------------------------------
    def _ride_on(self) -> bool:
        active = getattr(self._settings, "ride_active", None)
        return bool(self._market is not None and callable(active) and active())

    def _view(self, t: ShadowTrade) -> dict[str, Any] | None:
        try:
            return self._market(t.asset, t.target, t.first_mover, t.base_amount, t.direction)
        except Exception:  # noqa: BLE001
            log.exception("market view failed for shadow %s", t.id)
            return None

    def _ride_position(self, t: ShadowTrade, opp: Opportunity) -> RidePosition:
        d = t.direction
        pos = RidePosition(direction=d, entry_price=t.entry_price,
                           cost_pct=t.entry_fee_pct + t.exit_fee_pct + t.execution_loss_pct,
                           entry_mono=self._clock.mono(), last_fresh_mono=self._clock.mono())
        view = self._view(t)
        if view:  # pre-move prices from the measured moves: lag = leader move - target move
            sig = opp.signal
            pos.leader_anchor = view["leader_mid"] / (1.0 + d * float(sig.first_move_pct) / 100.0)
            pos.target_anchor = view["target_mid"] / (1.0 + d * float(sig.target_move_pct) / 100.0)
            pos.update(self._clock.mono(), view["target_exit"], view["leader_mid"])
        return pos

    def tick(self, now_mono: float | None = None) -> None:
        """Check every riding position on the latest prices (called by the pipeline sweep)."""
        if not self._ride:
            return
        now = self._clock.mono() if now_mono is None else now_mono
        cfg = RideConfig.from_settings(self._settings)
        stale_s = float(getattr(self._settings, "ride_stale_s", 30.0))
        for oid, pos in list(self._ride.items()):
            t = self._open.get(oid)
            if t is None:
                self._ride.pop(oid, None)
                continue
            view = self._view(t)
            if not view:
                if now - (pos.last_fresh_mono or pos.entry_mono) > stale_s:
                    self._ride.pop(oid, None)
                    self._close(t, t.last_exit_price, f"no fresh price for {stale_s:g} s: outcome unknown", "no_data")
                continue
            price = view["target_exit"]
            t.last_exit_price = price
            t.last_update_ts = self._clock.wall()
            t.max_net_pct = max(t.max_net_pct, t.net_at(price))
            d = decide(pos, now, price, view["leader_mid"], cfg, target_mid=view["target_mid"])
            if d.sell:
                self._ride.pop(oid, None)
                t.exit_kind = d.code
                self._close(t, price, d.text, "stop" if d.code == "stop" else "time")

    # ---- bookkeeping -------------------------------------------------------
    def _skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1
        log.debug("shadow skipped: %s", reason)

    def _close(self, t: ShadowTrade, price: float, reason: str, barrier: str = "time") -> None:
        self._open.pop(t.id, None)
        t.closed_ts = self._clock.wall()
        t.exit_price = price
        t.exit_reason = reason
        t.barrier = barrier
        self._closed.append(t)
        del self._closed[:-500]
        self.closed += 1
        if barrier == "no_data":
            self.no_data += 1
            self._save(t)
            log.info("shadow close %s %s@%s without a fresh price (not learned) - %s", t.id, t.asset, t.target, reason)
            return
        t.actual_net_pct = t.net_at(price)
        t.pnl_quote = t.capital_quote * t.actual_net_pct / 100.0
        if self._period_start is None or t.opened_ts >= self._period_start:
            self._period_pnl += t.pnl_quote
        t.pnl_leveraged_quote = t.pnl_quote * max(1.0, t.leverage_max)
        self._save(t)
        self._learn(t)
        log.info("shadow close %s %s@%s net %+.2f%% (expected %+.2f%%) - %s",
                 t.id, t.asset, t.target, t.actual_net_pct, t.expected_net_pct, reason)

    def _learn(self, t: ShadowTrade) -> None:
        if self._metalabel is None:
            return
        try:
            self._metalabel.record({
                "id": t.id, "asset": t.asset, "target": t.target, "first_mover": t.first_mover, "lag_pct": t.lag_pct,
                "btc_vol_pctile": t.btc_vol_pctile, "barrier": t.barrier, "net_pct": t.actual_net_pct,
                "quality_score": t.quality_score, "grade": t.grade, "ts": t.closed_ts,
            })
        except Exception:  # noqa: BLE001
            log.exception("meta-label record failed for shadow trade %s", t.id)

    def _save(self, t: ShadowTrade) -> None:
        if self._db is None or not hasattr(self._db, "save_shadow"):
            return
        try:
            self._db.save_shadow(t.to_dict())
        except Exception:  # noqa: BLE001
            log.exception("shadow trade %s not saved", t.id)

    # ---- queries -----------------------------------------------------------
    def trades(self, limit: int = 200) -> list[dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        if self._db is not None and hasattr(self._db, "list_shadow"):
            try:
                for d in self._db.list_shadow(limit):
                    rows[str(d.get("id"))] = d
            except Exception:  # noqa: BLE001
                log.exception("list_shadow failed")
        for t in [*self._closed, *self._open.values()]:
            rows[t.id] = t.to_dict()
        out = sorted(rows.values(), key=lambda d: float(d.get("opened_ts") or 0.0), reverse=True)
        return out[: max(0, int(limit))]

    # ---- statistics period -------------------------------------------------
    def _load_period(self) -> float | None:
        """The current Stats period; the first start of a build with periods opens one (older
        builds traded a different universe - their results would mislead)."""
        kv_get, kv_set = getattr(self._db, "kv_get", None), getattr(self._db, "kv_set", None)
        if kv_get is None or kv_set is None:
            return None
        try:
            v = kv_get(PERIOD_KEY)
            if v is None:
                v = self._clock.wall()
                kv_set(PERIOD_KEY, v)
            return float(v)
        except Exception:  # noqa: BLE001
            log.exception("stats period not available; counting every trade")
            return None

    def _balance_start(self) -> float:
        return float(getattr(self._settings, "shadow_balance_quote", 1000.0) or 0.0)

    def _load_period_pnl(self) -> float:
        rows = []
        if self._db is not None and hasattr(self._db, "list_shadow"):
            try:
                rows = self._db.list_shadow(5000)
            except Exception:  # noqa: BLE001
                log.exception("list_shadow failed")
        start = self._period_start
        return sum(float(r.get("pnl_quote") or 0.0) for r in rows
                   if r.get("closed_ts") is not None and (start is None or float(r.get("opened_ts") or 0.0) >= start))

    def _free_balance(self) -> float:
        """Virtual balance now minus what the open virtual positions hold."""
        held = sum(t.capital_quote for t in self._open.values())
        return self._balance_start() + self._period_pnl - held

    @property
    def period_start_ts(self) -> float | None:
        return self._period_start

    def new_period(self) -> float:
        """Owner: count from now on (the history stays in the DB and in the trades table)."""
        now = self._clock.wall()
        kv_set = getattr(self._db, "kv_set", None)
        if kv_set is not None:
            kv_set(PERIOD_KEY, now)
        self._period_start = now
        self._period_pnl = 0.0  # the virtual balance starts again from shadow_balance_quote
        log.info("stats: new period from %.0f", now)
        return now

    def summary(self, limit: int = 5000) -> dict[str, Any]:
        rows = self.trades(limit)
        start = self._period_start
        cur = rows if start is None else [r for r in rows if float(r.get("opened_ts") or 0.0) >= start]
        out = summarize(cur)
        out.update(_balance_stats(cur, self._balance_start(), self._clock.wall()))
        out["skipped"] = dict(self.skipped)
        out["period_start_ts"] = start
        out["all_time_closed"] = sum(1 for r in rows if r.get("closed_ts") is not None)
        return out


def _group(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        out.setdefault(str(r.get(key) or "?"), []).append(r)  # type: ignore[arg-type]
    return {k: _stats(v) for k, v in out.items()}  # type: ignore[arg-type]


def _stats(closed: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(closed)
    if n == 0:
        return {"closed": 0, "wins": 0, "win_rate": None, "avg_expected_net_pct": None,
                "avg_actual_net_pct": None, "pnl_quote": 0.0, "target_reached": 0}
    wins = sum(1 for r in closed if (r.get("actual_net_pct") or 0.0) > 0)
    return {
        "closed": n,
        "wins": wins,
        "win_rate": wins / n,
        "avg_expected_net_pct": sum(float(r.get("expected_net_pct") or 0.0) for r in closed) / n,
        "avg_actual_net_pct": sum(float(r.get("actual_net_pct") or 0.0) for r in closed) / n,
        "pnl_quote": sum(float(r.get("pnl_quote") or 0.0) for r in closed),
        "target_reached": sum(1 for r in closed if r.get("exit_reason") == "target reached"),
        "stopped": sum(1 for r in closed if r.get("barrier") == "stop"),
        "pnl_leveraged_quote": sum(float(r.get("pnl_leveraged_quote") or r.get("pnl_quote") or 0.0) for r in closed),
    }


def _balance_stats(rows: list[dict[str, Any]], start: float, now_wall: float) -> dict[str, Any]:
    """Virtual balance of the training period: start + closed results, today's result, max drawdown."""
    import datetime as _dt

    closed = sorted((r for r in rows if r.get("closed_ts") is not None and r.get("pnl_quote") is not None),
                    key=lambda r: float(r.get("closed_ts") or 0.0))
    midnight = _dt.datetime.fromtimestamp(now_wall).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    equity = peak = start
    max_dd = 0.0
    today_pnl, today_n = 0.0, 0
    for r in closed:
        pnl = float(r.get("pnl_quote") or 0.0)
        equity += pnl
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak * 100.0)
        if float(r.get("closed_ts") or 0.0) >= midnight:
            today_pnl += pnl
            today_n += 1
    return {"balance_start": start, "balance_now": equity,
            "balance_pct": (equity - start) / start * 100.0 if start > 0 else None,
            "today_pnl_quote": today_pnl, "today_closed": today_n, "max_drawdown_pct": max_dd}


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [r for r in rows if r.get("closed_ts") is not None and r.get("actual_net_pct") is not None]
    out = _stats(closed)
    out["open"] = sum(1 for r in rows if r.get("closed_ts") is None)
    out["no_data"] = sum(1 for r in rows if r.get("barrier") == "no_data")
    exits: dict[str, int] = {}
    for r in closed:
        k = str(r.get("exit_kind") or r.get("barrier") or "?")
        exits[k] = exits.get(k, 0) + 1
    out["by_exit"] = exits
    out["by_target"] = _group(closed, "target")
    out["by_asset"] = _group(closed, "asset")
    graded = [dict(r, grade=r.get("grade") or "unknown") for r in closed]
    out["by_grade"] = _group(graded, "grade")
    return out


def _number(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x and abs(x) != float("inf") else None


def _positive(v: Any) -> float | None:
    x = _number(v)
    return x if x is not None and x > 0 else None


__all__ = ["ShadowTrade", "ShadowTrader", "summarize"]
