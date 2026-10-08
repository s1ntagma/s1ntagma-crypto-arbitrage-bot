"""Shadow trading: every approved opportunity is traded virtually, on the real
book, without sending a single order.

* entry  = the opportunity's executable economics at detection (entry VWAP
  walked on the real target book, executable size, real fees);
* exit   = the latest real exit VWAP from the opportunity's refreshes - when it
  reaches ``exit_target`` (target caught up) or when the opportunity ends
  (window over -> sold at the last measured market price);
* result = price move after spread/slippage (already in the VWAPs) minus entry
  fee, exit fee and execution loss.  No book data -> no shadow trade.

Results go to SQLite (``shadow_trades``) and feed the strategy statistics:
expected NET vs what the market actually gave, per target exchange and asset.
Shadow trades never touch balances, risk limits or the exchange.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from .. import events
from ..models import TERMINAL_STATES, Opportunity, OppState

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

    def __init__(self, settings: Any, bus: events.EventBus, clock: Any, db: Any | None = None, diag: Any | None = None) -> None:
        self._settings = settings
        self._bus = bus
        self._clock = clock
        self._db = db
        self._diag = diag
        self._open: dict[str, ShadowTrade] = {}
        self._closed: list[ShadowTrade] = []
        self.opened = 0
        self.closed = 0
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
        if not (e.entry_vwap > 0 and e.exit_vwap_now > 0 and e.base_amount > 0 and e.liquidity_ok):
            return  # no real executable book -> no shadow trade
        now = self._clock.wall()
        t = ShadowTrade(
            id=opp.id, asset=opp.asset, target=opp.target_exchange, symbol=opp.target_symbol,
            quote=getattr(opp, "quote", "") or opp.signal.target_quote, first_mover=opp.first_mover,
            direction=int(opp.direction), opened_ts=now, entry_price=float(e.entry_vwap),
            base_amount=float(e.base_amount), capital_quote=float(e.executable_quote),
            entry_fee_pct=float(e.entry_fee_pct), exit_fee_pct=float(e.exit_fee_pct),
            execution_loss_pct=float(e.execution_loss_pct), expected_net_pct=float(e.net_pct),
            exit_target=float(e.exit_target), last_exit_price=float(e.exit_vwap_now), last_update_ts=now,
        )
        t.max_net_pct = t.net_at(t.last_exit_price)
        self._open[opp.id] = t
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
        reached = (price - t.exit_target) * t.direction >= 0
        if reached:
            self._close(t, price, "target reached")

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
        if state in TERMINAL_STATES:
            self._close(t, t.last_exit_price, f"opportunity {state.value.lower()}: sold at last market price")

    # ---- bookkeeping -------------------------------------------------------
    def _close(self, t: ShadowTrade, price: float, reason: str) -> None:
        self._open.pop(t.id, None)
        t.closed_ts = self._clock.wall()
        t.exit_price = price
        t.exit_reason = reason
        t.actual_net_pct = t.net_at(price)
        t.pnl_quote = t.capital_quote * t.actual_net_pct / 100.0
        self._closed.append(t)
        del self._closed[:-500]
        self.closed += 1
        self._save(t)
        log.info("shadow close %s %s@%s net %+.2f%% (expected %+.2f%%) - %s",
                 t.id, t.asset, t.target, t.actual_net_pct, t.expected_net_pct, reason)

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

    def summary(self, limit: int = 5000) -> dict[str, Any]:
        return summarize(self.trades(limit))


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
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [r for r in rows if r.get("closed_ts") is not None and r.get("actual_net_pct") is not None]
    out = _stats(closed)
    out["open"] = sum(1 for r in rows if r.get("closed_ts") is None)
    out["by_target"] = _group(closed, "target")
    out["by_asset"] = _group(closed, "asset")
    return out


__all__ = ["ShadowTrade", "ShadowTrader", "summarize"]
