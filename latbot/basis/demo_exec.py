"""Momentum positions mirrored with REAL orders on the Bybit DEMO account (demo money, real exchange fills).

The simulation in ``momentum.py`` stays the decision maker (entry rule, the bot's own stop, trailing stop,
max hold, the owner's Close all).  This executor copies each of its positions to Bybit Demo linear
perpetuals and reports what the exchange really filled.  It is the first code of the basis module that sends
orders, so the hard rules apply in full:

* DEMO ONLY.  The adapter must be configured ``demo`` (``CcxtAdapter`` then refuses any non-demo host before
  every order); a non-demo adapter is refused here as well.  There is no LIVE path in this module.
* ONE place sends orders: ``adapter.place_order``.  Every order has a deterministic ``client_id``
  (entry ``…e``, exit attempt n ``…x<n>``), so the same order is never sent twice - not after a timeout and
  not after a restart.
* A timeout / unknown status is NOT retried: the order is looked up with ``fetch_order(client_id=…)`` on the
  next steps.  If the exchange does not know it after ``_UNKNOWN_S``, the position itself decides
  (``fetch_position``).
* ``RiskManager.check`` runs before every entry (emergency stop, daily loss, exposure, trade size); exits are
  always attempted (closing reduces risk).
* A trade is closed only when the exchange reports the position flat.  A partly filled exit gets a NEW order
  (a new client id) for the rest, only after the previous one is known.
* Keys never appear in logs, in the status or on the trade.

State on the momentum trade (``MomTrade.demo``, persisted with it): ``state`` = entry_due | entry_unknown |
open | exit_due | exit_unknown | closed | skipped | rejected, plus qty, prices, fees, net, note.
"""
from __future__ import annotations

import dataclasses
import logging
from typing import Any

from ..config import ExchangeConfig, ExchangeCredentials
from ..exchanges.base import ExchangeError, InsufficientBalance, OrderRejected
from ..models import ExchangeRole, Mode, OrderRequest, OrderStatus, OrderType, Side

log = logging.getLogger(__name__)

VENUE = "bybit.demo"
_UNKNOWN_S = 60.0  # an order the exchange does not know after this long: the position decides
_MAX_EXIT_ATTEMPTS = 5
_BALANCE_EVERY_S = 60.0
_DONE = (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED, OrderStatus.FAILED)
_LIVE_STATES = ("entry_due", "entry_unknown", "open", "exit_due", "exit_unknown")


def demo_config() -> ExchangeConfig:
    return ExchangeConfig(name=VENUE, ccxt_id="bybit", role=ExchangeRole.EXECUTION, market_type="swap", demo=True,
                          taker_fee=0.00055, maker_fee=0.0002, label="Bybit Demo (perp)", poll_tickers=False)


class MomentumDemo:
    def __init__(self, momentum: Any, settings: Any, clock: Any, risk: Any, creds: ExchangeCredentials | None,
                 *, adapter: Any | None = None, bus: Any | None = None) -> None:
        self._mom = momentum
        self._s = settings
        self._clock = clock
        self._risk = risk
        self._creds = creds
        self._bus = bus
        self._adapter = adapter
        self._ready = False
        self._error = ""
        self._started_ts: float | None = None
        self._reconciled: set[str] = set()
        self._balance: float | None = None
        self._balance_mono = float("-inf")

    # ------------------------------------------------------------------ settings / status
    @property
    def enabled(self) -> bool:
        return bool(getattr(self._s, "mom_demo_enabled", False))

    def _notional(self) -> float:
        try:
            return float(getattr(self._s, "mom_demo_notional_quote", 100.0))
        except (TypeError, ValueError):
            return 100.0

    def status(self) -> dict[str, Any]:
        rows = [t for t in self._mom._trades if t.demo]
        closed = [t for t in rows if t.demo.get("state") == "closed" and t.demo.get("net") is not None]
        return {
            "enabled": self.enabled, "ready": self._ready, "error": self._error,
            "key": bool(self._creds is not None and self._creds.present),  # present or not - never the key
            "balance_quote": self._balance, "notional_quote": self._notional(),
            "open": sum(1 for t in rows if t.demo.get("state") in _LIVE_STATES),
            "closed": len(closed), "wins": sum(1 for t in closed if t.demo["net"] > 0),
            "net_quote": sum(t.demo["net"] for t in closed),
            "fees_quote": sum(float(t.demo.get("fees") or 0.0) for t in closed),
            "unknown": sum(1 for t in rows if t.demo.get("state") == "closed" and t.demo.get("net") is None),
            "skipped": sum(1 for t in rows if t.demo.get("state") in ("skipped", "rejected")),
        }

    # ------------------------------------------------------------------ lifecycle
    def _build_adapter(self) -> Any:
        from ..events import EventBus
        from ..exchanges.ccxt_adapter import CcxtAdapter  # lazy: ccxt.pro import is slow

        s = dataclasses.replace(self._s, quote_assets=["USDT"], signal_quote_assets=["USDT"])
        return CcxtAdapter(demo_config(), self._bus or EventBus(), self._clock, s, self._creds)

    async def start(self) -> None:
        self._started_ts = self._clock.wall()
        self._ready = False
        if not self.enabled:
            self._error = "switched off (mom_demo_enabled)"
            return
        if self._creds is None or not self._creds.present:
            self._error = "no Bybit Demo key in .env (BYBIT_API_KEY / BYBIT_API_SECRET)"
            return
        try:
            if self._adapter is None:
                self._adapter = self._build_adapter()
            if not bool(getattr(self._adapter.cfg, "demo", False)):
                self._error = "refused: the adapter is not a demo adapter (this module never trades live)"
                return
            assets = [str(a).upper() for a in (getattr(self._s, "mom_assets", None) or ["BTC", "ETH"])]
            await self._adapter.start(assets, ws_assets=0)  # execution-only: markets, demo host check, no streams
            self._ready, self._error = True, ""
            log.info("momentum demo: ready on %s (size %.0f per trade)", VENUE, self._notional())
        except Exception as exc:  # noqa: BLE001
            self._error = f"start failed: {type(exc).__name__}: {str(exc)[:120]}"
            log.warning("momentum demo: %s", self._error)

    async def stop(self) -> None:
        self._ready = False
        if self._adapter is not None and hasattr(self._adapter, "stop"):
            try:
                await self._adapter.stop()
            except Exception:  # noqa: BLE001
                log.debug("momentum demo: adapter stop failed")

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _cid(t: Any, leg: str) -> str:
        """Deterministic and short (Bybit orderLinkId is at most 36 characters)."""
        return f"md{int(t.signal_ts * 1000)}{t.asset}{leg}"[:36]

    def _set(self, t: Any, **kv: Any) -> None:
        t.demo.update(kv)
        self._mom._save(t)

    async def _lookup(self, t: Any, cid: str, symbol: str) -> Any | None:
        try:
            return await self._adapter.fetch_order("", symbol, client_id=cid)
        except Exception as exc:  # noqa: BLE001 - not found yet, network: asked again on the next step
            log.debug("momentum demo: fetch_order %s: %s", cid, type(exc).__name__)
            return None

    async def _position(self, symbol: str) -> float | None:
        try:
            return float(await self._adapter.fetch_position(symbol))
        except Exception as exc:  # noqa: BLE001
            log.debug("momentum demo: fetch_position %s: %s", symbol, type(exc).__name__)
            return None

    async def _refresh_balance(self) -> None:
        now = self._clock.mono()
        if now - self._balance_mono < _BALANCE_EVERY_S:
            return
        self._balance_mono = now
        try:
            b = (await self._adapter.fetch_balances()).get("USDT")
            self._balance = float(b.total) if b is not None else None
        except Exception as exc:  # noqa: BLE001
            self._error = f"balance: {type(exc).__name__}: {str(exc)[:100]}"

    # ------------------------------------------------------------------ the state machine (called ~2x a second)
    async def step(self) -> None:
        if not self._ready:
            return
        await self._refresh_balance()
        for t in list(self._mom._trades):
            try:
                await self._step_trade(t)
            except Exception:  # noqa: BLE001 - one trade never stops the others
                log.exception("momentum demo: step failed for %s", t.id)

    async def _step_trade(self, t: Any) -> None:
        state = t.demo.get("state")
        if not state:
            # a NEW simulated position (opened after this start) is mirrored; older ones are left alone
            if t.status == "open" and self._started_ts is not None and t.signal_ts >= self._started_ts:
                await self._enter(t)
            return
        if state not in _LIVE_STATES:
            return
        symbol = t.demo.get("symbol") or self._adapter.symbol_for(t.asset)
        if t.id not in self._reconciled and state in ("open", "exit_due", "exit_unknown"):
            self._reconciled.add(t.id)
            pos = await self._position(symbol)
            if pos is not None and abs(pos) <= 1e-12 and state == "open" and not t.demo.get("exit_n"):
                # the position is gone on the exchange (closed by hand or while the bot was down)
                self._risk.register_close(t.id, 0.0)
                self._set(t, state="closed", net=None, note="position gone on the exchange: result unknown")
                return
            self._risk.register_open(t.id, VENUE, t.asset, float(t.demo.get("qty") or 0.0) * float(t.demo.get("entry_px") or 0.0), Mode.PAPER)
        if state == "entry_unknown":
            await self._resolve_entry(t, symbol)
        elif state == "open":
            if t.status not in ("pending", "open"):  # the bot decided to leave (stop, trailing, max hold, owner)
                self._set(t, state="exit_due", exit_why=t.exit_reason or t.status)
                await self._exit(t, symbol)
        elif state == "exit_due":
            await self._exit(t, symbol)
        elif state == "exit_unknown":
            await self._resolve_exit(t, symbol)

    # ---- entry
    async def _enter(self, t: Any) -> None:
        self._reconciled.add(t.id)
        symbol = self._adapter.symbol_for(t.asset)
        if not symbol:
            self._set(t, state="skipped", note=f"no {t.asset} perpetual on Bybit Demo")
            return
        ok, why, allowed = self._risk.check(VENUE, t.asset, self._notional(), Mode.PAPER)
        if not ok:
            self._set(t, state="skipped", note=f"risk: {why}")
            return
        qty = self._adapter.order_amount(symbol, allowed / t.entry_px) if t.entry_px else None
        if not qty:
            self._set(t, state="skipped", note=f"size ${allowed:.0f} is below the minimum order of {symbol}")
            return
        side = Side.BUY if t.direction > 0 else Side.SELL
        cid = self._cid(t, "e")
        self._set(t, state="entry_unknown", symbol=symbol, side=side.value, want_qty=qty, entry_cid=cid,
                  sent_ts=self._clock.wall(), fees=0.0, note="")  # stored BEFORE sending: never sent twice
        try:
            o = await self._adapter.place_order(
                OrderRequest(exchange=VENUE, symbol=symbol, side=side, amount=qty, type=OrderType.MARKET,
                             client_id=cid, price=t.entry_px))
        except (OrderRejected, InsufficientBalance, ExchangeError) as exc:
            self._set(t, state="rejected", note=f"entry rejected: {str(exc)[:120]}")
            return
        await self._entry_result(t, o)

    async def _entry_result(self, t: Any, o: Any) -> None:
        if o is None or o.status not in _DONE or getattr(o, "error", None):
            return  # still unknown: looked up again on the next step
        if o.filled and o.filled > 0:
            px = float(o.avg_price or t.entry_px)
            self._risk.register_open(t.id, VENUE, t.asset, o.filled * px, Mode.PAPER)
            self._set(t, state="open", qty=float(o.filled), entry_px=px, fees=float(o.fee_cost_quote or 0.0),
                      note="" if o.filled >= t.demo["want_qty"] * 0.999 else "entry partly filled")
            log.info("momentum demo: %s %s %.6g @ %.8g on %s", t.demo["side"], t.asset, o.filled, px, VENUE)
        else:
            self._set(t, state="rejected", note=f"entry not filled ({o.status.value})")

    async def _resolve_entry(self, t: Any, symbol: str) -> None:
        o = await self._lookup(t, t.demo["entry_cid"], symbol)
        if o is not None and o.status in _DONE:
            await self._entry_result(t, o)
            return
        if self._clock.wall() - float(t.demo.get("sent_ts") or 0.0) < _UNKNOWN_S:
            return
        pos = await self._position(symbol)  # the exchange does not know the order: the position decides
        if pos is None:
            return
        if abs(pos) > 1e-12:
            self._risk.register_open(t.id, VENUE, t.asset, abs(pos) * t.entry_px, Mode.PAPER)
            self._set(t, state="open", qty=abs(pos), entry_px=None, note="entry price unknown (order not found, position exists)")
        else:
            self._set(t, state="rejected", note="entry order not found and no position: nothing was opened")

    # ---- exit
    async def _exit(self, t: Any, symbol: str) -> None:
        pos = await self._position(symbol)
        if pos is None:
            return  # the exchange did not answer: asked again on the next step
        left = abs(pos)
        if left <= 1e-12:
            self._finish(t)
            return
        n = int(t.demo.get("exit_n") or 0) + 1
        if n > _MAX_EXIT_ATTEMPTS:
            self._set(t, note=f"position still open after {_MAX_EXIT_ATTEMPTS} exit orders: close it by hand on Bybit Demo")
            return
        qty = min(left, float(t.demo.get("qty") or left))
        side = Side.SELL if t.demo.get("side") == "buy" else Side.BUY
        cid = self._cid(t, f"x{n}")
        self._set(t, state="exit_unknown", exit_n=n, exit_cid=cid, exit_sent_ts=self._clock.wall())
        try:
            o = await self._adapter.place_order(
                OrderRequest(exchange=VENUE, symbol=symbol, side=side, amount=qty, type=OrderType.MARKET, client_id=cid),
                reduce_only=True)
        except (OrderRejected, InsufficientBalance, ExchangeError) as exc:
            self._set(t, state="exit_due", note=f"exit order {n} rejected: {str(exc)[:100]}")
            return
        await self._exit_result(t, o, symbol)

    async def _exit_result(self, t: Any, o: Any, symbol: str) -> None:
        if o is None or o.status not in _DONE or getattr(o, "error", None):
            return
        if o.filled and o.filled > 0:
            done = float(t.demo.get("exit_qty") or 0.0)
            px_prev = float(t.demo.get("exit_px") or 0.0)
            total = done + float(o.filled)
            vwap = (px_prev * done + float(o.avg_price or 0.0) * float(o.filled)) / total if total > 0 else None
            self._set(t, exit_qty=total, exit_px=vwap, fees=float(t.demo.get("fees") or 0.0) + float(o.fee_cost_quote or 0.0))
        pos = await self._position(symbol)
        if pos is not None and abs(pos) <= 1e-12:
            self._finish(t)
        else:
            self._set(t, state="exit_due")  # not flat (or not confirmed): the rest gets a new order

    async def _resolve_exit(self, t: Any, symbol: str) -> None:
        o = await self._lookup(t, t.demo["exit_cid"], symbol)
        if o is not None and o.status in _DONE:
            await self._exit_result(t, o, symbol)
            return
        if self._clock.wall() - float(t.demo.get("exit_sent_ts") or 0.0) >= _UNKNOWN_S:
            self._set(t, state="exit_due")  # the position decides: flat -> closed, else a new order

    def _finish(self, t: Any) -> None:
        d = t.demo
        qty, entry, exit_px = float(d.get("exit_qty") or 0.0), d.get("entry_px"), d.get("exit_px")
        net = None
        if qty > 0 and entry and exit_px:
            sign = 1.0 if d.get("side") == "buy" else -1.0
            net = sign * (float(exit_px) - float(entry)) * qty - float(d.get("fees") or 0.0)
        self._risk.register_close(t.id, net or 0.0)
        note = d.get("note") or ""
        if net is None:
            note = (note + "; " if note else "") + "result unknown (a fill price is missing)"
        self._set(t, state="closed", net=net, note=note, closed_ts=self._clock.wall())
        log.info("momentum demo: %s closed on %s, net %s", t.asset, VENUE, "unknown" if net is None else f"{net:+.2f}")


__all__ = ["MomentumDemo", "VENUE", "demo_config"]
