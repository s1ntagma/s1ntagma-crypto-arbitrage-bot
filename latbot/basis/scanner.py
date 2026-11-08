"""Spot-Futures basis scanner and virtual pair simulator (research only - never sends an order).

Per (venue, asset) the scanner keeps the latest SPOT and PERP order books of the same quote currency
(USDT or USD on both legs: no FX in the basis) and computes:

* basis on mids:           abs = perp_mid - spot_mid,  pct = (perp_mid / spot_mid - 1) * 100;
* executable entry basis:  long spot (buy the spot asks) + short perp (sell the perp bids) for the
                           configured notional, walked on the real books (VWAP);
* executable exit basis:   sell the spot bids + buy back the perp asks;
* potential NET:           entry - exit target - 4 taker fees + expected funding over the max hold.

Data quality (hard rule 1): both books must exist, be sane (not crossed / broken), younger than
``basis_max_age_ms`` and received within ``basis_max_skew_ms`` of each other (and, when the venue
stamps them, their exchange timestamps too); otherwise nothing is computed for that moment.

Conditions (two independent virtual price conditions, edge-triggered):

* ``fut_upper``  - the perp best bid reaches its upper bound: rolling mean perp mid * (1 + fut_upper_pct);
* ``spot_lower`` - the spot best ask reaches its lower bound: rolling mean spot mid * (1 - spot_lower_pct);
* ``both``       - both hold at the same moment.

``basis_mode`` picks what opens a virtual pair: ``fut_upper`` | ``spot_lower`` | ``any`` | ``both``.

Exit: the executable exit basis falls to ``min(basis_exit_basis_pct, entry - basis_min_convergence_pct)``
(the basis really moved in the pair's favour), or ``basis_max_hold_s``.

Simulated execution: each leg fills ``basis_latency_ms`` after the signal on the book of THAT moment,
walked for the target size within ``basis_max_slippage_pct`` (a thin book fills partially).  The hedged
size is the smaller leg; an excess on one leg is unwound at once (its loss counts).  A pair where only
one leg filled is a ``one_leg`` incident - recorded, unwound, never counted as a hedged pair.  Exits
(basis converged to ``basis_exit_basis_pct`` or ``basis_max_hold_s``) are simulated the same way and
retried until both legs are flat.  Funding: the short perp receives ``rate * notional`` at every funding
time it holds through when the rate is positive (pays when negative); an unknown rate is flagged, never
guessed.  Fees: taker on every fill.  The largest adverse basis move while open is recorded.
"""
from __future__ import annotations

import itertools
import logging
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable

from ..engine.economics import walk_book
from ..models import OrderBook, Side

log = logging.getLogger(__name__)

LEGS = ("spot", "perp")
CONDITIONS = ("fut_upper", "spot_lower", "both")
MODES = ("fut_upper", "spot_lower", "any", "both")
_ACTIVE = ("pending", "open", "closing")
_COUNTERS_KEY = "basis_counters"
_COUNTERS_SAVE_S = 5.0
_SAMPLE_S = 1.0
_TRADES_KEEP = 2000
_EPS = 1e-12


def _f(settings: Any, name: str, default: float) -> float:
    try:
        return float(getattr(settings, name, default))
    except (TypeError, ValueError):
        return default


@dataclass
class BasisTrade:
    """One simulated pair (or an incident: ``one_leg`` / ``no_fill``)."""

    id: str
    venue: str
    asset: str
    trigger: str  # the condition that opened it
    signal_ts: float
    signal_mono: float
    target_qty: float  # base amount of each leg
    signal_basis_pct: float  # executable entry basis at the signal (best effort)
    status: str = "pending"  # pending | open | closing | closed | one_leg | no_fill | abandoned
    opened_ts: float | None = None
    closed_ts: float | None = None
    spot_qty: float = 0.0  # open position: long spot
    perp_qty: float = 0.0  # open position: short perp
    entry_spot_vwap: float = 0.0
    entry_perp_vwap: float = 0.0
    entry_basis_pct: float | None = None  # from the entry fills
    exit_spot_vwap: float = 0.0
    exit_perp_vwap: float = 0.0
    exit_basis_pct: float | None = None  # from the exit fills
    cash_quote: float = 0.0  # gross cash flows of every fill (sells +, buys -)
    fees_quote: float = 0.0
    funding_quote: float = 0.0
    funding_unknown: bool = False
    funding_applied: list[float] = field(default_factory=list)
    partial: bool = False  # a leg filled less than the target, or the legs filled unequally
    incident: str = ""  # what went wrong in the execution, in words
    unwind_unknown: bool = False  # an excess leg could not be priced (no fresh book)
    exit_attempts: int = 0
    exit_reason: str = ""
    max_adverse_pct: float = 0.0  # worst widening of the exit basis over the entry basis
    gross_quote: float | None = None
    net_quote: float | None = None
    net_pct: float | None = None
    # simulator internals (persisted for transparency)
    phase: str = "entry"  # entry | exit
    due: dict[str, float] = field(default_factory=dict)  # leg -> mono time its fill is due
    done: dict[str, bool] = field(default_factory=dict)
    fills: dict[str, list[dict[str, float]]] = field(default_factory=lambda: {"spot": [], "perp": []})

    @property
    def hedged(self) -> bool:
        return self.status in ("open", "closing", "closed") and self.entry_basis_pct is not None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["hedged"] = self.hedged
        return d


@dataclass
class _Pair:
    venue: str
    asset: str
    spot_symbol: str = ""
    perp_symbol: str = ""
    quote_ok: bool = True
    note: str = ""
    books: dict[str, OrderBook | None] = field(default_factory=lambda: {"spot": None, "perp": None})
    taker: dict[str, float] = field(default_factory=lambda: {"spot": 0.001, "perp": 0.0005})
    funding_rate: float | None = None
    funding_interval_h: float = 8.0
    funding_next_ts: float | None = None
    history: deque = field(default_factory=lambda: deque(maxlen=3600))
    last_sample_mono: float = float("-inf")
    cond: dict[str, bool] = field(default_factory=lambda: {c: False for c in CONDITIONS})
    status: dict[str, Any] = field(default_factory=dict)


class BasisScanner:
    """Books -> basis, conditions, simulated pairs and statistics.  No network, no orders."""

    def __init__(self, settings: Any, clock: Any, db: Any | None = None) -> None:
        self._settings = settings
        self._clock = clock
        self._db = db
        self._pairs: dict[tuple[str, str], _Pair] = {}
        self._trades: list[BasisTrade] = []
        self._ids = itertools.count(1)
        self._counters_dirty_mono: float | None = None
        self.counters: dict[str, int] = {
            "signals_fut_upper": 0, "signals_spot_lower": 0, "signals_both": 0, "signals_while_open": 0,
            "entries": 0, "stale_evals": 0, "skew_evals": 0, "evaluations": 0,
        }
        self._load()

    # ------------------------------------------------------------------ settings
    def _s(self, name: str, default: float) -> float:
        return _f(self._settings, name, default)

    @property
    def mode(self) -> str:
        m = str(getattr(self._settings, "basis_mode", "both") or "both")
        return m if m in MODES else "both"

    # ------------------------------------------------------------------ registration / inputs
    def register_pair(self, venue: str, asset: str, spot_symbol: str, perp_symbol: str) -> bool:
        """A spot and a perp market of ``asset`` on ``venue``.  The quote currency must be the same on both
        legs (USDT/USDT or USD/USD): a basis across currencies would be an FX rate, not a basis."""
        p = self._pair(venue, asset)
        p.spot_symbol, p.perp_symbol = spot_symbol, perp_symbol
        sq, pq = _quote(spot_symbol), _quote(perp_symbol)
        p.quote_ok = bool(sq) and sq == pq
        p.note = "" if p.quote_ok else f"quote mismatch: spot {spot_symbol} vs perp {perp_symbol} (no FX basis)"
        if not p.quote_ok:
            log.info("basis %s %s skipped: %s", venue, asset, p.note)
        return p.quote_ok

    def set_fee(self, venue: str, asset: str, leg: str, taker: float) -> None:
        if leg in LEGS and taker is not None and taker >= 0:
            self._pair(venue, asset).taker[leg] = float(taker)

    def set_funding(self, venue: str, asset: str, rate: float | None, interval_h: float | None = None,
                    next_ts: float | None = None) -> None:
        p = self._pair(venue, asset)
        p.funding_rate = None if rate is None else float(rate)
        if interval_h and interval_h > 0:
            p.funding_interval_h = float(interval_h)
        if next_ts:
            p.funding_next_ts = float(next_ts)

    def on_book(self, venue: str, leg: str, book: OrderBook) -> None:
        """The latest ``leg`` book (sizes already in base units) of ``venue``; evaluates the pair."""
        if leg not in LEGS:
            return
        asset = (book.asset or book.symbol.split("/", 1)[0]).upper()
        p = self._pairs.get((venue, asset))
        if p is None:
            return  # not a registered pair
        p.books[leg] = book
        self._evaluate(p, self._clock.mono())

    # ------------------------------------------------------------------ evaluation
    def _pair(self, venue: str, asset: str) -> _Pair:
        key = (venue, asset.upper())
        p = self._pairs.get(key)
        if p is None:
            hist = max(60, int(self._s("basis_history_s", 3600.0) / _SAMPLE_S))
            p = self._pairs[key] = _Pair(venue=venue, asset=asset.upper(), history=deque(maxlen=hist))
        return p

    def _fresh(self, p: _Pair, now: float) -> tuple[bool, str]:
        s, f = p.books.get("spot"), p.books.get("perp")
        if not p.quote_ok:
            return False, p.note
        if s is None or f is None:
            return False, "waiting for both books"
        for leg, b in (("spot", s), ("perp", f)):
            if b.is_broken or not b.bids or not b.asks:
                return False, f"{leg} book unusable"
        max_age = self._s("basis_max_age_ms", 2000.0)
        for leg, b in (("spot", s), ("perp", f)):
            if b.age_ms(now) > max_age:
                return False, f"{leg} book stale ({b.age_ms(now):.0f} ms)"
        skew = abs(s.ts.recv_mono - f.ts.recv_mono) * 1000.0
        if s.ts.exchange_ts and f.ts.exchange_ts:
            skew = max(skew, abs(s.ts.exchange_ts - f.ts.exchange_ts) * 1000.0)
        if skew > self._s("basis_max_skew_ms", 500.0):
            return False, f"timestamps {skew:.0f} ms apart"
        return True, ""

    def _target_qty(self, p: _Pair) -> float:
        s = p.books["spot"]
        mid = s.mid if s is not None else None
        return self._s("basis_notional_quote", 1000.0) / mid if mid else 0.0

    def _exec_basis(self, p: _Pair, qty: float) -> tuple[float | None, float | None]:
        """(entry, exit) executable basis % for ``qty`` on the current books (None: book too thin)."""
        s, f = p.books["spot"], p.books["perp"]
        sa = walk_book(s.asks, base_amount=qty, side=Side.BUY)
        fb = walk_book(f.bids, base_amount=qty, side=Side.SELL)
        sb = walk_book(s.bids, base_amount=qty, side=Side.SELL)
        fa = walk_book(f.asks, base_amount=qty, side=Side.BUY)
        entry = (fb.vwap / sa.vwap - 1.0) * 100.0 if sa.vwap and fb.vwap and not (sa.exhausted or fb.exhausted) else None
        exit_ = (fa.vwap / sb.vwap - 1.0) * 100.0 if sb.vwap and fa.vwap and not (sb.exhausted or fa.exhausted) else None
        return entry, exit_

    def _fees_pct(self, p: _Pair) -> float:
        return 2.0 * (p.taker["spot"] + p.taker["perp"]) * 100.0

    def _expected_funding_pct(self, p: _Pair) -> float | None:
        if p.funding_rate is None:
            return None
        periods = self._s("basis_max_hold_s", 3600.0) / 3600.0 / max(p.funding_interval_h, 1e-9)
        return p.funding_rate * 100.0 * periods  # the short perp receives a positive rate

    def _references(self, p: _Pair, now: float) -> tuple[float | None, float | None]:
        window = self._s("basis_ref_window_s", 300.0)
        pts = [h for h in p.history if now - h[1] <= window]
        if not pts or now - pts[0][1] < window * 0.5:
            return None, None  # warming up: no reference yet, no signal
        return (sum(h[2] for h in pts) / len(pts), sum(h[3] for h in pts) / len(pts))

    def _evaluate(self, p: _Pair, now: float) -> None:
        self.counters["evaluations"] += 1
        ok, why = self._fresh(p, now)
        if not ok:
            key = "skew_evals" if why.startswith("timestamps") else "stale_evals"
            if p.quote_ok and not why.startswith("waiting"):
                self.counters[key] += 1
            p.status = {"ok": False, "reason": why}
            return
        s, f = p.books["spot"], p.books["perp"]
        qty = self._target_qty(p)
        entry, exit_ = self._exec_basis(p, qty)
        basis_abs = f.mid - s.mid
        basis_pct = (f.mid / s.mid - 1.0) * 100.0
        fees_pct = self._fees_pct(p)
        funding_pct = self._expected_funding_pct(p)
        target = None if entry is None else min(self._s("basis_exit_basis_pct", 0.02),
                                                 entry - self._s("basis_min_convergence_pct", 0.05))
        potential = None if entry is None else entry - target - fees_pct + (funding_pct or 0.0)
        ref_spot, ref_fut = self._references(p, now)
        up = ref_fut * (1.0 + self._s("basis_fut_upper_pct", 0.15) / 100.0) if ref_fut else None
        low = ref_spot * (1.0 - self._s("basis_spot_lower_pct", 0.15) / 100.0) if ref_spot else None
        cond = {
            "fut_upper": bool(up and f.best_bid >= up),
            "spot_lower": bool(low and s.best_ask <= low),
        }
        cond["both"] = cond["fut_upper"] and cond["spot_lower"]
        p.status = {
            "ok": True, "reason": "", "spot_mid": s.mid, "perp_mid": f.mid, "basis_abs": basis_abs,
            "basis_pct": basis_pct, "entry_basis_pct": entry, "exit_basis_pct": exit_, "fees_pct": fees_pct,
            "funding_rate": p.funding_rate, "funding_interval_h": p.funding_interval_h,
            "funding_expected_pct": funding_pct, "potential_net_pct": potential, "qty": qty,
            "spot_age_ms": s.age_ms(now), "perp_age_ms": f.age_ms(now),
            "skew_ms": abs(s.ts.recv_mono - f.ts.recv_mono) * 1000.0,
            "fut_upper_bound": up, "spot_lower_bound": low, "conditions": dict(cond),
        }
        if now - p.last_sample_mono >= _SAMPLE_S:
            p.last_sample_mono = now
            p.history.append((self._clock.wall(), now, s.mid, f.mid, basis_pct, entry, exit_))
        fired = [c for c in CONDITIONS if cond[c] and not p.cond[c]]
        p.cond = cond
        for c in fired:
            self.counters[f"signals_{c}"] += 1
        self._mark_counters(now)
        if not fired:
            return
        trigger = self._trigger(fired)
        if trigger is None:
            return
        if any(t.venue == p.venue and t.asset == p.asset and t.status in _ACTIVE for t in self._trades):
            self.counters["signals_while_open"] += 1
            return
        if qty <= 0:
            return
        self._open(p, trigger, qty, entry, now)

    def _trigger(self, fired: list[str]) -> str | None:
        mode = self.mode
        if mode == "any":
            return next((c for c in ("fut_upper", "spot_lower") if c in fired), None)
        return mode if mode in fired else None

    # ------------------------------------------------------------------ simulator
    def _open(self, p: _Pair, trigger: str, qty: float, entry: float | None, now: float) -> None:
        lat = self._s("basis_latency_ms", 300.0) / 1000.0
        t = BasisTrade(id=f"basis-{p.venue}-{p.asset}-{int(self._clock.wall() * 1000)}-{next(self._ids)}",
                       venue=p.venue, asset=p.asset, trigger=trigger, signal_ts=self._clock.wall(), signal_mono=now,
                       target_qty=qty, signal_basis_pct=entry if entry is not None else float("nan"))
        t.due = {leg: now + lat for leg in LEGS}
        t.done = {leg: False for leg in LEGS}
        self._trades.append(t)
        del self._trades[:-_TRADES_KEEP]
        self.counters["entries"] += 1
        self._save(t)

    def tick(self, now: float | None = None) -> None:
        """Due fills, exits, funding and adverse moves (called by the runner a few times a second)."""
        now = self._clock.mono() if now is None else now
        for t in list(self._trades):
            if t.status not in _ACTIVE:
                continue
            p = self._pairs.get((t.venue, t.asset))
            if p is None:
                continue
            if t.status in ("open", "closing"):
                self._funding(t, p)
                self._adverse(t, p, now)
            if t.due:
                self._run_due(t, p, now)
            elif t.status == "open":
                self._maybe_exit(t, p, now)
        self._flush_counters(now)

    def _leg_book(self, p: _Pair, leg: str, now: float) -> OrderBook | None:
        b = p.books.get(leg)
        if b is None or b.is_broken or not b.bids or not b.asks:
            return None
        if b.age_ms(now) > self._s("basis_max_age_ms", 2000.0):
            return None
        return b

    def _fill(self, p: _Pair, leg: str, side: Side, qty: float, now: float) -> tuple[float, float]:
        """(filled base, vwap) of a market order on the leg's current book within the slippage cap."""
        b = self._leg_book(p, leg, now)
        if b is None or qty <= _EPS:
            return 0.0, 0.0
        levels = b.asks if side == Side.BUY else b.bids
        best = levels[0].price
        slip = self._s("basis_max_slippage_pct", 0.3) / 100.0
        limit = best * (1.0 + slip) if side == Side.BUY else best * (1.0 - slip)
        w = walk_book(levels, base_amount=qty, limit_price=limit, side=side)
        if not w.vwap or w.filled_base <= _EPS:
            return 0.0, 0.0
        return w.filled_base, w.vwap

    def _book_cash(self, t: BasisTrade, p: _Pair, leg: str, side: Side, qty: float, px: float, kind: str) -> None:
        notional = qty * px
        t.cash_quote += notional if side == Side.SELL else -notional
        t.fees_quote += notional * p.taker[leg]
        t.fills[leg].append({"kind": kind, "side": 1.0 if side == Side.BUY else -1.0, "qty": qty, "px": px,
                             "ts": self._clock.wall()})

    def _run_due(self, t: BasisTrade, p: _Pair, now: float) -> None:
        for leg in LEGS:
            if t.done.get(leg) or now < t.due.get(leg, float("inf")):
                continue
            t.done[leg] = True
            if t.phase == "entry":
                side = Side.BUY if leg == "spot" else Side.SELL
                q, px = self._fill(p, leg, side, t.target_qty, now)
                if q > 0:
                    self._book_cash(t, p, leg, side, q, px, "entry")
                    if leg == "spot":
                        t.spot_qty, t.entry_spot_vwap = q, px
                    else:
                        t.perp_qty, t.entry_perp_vwap = q, px
            else:
                side = Side.SELL if leg == "spot" else Side.BUY
                left = t.spot_qty if leg == "spot" else t.perp_qty
                q, px = self._fill(p, leg, side, left, now)
                if q > 0:
                    self._book_cash(t, p, leg, side, q, px, "exit")
                    if leg == "spot":
                        t.spot_qty = max(0.0, t.spot_qty - q)
                        t.exit_spot_vwap = _kind_vwap(t.fills["spot"], "exit")
                    else:
                        t.perp_qty = max(0.0, t.perp_qty - q)
                        t.exit_perp_vwap = _kind_vwap(t.fills["perp"], "exit")
        if not all(t.done.get(leg) for leg in LEGS):
            return
        t.due, t.done = {}, {}
        if t.phase == "entry":
            self._finish_entry(t, p, now)
        else:
            self._finish_exit_attempt(t, p, now)
        self._save(t)

    def _finish_entry(self, t: BasisTrade, p: _Pair, now: float) -> None:
        s, f = t.spot_qty, t.perp_qty
        hedged = min(s, f)
        if hedged <= _EPS:
            t.status = "one_leg" if (s > _EPS or f > _EPS) else "no_fill"
            t.incident = ("only the spot leg filled" if s > _EPS else "only the perp leg filled") if t.status == "one_leg" \
                else "no leg filled (no fresh book or no depth within the slippage cap)"
            self._unwind(t, p, now)
            self._close(t, "entry incident: " + t.incident)
            return
        notes = []
        if s - hedged > _EPS or f - hedged > _EPS:
            leg = "spot" if s > f else "perp"
            notes.append(f"{leg} filled {max(s, f) - hedged:.8g} more than the other leg: excess unwound")
        if hedged < t.target_qty * (1.0 - 1e-9):
            notes.append(f"hedged {hedged:.8g} of {t.target_qty:.8g} (book depth within the slippage cap)")
        if notes:
            t.partial = True
            t.incident = "; ".join(notes)
        excess_spot, excess_perp = s - hedged, f - hedged
        t.spot_qty = t.perp_qty = hedged
        if excess_spot > _EPS:
            self._unwind_leg(t, p, "spot", excess_spot, now)
        if excess_perp > _EPS:
            self._unwind_leg(t, p, "perp", excess_perp, now)
        t.entry_basis_pct = (t.entry_perp_vwap / t.entry_spot_vwap - 1.0) * 100.0
        t.status = "open"
        t.opened_ts = self._clock.wall()

    def _unwind(self, t: BasisTrade, p: _Pair, now: float) -> None:
        if t.spot_qty > _EPS:
            self._unwind_leg(t, p, "spot", t.spot_qty, now)
        if t.perp_qty > _EPS:
            self._unwind_leg(t, p, "perp", t.perp_qty, now)
        t.spot_qty = t.perp_qty = 0.0

    def _unwind_leg(self, t: BasisTrade, p: _Pair, leg: str, qty: float, now: float) -> None:
        """Close an unhedged excess at once on the leg's current book (the whole side, no cap)."""
        side = Side.SELL if leg == "spot" else Side.BUY
        b = self._leg_book(p, leg, now)
        if b is None:
            t.unwind_unknown = True  # no fresh book: the loss of this excess is not known
            entry_px = t.entry_spot_vwap if leg == "spot" else t.entry_perp_vwap
            self._book_cash(t, p, leg, side, qty, entry_px, "unwind")
            return
        levels = b.bids if side == Side.SELL else b.asks
        w = walk_book(levels, base_amount=qty, side=side)
        px = w.vwap or levels[0].price
        self._book_cash(t, p, leg, side, qty, px, "unwind")

    def _maybe_exit(self, t: BasisTrade, p: _Pair, now: float) -> None:
        b_s, b_f = self._leg_book(p, "spot", now), self._leg_book(p, "perp", now)
        held = now - t.signal_mono
        reason = ""
        if held >= self._s("basis_max_hold_s", 3600.0):
            reason = "max hold"
        elif b_s is not None and b_f is not None and t.entry_basis_pct is not None:
            exit_best = (b_f.best_ask / b_s.best_bid - 1.0) * 100.0
            # converged: down to the target level, and in any case at least min_convergence below the entry
            # (a pair entered below the target would otherwise "converge" at once and only pay fees)
            target = min(self._s("basis_exit_basis_pct", 0.02),
                         t.entry_basis_pct - self._s("basis_min_convergence_pct", 0.05))
            if exit_best <= target:
                reason = f"basis converged ({exit_best:+.3f}% <= {target:+.3f}%)"
        if reason:
            self._schedule_exit(t, now, reason)

    def _schedule_exit(self, t: BasisTrade, now: float, reason: str) -> None:
        lat = self._s("basis_latency_ms", 300.0) / 1000.0
        t.phase = "exit"
        t.status = "closing"
        t.exit_attempts += 1
        if not t.exit_reason:
            t.exit_reason = reason
        t.due = {leg: now + lat for leg in LEGS if (t.spot_qty if leg == "spot" else t.perp_qty) > _EPS}
        t.done = {leg: leg not in t.due for leg in LEGS}

    def _finish_exit_attempt(self, t: BasisTrade, p: _Pair, now: float) -> None:
        if t.spot_qty <= _EPS and t.perp_qty <= _EPS:
            if t.exit_spot_vwap and t.exit_perp_vwap:
                t.exit_basis_pct = (t.exit_perp_vwap / t.exit_spot_vwap - 1.0) * 100.0
            self._close(t, t.exit_reason)
            return
        t.partial = True
        note = f"exit attempt {t.exit_attempts} left spot {t.spot_qty:.8g}, perp {t.perp_qty:.8g} open"
        t.incident = (t.incident + "; " if t.incident else "") + note
        self._schedule_exit(t, now, t.exit_reason)

    def _close(self, t: BasisTrade, reason: str) -> None:
        t.closed_ts = self._clock.wall()
        t.exit_reason = reason
        t.gross_quote = t.cash_quote
        t.net_quote = t.cash_quote - t.fees_quote + t.funding_quote
        base = t.target_qty * (t.entry_spot_vwap or 0.0)
        t.net_pct = t.net_quote / base * 100.0 if base > 0 else None
        if t.status in _ACTIVE:
            t.status = "closed"
        t.due, t.done = {}, {}

    def _funding(self, t: BasisTrade, p: _Pair) -> None:
        if t.perp_qty <= _EPS:
            return
        if p.funding_rate is None:
            t.funding_unknown = True
            return
        ts = p.funding_next_ts
        if ts is None or ts in t.funding_applied or t.opened_ts is None:
            return
        if t.opened_ts < ts <= self._clock.wall():
            f = p.books.get("perp")
            px = f.mid if f is not None and f.mid else t.entry_perp_vwap
            t.funding_quote += p.funding_rate * t.perp_qty * px  # the short receives a positive rate
            t.funding_applied.append(ts)
            p.funding_next_ts = ts + p.funding_interval_h * 3600.0  # until the next poll says otherwise
            self._save(t)

    def _adverse(self, t: BasisTrade, p: _Pair, now: float) -> None:
        if t.entry_basis_pct is None:
            return
        s, f = self._leg_book(p, "spot", now), self._leg_book(p, "perp", now)
        if s is None or f is None:
            return
        exit_best = (f.best_ask / s.best_bid - 1.0) * 100.0
        t.max_adverse_pct = max(t.max_adverse_pct, exit_best - t.entry_basis_pct)

    # ------------------------------------------------------------------ persistence
    def _save(self, t: BasisTrade) -> None:
        fn = getattr(self._db, "save_basis_trade", None) if self._db is not None else None
        if fn is None:
            return
        try:
            fn(t.to_dict())
        except Exception:  # noqa: BLE001
            log.exception("basis trade %s not saved", t.id)

    def _mark_counters(self, now: float) -> None:
        if self._counters_dirty_mono is None:
            self._counters_dirty_mono = now

    def _flush_counters(self, now: float, force: bool = False) -> None:
        if self._counters_dirty_mono is None or (not force and now - self._counters_dirty_mono < _COUNTERS_SAVE_S):
            return
        self._counters_dirty_mono = None
        kv_set = getattr(self._db, "kv_set", None) if self._db is not None else None
        if kv_set is not None:
            try:
                kv_set(_COUNTERS_KEY, dict(self.counters))
            except Exception:  # noqa: BLE001
                log.exception("basis counters not saved")

    def flush(self) -> None:
        self._flush_counters(self._clock.mono(), force=True)

    def _load(self) -> None:
        if self._db is None:
            return
        kv_get = getattr(self._db, "kv_get", None)
        if kv_get is not None:
            try:
                saved = kv_get(_COUNTERS_KEY) or {}
                for k, v in dict(saved).items():
                    if k in self.counters:
                        self.counters[k] = int(v)
            except Exception:  # noqa: BLE001
                log.exception("basis counters not loaded")
        fn = getattr(self._db, "list_basis_trades", None)
        if fn is None:
            return
        try:
            rows = fn(_TRADES_KEEP)
        except Exception:  # noqa: BLE001
            log.exception("basis trades not loaded")
            return
        known = {f for f in BasisTrade.__dataclass_fields__}
        for r in reversed(rows):  # oldest first
            try:
                t = BasisTrade(**{k: v for k, v in r.items() if k in known})
            except TypeError:
                continue
            if t.status in _ACTIVE:
                # a simulated position of a previous run cannot be resumed: outcome unknown, never guessed
                t.status = "abandoned"
                t.closed_ts = self._clock.wall()
                t.exit_reason = "process restarted: outcome unknown"
                t.due, t.done = {}, {}
                self._save(t)
            self._trades.append(t)

    # ------------------------------------------------------------------ queries
    def trades(self, limit: int = 200) -> list[dict[str, Any]]:
        return [t.to_dict() for t in reversed(self._trades[-max(0, int(limit)):])]

    def pairs(self) -> list[dict[str, Any]]:
        out = []
        for p in sorted(self._pairs.values(), key=lambda x: (x.venue, x.asset)):
            out.append({"venue": p.venue, "asset": p.asset, "spot_symbol": p.spot_symbol,
                        "perp_symbol": p.perp_symbol, "quote_ok": p.quote_ok, "note": p.note,
                        "taker": dict(p.taker), **p.status})
        return out

    def history(self, venue: str, asset: str, limit: int = 600) -> list[dict[str, Any]]:
        p = self._pairs.get((venue, asset.upper()))
        if p is None:
            return []
        rows = list(p.history)[-max(1, int(limit)):]
        return [{"ts": h[0], "spot_mid": h[2], "perp_mid": h[3], "basis_pct": h[4], "entry_basis_pct": h[5],
                 "exit_basis_pct": h[6]} for h in rows]

    def stats(self) -> dict[str, Any]:
        trades = self._trades
        hedged = [t for t in trades if t.entry_basis_pct is not None and t.status != "abandoned"]
        closed = [t for t in hedged if t.status == "closed"]
        incidents = [t for t in trades if t.status in ("one_leg", "no_fill")]
        incomplete = [t for t in trades if t.status in ("one_leg", "no_fill") or t.partial]
        done = closed + incidents  # every finished simulation carries its fees / cash

        def avg(xs: Iterable[float | None]) -> float | None:
            v = [x for x in xs if x is not None]
            return sum(v) / len(v) if v else None

        c = self.counters
        return {
            "signals_total": c["signals_fut_upper"] + c["signals_spot_lower"] + c["signals_both"],
            "signals_fut_upper": c["signals_fut_upper"], "signals_spot_lower": c["signals_spot_lower"],
            "signals_both": c["signals_both"], "signals_while_open": c["signals_while_open"],
            "entries_attempted": c["entries"], "pairs_executed": len(hedged), "pairs_closed": len(closed),
            "pairs_open": sum(1 for t in hedged if t.status in ("open", "closing")),
            "incomplete": len(incomplete), "one_leg": sum(1 for t in incidents if t.status == "one_leg"),
            "no_fill": sum(1 for t in incidents if t.status == "no_fill"),
            "abandoned": sum(1 for t in trades if t.status == "abandoned"),
            "avg_entry_basis_pct": avg(t.entry_basis_pct for t in hedged),
            "avg_exit_basis_pct": avg(t.exit_basis_pct for t in closed),
            "gross_pnl_quote": sum(t.gross_quote or 0.0 for t in done),
            "fees_quote": sum(t.fees_quote for t in done),
            "funding_quote": sum(t.funding_quote for t in done),
            "net_pnl_quote": sum(t.net_quote or 0.0 for t in done),
            "incident_pnl_quote": sum(t.net_quote or 0.0 for t in incidents),
            "funding_unknown": sum(1 for t in hedged if t.funding_unknown),
            "max_adverse_basis_pct": max((t.max_adverse_pct for t in hedged), default=None),
            "avg_max_adverse_basis_pct": avg(t.max_adverse_pct for t in closed),
            "stale_evals": c["stale_evals"], "skew_evals": c["skew_evals"], "evaluations": c["evaluations"],
            "mode": self.mode,
        }


def _quote(symbol: str) -> str:
    """Quote currency of a ccxt symbol ("BTC/USDT" and "BTC/USDT:USDT" -> "USDT")."""
    if "/" not in symbol:
        return ""
    return symbol.split("/", 1)[1].split(":", 1)[0].upper()


def _kind_vwap(fills: list[dict[str, Any]], kind: str) -> float:
    qty = sum(f["qty"] for f in fills if f.get("kind") == kind)
    return sum(f["qty"] * f["px"] for f in fills if f.get("kind") == kind) / qty if qty > 0 else 0.0

__all__ = ["BasisScanner", "BasisTrade", "CONDITIONS", "MODES"]
