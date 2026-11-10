"""Cross-venue funding arbitrage (virtual, research only - never sends an order).

Long the perpetual of a coin on the venue whose funding is LOW and short the same coin's perpetual on the
venue whose funding is HIGH.  The price risk cancels (long and short of the same coin); what is left is the
funding difference: the short receives the high rate, the long pays the low one (or receives a negative
one).  Data comes from the basis scanner: the perp books (sizes in base units), taker fees, the polled
funding rates and the settled funding history per (venue, asset).

* Rate per hour = rate / funding interval (venues settle every 1, 4 or 8 h).  Conservative estimate per
  leg: the short counts the LOWER of its current rate and its settled mean, the long the HIGHER.
* Edge % = (short rate/h - long rate/h) x ``xfund_horizon_h`` x 100 - 4 taker fees (entry and exit of both
  legs) - the cost of crossing both books now (exit spread - entry spread on the notional) - the price gap
  when the long venue is dearer than the short one (lost if the venues converge).
* Entry: the difference positive on the last ``carry_confirm_polls`` polls and edge >= ``xfund_min_edge_pct``;
  ``xfund_review`` (after every funding poll) approves the best pairs, highest edge first, within
  ``xfund_max_positions``, one per coin; an approved pair enters on its next fresh books (both legs within
  ``basis_max_age_ms`` and ``basis_max_skew_ms``), revalidated there.  Each leg fills ``basis_latency_ms``
  later on the book of that moment within ``basis_max_slippage_pct``; a one-leg fill is an incident, the
  unhedged excess is unwound at once.
* Funding: each leg at each of its venue's settlements: short + rate x qty x price, long - rate x qty x price.
* Exit: the difference turned (below 0 on the confirmation polls) AND holding ``xfund_exit_horizon_h`` at it
  costs more than leaving (2 fees + crossing); or ``xfund_max_hold_h``; or a leg within
  ``xfund_liq_guard_pct`` of its liquidation price (margin = notional / ``xfund_leverage`` per leg).
* Capital = the margin on both venues; APR on capital.
* ``xfund_exec = "maker"``: the leg whose venue saves the most with its maker fee rests as a limit order at
  its own side and fills only when the book trades through its price; the other leg then hedges by market.
  An entry not filled in ``maker_wait_s`` is cancelled (no trade); an exit then leaves by market.
"""
from __future__ import annotations

import itertools
import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from ..engine.economics import walk_book
from ..models import Side
from .scanner import own_side_price, quote_of, resting_filled

log = logging.getLogger(__name__)

LEGS = ("long", "short")
_ACTIVE = ("pending", "open", "closing")
_KEEP = 1000
_EPS = 1e-12
_COUNTERS_KEY = "xfund_counters"


@dataclass
class XFundTrade:
    id: str
    asset: str
    long_venue: str
    short_venue: str
    signal_ts: float
    target_qty: float
    edge_pct: float  # edge at the entry decision
    rate_long: float  # rate per hour at entry (fraction)
    rate_short: float
    status: str = "pending"  # pending | open | closing | closed | one_leg | no_fill | abandoned
    opened_ts: float | None = None
    closed_ts: float | None = None
    qty: dict[str, float] = field(default_factory=lambda: {"long": 0.0, "short": 0.0})
    entry_px: dict[str, float] = field(default_factory=lambda: {"long": 0.0, "short": 0.0})
    exit_px: dict[str, float] = field(default_factory=lambda: {"long": 0.0, "short": 0.0})
    cash_quote: float = 0.0
    fees_quote: float = 0.0
    funding_quote: float = 0.0
    funding_legs: dict[str, float] = field(default_factory=lambda: {"long": 0.0, "short": 0.0})
    funding_applied: dict[str, list[float]] = field(default_factory=lambda: {"long": [], "short": []})
    funding_unknown: bool = False
    capital_quote: float = 0.0
    liq_price: dict[str, float] = field(default_factory=dict)
    liq_buffer_pct: float | None = None
    liquidated: bool = False
    partial: bool = False
    incident: str = ""
    exit_reason: str = ""
    exit_attempts: int = 0
    net_quote: float | None = None
    net_pct: float | None = None
    phase: str = "entry"
    due: dict[str, float] = field(default_factory=dict)
    done: dict[str, bool] = field(default_factory=dict)
    order: dict[str, Any] = field(default_factory=dict)  # maker mode: {leg, side, px, qty, posted_mono}
    maker_fills: int = 0

    @property
    def hedged(self) -> bool:
        return self.status in ("open", "closing", "closed") and self.opened_ts is not None

    def venue(self, leg: str) -> str:
        return self.long_venue if leg == "long" else self.short_venue

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["hedged"] = self.hedged
        d["due"] = {k: (None if v == float("-inf") else v) for k, v in self.due.items()}
        return d


class FundingArb:
    """Reads the scanner's perp books, fees and funding; simulates cross-venue funding pairs."""

    def __init__(self, scanner: Any, settings: Any, clock: Any, db: Any | None = None) -> None:
        self.sc = scanner
        self._s = settings
        self._clock = clock
        self._db = db
        self._trades: list[XFundTrade] = []
        self._ids = itertools.count(1)
        self._approved: dict[str, tuple[str, str]] = {}  # asset -> (long venue, short venue)
        self._next_ts: dict[tuple[str, str], float] = {}  # (venue, asset) -> next settlement seen
        self.candidates: list[dict[str, Any]] = []
        self.counters = {"entries": 0, "approved": 0, "revalidation_failed": 0, "maker_fills": 0,
                         "taker_hedges": 0, "maker_unfilled": 0}
        self._load()

    # ------------------------------------------------------------------ settings
    def _f(self, name: str, default: float) -> float:
        try:
            return float(getattr(self._s, name, default))
        except (TypeError, ValueError):
            return default

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._s, "xfund_enabled", False))

    # ------------------------------------------------------------------ rates
    def _rate_h(self, p: Any, leg: str) -> float | None:
        """Conservative rate per hour of one leg (short: the lower estimate, long: the higher)."""
        cur = p.funding_rate
        if cur is None:
            return None
        hist = list(getattr(p, "funding_history", []) or [])
        if len(hist) >= 3:
            mean = sum(hist) / len(hist)
            est = min(cur, mean) if leg == "short" else max(cur, mean)
        else:
            est = cur
        return est / max(float(p.funding_interval_h or 8.0), 1e-9)

    def _diff_confirmed(self, pl: Any, ps: Any) -> bool:
        n = max(1, int(self._f("carry_confirm_polls", 2)))
        a, b = list(pl.funding_samples)[-n:], list(ps.funding_samples)[-n:]
        if len(a) < n or len(b) < n or any(x is None for x in a + b):
            return False
        il, is_ = max(pl.funding_interval_h, 1e-9), max(ps.funding_interval_h, 1e-9)
        return all(s / is_ - lo / il > 0 for lo, s in zip(a, b))

    def _perps(self, asset: str) -> dict[str, Any]:
        return {v: p for (v, a), p in self.sc._pairs.items() if a == asset and p.quote_ok and p.funding_rate is not None}

    def _maker_leg(self, pl: Any, ps: Any) -> str | None:
        """Maker mode: the leg (venue) whose maker fee saves the most rests as a limit order; None: taker."""
        if str(getattr(self._s, "xfund_exec", "taker")) != "maker":
            return None
        sl, ss = pl.taker["perp"] - pl.maker_fee("perp"), ps.taker["perp"] - ps.maker_fee("perp")
        leg, best = ("long", sl) if sl >= ss else ("short", ss)
        return leg if best > 0 else None

    # ------------------------------------------------------------------ edge on the books
    def _exec(self, pl: Any, ps: Any, qty: float, now: float) -> tuple[float, float] | None:
        """(entry spread %, exit spread %) for qty: buy the long venue's asks / sell the short venue's bids,
        and back.  None when a book is stale, too thin or the two are too far apart in time."""
        bl, bs = self.sc._leg_book(pl, "perp", now), self.sc._leg_book(ps, "perp", now)
        if bl is None or bs is None:
            return None
        if abs(bl.ts.recv_mono - bs.ts.recv_mono) * 1000.0 > self._f("basis_max_skew_ms", 500.0):
            return None
        la = walk_book(bl.asks, base_amount=qty, side=Side.BUY)
        sb = walk_book(bs.bids, base_amount=qty, side=Side.SELL)
        lb = walk_book(bl.bids, base_amount=qty, side=Side.SELL)
        sa = walk_book(bs.asks, base_amount=qty, side=Side.BUY)
        if any(w.exhausted or not w.vwap for w in (la, sb, lb, sa)):
            return None
        # crossing = both spreads (exit spread - entry spread); plus the price gap when the LONG venue is the
        # dearer one: if the two venues converge, that gap is lost (a gap the other way is never counted)
        gap = max(0.0, (bl.mid / bs.mid - 1.0) * 100.0) if bl.mid and bs.mid else 0.0
        return (sb.vwap / la.vwap - 1.0) * 100.0, (sa.vwap / lb.vwap - 1.0) * 100.0 + gap

    def edge(self, pl: Any, ps: Any, now: float) -> dict[str, Any] | None:
        rl, rs = self._rate_h(pl, "long"), self._rate_h(ps, "short")
        if rl is None or rs is None:
            return None
        b = self.sc._leg_book(pl, "perp", now) or pl.books.get("perp")
        mid = b.mid if b is not None else None
        if not mid:
            return None
        qty = self._f("xfund_notional_quote", 1000.0) / mid
        funding_pct = (rs - rl) * 100.0 * self._f("xfund_horizon_h", 72.0)
        fees_pct = 2.0 * (pl.taker["perp"] + ps.taker["perp"]) * 100.0
        ex = self._exec(pl, ps, qty, now)
        cross = None if ex is None else ex[1] - ex[0]
        mleg = self._maker_leg(pl, ps)
        if mleg is not None:
            # the maker leg rests at its own side (maker fee, no crossing); the other hedges by market:
            # taker fee + its spread for the size; the price gap against the pair still counts
            pm, po = (pl, ps) if mleg == "long" else (ps, pl)
            fees_pct = 2.0 * (pm.maker_fee("perp") + po.taker["perp"]) * 100.0
            if ex is not None:
                bo = self.sc._leg_book(po, "perp", now)
                bl, bs = self.sc._leg_book(pl, "perp", now), self.sc._leg_book(ps, "perp", now)
                a = walk_book(bo.asks, base_amount=qty, side=Side.BUY)
                b = walk_book(bo.bids, base_amount=qty, side=Side.SELL)
                cross = (a.vwap / b.vwap - 1.0) * 100.0 + max(0.0, (bl.mid / bs.mid - 1.0) * 100.0)
        return {"maker_leg": mleg,"rate_long_h": rl, "rate_short_h": rs, "funding_pct": funding_pct, "fees_pct": fees_pct,
                "cross_pct": cross, "qty": qty,
                "edge_pct": None if cross is None else funding_pct - fees_pct - cross,
                "edge_est_pct": funding_pct - fees_pct}  # before crossing: ranking only

    # ------------------------------------------------------------------ review (after every poll)
    def review(self, now: float | None = None) -> int:
        if not self.enabled:
            return 0
        now = self._clock.mono() if now is None else now
        self._review_exits(now)
        assets = sorted({a for (_, a) in self.sc._pairs})
        cands = []
        for asset in assets:
            perps = self._perps(asset)
            if len(perps) < 2:
                continue
            rated = [(v, self._rate_h(p, "long"), self._rate_h(p, "short")) for v, p in perps.items()]
            lo = min(rated, key=lambda r: r[1])
            hi = max(rated, key=lambda r: r[2])
            if lo[0] == hi[0]:
                continue
            pl, ps = perps[lo[0]], perps[hi[0]]
            e = self.edge(pl, ps, now)
            if e is None:
                continue
            ql, qs = quote_of(pl.perp_symbol), quote_of(ps.perp_symbol)
            cands.append({"asset": asset, "long": lo[0], "short": hi[0], "confirmed": self._diff_confirmed(pl, ps),
                          "quotes": f"{ql}/{qs}" if ql != qs else "",  # e.g. USDT/USDC: the pair carries that rate
                          "diff_day_pct": (e["rate_short_h"] - e["rate_long_h"]) * 100.0 * 24.0, **e})
        cands.sort(key=lambda c: c["edge_pct"] if c["edge_pct"] is not None else c["edge_est_pct"], reverse=True)
        self.candidates = cands[:30]
        self._approved = {}
        busy = {t.asset for t in self._trades if t.status in _ACTIVE}
        room = max(0, int(self._f("xfund_max_positions", 5)) - len(busy))
        min_edge = self._f("xfund_min_edge_pct", 0.10)
        for c in cands:
            if room <= 0:
                break
            if c["asset"] in busy or not c["confirmed"] or c["edge_est_pct"] < min_edge:
                continue
            self._approved[c["asset"]] = (c["long"], c["short"])
            room -= 1
        self.counters["approved"] += len(self._approved)
        return len(self._approved)

    def _review_exits(self, now: float) -> None:
        for t in self._trades:
            if t.status != "open" or t.due:
                continue
            pl, ps = self.sc._pairs.get((t.long_venue, t.asset)), self.sc._pairs.get((t.short_venue, t.asset))
            if pl is None or ps is None:
                continue
            n = max(1, int(self._f("carry_confirm_polls", 2)))
            a, b = list(pl.funding_samples)[-n:], list(ps.funding_samples)[-n:]
            if len(a) < n or len(b) < n or any(x is None for x in a + b):
                continue
            il, is_ = max(pl.funding_interval_h, 1e-9), max(ps.funding_interval_h, 1e-9)
            diffs = [s / is_ - lo / il for lo, s in zip(a, b)]
            if not all(d < 0 for d in diffs):
                continue
            hold_cost = -sum(diffs) / len(diffs) * 100.0 * self._f("xfund_exit_horizon_h", 24.0)
            ex = self._exec(pl, ps, t.qty["long"], now)
            exit_cost = (pl.taker["perp"] + ps.taker["perp"]) * 100.0 + (max(0.0, (ex[1] - ex[0]) / 2) if ex else 0.0)
            if hold_cost > exit_cost:
                self._schedule_exit(t, now, f"funding difference turned: holding would cost {hold_cost:.3f}% "
                                            f"> leaving {exit_cost:.3f}%", maker=True)

    # ------------------------------------------------------------------ simulation (every scanner tick)
    def tick(self, now: float | None = None) -> None:
        if not self.enabled and not any(t.status in _ACTIVE for t in self._trades):
            return
        now = self._clock.mono() if now is None else now
        self._settlements()
        for asset, (lv, sv) in list(self._approved.items()):
            self._try_enter(asset, lv, sv, now)
        for t in list(self._trades):
            if t.status not in _ACTIVE:
                continue
            if t.order:
                if t.status == "closing":
                    self._risk(t, now)  # a leg near liquidation while the exit order rests: out by market
                if t.order:
                    self._check_resting(t, now)
            elif t.due:
                self._run_due(t, now)
            elif t.status == "open":
                self._risk(t, now)
                if t.status == "open" and not t.due and \
                        self._clock.wall() - t.signal_ts >= self._f("xfund_max_hold_h", 336.0) * 3600.0:
                    self._schedule_exit(t, now, "max hold", maker=True)

    def _try_enter(self, asset: str, lv: str, sv: str, now: float) -> None:
        pl, ps = self.sc._pairs.get((lv, asset)), self.sc._pairs.get((sv, asset))
        if pl is None or ps is None:
            self._approved.pop(asset, None)
            return
        e = self.edge(pl, ps, now)
        if e is None or e["edge_pct"] is None:
            return  # books not fresh / not synchronous yet: wait for the next tick
        self._approved.pop(asset, None)
        if e["edge_pct"] < self._f("xfund_min_edge_pct", 0.10):
            self.counters["revalidation_failed"] += 1  # the book of this moment says no (hard rule 4)
            return
        if any(t.asset == asset and t.status in _ACTIVE for t in self._trades):
            return
        lat = self._f("basis_latency_ms", 300.0) / 1000.0
        t = XFundTrade(id=f"xfund-{asset}-{lv}-{sv}-{int(self._clock.wall() * 1000)}-{next(self._ids)}",
                       asset=asset, long_venue=lv, short_venue=sv, signal_ts=self._clock.wall(),
                       target_qty=e["qty"], edge_pct=e["edge_pct"], rate_long=e["rate_long_h"], rate_short=e["rate_short_h"])
        if e.get("maker_leg") is None or not self._post(t, e["maker_leg"], t.target_qty, now):
            t.due = {leg: now + lat for leg in LEGS}
            t.done = {leg: False for leg in LEGS}
        self._trades.append(t)
        del self._trades[:-_KEEP]
        self.counters["entries"] += 1
        if not t.order:  # a resting entry order is stored once it fills (unfilled ones leave no row)
            self._save(t)
        self._save_counters()

    def _pair(self, t: XFundTrade, leg: str) -> Any:
        return self.sc._pairs.get((t.venue(leg), t.asset))

    def _book(self, t: XFundTrade, leg: str, side: Side, qty: float, px: float, kind: str,
              maker: bool = False) -> None:
        p = self._pair(t, leg)
        notional = qty * px
        t.cash_quote += notional if side == Side.SELL else -notional
        fee = 0.0005 if p is None else (p.maker_fee("perp") if maker else p.taker["perp"])
        t.fees_quote += notional * fee

    # ------------------------------------------------------------------ maker execution
    def _post(self, t: XFundTrade, leg: str, qty: float, now: float) -> bool:
        """Rest a limit order of ``leg`` at its own side (buy at the best bid, sell at the best ask)."""
        p = self._pair(t, leg)
        b = self.sc._leg_book(p, "perp", now) if p is not None else None
        if b is None or qty <= _EPS:
            return False
        buy = (leg == "long") == (t.phase == "entry")
        t.order = {"leg": leg, "side": 1.0 if buy else -1.0, "px": own_side_price(b, buy), "qty": qty,
                   "posted_mono": now}
        return True

    def _check_resting(self, t: XFundTrade, now: float) -> None:
        """Filled only when the book trades through the order's price; then the other leg hedges by market.
        Not filled in ``maker_wait_s``: an entry is cancelled (no trade), an exit leaves by market."""
        o = t.order
        leg = o["leg"]
        other = "short" if leg == "long" else "long"
        p = self._pair(t, leg)
        b = self.sc._leg_book(p, "perp", now) if p is not None else None
        if b is not None and resting_filled(b, o["side"] > 0, o["px"]):
            side = Side.BUY if o["side"] > 0 else Side.SELL
            self._book(t, leg, side, o["qty"], o["px"], t.phase, maker=True)
            if t.phase == "entry":
                t.qty[leg], t.entry_px[leg] = o["qty"], o["px"]
                need_other = True
            else:
                t.qty[leg], t.exit_px[leg] = max(0.0, t.qty[leg] - o["qty"]), o["px"]
                need_other = t.qty[other] > _EPS
            t.order = {}
            t.maker_fills += 1
            self.counters["maker_fills"] += 1
            if need_other:
                self.counters["taker_hedges"] += 1
                t.due = {other: now + self._f("basis_latency_ms", 300.0) / 1000.0}
                t.done = {leg: True, other: False}
            else:
                t.due, t.done = {}, {}
                self._finish_exit(t, now)
            self._save(t)
            self._save_counters()
            return
        wait = self._f("maker_wait_s", 60.0)
        if now - o["posted_mono"] < wait:
            return
        t.order = {}
        self.counters["maker_unfilled"] += 1
        self._save_counters()
        if t.phase == "entry":
            # no fill, no position, no cost: counted, not kept (retried at the next funding poll)
            self._trades = [x for x in self._trades if x is not t]
        else:
            self._schedule_exit(t, now, t.exit_reason)

    def _run_due(self, t: XFundTrade, now: float) -> None:
        for leg in LEGS:
            if t.done.get(leg) or now < t.due.get(leg, float("inf")):
                continue
            t.done[leg] = True
            p = self._pair(t, leg)
            if p is None:
                continue
            if t.phase == "entry":
                side = Side.BUY if leg == "long" else Side.SELL
                q, px = self.sc._fill(p, "perp", side, t.target_qty, now)
                if q > 0:
                    self._book(t, leg, side, q, px, "entry")
                    t.qty[leg], t.entry_px[leg] = q, px
            else:
                side = Side.SELL if leg == "long" else Side.BUY
                q, px = self.sc._fill(p, "perp", side, t.qty[leg], now)
                if q > 0:
                    self._book(t, leg, side, q, px, "exit")
                    t.qty[leg] = max(0.0, t.qty[leg] - q)
                    t.exit_px[leg] = px
        if not all(t.done.get(leg) for leg in LEGS):
            return
        t.due, t.done = {}, {}
        if t.phase == "entry":
            self._finish_entry(t, now)
        else:
            self._finish_exit(t, now)
        self._save(t)

    def _unwind(self, t: XFundTrade, leg: str, qty: float, now: float) -> None:
        p = self._pair(t, leg)
        side = Side.SELL if leg == "long" else Side.BUY
        q, px = self.sc._fill(p, "perp", side, qty, now) if p is not None else (0.0, 0.0)
        if q <= 0:
            px = t.entry_px[leg]  # no fresh book: the excess is booked flat and flagged
            t.incident = (t.incident + "; " if t.incident else "") + f"{leg} excess unwound without a fresh price"
        self._book(t, leg, side, qty, px, "unwind")

    def _finish_entry(self, t: XFundTrade, now: float) -> None:
        ql, qs = t.qty["long"], t.qty["short"]
        hedged = min(ql, qs)
        if hedged <= _EPS:
            t.status = "one_leg" if (ql > _EPS or qs > _EPS) else "no_fill"
            t.incident = ("only the long leg filled" if ql > _EPS else "only the short leg filled") \
                if t.status == "one_leg" else "no leg filled (no fresh book or no depth within the slippage cap)"
            for leg in LEGS:
                if t.qty[leg] > _EPS:
                    self._unwind(t, leg, t.qty[leg], now)
                    t.qty[leg] = 0.0
            self._close(t, "entry incident: " + t.incident)
            return
        for leg in LEGS:
            if t.qty[leg] - hedged > _EPS:
                t.partial = True
                self._unwind(t, leg, t.qty[leg] - hedged, now)
                t.qty[leg] = hedged
        lev = max(1.0, self._f("xfund_leverage", 2.0))
        mmr = self._f("carry_maint_margin", 0.005)
        t.capital_quote = hedged * (t.entry_px["long"] + t.entry_px["short"]) / lev
        t.liq_price = {"long": t.entry_px["long"] * (1.0 - 1.0 / lev + mmr),
                       "short": t.entry_px["short"] * (1.0 + 1.0 / lev - mmr)}
        t.status = "open"
        t.opened_ts = self._clock.wall()

    def _schedule_exit(self, t: XFundTrade, now: float, reason: str, maker: bool = False) -> None:
        """Both legs out by market; ``maker=True`` (funding exit, max hold) first rests the maker leg."""
        lat = self._f("basis_latency_ms", 300.0) / 1000.0
        t.phase, t.status = "exit", "closing"
        t.exit_attempts += 1
        t.order = {}
        if not t.exit_reason:
            t.exit_reason = reason
        pl, ps = self._pair(t, "long"), self._pair(t, "short")
        mleg = self._maker_leg(pl, ps) if maker and pl is not None and ps is not None else None
        if mleg is not None and all(t.qty[leg] > _EPS for leg in LEGS) and self._post(t, mleg, t.qty[mleg], now):
            t.due, t.done = {}, {}
            self._save(t)
            return
        t.due = {leg: now + lat for leg in LEGS if t.qty[leg] > _EPS}
        t.done = {leg: leg not in t.due for leg in LEGS}
        self._save(t)

    def _finish_exit(self, t: XFundTrade, now: float) -> None:
        if t.qty["long"] <= _EPS and t.qty["short"] <= _EPS:
            self._close(t, t.exit_reason)
            return
        t.partial = True
        self._schedule_exit(t, now, t.exit_reason)

    def _close(self, t: XFundTrade, reason: str) -> None:
        t.closed_ts = self._clock.wall()
        t.exit_reason = reason
        t.net_quote = t.cash_quote - t.fees_quote + t.funding_quote
        t.net_pct = t.net_quote / t.capital_quote * 100.0 if t.capital_quote > 0 else None
        if t.status in _ACTIVE:
            t.status = "closed"
        t.due, t.done = {}, {}

    def _risk(self, t: XFundTrade, now: float) -> None:
        worst = None
        for leg in LEGS:
            p = self._pair(t, leg)
            b = self.sc._leg_book(p, "perp", now) if p is not None else None
            liq = t.liq_price.get(leg)
            if b is None or not b.mid or not liq:
                continue
            buf = (b.mid / liq - 1.0) * 100.0 if leg == "long" else (liq / b.mid - 1.0) * 100.0
            worst = buf if worst is None else min(worst, buf)
            if buf <= 0:  # through the liquidation price: that leg's margin is gone, the other closes
                lev = max(1.0, self._f("xfund_leverage", 2.0))
                bankrupt = t.entry_px[leg] * ((1.0 - 1.0 / lev) if leg == "long" else (1.0 + 1.0 / lev))
                self._book(t, leg, Side.SELL if leg == "long" else Side.BUY, t.qty[leg], bankrupt, "liquidation")
                t.qty[leg] = 0.0
                t.liquidated = True
                t.incident = (t.incident + "; " if t.incident else "") + f"{leg} leg on {t.venue(leg)} liquidated"
                self._schedule_exit(t, now, "liquidated")
                return
        if worst is not None:
            t.liq_buffer_pct = worst if t.liq_buffer_pct is None else min(t.liq_buffer_pct, worst)
            if worst < self._f("xfund_liq_guard_pct", 5.0):
                self._schedule_exit(t, now, f"liquidation guard: {worst:.1f}% to a leg's liquidation price")

    def _settlements(self) -> None:
        """Funding at every settlement of each venue: the short receives rate x notional, the long pays."""
        wall = self._clock.wall()
        open_ = [t for t in self._trades if t.status in ("open", "closing") and t.opened_ts is not None]
        keys = {(t.venue(leg), t.asset) for t in open_ for leg in LEGS}
        for key in keys:
            p = self.sc._pairs.get(key)
            if p is None:
                continue
            seen = self._next_ts.get(key)
            nxt = p.funding_next_ts
            if seen is None:
                if nxt:
                    self._next_ts[key] = float(nxt)
                continue
            if seen > wall:
                if nxt and nxt < seen:
                    self._next_ts[key] = float(nxt)
                continue
            for t in open_:
                for leg in LEGS:
                    if (t.venue(leg), t.asset) != key or t.qty[leg] <= _EPS or seen in t.funding_applied[leg]:
                        continue
                    if not (t.opened_ts < seen <= wall):
                        continue
                    if p.funding_rate is None:
                        t.funding_unknown = True
                        continue
                    b = p.books.get("perp")
                    px = b.mid if b is not None and b.mid else t.entry_px[leg]
                    amount = p.funding_rate * t.qty[leg] * px
                    amount = amount if leg == "short" else -amount
                    t.funding_quote += amount
                    t.funding_legs[leg] += amount
                    t.funding_applied[leg].append(seen)
                    self._save(t)
            step = max(float(p.funding_interval_h or 8.0), 1e-9) * 3600.0
            self._next_ts[key] = float(nxt) if nxt and nxt > seen else seen + step

    # ------------------------------------------------------------------ persistence
    def _save(self, t: XFundTrade) -> None:
        fn = getattr(self._db, "save_xfund_trade", None) if self._db is not None else None
        if fn is None:
            return
        try:
            fn(t.to_dict())
        except Exception:  # noqa: BLE001
            log.exception("xfund trade %s not saved", t.id)

    def _save_counters(self) -> None:
        kv_set = getattr(self._db, "kv_set", None) if self._db is not None else None
        if kv_set is not None:
            try:
                kv_set(_COUNTERS_KEY, dict(self.counters))
            except Exception:  # noqa: BLE001
                log.exception("xfund counters not saved")

    def _load(self) -> None:
        if self._db is None:
            return
        kv_get = getattr(self._db, "kv_get", None)
        if kv_get is not None:
            try:
                for k, v in dict(kv_get(_COUNTERS_KEY) or {}).items():
                    if k in self.counters:
                        self.counters[k] = int(v)
            except Exception:  # noqa: BLE001
                log.exception("xfund counters not loaded")
        fn = getattr(self._db, "list_xfund_trades", None)
        if fn is None:
            return
        try:
            rows = fn(_KEEP)
        except Exception:  # noqa: BLE001
            log.exception("xfund trades not loaded")
            return
        known = set(XFundTrade.__dataclass_fields__)
        for r in reversed(rows):
            try:
                t = XFundTrade(**{k: v for k, v in r.items() if k in known})
            except TypeError:
                continue
            if t.status in ("open", "closing"):
                t.funding_unknown = True  # funding while the process was down is not counted
                t.order = {}  # a resting order of the previous run is gone
                if t.status == "closing":
                    t.due = {leg: float("-inf") for leg in LEGS if t.qty[leg] > _EPS}
                    t.done = {leg: leg not in t.due for leg in LEGS}
                else:
                    t.due, t.done = {}, {}
            elif t.status == "pending":
                t.status, t.closed_ts, t.exit_reason = "abandoned", self._clock.wall(), "process restarted: outcome unknown"
                t.due, t.done = {}, {}
            self._trades.append(t)
            self._save(t)

    # ------------------------------------------------------------------ queries
    def trades(self, limit: int = 100) -> list[dict[str, Any]]:
        return [t.to_dict() for t in reversed(self._trades[-max(0, int(limit)):])]

    def stats(self) -> dict[str, Any]:
        ts = self._trades
        live = [t for t in ts if t.status in ("open", "closing") and t.hedged]
        closed = [t for t in ts if t.status == "closed" and t.hedged]
        incidents = [t for t in ts if t.status in ("one_leg", "no_fill")]
        now = self._clock.wall()

        def apr(rows: list[XFundTrade], value) -> float | None:
            years = sum(t.capital_quote * max(0.0, (t.closed_ts or now) - (t.opened_ts or now)) for t in rows) / 31_536_000.0
            return sum(value(t) for t in rows) / years * 100.0 if years > 0 else None

        return {
            "enabled": self.enabled, "entries": self.counters["entries"], "open": len(live), "closed": len(closed),
            "incidents": len(incidents), "revalidation_failed": self.counters["revalidation_failed"],
            "funding_quote": sum(t.funding_quote for t in live + closed),
            "funding_open_quote": sum(t.funding_quote for t in live),
            "fees_quote": sum(t.fees_quote for t in closed + incidents),
            "net_quote": sum(t.net_quote or 0.0 for t in closed + incidents),
            "open_capital_quote": sum(t.capital_quote for t in live),
            "liquidations": sum(1 for t in ts if t.liquidated),
            "funding_unknown": sum(1 for t in live + closed if t.funding_unknown),
            "realized_apr_pct": apr(closed, lambda t: t.net_quote or 0.0),
            "funding_apr_open_pct": apr(live, lambda t: t.funding_quote),
            "leverage": self._f("xfund_leverage", 2.0),
            "liq_distance_pct": (1.0 / max(1.0, self._f("xfund_leverage", 2.0))
                                 - self._f("carry_maint_margin", 0.005)) * 100.0,
            "exec": str(getattr(self._s, "xfund_exec", "taker")), "maker_fills": self.counters["maker_fills"],
            "taker_hedges": self.counters["taker_hedges"], "maker_unfilled": self.counters["maker_unfilled"],
        }


__all__ = ["FundingArb", "XFundTrade"]
