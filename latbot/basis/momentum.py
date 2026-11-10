"""Momentum on BTC / ETH perpetuals (virtual, research only - never sends an order).

Owner (2026-11-09): "BTC and ETH rose ~1 % in 15 min - ride it".  This module measures on live books, with
fees, latency and slippage, whether riding such moves pays.  Fixed rules (settings ``mom_*``, never fitted
to past trades):

* Price series: the perp mid of each coin, sampled once a second on EVERY venue of ``mom_venues``, one
  series per venue (two venues' prices are never mixed into one move).  The signal comes from the first
  configured venue whose series is warm and fresh; a venue silent for ``mom_stale_s`` restarts only its
  own series.
* Entry LONG when the move over ``mom_window_s`` >= ``mom_entry_pct`` AND the move over ``mom_trend_s``
  >= ``mom_trend_pct`` (same direction); SHORT is the mirror.  Edge-triggered (one entry per move), one
  position per coin, ``mom_cooldown_s`` after an exit.  A series shorter than 80 % of a window gives no
  signal (warming up).
* Fill: market on the perp ``basis_latency_ms`` after the signal, on the book of that moment, size
  ``mom_notional_quote`` at 1x, walked within ``basis_max_slippage_pct`` (a partial fill keeps its size).
* Exit (market, on the exit side of the book): hard stop ``mom_stop_pct`` against the entry price; a trailing
  stop ``mom_trail_pct`` from the best price, armed once the position was ``mom_trail_pct`` in profit; or
  ``mom_max_hold_s``.  No fresh book: no stop
  check; ``mom_stale_s`` without one closes the position as "no data" (outcome unknown, not a result).
* NET = cash of the fills - 2 taker fees - funding paid (+ received) at a settlement while open.
  Positions open at shutdown are "abandoned" (outcome unknown).
"""
from __future__ import annotations

import itertools
import logging
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any

from ..models import Side

log = logging.getLogger(__name__)

_ACTIVE = ("pending", "open", "closing")
_KEEP = 1000
_EPS = 1e-12
_SAMPLE_S = 1.0


@dataclass
class MomTrade:
    id: str
    asset: str
    venue: str
    direction: int  # +1 long, -1 short
    signal_ts: float
    move_window_pct: float
    move_trend_pct: float
    target_qty: float
    status: str = "pending"  # pending | open | closing | closed | no_fill | no_data | abandoned
    opened_ts: float | None = None
    closed_ts: float | None = None
    qty: float = 0.0  # still open
    entry_qty: float = 0.0  # filled at the entry
    entry_px: float = 0.0
    exit_px: float = 0.0
    best_px: float = 0.0
    cash_quote: float = 0.0
    fees_quote: float = 0.0
    funding_quote: float = 0.0
    funding_applied: float | None = None
    partial: bool = False
    exit_reason: str = ""
    net_quote: float | None = None
    net_pct: float | None = None
    open_net_quote: float | None = None  # result if closed now (exit side of the book, exit fee included)
    open_net_ts: float | None = None
    resumed: bool = False  # the bot was restarted while this position was open (it was unwatched meanwhile)
    due_mono: float | None = None
    opened_mono: float | None = None
    last_fresh_mono: float | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["side"] = "long" if self.direction > 0 else "short"
        return d


class _Series:
    def __init__(self, maxlen: int) -> None:
        self.venue: str | None = None
        self.points: deque = deque(maxlen=maxlen)  # (mono, mid)
        self.last_fresh_mono: float | None = None
        self.armed = {1: False, -1: False}  # condition state per direction (edge trigger)
        self.cooldown_until = float("-inf")

    def move(self, now: float, window: float) -> float | None:
        if not self.points:
            return None
        start = now - window
        old = next((pt for pt in self.points if pt[0] >= start), None)
        if old is None or now - old[0] < 0.8 * window or old[1] <= 0:
            return None  # warming up
        return (self.points[-1][1] / old[1] - 1.0) * 100.0


class Momentum:
    def __init__(self, scanner: Any, settings: Any, clock: Any, db: Any | None = None) -> None:
        self.sc = scanner
        self._s = settings
        self._clock = clock
        self._db = db
        self._trades: list[MomTrade] = []
        self._ids = itertools.count(1)
        self._series: dict[tuple[str, str], _Series] = {}  # (asset, venue)
        self._cooldown: dict[str, float] = {}  # asset -> mono time until which no new entry
        self._load()

    def _f(self, name: str, default: float) -> float:
        try:
            return float(getattr(self._s, name, default))
        except (TypeError, ValueError):
            return default

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._s, "mom_enabled", False))

    def _assets(self) -> list[str]:
        return [str(a).upper() for a in (getattr(self._s, "mom_assets", None) or ["BTC", "ETH"])]

    def _venues(self) -> list[str]:
        return [str(v).lower() for v in (getattr(self._s, "mom_venues", None) or [])]

    # ------------------------------------------------------------------ data
    def _book(self, venue: str, asset: str, now: float) -> tuple[Any, Any] | tuple[None, None]:
        p = self.sc._pairs.get((venue, asset))
        if p is None:
            return None, None
        b = self.sc._leg_book(p, "perp", now)
        return (p, b) if b is not None and b.mid else (None, None)

    def _series_of(self, asset: str, venue: str) -> _Series:
        sr = self._series.get((asset, venue))
        if sr is None:
            n = int(max(self._f("mom_trend_s", 3600.0), self._f("mom_window_s", 900.0)) / _SAMPLE_S) + 120
            sr = self._series[(asset, venue)] = _Series(n)
            sr.venue = venue
        return sr

    def _sample(self, asset: str, now: float) -> None:
        """One series per venue, sampled in parallel and never mixed: a venue that goes silent keeps its
        own gap and does not cost the others their history."""
        stale_s = self._f("mom_stale_s", 60.0)
        for v in self._venues():
            _, b = self._book(v, asset, now)
            if b is None:
                sr = self._series.get((asset, v))
                if sr is not None and sr.points and sr.last_fresh_mono is not None \
                        and now - sr.last_fresh_mono > stale_s:
                    sr.points.clear()  # a gap this long: the series starts again when the venue is back
                    sr.armed = {1: False, -1: False}
                continue
            sr = self._series_of(asset, v)
            sr.last_fresh_mono = now
            if not sr.points or now - sr.points[-1][0] >= _SAMPLE_S:
                sr.points.append((now, b.mid))

    def _fresh_now(self, sr: _Series, now: float) -> bool:
        return bool(sr.points) and sr.last_fresh_mono is not None and now - sr.last_fresh_mono <= 2.0 * _SAMPLE_S

    def _lead(self, asset: str, now: float) -> _Series | None:
        """The first configured venue whose series is warm (both windows) and fresh right now."""
        for v in self._venues():
            sr = self._series.get((asset, v))
            if sr is not None and self._fresh_now(sr, now) \
                    and sr.move(now, self._f("mom_trend_s", 3600.0)) is not None \
                    and sr.move(now, self._f("mom_window_s", 900.0)) is not None:
                return sr
        return None

    # ------------------------------------------------------------------ main loop
    def tick(self, now: float | None = None) -> None:
        active = any(t.status in _ACTIVE for t in self._trades)
        if not self.enabled and not active:
            return
        now = self._clock.mono() if now is None else now
        for asset in self._assets():
            self._sample(asset, now)
            if self.enabled:
                self._signal(asset, now)
        for t in list(self._trades):
            if t.status in _ACTIVE:
                self._manage(t, now)

    def _signal(self, asset: str, now: float) -> None:
        entry, trend = self._f("mom_entry_pct", 0.6), self._f("mom_trend_pct", 0.3)
        lead = self._lead(asset, now)
        for v in self._venues():  # every warm venue keeps its own edge state (no re-fire after a switch)
            sr = self._series.get((asset, v))
            if sr is None or not self._fresh_now(sr, now):
                continue
            w = sr.move(now, self._f("mom_window_s", 900.0))
            tr = sr.move(now, self._f("mom_trend_s", 3600.0))
            if w is None or tr is None:
                continue
            for d in (1, -1):
                cond = w * d >= entry and tr * d >= trend
                fired = cond and not sr.armed[d]
                sr.armed[d] = cond
                if not fired or sr is not lead:
                    continue
                if now < self._cooldown.get(asset, float("-inf")) or \
                        any(t.asset == asset and t.status in _ACTIVE for t in self._trades):
                    continue
                mid = sr.points[-1][1]
                t = MomTrade(id=f"mom-{asset}-{v}-{int(self._clock.wall() * 1000)}-{next(self._ids)}",
                             asset=asset, venue=v, direction=d, signal_ts=self._clock.wall(),
                             move_window_pct=w, move_trend_pct=tr,
                             target_qty=self._f("mom_notional_quote", 1000.0) / mid)
                t.due_mono = now + self._f("basis_latency_ms", 300.0) / 1000.0
                self._trades.append(t)
                del self._trades[:-_KEEP]
                self._save(t)
                log.info("momentum %s %s on %s: %+.2f%% / %gs, trend %+.2f%%", "LONG" if d > 0 else "SHORT",
                         asset, v, w, self._f("mom_window_s", 900.0), tr)

    def _fill(self, t: MomTrade, side: Side, qty: float, now: float) -> tuple[float, float, Any]:
        p = self.sc._pairs.get((t.venue, t.asset))
        if p is None:
            return 0.0, 0.0, None
        q, px = self.sc._fill(p, "perp", side, qty, now)
        if q > 0:
            notional = q * px
            t.cash_quote += notional if side == Side.SELL else -notional
            t.fees_quote += notional * p.taker["perp"]
        return q, px, p

    def _manage(self, t: MomTrade, now: float) -> None:
        if t.status == "pending":
            if now < (t.due_mono or 0.0):
                return
            q, px, _ = self._fill(t, Side.BUY if t.direction > 0 else Side.SELL, t.target_qty, now)
            if q <= _EPS:
                t.status, t.exit_reason = "no_fill", "no fresh book or no depth within the slippage cap"
                self._close(t)
                return
            t.qty, t.entry_qty, t.entry_px, t.best_px = q, q, px, px
            t.partial = q < t.target_qty * (1.0 - 1e-9)
            t.status, t.opened_ts, t.opened_mono, t.last_fresh_mono = "open", self._clock.wall(), now, now
            self._save(t)
            return
        p = self.sc._pairs.get((t.venue, t.asset))
        b = self.sc._leg_book(p, "perp", now) if p is not None else None
        if b is None or not b.bids or not b.asks:
            if now - (t.last_fresh_mono or now) > self._f("mom_stale_s", 60.0):
                t.status, t.exit_reason = "no_data", f"no fresh price for {self._f('mom_stale_s', 60.0):g} s: outcome unknown"
                self._close(t)
            return
        t.last_fresh_mono = now
        self._funding(t, p)
        if t.status == "closing":
            self._exit(t, now)
            return
        px = b.bids[0].price if t.direction > 0 else b.asks[0].price  # what the position would sell at
        out = t.qty * px
        t.open_net_quote = t.cash_quote + (out if t.direction > 0 else -out) - t.fees_quote \
            - out * p.taker["perp"] + t.funding_quote
        t.open_net_ts = self._clock.wall()
        t.best_px = max(t.best_px, px) if t.direction > 0 else min(t.best_px, px)
        stop = t.entry_px * (1.0 - t.direction * self._f("mom_stop_pct", 0.5) / 100.0)
        trail = t.best_px * (1.0 - t.direction * self._f("mom_trail_pct", 0.4) / 100.0)
        reason = ""
        if (px - stop) * t.direction <= 0:
            reason = f"stop {self._f('mom_stop_pct', 0.5):g}% hit"
        elif (px - trail) * t.direction <= 0 and \
                (t.best_px / t.entry_px - 1.0) * 100.0 * t.direction >= self._f("mom_trail_pct", 0.4):
            # the trail arms once the position was at least trail_pct in profit (before that: the hard stop)
            reason = f"trailing stop {self._f('mom_trail_pct', 0.4):g}% from the best price"
        elif now - (t.opened_mono or now) >= self._f("mom_max_hold_s", 3600.0):
            reason = "max hold"
        if reason:
            t.status, t.exit_reason = "closing", reason
            self._exit(t, now)

    def _exit(self, t: MomTrade, now: float) -> None:
        q, px, _ = self._fill(t, Side.SELL if t.direction > 0 else Side.BUY, t.qty, now)
        if q > 0:
            t.exit_px = px  # the last exit fill's VWAP
            t.qty = max(0.0, t.qty - q)
        if t.qty <= _EPS:
            t.status = "closed"
            self._close(t)
        else:
            t.partial = True
            self._save(t)  # the rest sells on the next tick

    def _funding(self, t: MomTrade, p: Any) -> None:
        ts = p.funding_next_ts
        if not ts or p.funding_rate is None or t.opened_ts is None or t.funding_applied == ts:
            return
        if t.opened_ts < ts <= self._clock.wall():
            b = p.books.get("perp")
            px = b.mid if b is not None and b.mid else t.entry_px
            t.funding_quote += -t.direction * p.funding_rate * t.qty * px  # a long pays a positive rate
            t.funding_applied = ts

    def _close(self, t: MomTrade) -> None:
        t.closed_ts = self._clock.wall()
        t.due_mono = None
        if t.status == "closed":
            t.net_quote = t.cash_quote - t.fees_quote + t.funding_quote
            notional = t.entry_px * t.entry_qty
            t.net_pct = t.net_quote / notional * 100.0 if notional > 0 else None
        self._save(t)
        if t.status in ("closed", "no_data"):
            self._cooldown[t.asset] = self._clock.mono() + self._f("mom_cooldown_s", 900.0)

    # ------------------------------------------------------------------ persistence
    def _save(self, t: MomTrade) -> None:
        fn = getattr(self._db, "save_momentum_trade", None) if self._db is not None else None
        if fn is None:
            return
        try:
            fn(t.to_dict())
        except Exception:  # noqa: BLE001
            log.exception("momentum trade %s not saved", t.id)

    def _load(self) -> None:
        fn = getattr(self._db, "list_momentum_trades", None) if self._db is not None else None
        if fn is None:
            return
        try:
            rows = fn(_KEEP)
        except Exception:  # noqa: BLE001
            log.exception("momentum trades not loaded")
            return
        known = set(MomTrade.__dataclass_fields__)
        for r in reversed(rows):
            try:
                t = MomTrade(**{k: v for k, v in r.items() if k in known})
            except TypeError:
                continue
            if t.status in ("open", "closing") and t.opened_ts is not None and t.qty > _EPS:
                # the position is still there (the stop is the bot's own, so nothing closed it while the bot
                # was down): it is managed again from the first fresh book, at whatever the price is now
                now = self._clock.mono()
                t.status, t.resumed, t.exit_reason = "open", True, ""
                t.opened_mono = now - max(0.0, self._clock.wall() - t.opened_ts)
                t.last_fresh_mono, t.due_mono = now, None
                self._save(t)
            elif t.status in _ACTIVE:  # an entry that never filled
                t.status, t.closed_ts, t.exit_reason = "abandoned", self._clock.wall(), "process restarted: outcome unknown"
                t.due_mono = t.opened_mono = t.last_fresh_mono = None
                self._save(t)
            else:
                t.due_mono = t.opened_mono = t.last_fresh_mono = None
            self._trades.append(t)

    # ------------------------------------------------------------------ queries
    def trades(self, limit: int = 100) -> list[dict[str, Any]]:
        now = self._clock.wall()
        out = []
        for t in reversed(self._trades[-max(0, int(limit)):]):
            d = t.to_dict()
            live = t.status in ("open", "closing") and t.open_net_ts is not None
            d["open_net_age_s"] = max(0.0, now - t.open_net_ts) if live else None
            if not live:
                d["open_net_quote"] = None
            out.append(d)
        return out

    def balance(self) -> dict[str, Any]:
        """Plus or minus of the strategy in one number: finished trades + open positions if closed now."""
        done = [t for t in self._trades if t.status == "closed" and t.net_quote is not None]
        live = [t for t in self._trades if t.status in ("open", "closing")]
        priced = [t for t in live if t.open_net_quote is not None]
        closed_q, open_q = sum(t.net_quote for t in done), sum(t.open_net_quote for t in priced)
        return {"closed_quote": closed_q, "closed_n": len(done), "wins": sum(1 for t in done if t.net_quote > 0),
                "open_quote": open_q, "open_n": len(live), "open_priced": len(priced),
                "unknown": sum(1 for t in self._trades if t.status in ("no_data", "abandoned")),
                "in_use_quote": sum(t.qty * t.entry_px for t in live), "total_quote": closed_q + open_q}

    def status(self) -> list[dict[str, Any]]:
        now = self._clock.mono()
        out = []
        for asset in self._assets():
            all_sr = [sr for (a, _), sr in self._series.items() if a == asset and sr.points]
            warm = [sr for sr in all_sr if sr.move(now, self._f("mom_trend_s", 3600.0)) is not None]
            # shown: the venue that would give the signal, else the one with the longest history
            sr = self._lead(asset, now) or max(all_sr, key=lambda x: x.points[-1][0] - x.points[0][0], default=None)
            out.append({
                "asset": asset, "venue": sr.venue if sr else None,
                "mid": sr.points[-1][1] if sr else None,
                "move_window_pct": sr.move(now, self._f("mom_window_s", 900.0)) if sr else None,
                "move_trend_pct": sr.move(now, self._f("mom_trend_s", 3600.0)) if sr else None,
                "series_s": (sr.points[-1][0] - sr.points[0][0]) if sr and len(sr.points) > 1 else 0.0,
                "venues": len(all_sr), "warm_venues": len(warm),
                "cooldown_s": max(0.0, self._cooldown.get(asset, float("-inf")) - now),
            })
        return out

    def stats(self) -> dict[str, Any]:
        closed = [t for t in self._trades if t.status == "closed"]
        wins = [t for t in closed if (t.net_quote or 0.0) > 0]
        peak = cum = dd = 0.0
        for t in sorted(closed, key=lambda x: x.closed_ts or 0.0):
            cum += t.net_quote or 0.0
            peak = max(peak, cum)
            dd = max(dd, peak - cum)
        exits: dict[str, int] = {}
        for t in closed:
            key = t.exit_reason.split(" ")[0] if t.exit_reason else "?"
            exits[key] = exits.get(key, 0) + 1

        def side(d: int) -> dict[str, Any]:
            rows = [t for t in closed if t.direction == d]
            return {"closed": len(rows), "wins": sum(1 for t in rows if (t.net_quote or 0) > 0),
                    "pnl_quote": sum(t.net_quote or 0.0 for t in rows)}

        return {
            "enabled": self.enabled, "closed": len(closed), "open": sum(1 for t in self._trades if t.status in _ACTIVE),
            "wins": len(wins), "win_rate": len(wins) / len(closed) if closed else None,
            "avg_net_pct": sum(t.net_pct or 0.0 for t in closed) / len(closed) if closed else None,
            "pnl_quote": sum(t.net_quote or 0.0 for t in closed), "fees_quote": sum(t.fees_quote for t in closed),
            "max_drawdown_quote": dd, "long": side(1), "short": side(-1), "exits": exits,
            "no_data": sum(1 for t in self._trades if t.status == "no_data"),
            "no_data_cost_quote": sum(t.fees_quote for t in self._trades if t.status == "no_data"),
            "no_fill": sum(1 for t in self._trades if t.status == "no_fill"),
            "abandoned": sum(1 for t in self._trades if t.status == "abandoned"),
        }


__all__ = ["Momentum", "MomTrade"]
