"""ContextHub - runs the context collectors and turns their measurements into
``Factor`` objects for the quality model (ARCHITECTURE P3.8 / P3.12).

Every factor is relative to the TRADE DIRECTION (+ supports, - argues against)
and carries the numbers behind it in ``detail`` (Russian).  A component that is
missing or has no fresh data gives ``available=False`` - never support.
``factors()`` does no I/O: it reads what the collectors already measured.
"""
from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

from .bars import correlation
from .base import Factor, SourceStatus

log = logging.getLogger(__name__)

DEFAULT_CONTEXT_WEIGHTS: dict[str, float] = {
    "macro_event": 1.0, "news": 1.0, "whale": 0.4, "funding": 0.6, "open_interest": 0.5, "liquidations": 0.5,
    "btc_regime": 0.8, "tradfi": 0.3, "sentiment": 0.2, "event_reaction": 0.8, "erratic": 1.0,
}
LABELS: dict[str, str] = {
    "macro_event": "Macro calendar", "news": "News", "whale": "Whale flows", "funding": "Funding",
    "open_interest": "Open interest", "liquidations": "Liquidations", "btc_regime": "BTC regime",
    "tradfi": "US markets", "sentiment": "Fear & Greed", "event_reaction": "Event history", "erratic": "Erratic moves",
}
_OI_LONG = {"NEW_LONGS": 0.5, "SHORT_COVERING": 0.2, "NEW_SHORTS": -0.5, "LONG_LIQUIDATION": -0.3, "QUIET": 0.0}
_OI_SHORT = {"NEW_SHORTS": 0.5, "LONG_LIQUIDATION": 0.2, "NEW_LONGS": -0.5, "SHORT_COVERING": -0.3, "QUIET": 0.0}
_RU_DIR = {1: "рост", -1: "падение"}


def _clip(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _num(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _fmt_s(seconds: float) -> str:
    s = int(abs(seconds))
    if s < 90:
        return f"{s} с"
    if s < 5400:
        return f"{s // 60} мин"
    return f"{s // 3600} ч {s % 3600 // 60} мин"


def _ok(component: Any) -> bool:
    st = getattr(component, "status", None)
    return bool(getattr(st, "ok", False)) or bool(getattr(st, "last_ok_ts", None))


class ContextHub:
    def __init__(self, settings: Any, clock: Any, *, pool: Any = None, calendar: Any = None, news: Any = None,
                 markets: Any = None, derivs: Any = None, fear_greed: Any = None, etf: Any = None, bars: Any = None,
                 reactions: Any = None, actuals: Any = None, stats: Any = None, metalabel: Any = None) -> None:
        self._settings = settings
        self._clock = clock
        self.pool, self.calendar, self.news, self.markets = pool, calendar, news, markets
        self.derivs, self.fear_greed, self.etf, self.bars = derivs, fear_greed, etf, bars
        self.reactions, self.actuals, self.stats, self.metalabel = reactions, actuals, stats, metalabel
        self._watched: set[str] = set()
        self._surprised: set[str] = set()
        self._last_pool_mono = -1e18
        self._last_stability_mono = -1e18
        self._bootstrapped: set[str] = set()
        self._bg: set[asyncio.Task] = set()

    # ------------------------------------------------------------------ #
    # settings
    # ------------------------------------------------------------------ #
    def _w(self, key: str) -> float:
        weights = getattr(self._settings, "context_weights", None) or {}
        try:
            return float(weights.get(key, DEFAULT_CONTEXT_WEIGHTS.get(key, 0.5)))
        except (TypeError, ValueError, AttributeError):
            return DEFAULT_CONTEXT_WEIGHTS.get(key, 0.5)

    def _s(self, name: str, default: float) -> float:
        v = _num(getattr(self._settings, name, default))
        return default if v is None else v

    def _f(self, key: str, available: bool, score: float = 0.0, detail: str = "", veto: bool = False, **data: Any) -> Factor:
        return Factor(key=key, label=LABELS.get(key, key), available=available, score=score, weight=self._w(key),
                      detail=detail, veto=veto, data=data)

    def _pool_assets(self) -> list[str]:
        if self.pool is None:
            return []
        try:
            return list(self.pool.assets())
        except Exception:  # noqa: BLE001
            return []

    # ------------------------------------------------------------------ #
    # run loop
    # ------------------------------------------------------------------ #
    def collectors(self) -> list[Any]:
        return [c for c in (self.pool, self.calendar, self.news, self.markets, self.derivs, self.fear_greed, self.etf,
                            self.actuals) if c is not None and hasattr(c, "run")]

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [asyncio.create_task(c.run(stop), name=f"context-{getattr(c, 'name', 'collector')}") for c in self.collectors()]
        period = max(1.0, self._s("context_housekeeping_s", 5.0))
        try:
            while not stop.is_set():
                try:
                    self.housekeeping()
                except Exception:  # noqa: BLE001
                    log.exception("context housekeeping failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=period)
                except asyncio.TimeoutError:
                    pass
        finally:
            for t in [*tasks, *self._bg]:
                t.cancel()
            await asyncio.gather(*tasks, *self._bg, return_exceptions=True)

    def _spawn(self, coro: Any, name: str) -> None:
        try:
            task = asyncio.get_running_loop().create_task(coro, name=name)
        except RuntimeError:
            coro.close()
            return
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    def housekeeping(self, now_wall: float | None = None) -> None:
        """Bars sampling, reaction capture, event registration, pool upkeep (sync, cheap)."""
        now = self._clock.wall() if now_wall is None else now_wall
        mono = self._clock.mono()
        pool_assets = self._pool_assets()
        if self.bars is not None:
            self.bars.track(list(dict.fromkeys(["BTC", "ETH", *pool_assets])))
            self.bars.sample(now)
        if self.reactions is not None:
            self._register_events(now, pool_assets)
            self.reactions.tick(now)
        if self.pool is not None and mono - self._last_pool_mono >= self._s("context_pool_reevaluate_s", 300.0):
            self._last_pool_mono = mono
            try:
                self.pool.reevaluate()
            except Exception:  # noqa: BLE001
                log.exception("pool reevaluate failed")
            if self.bars is not None:
                new = [a for a in ["BTC", "ETH", *pool_assets] if a not in self._bootstrapped]
                if new and hasattr(self.bars, "bootstrap"):
                    self._bootstrapped.update(new)
                    self._spawn(self.bars.bootstrap(new), "context-bars-bootstrap")
        refresh_s = self._s("stability_refresh_h", 24.0) * 3600.0
        if (self.bars is not None and self.pool is not None and hasattr(self.bars, "refresh_stability")
                and mono - self._last_stability_mono >= refresh_s):
            self._last_stability_mono = mono
            cands = list(self.pool.candidates()) if hasattr(self.pool, "candidates") else pool_assets
            if cands:
                self._spawn(self._stability_then_reevaluate(cands), "context-stability")

    async def _stability_then_reevaluate(self, assets: list[str]) -> None:
        try:
            await self.bars.refresh_stability(assets)
        finally:
            try:
                self.pool.reevaluate()
            except Exception:  # noqa: BLE001
                log.exception("pool reevaluate after stability failed")

    def _register_events(self, now: float, pool_assets: list[str]) -> None:
        majors = list(dict.fromkeys(["BTC", "ETH", *pool_assets[:5]]))
        cal = self.calendar
        if cal is not None:
            try:
                upcoming = cal.upcoming(now, 120.0, "high") + cal.recent(now, 4.0, "high")
            except Exception:  # noqa: BLE001
                upcoming = []
            for ev in upcoming:
                key = f"macro:{ev.id}"
                if key in self._watched:
                    continue
                if self.reactions.watch(key, ev.event_type, ev.ts, majors, ev.surprise):
                    self._watched.add(key)
        news = self.news
        if news is not None:
            try:
                items = news.items(since_wall=now - 60.0, limit=50)
            except Exception:  # noqa: BLE001
                items = []
            for it in items:
                key = f"news:{it.id}"
                if key in self._watched or it.importance < 0.7 or it.category in ("market_move", "whale_transfer"):
                    continue
                assets = ["BTC", "ETH"] if "*" in it.assets else [a for a in it.assets if a in pool_assets or a in ("BTC", "ETH")]
                if assets and self.reactions.watch(key, f"news:{it.category}", now, assets[:6], None):
                    self._watched.add(key)
            # surprise sign of a release from the squawk channels -> calendar + its reaction record
            if cal is not None and hasattr(news, "macro_surprises"):
                for m in news.macro_surprises(now, 900.0):
                    if m.get("surprise") is None or m.get("id") in self._surprised:
                        continue
                    for ev in cal.recent(now, 1800.0, "medium"):
                        if ev.event_type == m.get("event_type") and abs(float(m.get("ts", now)) - ev.ts) <= 900.0:
                            cal.set_surprise(ev.id, int(m["surprise"]))
                            self.reactions.set_surprise(f"macro:{ev.id}", int(m["surprise"]))
                            self._surprised.add(m.get("id"))
                            break
        if len(self._watched) > 5000:
            self._watched = set(list(self._watched)[-2000:])

    # ------------------------------------------------------------------ #
    # queries used by the pipeline (no I/O)
    # ------------------------------------------------------------------ #
    def asset_stats(self, asset: str) -> dict[str, Any]:
        if self.bars is None:
            return {}
        try:
            return dict(self.bars.stats(asset) or {})
        except Exception:  # noqa: BLE001
            return {}

    def funding(self, asset: str) -> tuple[float | None, float | None, float | None] | None:
        d = self.derivs.get(asset) if self.derivs is not None else None
        if not d or d.get("funding_rate_8h") is None:
            return None
        return d.get("funding_rate_8h"), d.get("funding_interval_h"), d.get("next_funding_ts")

    def event_window(self, now_wall: float | None = None) -> bool:
        if self.calendar is None:
            return False
        now = self._clock.wall() if now_wall is None else now_wall
        try:
            return bool(self.calendar.event_window(now))
        except Exception:  # noqa: BLE001
            return False

    def factors(self, asset: str, direction: int, now_wall: float | None = None) -> list[Factor]:
        now = self._clock.wall() if now_wall is None else now_wall
        d = 1 if int(direction or 1) >= 0 else -1
        out: list[Factor] = []
        for fn in (self._macro, self._news, self._whale, self._funding, self._open_interest, self._liquidations,
                   self._btc_regime, self._tradfi, self._sentiment, self._event_reaction, self._erratic):
            try:
                out.append(fn(asset, d, now))
            except Exception:  # noqa: BLE001
                log.exception("context factor %s failed for %s", fn.__name__, asset)
                key = fn.__name__.lstrip("_")
                out.append(self._f(key, False, detail="ошибка расчёта фактора"))
        return out

    # ------------------------------------------------------------------ #
    # factors
    # ------------------------------------------------------------------ #
    def _macro(self, asset: str, d: int, now: float) -> Factor:
        cal = self.calendar
        if cal is None or not (cal.available(now) if hasattr(cal, "available") else _ok(cal)):
            return self._f("macro_event", False, detail="экономический календарь недоступен")
        before = self._s("macro_block_before_s", 300.0)
        after = self._s("macro_block_after_s", 120.0)
        for ev, phase in cal.event_window(now):
            dt = ev.ts - now
            if phase == "before" and 0 <= dt <= before:
                return self._f("macro_event", True, veto=True, detail=f"{ev.title}: релиз США через {_fmt_s(dt)} - вход до цифры запрещён",
                               event=ev.event_type, seconds=round(dt))
            if phase == "after" and 0 <= -dt <= after:
                prof = self._profile(ev.event_type, asset, ev.surprise, 60)
                if prof is not None:
                    up = prof["up_share"]
                    score = 2.0 * (up - 0.5) * d
                    return self._f("macro_event", True, score=score,
                                   detail=f"{ev.title} {_fmt_s(-dt)} назад; история: {_RU_DIR[1 if up >= 0.5 else -1]} в {up:.0%} из {prof['n']} случаев за 60 с",
                                   event=ev.event_type)
                return self._f("macro_event", True, veto=True,
                               detail=f"{ev.title} {_fmt_s(-dt)} назад: реакция непредсказуема (мало истории)", event=ev.event_type)
        try:
            medium = [e for e in cal.upcoming(now, before, "medium") if e.impact != "high"]
        except Exception:  # noqa: BLE001
            medium = []
        if medium:
            return self._f("macro_event", True, score=-0.3, detail=f"{medium[0].title} через {_fmt_s(medium[0].ts - now)} (средняя важность)")
        nxt = cal.next_event(now, "high")
        tail = f"следующий важный релиз: {nxt.title} через {_fmt_s(nxt.ts - now)}" if nxt is not None else "важных релизов США на неделе больше нет"
        return self._f("macro_event", True, detail=f"рядом нет релизов; {tail}")

    def _profile(self, event_type: str, asset: str, surprise: int | None, horizon: int) -> dict[str, Any] | None:
        if self.reactions is None:
            return None
        min_n = int(self._s("reaction_min_n", 5))
        for a in (asset, "BTC"):
            prof = self.reactions.profile(event_type, a, surprise) or {}
            row = prof.get(horizon)
            if row and row.get("n", 0) >= min_n:
                up = float(row.get("up_share") or 0.5)
                if up >= 0.65 or up <= 0.35:
                    return {**row, "asset": a}
        return None

    def _news(self, asset: str, d: int, now: float) -> Factor:
        if self.news is None or not _ok(self.news):
            return self._f("news", False, detail="новостные источники недоступны")
        window = self._s("news_window_s", 1800.0)
        half = self._s("news_half_life_s", 900.0)
        total, used = 0.0, []
        for it in self.news.relevant(asset, now, window, 0.5):
            if it.category in ("whale_transfer", "market_move") or not it.direction:
                continue
            decay = 0.5 ** (max(0.0, now - it.ts) / max(1.0, half))
            total += it.importance * max(0.1, it.confidence) * it.direction * d * decay
            used.append(it)
        if not used:
            return self._f("news", True, detail=f"важных новостей по {asset} и рынку за {int(window // 60)} мин нет")
        top = used[0]
        return self._f("news", True, score=_clip(total),
                       detail=f"{len(used)} новост.: «{top.title[:90]}» ({'за' if top.direction * d > 0 else 'против'} сделки)",
                       items=[it.id for it in used[:5]])

    def _whale(self, asset: str, d: int, now: float) -> Factor:
        if self.news is None or not hasattr(self.news, "whale_flows") or not _ok(self.news):
            return self._f("whale", False, detail="Whale Alert недоступен")
        net = 0.0
        flows = self.news.whale_flows(asset, now, 3600.0)
        for w in flows:
            usd = _num(w.get("usd")) or 0.0
            if w.get("flow") == "to_exchange":
                net += usd
            elif w.get("flow") == "from_exchange":
                net -= usd
        if not flows:
            return self._f("whale", True, detail=f"крупных переводов {asset} за час нет")
        score = -math.tanh(net / 5e7) * d  # coins to exchanges = supply for sale: against longs
        side = "на биржи" if net > 0 else "с бирж"
        return self._f("whale", True, score=score, detail=f"китов: {len(flows)}, нетто ${abs(net) / 1e6:.0f}M {side} за час", net_usd=net)

    def _funding(self, asset: str, d: int, now: float) -> Factor:
        info = self.derivs.get(asset) if self.derivs is not None else None
        if not info or info.get("funding_rate_8h") is None:
            return self._f("funding", False, detail=f"фандинг {asset}: нет данных (нет перпетуала или источник недоступен)")
        pct = _num(info.get("funding_pct"))
        rate = float(info["funding_rate_8h"])
        if pct is None:
            return self._f("funding", False, detail=f"фандинг {rate * 100:+.3f}%/8ч; история копится {info.get('funding_n', 0)}/{info.get('funding_min_n', 288)}")
        crowded_long, crowded_short = pct >= 95.0, pct <= 5.0
        if (crowded_long and d > 0) or (crowded_short and d < 0):
            score, word = -0.7, "толпа уже в эту сторону"
        elif (crowded_short and d > 0) or (crowded_long and d < 0):
            score, word = 0.4, "толпа против: возможен сквиз"
        else:
            score, word = 0.0, "в норме"
        return self._f("funding", True, score=score, detail=f"фандинг {rate * 100:+.3f}%/8ч, {pct:.0f}-й перцентиль - {word}", pct=pct)

    def _open_interest(self, asset: str, d: int, now: float) -> Factor:
        if self.derivs is None:
            return self._f("open_interest", False, detail="открытый интерес: нет источника")
        ret = _num(self.asset_stats(asset).get("ret_15m_pct"))
        quad = self.derivs.oi_quadrant(asset, ret)
        if quad is None:
            return self._f("open_interest", False, detail="открытый интерес: недостаточно истории (нужно 15 мин)")
        score = (_OI_LONG if d > 0 else _OI_SHORT).get(quad, 0.0)
        return self._f("open_interest", True, score=score, detail=f"OI и цена за 15 мин: {quad}", quadrant=quad)

    def _liquidations(self, asset: str, d: int, now: float) -> Factor:
        info = self.derivs.get(asset) if self.derivs is not None else None
        longs = _num((info or {}).get("liq_long_usd_5m"))
        shorts = _num((info or {}).get("liq_short_usd_5m"))
        if longs is None or shorts is None:
            return self._f("liquidations", False, detail="ликвидации: нет данных")
        scale = 1e6 if asset in ("BTC", "ETH") else 2e5
        score = math.tanh((shorts - longs) / scale) * d  # shorts liquidated = forced buying
        return self._f("liquidations", True, score=score, detail=f"ликвидации 5 мин: лонги ${longs / 1e3:.0f}K, шорты ${shorts / 1e3:.0f}K")

    def _btc_regime(self, asset: str, d: int, now: float) -> Factor:
        ref = "ETH" if asset == "BTC" else "BTC"
        st = self.asset_stats(ref)
        ret = _num(st.get("ret_15m_pct"))
        if ret is None or int(st.get("minutes") or 0) < 15:
            return self._f("btc_regime", False, detail=f"{ref}: мало минутных данных")
        score = _clip(ret / 0.5) * d
        notes = [f"{ref} за 15 мин {ret:+.2f}%"]
        shock = int(st.get("shock") or 0)
        if shock and shock != d:
            score = -1.0
            notes.append(f"резкий шок {ref} против сделки")
        pctile = _num(st.get("vol_pctile"))
        if pctile is not None and pctile > 0.9:
            score -= 0.3
            notes.append("волатильность высокая (непредсказуемо)")
        return self._f("btc_regime", True, score=_clip(score), detail="; ".join(notes))

    def _tradfi(self, asset: str, d: int, now: float) -> Factor:
        mk = self.markets
        if mk is None or self.bars is None:
            return self._f("tradfi", False, detail="рынки США: нет источника")
        btc_r = self.bars.returns("BTC", 240)
        parts, notes = [], []
        for key, scale, name in (("SPX_FUT", 0.3, "S&P фьючерс"), ("DXY", 0.15, "DXY")):
            q = mk.quote(key, now)
            if not q or not q.get("market_open"):
                continue
            chg = _num(q.get("chg_15m_pct"))
            corr = correlation(btc_r, mk.returns_1m(key, 240), 30) if hasattr(mk, "returns_1m") else None
            if chg is None or corr is None or abs(corr) < 0.2:
                continue
            parts.append(_clip(chg / scale) * corr * d)
            notes.append(f"{name} {chg:+.2f}% за 15 мин, корр. с BTC {corr:+.2f}")
        if not parts:
            return self._f("tradfi", False, detail="рынки США закрыты, нет данных или связь с BTC слабая")
        return self._f("tradfi", True, score=_clip(sum(parts) / len(parts)), detail="; ".join(notes) + " (данные с задержкой ~10 мин)")

    def _sentiment(self, asset: str, d: int, now: float) -> Factor:
        fg = self.fear_greed.get() if self.fear_greed is not None else None
        value = _num((fg or {}).get("value"))
        if value is None:
            return self._f("sentiment", False, detail="индекс страха и жадности: нет данных")
        score = 0.0
        if value >= 80:
            score = -0.3 if d > 0 else 0.2
        elif value <= 20:
            score = 0.2 if d > 0 else -0.3
        return self._f("sentiment", True, score=score, detail=f"страх/жадность: {value:.0f} ({fg.get('classification', '')})")

    def _event_reaction(self, asset: str, d: int, now: float) -> Factor:
        if self.reactions is None or self.news is None:
            return self._f("event_reaction", False, detail="история реакций недоступна")
        recent = [it for it in self.news.relevant(asset, now, 1800.0, 0.7) if it.category not in ("market_move", "whale_transfer")]
        if not recent:
            return self._f("event_reaction", True, detail="свежих важных событий нет")
        it = recent[0]
        prof = self._profile(f"news:{it.category}", asset, None, 300)
        if prof is None:
            return self._f("event_reaction", True, detail=f"«{it.category}»: мало истории похожих событий")
        up = prof["up_share"]
        return self._f("event_reaction", True, score=2.0 * (up - 0.5) * d,
                       detail=f"после похожих событий ({it.category}) {prof['asset']} шёл в {_RU_DIR[1 if up >= 0.5 else -1]} в {up:.0%} из {prof['n']}")

    def _erratic(self, asset: str, d: int, now: float) -> Factor:
        if self.bars is None:
            return self._f("erratic", False, detail="минутные данные недоступны")
        e = self.bars.erratic(asset) or {}
        if not e.get("available"):
            return self._f("erratic", False, detail=str(e.get("reason") or "мало минутных данных"))
        if e.get("suspended"):
            return self._f("erratic", True, veto=True, detail=str(e.get("reason") or "монета скачет: новые сигналы на паузе"))
        return self._f("erratic", True, detail="резких скачков за час нет")

    # ------------------------------------------------------------------ #
    # reporting
    # ------------------------------------------------------------------ #
    def status(self) -> list[SourceStatus]:
        out = []
        for c in [*self.collectors(), self.bars]:
            st = getattr(c, "status", None)
            if isinstance(st, SourceStatus):
                out.append(st)
        sources = getattr(self.news, "sources", None)
        if isinstance(sources, dict):
            out.extend(s for s in sources.values() if isinstance(s, SourceStatus))
        return out

    def snapshot(self) -> dict[str, Any]:
        now = self._clock.wall()

        def snap(c: Any, *args: Any) -> Any:
            if c is None or not hasattr(c, "snapshot"):
                return None
            try:
                return c.snapshot(*args)
            except Exception:  # noqa: BLE001
                log.exception("snapshot of %s failed", type(c).__name__)
                return None

        macro: dict[str, Any] = {"calendar": None, "next": [], "recent": []}
        if self.calendar is not None:
            macro["calendar"] = snap(self.calendar, now)
            try:
                for ev in self.calendar.upcoming(now, 7 * 86400.0, "medium")[:12]:
                    row = ev.to_dict()
                    row["in_s"] = round(ev.ts - now)
                    if self.reactions is not None:
                        row["btc_profile"] = {str(h): v for h, v in (self.reactions.profile(ev.event_type, "BTC") or {}).items()}
                    macro["next"].append(row)
                for ev in self.calendar.recent(now, 86400.0, "medium")[:12]:
                    row = ev.to_dict()
                    row["ago_s"] = round(now - ev.ts)
                    macro["recent"].append(row)
            except Exception:  # noqa: BLE001
                log.exception("macro snapshot failed")
        news_items: list[dict[str, Any]] = []
        if self.news is not None:
            try:
                news_items = [it.to_dict() for it in self.news.items(limit=50)]
            except Exception:  # noqa: BLE001
                news_items = []
        regime: dict[str, Any] = {}
        for a in ("BTC", "ETH"):
            regime[a] = self.asset_stats(a)
        if self.bars is not None and self.markets is not None and hasattr(self.markets, "returns_1m"):
            btc_r = self.bars.returns("BTC", 240)
            regime["corr_btc_spx"] = correlation(btc_r, self.markets.returns_1m("SPX_FUT", 240), 30)
            regime["corr_btc_dxy"] = correlation(btc_r, self.markets.returns_1m("DXY", 240), 30)
        return {
            "ts": now,
            "event_window": self.event_window(now),
            "macro": macro,
            "news": {"items": news_items, "feed": snap(self.news)},
            "tradfi": snap(self.markets, now),
            "regime": regime,
            "derivs": snap(self.derivs),
            "fear_greed": snap(self.fear_greed),
            "etf": snap(self.etf),
            "pool": snap(self.pool),
            "bars": snap(self.bars),
            "reactions": snap(self.reactions),
            "metalabel": snap(self.metalabel),
            "sources": [s.to_dict() for s in self.status()],
            "weights": {k: self._w(k) for k in DEFAULT_CONTEXT_WEIGHTS},
        }


__all__ = ["ContextHub", "DEFAULT_CONTEXT_WEIGHTS", "LABELS"]
