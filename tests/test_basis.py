"""Spot-Futures Arbitrage Scanner: basis math on the book sides, data quality, conditions, the execution
simulator (latency, partial fills, one-leg incidents), funding, fees, adverse move, statistics, runner
routing and the API.  Research only: nothing here can send an order."""
from __future__ import annotations

import pytest

from latbot.basis.runner import BasisRunner, leg_configs
from latbot.basis.scanner import BasisScanner
from latbot.clock import SimClock
from latbot.config import Settings
from latbot.models import BookLevel, OrderBook, Timestamps
from latbot.storage.db import Database


def settings(**over) -> Settings:
    base = dict(basis_enabled=True, basis_notional_quote=500.0, basis_mode="both", basis_fut_upper_pct=0.1,
                basis_spot_lower_pct=0.1, basis_ref_window_s=10.0, basis_exit_basis_pct=0.02,
                basis_min_convergence_pct=0.05, basis_max_hold_s=3600.0, basis_latency_ms=300.0,
                basis_max_skew_ms=500.0, basis_max_age_ms=2000.0, basis_max_slippage_pct=0.3)
    base.update(over)
    return Settings(**base)


def book(clock: SimClock, venue: str, leg: str, bid: float, ask: float, size: float = 10.0,
         levels: int = 3, exchange_ts: float | None = None) -> OrderBook:
    step = (ask - bid) or 0.1
    bids = [BookLevel(bid - i * step, size) for i in range(levels)]
    asks = [BookLevel(ask + i * step, size) for i in range(levels)]
    sym = "BTC/USDT" if leg == "spot" else "BTC/USDT:USDT"
    return OrderBook(exchange=f"{venue}.{leg}", symbol=sym, bids=bids, asks=asks, asset="BTC",
                     ts=Timestamps(recv_ts=clock.wall(), recv_mono=clock.mono(), exchange_ts=exchange_ts))


class Env:
    def __init__(self, **over) -> None:
        self.clock = SimClock()
        self.db = Database(":memory:", clock=self.clock)
        self.s = settings(**over)
        self.sc = BasisScanner(self.s, self.clock, db=self.db)
        self.sc.register_pair("okx", "BTC", "BTC/USDT", "BTC/USDT:USDT")
        self.sc.set_fee("okx", "BTC", "spot", 0.001)
        self.sc.set_fee("okx", "BTC", "perp", 0.0005)

    def push(self, spot: tuple[float, float], perp: tuple[float, float], size: float = 10.0) -> None:
        self.sc.on_book("okx", "spot", book(self.clock, "okx", "spot", *spot, size=size))
        self.sc.on_book("okx", "perp", book(self.clock, "okx", "perp", *perp, size=size))

    def warm(self, seconds: int = 10, spot=(99.99, 100.01), perp=(100.04, 100.06)) -> None:
        for _ in range(seconds):
            self.push(spot, perp)
            self.clock.advance(1.0)
            self.sc.tick()

    def pair(self) -> dict:
        return self.sc.pairs()[0]


# ---- basis on the book sides ------------------------------------------------------------------------
def test_basis_is_computed_on_mids_and_on_the_executable_sides():
    env = Env()
    env.push((99.99, 100.01), (100.09, 100.11))
    p = env.pair()
    assert p["basis_abs"] == pytest.approx(0.10)
    assert p["basis_pct"] == pytest.approx(0.1)
    # entry: buy the spot ask (100.01), sell the perp bid (100.09); exit: sell spot bid, buy perp ask
    assert p["entry_basis_pct"] == pytest.approx((100.09 / 100.01 - 1) * 100)
    assert p["exit_basis_pct"] == pytest.approx((100.11 / 99.99 - 1) * 100)
    assert p["fees_pct"] == pytest.approx(2 * (0.1 + 0.05))


def test_quote_mismatch_is_never_a_basis():
    env = Env()
    assert env.sc.register_pair("kraken", "BTC", "BTC/USDT", "BTC/USD:USD") is False
    assert "quote mismatch" in [p for p in env.sc.pairs() if p["venue"] == "kraken"][0]["note"]


def test_stale_or_skewed_books_give_no_basis():
    env = Env()
    env.sc.on_book("okx", "spot", book(env.clock, "okx", "spot", 99.99, 100.01))
    env.clock.advance(1.0)
    env.sc.on_book("okx", "perp", book(env.clock, "okx", "perp", 100.09, 100.11))
    assert env.pair()["ok"] is False and "apart" in env.pair()["reason"]
    assert env.sc.counters["skew_evals"] == 1
    env.clock.advance(3.0)
    env.sc.on_book("okx", "perp", book(env.clock, "okx", "perp", 100.09, 100.11))
    assert env.pair()["ok"] is False and "stale" in env.pair()["reason"]


def test_exchange_timestamps_apart_are_rejected_too():
    env = Env()
    env.sc.on_book("okx", "spot", book(env.clock, "okx", "spot", 99.99, 100.01, exchange_ts=1000.0))
    env.sc.on_book("okx", "perp", book(env.clock, "okx", "perp", 100.09, 100.11, exchange_ts=1001.0))
    assert env.pair()["ok"] is False and "apart" in env.pair()["reason"]


# ---- conditions --------------------------------------------------------------------------------------
def test_conditions_are_edge_triggered_and_counted_separately():
    env = Env(basis_mode="any")
    env.warm()
    env.push((99.99, 100.01), (100.30, 100.32))  # perp jumps: futures upper bound only
    env.push((99.99, 100.01), (100.30, 100.32))  # still true: not a new signal
    s = env.sc.stats()
    assert s["signals_fut_upper"] == 1 and s["signals_spot_lower"] == 0 and s["signals_both"] == 0
    assert s["entries_attempted"] == 1


def test_both_mode_needs_both_conditions_at_once():
    env = Env(basis_mode="both")
    env.warm()
    env.push((99.99, 100.01), (100.30, 100.32))  # only the futures condition
    assert env.sc.stats()["entries_attempted"] == 0
    env.push((99.70, 99.72), (100.30, 100.32))  # spot drops too: both
    assert env.sc.stats()["signals_both"] == 1 and env.sc.stats()["entries_attempted"] == 1


def test_no_signal_before_the_reference_window_is_filled():
    env = Env(basis_mode="any")
    env.push((99.99, 100.01), (100.30, 100.32))
    assert env.sc.stats()["signals_total"] == 0


# ---- execution simulator -----------------------------------------------------------------------------
def enter(env: Env, spot=(99.70, 99.72), perp=(100.30, 100.32)) -> None:
    env.warm()
    env.push(spot, perp)


def test_fills_use_the_book_after_the_latency_and_open_a_hedged_pair():
    env = Env()
    enter(env)
    env.sc.tick()
    assert env.sc.trades()[0]["status"] == "pending"  # 300 ms latency not over
    env.clock.advance(0.2)
    env.push((99.70, 99.80), (100.25, 100.32))  # the book moved before the fills
    env.clock.advance(0.2)
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["status"] == "open" and t["hedged"]
    assert t["entry_spot_vwap"] == pytest.approx(99.80) and t["entry_perp_vwap"] == pytest.approx(100.25)
    assert t["entry_basis_pct"] == pytest.approx((100.25 / 99.80 - 1) * 100)


def test_a_thin_leg_fills_partially_and_the_excess_is_unwound():
    env = Env()
    enter(env)
    env.clock.advance(0.2)
    thin = book(env.clock, "okx", "perp", 100.30, 100.32, size=2.0, levels=1)  # perp book holds 2 of ~5 BTC
    env.sc.on_book("okx", "spot", book(env.clock, "okx", "spot", 99.70, 99.72))
    env.sc.on_book("okx", "perp", thin)
    env.clock.advance(0.2)
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["status"] == "open" and t["partial"] and t["spot_qty"] == pytest.approx(2.0) == t["perp_qty"]
    assert "excess unwound" in t["incident"]
    assert env.sc.stats()["incomplete"] == 1 and env.sc.stats()["pairs_executed"] == 1


def test_only_one_leg_filled_is_an_incident_never_a_hedged_pair():
    env = Env()
    enter(env)
    env.clock.advance(0.2)
    env.sc.on_book("okx", "spot", book(env.clock, "okx", "spot", 99.70, 99.72))
    env.clock.advance(2.5)  # the perp book is stale at the fill time: that leg cannot fill
    env.sc.on_book("okx", "spot", book(env.clock, "okx", "spot", 99.70, 99.72))
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["status"] == "one_leg" and not t["hedged"] and "only the spot leg" in t["incident"]
    s = env.sc.stats()
    assert s["pairs_executed"] == 0 and s["one_leg"] == 1 and s["incomplete"] == 1
    assert t["fees_quote"] > 0 and t["net_quote"] < 0  # bought and sold back: fees and the spread are lost


def test_exit_on_convergence_with_fees_and_pnl():
    env = Env()
    enter(env)
    env.clock.advance(0.4)
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["status"] == "open"
    env.push((100.00, 100.01), (100.01, 100.02))  # basis converged
    env.sc.tick()
    env.clock.advance(0.4)
    env.push((100.00, 100.01), (100.01, 100.02))
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["status"] == "closed" and "converged" in t["exit_reason"]
    qty = t["target_qty"]
    gross = qty * (100.00 - 99.72) + qty * (100.30 - 100.02)
    assert t["gross_quote"] == pytest.approx(gross, rel=1e-6)
    fees = qty * (99.72 * 0.001 + 100.30 * 0.0005 + 100.00 * 0.001 + 100.02 * 0.0005)
    assert t["fees_quote"] == pytest.approx(fees, rel=1e-6)
    assert t["net_quote"] == pytest.approx(gross - fees, rel=1e-6)
    s = env.sc.stats()
    assert s["pairs_closed"] == 1 and s["net_pnl_quote"] == pytest.approx(gross - fees, rel=1e-6)


def test_no_exit_before_the_basis_moved_in_favour():
    env = Env(basis_exit_basis_pct=0.5)  # the entry basis (~0.58 %) is barely above the target
    enter(env)
    env.clock.advance(0.4)
    env.sc.tick()
    env.push((99.70, 99.72), (100.25, 100.32))
    env.sc.tick()
    assert env.sc.trades()[0]["status"] == "open"


def test_max_adverse_basis_move_is_recorded():
    env = Env()
    enter(env)
    env.clock.advance(0.4)
    env.sc.tick()
    entry = env.sc.trades()[0]["entry_basis_pct"]
    env.push((99.50, 99.52), (100.50, 100.52))  # basis widens against the pair
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["max_adverse_pct"] == pytest.approx((100.52 / 99.50 - 1) * 100 - entry)


def test_funding_is_received_by_the_short_perp_at_the_funding_time():
    env = Env()
    enter(env)
    env.clock.advance(0.4)
    env.sc.tick()
    t = env.sc.trades()[0]
    env.sc.set_funding("okx", "BTC", 0.0001, 8.0, env.clock.wall() + 5.0)
    env.clock.advance(6.0)
    env.push((99.70, 99.72), (100.30, 100.32))
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["funding_quote"] == pytest.approx(0.0001 * t["perp_qty"] * 100.31)
    assert len(t["funding_applied"]) == 1


def test_unknown_funding_is_flagged_not_guessed():
    env = Env()
    enter(env)
    env.clock.advance(0.4)
    env.sc.tick()
    env.sc.tick()
    t = env.sc.trades()[0]
    assert t["funding_unknown"] is True and t["funding_quote"] == 0.0


def test_one_pair_per_market_at_a_time():
    env = Env(basis_mode="any")
    enter(env)
    env.push((100.0, 100.02), (100.0, 100.02))  # conditions reset
    env.push((99.70, 99.72), (100.30, 100.32))  # fire again while the first pair is still pending
    assert env.sc.stats()["entries_attempted"] == 1 and env.sc.stats()["signals_while_open"] >= 1


def test_trades_survive_a_restart_and_open_ones_are_abandoned():
    env = Env()
    enter(env)
    env.clock.advance(0.4)
    env.sc.tick()
    env.sc.flush()
    again = BasisScanner(env.s, env.clock, db=env.db)
    [t] = again.trades()
    assert t["status"] == "abandoned" and "restarted" in t["exit_reason"]
    assert again.stats()["entries_attempted"] == 1 and again.stats()["pairs_executed"] == 0


# ---- runner: routing, contract size, funding ------------------------------------------------------
class FakeLeg:
    def __init__(self, cfg, bus, clock, contract=1.0):
        self.cfg, self.name, self.bus, self.clock, self.contract = cfg, cfg.name, bus, clock, contract
        self.started = []

    async def start(self, assets, ws_assets=None):
        self.started = list(assets)

    async def stop(self):
        pass

    def symbol_for(self, asset):
        return f"{asset}/USDT" if self.cfg.market_type == "spot" else f"{asset}/USDT:USDT"

    def contract_size(self, symbol):
        return self.contract

    async def fees(self, symbol):
        from latbot.models import Fees
        return Fees(maker=0.0, taker=0.0004 if self.cfg.market_type == "swap" else 0.0008)

    async def fetch_funding_rate(self, symbol):
        return {"rate": 0.0002, "interval_h": 8.0, "next_ts": 2_000_000_000.0}


async def test_runner_routes_private_books_scales_contracts_and_polls_funding():
    clock = SimClock()
    s = settings(basis_venues=["okx"], basis_assets=["BTC"])
    holder = {}

    def factory(cfg, creds):
        holder[cfg.name] = FakeLeg(cfg, None, clock, contract=0.01 if cfg.market_type == "swap" else 1.0)
        return holder[cfg.name]

    r = BasisRunner(s, clock, leg_factory=factory)
    await r._start_venue("okx", r.legs["okx"], ["BTC"])
    from latbot import events
    await r.bus.publish(events.BOOK, book(clock, "okx", "spot", 99.99, 100.01))
    perp = book(clock, "okx", "perp", 100.09, 100.11, size=100.0)  # 100 contracts of 0.01 BTC
    await r.bus.publish(events.BOOK, perp)
    [p] = r.scanner.pairs()
    assert p["ok"] and p["taker"] == {"spot": 0.0008, "perp": 0.0004}
    assert r.scanner._pairs[("okx", "BTC")].books["perp"].bids[0].size == pytest.approx(1.0)
    assert await r.poll_funding_once() == 1
    assert r.scanner._pairs[("okx", "BTC")].funding_rate == pytest.approx(0.0002)


def test_leg_configs_are_public_and_pin_one_quote():
    cfgs = leg_configs("kraken")
    assert cfgs["spot"].ccxt_name == "kraken" and cfgs["perp"].ccxt_name == "krakenfutures"
    assert cfgs["perp"].market_type == "swap" and cfgs["spot"].role.value == "signal"  # no keys, never orders
    assert leg_configs("nowhere") is None


def test_api_reports_the_basis_scanner():
    from fastapi.testclient import TestClient

    from latbot.api.server import create_app

    class Ctx:
        basis = None

    app = create_app(Ctx())  # type: ignore[arg-type]
    assert TestClient(app).get("/api/basis").json()["enabled"] is False
