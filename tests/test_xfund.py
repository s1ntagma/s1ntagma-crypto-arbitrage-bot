"""Cross-venue funding arbitrage (virtual): long the perp where funding is low, short where it is high."""
from __future__ import annotations

import pytest

from latbot.basis.scanner import BasisScanner
from latbot.basis.xfund import FundingArb
from latbot.clock import SimClock
from latbot.storage.db import Database
from test_basis import book, settings

PERP = (100.00, 100.02)


class X:
    def __init__(self, venues=("okx", "mexc"), assets=("BTC",), db=None, clock=None, **over):
        base = dict(xfund_enabled=True, xfund_notional_quote=500.0, xfund_horizon_h=72.0, xfund_min_edge_pct=0.10,
                    xfund_max_positions=5, xfund_leverage=2.0, xfund_liq_guard_pct=5.0, xfund_exit_horizon_h=24.0,
                    xfund_max_hold_h=336.0, carry_enabled=False, carry_confirm_polls=2,
                    basis_fut_upper_pct=50.0, basis_spot_lower_pct=50.0)
        base.update(over)
        self.clock = clock or SimClock()
        self.db = db if db is not None else Database(":memory:", clock=self.clock)
        self.s = settings(**base)
        self.sc = BasisScanner(self.s, self.clock, db=self.db)
        self.xf = FundingArb(self.sc, self.s, self.clock, db=self.db)
        self.venues, self.assets = list(venues), list(assets)
        for v in self.venues:
            for a in self.assets:
                self.sc.register_pair(v, a, f"{a}/USDT", f"{a}/USDT:USDT")
                self.sc.set_fee(v, a, "perp", 0.0005)
                self.sc.set_fee(v, a, "spot", 0.001)

    def push(self, perp=None):
        perp = perp or {}
        for v in self.venues:
            for a in self.assets:
                for leg, (bid, ask) in (("spot", (99.99, 100.01)), ("perp", perp.get(v, PERP))):
                    b = book(self.clock, v, leg, bid, ask)
                    b.asset, b.symbol = a, (f"{a}/USDT" if leg == "spot" else f"{a}/USDT:USDT")
                    self.sc.on_book(v, leg, b)

    def poll(self, rates, interval_h=8.0, nxt=None):
        nxt = nxt or self.clock.wall() + 3600.0
        for (v, a), r in rates.items():
            self.sc.set_funding(v, a, r, interval_h, nxt)
        self.push()
        self.xf.review()

    def run(self, s=0.5, perp=None):
        self.clock.advance(s)
        self.push(perp)
        self.sc.tick()
        self.xf.tick()

    def trades(self):
        return self.xf.trades()


RATES = {("okx", "BTC"): -0.0002, ("mexc", "BTC"): 0.0006}


def opened(**over):
    x = X(**over)
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.1)  # enters on fresh books
    x.run(0.5)  # both legs fill after the latency
    return x


def test_the_edge_is_the_funding_difference_minus_four_fees_and_the_crossing_cost():
    x = X()
    x.poll(RATES)
    [c] = x.xf.candidates
    assert c["long"] == "okx" and c["short"] == "mexc"
    funding = (0.0006 / 8 - (-0.0002) / 8) * 100 * 72
    fees = 2 * (0.0005 + 0.0005) * 100
    entry, exit_ = (100.00 / 100.02 - 1) * 100, (100.02 / 100.00 - 1) * 100  # same mids: no gap
    assert c["edge_pct"] == pytest.approx(funding - fees - (exit_ - entry))
    assert c["diff_day_pct"] == pytest.approx(0.0008 / 8 * 100 * 24)


def test_one_poll_is_not_enough_and_two_open_a_hedged_pair():
    x = X()
    x.poll(RATES)
    x.run(0.6)
    assert x.trades() == []
    x.poll(RATES)
    x.run(0.1)
    x.run(0.5)
    [t] = x.trades()
    assert t["status"] == "open" and t["hedged"] and t["long_venue"] == "okx" and t["short_venue"] == "mexc"
    assert t["qty"]["long"] == pytest.approx(500 / 100.01) and t["qty"]["short"] == pytest.approx(500 / 100.01)
    assert t["capital_quote"] == pytest.approx(500 / 100.01 * (100.02 + 100.00) / 2)


def test_a_small_difference_never_enters():
    x = X()
    small = {("okx", "BTC"): 0.0001, ("mexc", "BTC"): 0.00012}
    x.poll(small)
    x.poll(small)
    x.run(0.6)
    assert x.trades() == []


def test_the_edge_is_revalidated_on_the_books_of_the_entry():
    x = X()
    x.poll(RATES)
    x.poll(RATES)
    # before the entry tick the books split: buying the long venue now costs 2 %
    x.run(0.1, perp={"okx": (102.0, 102.04)})
    assert x.trades() == [] and x.xf.counters["revalidation_failed"] == 1


def test_funding_settles_on_both_legs():
    x = opened()
    x.clock.advance(3700.0)
    x.run(0.1)
    x.run(0.1)
    t = x.trades()[0]
    # short receives 0.0006 x 5 x 100.01, the long receives 0.0002 x 5 x 100.01 (negative rate)
    assert t["funding_legs"]["short"] == pytest.approx(0.0006 * 5 * 100.01, rel=1e-3)
    assert t["funding_legs"]["long"] == pytest.approx(0.0002 * 5 * 100.01, rel=1e-3)
    assert x.xf.stats()["funding_open_quote"] == pytest.approx(t["funding_quote"])


def test_a_turned_difference_closes_only_when_holding_costs_more_than_leaving():
    x = opened()
    slight = {("okx", "BTC"): 0.0001, ("mexc", "BTC"): 0.00009}
    x.poll(slight)
    x.poll(slight)
    x.run(0.6)
    assert x.trades()[0]["status"] == "open"
    deep = {("okx", "BTC"): 0.003, ("mexc", "BTC"): -0.003}
    x.poll(deep)
    x.poll(deep)
    x.run(0.6)
    t = x.trades()[0]
    assert t["status"] == "closed" and "turned" in t["exit_reason"]
    assert t["net_quote"] == pytest.approx(t["cash_quote"] - t["fees_quote"] + t["funding_quote"])


def test_the_liquidation_guard_closes_both_legs():
    x = opened(xfund_leverage=5.0)
    x.run(0.5, perp={"okx": (100.0, 100.02), "mexc": (118.0, 118.02)})  # short ~1.5 % from its liquidation
    x.run(0.5, perp={"okx": (100.0, 100.02), "mexc": (118.0, 118.02)})
    t = x.trades()[0]
    assert t["status"] == "closed" and t["exit_reason"].startswith("liquidation guard")


def test_one_leg_is_an_incident_not_a_hedged_pair():
    x = X()
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.1)
    x.clock.advance(2.2)  # only okx books arrive: mexc is stale when its leg is due
    for leg, bidask in (("spot", (99.99, 100.01)), ("perp", PERP)):
        b = book(x.clock, "okx", leg, *bidask)
        b.symbol = "BTC/USDT" if leg == "spot" else "BTC/USDT:USDT"
        x.sc.on_book("okx", leg, b)
    x.xf.tick()
    t = x.trades()[0]
    assert t["status"] == "one_leg" and not t["hedged"] and x.xf.stats()["open"] == 0


def test_highest_edge_first_within_max_positions():
    x = X(venues=("okx", "mexc"), assets=("BTC", "ETH", "SOL"), xfund_max_positions=2)
    rates = {("okx", "BTC"): 0.0, ("mexc", "BTC"): 0.0004, ("okx", "ETH"): 0.0, ("mexc", "ETH"): 0.0012,
             ("okx", "SOL"): 0.0, ("mexc", "SOL"): 0.0008}
    x.poll(rates)
    x.poll(rates)
    x.run(0.1)
    assert sorted(t["asset"] for t in x.trades()) == ["ETH", "SOL"]


def test_an_open_pair_survives_a_restart(tmp_path):
    clock = SimClock()
    db = Database(str(tmp_path / "x.sqlite3"), clock=clock)
    x = opened(db=db, clock=clock)
    again = FundingArb(x.sc, x.s, clock, db=db)
    [t] = again.trades()
    assert t["status"] == "open" and t["funding_unknown"] and again.stats()["open"] == 1


def test_off_means_no_entries():
    x = X(xfund_enabled=False)
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.6)
    assert x.trades() == [] and x.xf.candidates == []
