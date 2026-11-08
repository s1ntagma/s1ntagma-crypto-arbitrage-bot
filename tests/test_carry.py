"""Funding carry (virtual): long spot + short perp on one venue while funding pays the short.
Entry on a confirmed positive rate whose edge over the horizon beats the round trip; exit when the rate
turns, or after the max hold.  Research only: nothing here can send an order."""
from __future__ import annotations

import pytest

from latbot.basis.scanner import BasisScanner
from latbot.clock import SimClock
from latbot.storage.db import Database
from test_basis import book, settings

SPOT, PERP = (99.99, 100.01), (100.04, 100.06)


class Carry:
    def __init__(self, assets=("BTC",), db=None, clock=None, **over) -> None:
        base = dict(basis_mode="both", carry_enabled=True, carry_notional_quote=500.0, carry_horizon_h=72.0,
                    carry_min_edge_pct=0.10, carry_exit_rate=0.0, carry_confirm_polls=2, carry_max_hold_h=336.0,
                    carry_max_positions=5, basis_fut_upper_pct=50.0, basis_spot_lower_pct=50.0)
        base.update(over)
        self.clock = clock or SimClock()
        self.db = db if db is not None else Database(":memory:", clock=self.clock)
        self.s = settings(**base)
        self.sc = BasisScanner(self.s, self.clock, db=self.db)
        self.assets = list(assets)
        for a in self.assets:
            self.sc.register_pair("okx", a, f"{a}/USDT", f"{a}/USDT:USDT")
            self.sc.set_fee("okx", a, "spot", 0.001)
            self.sc.set_fee("okx", a, "perp", 0.0005)

    def push(self, spot=SPOT, perp=PERP):
        for a in self.assets:
            for leg, (bid, ask) in (("spot", spot), ("perp", perp)):
                b = book(self.clock, "okx", leg, bid, ask)
                b.asset = a
                b.symbol = f"{a}/USDT" if leg == "spot" else f"{a}/USDT:USDT"
                self.sc.on_book("okx", leg, b)

    def poll(self, rates: dict[str, float | None], interval_h: float = 8.0):
        nxt = self.clock.wall() + 3600.0
        for a, r in rates.items():
            self.sc.set_funding("okx", a, r, interval_h, nxt)
        self.push()  # the live edge on fresh books
        self.sc.carry_review()  # approves the best pairs
        self.push()  # an approved pair enters on its next fresh books

    def run(self, seconds: float = 1.0):
        self.clock.advance(seconds)
        self.push()
        self.sc.tick()

    def carry(self):
        return [t for t in self.sc.trades() if t["trigger"] == "carry"]


def test_edge_is_funding_over_the_horizon_minus_fees_and_the_crossing_cost():
    c = Carry()
    c.poll({"BTC": 0.0005})
    [p] = c.sc.pairs()
    entry = (100.04 / 100.01 - 1) * 100
    exit_ = (100.06 / 99.99 - 1) * 100
    funding = 0.0005 * 100 * 72 / 8
    assert p["carry_edge_pct"] == pytest.approx(funding - 2 * (0.1 + 0.05) - (exit_ - entry))


def test_one_positive_poll_is_not_enough():
    c = Carry()
    c.poll({"BTC": 0.0005})
    assert c.carry() == []
    c.poll({"BTC": 0.0005})
    [t] = c.carry()
    c.run(0.5)
    t = c.carry()[0]
    assert t["status"] == "open" and t["hedged"] and t["spot_qty"] == pytest.approx(500 / 100.0)


def test_a_rate_too_small_for_the_round_trip_never_enters():
    c = Carry()
    c.poll({"BTC": 0.00001})
    c.poll({"BTC": 0.00001})
    assert c.carry() == [] and c.sc.pairs()[0]["carry_edge_pct"] < 0.10


def test_a_negative_rate_in_the_window_blocks_the_entry():
    c = Carry()
    c.poll({"BTC": -0.0005})
    c.poll({"BTC": 0.0005})
    assert c.carry() == []


def test_max_positions_take_the_highest_edge_first():
    c = Carry(assets=("BTC", "ETH", "SOL"), carry_max_positions=2)
    rates = {"BTC": 0.0003, "ETH": 0.0009, "SOL": 0.0006}
    c.poll(rates)
    c.poll(rates)
    assert sorted(t["asset"] for t in c.carry()) == ["ETH", "SOL"]


def test_funding_accrues_to_the_short_and_a_turned_rate_closes_the_position():
    c = Carry()
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": 0.0005})
    c.run(0.5)
    c.clock.advance(3700.0)  # through the funding time
    c.run(0.1)
    t = c.carry()[0]
    assert t["funding_quote"] == pytest.approx(0.0005 * 5.0 * 100.05, rel=1e-3)
    c.poll({"BTC": -0.0001})
    c.run(0.1)
    assert c.carry()[0]["status"] == "open"  # one negative poll: not yet
    c.poll({"BTC": -0.0001})
    c.run(0.5)
    t = c.carry()[0]
    assert t["status"] == "closed" and "funding" in t["exit_reason"]
    assert t["net_quote"] == pytest.approx(t["cash_quote"] - t["fees_quote"] + t["funding_quote"])
    st = c.sc.stats()["carry"]
    assert st["closed"] == 1 and st["open"] == 0 and st["funding_quote"] == pytest.approx(t["funding_quote"])


def test_carry_ignores_the_basis_exit_rules_and_holds_until_max_hold():
    c = Carry(carry_max_hold_h=24.0)
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": 0.0005})
    c.run(0.5)
    for _ in range(5):  # 1 h basis max hold and convergence do not apply
        c.run(3600.0)
    assert c.carry()[0]["status"] == "open"
    c.run(20 * 3600.0)
    c.run(0.5)
    t = c.carry()[0]
    assert t["status"] == "closed" and t["exit_reason"] == "max hold"


def test_one_leg_entry_is_an_incident_not_a_carry_position():
    c = Carry()
    c.poll({"BTC": 0.0005})
    c.sc.set_funding("okx", "BTC", 0.0005, 8.0, c.clock.wall() + 3600.0)
    c.sc.carry_review()  # opens on the current books
    c.clock.advance(0.5)
    c.sc.on_book("okx", "spot", book(c.clock, "okx", "spot", *SPOT))  # only the spot book stays fresh
    c.clock.advance(1.8)
    c.sc.on_book("okx", "spot", book(c.clock, "okx", "spot", *SPOT))
    c.sc.tick()
    t = c.carry()[0]
    assert t["status"] in ("one_leg", "no_fill") and not t["hedged"]
    assert c.sc.stats()["carry"]["open"] == 0


def test_a_restart_resumes_an_open_carry_position():
    clock = SimClock()
    db = Database(":memory:", clock=clock)
    c = Carry(db=db, clock=clock)
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": 0.0005})
    c.run(0.5)
    c.sc.flush()
    again = BasisScanner(c.s, clock, db=db)
    [t] = [t for t in again.trades() if t["trigger"] == "carry"]
    assert t["status"] == "open" and t["funding_unknown"]  # funding while the bot was down is not counted
    assert again.stats()["carry"]["open"] == 1


def test_carry_off_opens_nothing_and_basis_signals_are_unaffected():
    c = Carry(carry_enabled=False)
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": 0.0005})
    assert c.carry() == []


def test_funding_samples_are_stored_for_analysis():
    c = Carry()
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": None})  # unknown: not stored, but it breaks the confirmation run
    c.poll({"BTC": 0.0004})
    assert [r["rate"] for r in c.db.list_funding_samples("okx", "BTC")] == [0.0005, 0.0004]
    assert c.carry() == []


def test_an_approved_pair_enters_only_on_fresh_books_and_revalidates_the_edge():
    c = Carry()
    c.poll({"BTC": 0.0005})
    c.sc.set_funding("okx", "BTC", 0.0005, 8.0, c.clock.wall() + 3600.0)
    c.sc.carry_review()
    assert c.sc.pairs()[0]["carry_approved"] and c.carry() == []  # no entry from the poll itself
    c.sc.set_funding("okx", "BTC", 0.00001, 8.0, c.clock.wall() + 3600.0)  # edge gone before the next book
    c.push()
    assert c.carry() == [] and "fell" in c.sc.pairs()[0]["carry_reject"]
