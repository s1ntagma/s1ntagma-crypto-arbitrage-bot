"""Momentum on BTC / ETH perpetuals (virtual): ride a 15-minute move that agrees with the 1-hour trend."""
from __future__ import annotations

import pytest

from latbot.basis.momentum import Momentum
from latbot.basis.scanner import BasisScanner
from latbot.clock import SimClock
from latbot.models import BookLevel, OrderBook, Timestamps
from latbot.storage.db import Database
from test_basis import settings

SPREAD = 0.01  # half spread in price units around the mid


class M:
    def __init__(self, db=None, clock=None, **over):
        base = dict(mom_enabled=True, mom_assets=["BTC"], mom_venues=["okx", "mexc"], mom_notional_quote=1000.0,
                    mom_window_s=900.0, mom_entry_pct=0.6, mom_trend_s=3600.0, mom_trend_pct=0.3,
                    mom_stop_pct=0.5, mom_trail_pct=0.4, mom_max_hold_s=3600.0, mom_cooldown_s=900.0,
                    mom_stale_s=60.0, carry_enabled=False, xfund_enabled=False,
                    basis_fut_upper_pct=50.0, basis_spot_lower_pct=50.0)
        base.update(over)
        self.clock = clock or SimClock()
        self.db = db if db is not None else Database(":memory:", clock=self.clock)
        self.s = settings(**base)
        self.sc = BasisScanner(self.s, self.clock, db=self.db)
        for v in ("okx", "mexc"):
            self.sc.register_pair(v, "BTC", "BTC/USDT", "BTC/USDT:USDT")
            self.sc.set_fee(v, "BTC", "perp", 0.0005)
        self.m = Momentum(self.sc, self.s, self.clock, db=self.db)
        self.mid = 100.0

    def book(self, venue, mid, size=1000.0):
        ts = Timestamps(recv_ts=self.clock.wall(), recv_mono=self.clock.mono())
        bids = [BookLevel(mid - SPREAD - i * 0.01, size) for i in range(3)]
        asks = [BookLevel(mid + SPREAD + i * 0.01, size) for i in range(3)]
        return OrderBook(exchange=f"{venue}.perp", symbol="BTC/USDT:USDT", bids=bids, asks=asks, asset="BTC", ts=ts)

    def step(self, mid=None, venue="okx", seconds=1.0):
        self.clock.advance(seconds)
        if mid is not None:
            self.mid = mid
            self.sc._pairs[(venue, "BTC")].books["perp"] = self.book(venue, mid)
        self.m.tick()

    def path(self, start, end, seconds, venue="okx"):
        n = int(seconds)
        for i in range(1, n + 1):
            self.step(start + (end - start) * i / n, venue)

    def trades(self):
        return self.m.trades()


def warm(m, seconds=3000, mid=100.0):
    m.path(mid, mid, seconds)


def test_a_15_minute_move_with_the_hour_trend_opens_a_long():
    m = M()
    warm(m)
    m.path(100.0, 100.8, 600)  # +0.8 % in 10 min, the hour is +0.8 % too
    [t] = m.trades()
    assert t["side"] == "long" and t["status"] == "open" and t["venue"] == "okx"
    assert t["move_window_pct"] >= 0.6 and t["move_trend_pct"] >= 0.3
    assert 100.6 < t["entry_px"] < 100.7  # bought on the ask as soon as the 15-minute move reached 0.6 %


def test_no_hour_trend_means_no_entry():
    m = M(mom_trend_pct=1.5)
    warm(m)
    m.path(100.0, 100.8, 600)
    assert m.trades() == []


def test_a_fall_opens_a_short():
    m = M()
    warm(m)
    m.path(100.0, 99.2, 600)
    [t] = m.trades()
    assert t["side"] == "short" and t["status"] == "open"


def test_the_trailing_stop_locks_in_a_profit_after_fees():
    m = M()
    warm(m)
    m.path(100.0, 100.8, 600)
    m.path(100.8, 102.0, 300)  # keeps going
    m.path(102.0, 101.4, 60)  # pulls back 0.6 % > trail 0.4 %
    t = m.trades()[0]
    assert t["status"] == "closed" and t["exit_reason"].startswith("trailing")
    assert t["net_quote"] > 0
    assert t["net_quote"] == pytest.approx(t["cash_quote"] - t["fees_quote"] + t["funding_quote"])
    assert t["fees_quote"] == pytest.approx(0.0005 * (t["entry_qty"] * t["entry_px"] + t["entry_qty"] * t["exit_px"]), rel=1e-6)


def test_the_hard_stop_cuts_a_loss():
    m = M()
    warm(m)
    m.path(100.0, 100.8, 600)
    entry = m.trades()[0]["entry_px"]
    m.path(100.8, 99.9, 30)  # below entry (~100.61) - 0.5 %
    t = m.trades()[0]
    assert t["status"] == "closed" and t["exit_reason"].startswith("stop")
    assert t["exit_px"] <= entry * (1 - 0.005) + 0.02 and t["net_quote"] < 0


def test_cooldown_and_one_position_per_coin():
    m = M()
    warm(m)
    m.path(100.0, 100.8, 600)
    m.path(100.8, 101.9, 400)  # still trending: no second position while one is open
    assert len(m.trades()) == 1
    m.path(101.9, 101.3, 30)  # trailing exit
    assert m.trades()[0]["status"] == "closed"
    m.path(101.3, 101.3, 120)
    m.path(101.3, 102.5, 200)  # a new move inside the cooldown: nothing
    assert len(m.trades()) == 1


def test_a_stale_book_gives_no_entry_and_a_stale_position_closes_as_no_data():
    m = M()
    warm(m)
    m.clock.advance(5.0)
    m.m.tick()  # okx book 5 s old: stale, nothing is sampled
    m.path(100.0, 100.8, 600)
    [t] = m.trades()
    for _ in range(70):
        m.step(None)  # no book updates any more
    t = m.trades()[0]
    assert t["status"] == "no_data" and t["net_quote"] is None
    assert m.m.stats()["closed"] == 0 and m.m.stats()["no_data"] == 1


def test_two_venues_are_never_mixed_into_one_move():
    m = M()
    warm(m, seconds=1000)
    for _ in range(70):
        m.step(None)  # okx goes silent
    m.step(102.0, venue="mexc")  # mexc is 2 % higher: a restart of the series, not a +2 % move
    st = m.m.status()[0]
    assert st["venue"] == "mexc" and st["move_window_pct"] is None and m.trades() == []


def test_a_pending_entry_is_dropped_after_a_restart(tmp_path):
    clock = SimClock()
    db = Database(str(tmp_path / "m.sqlite3"), clock=clock)
    m = M(db=db, clock=clock)
    warm(m)
    i = 0
    while not m.trades():  # stop at the tick the signal fires: the order is not filled yet
        i += 1
        m.step(100.0 + 0.8 * i / 600)
    assert [t["status"] for t in m.trades()] == ["pending"]
    again = Momentum(m.sc, m.s, clock, db=db)
    [t] = again.trades()
    assert t["status"] == "abandoned" and "restarted" in t["exit_reason"]


def test_off_means_nothing_happens():
    m = M(mom_enabled=False)
    warm(m)
    m.path(100.0, 100.8, 600)
    assert m.trades() == []


def test_a_silent_venue_does_not_reset_the_other_venues_history():
    m = M()
    for i in range(3000):  # both venues warm up in parallel (mexc 2 % higher: never mixed with okx)
        m.clock.advance(1.0)
        m.sc._pairs[("okx", "BTC")].books["perp"] = m.book("okx", 100.0)
        m.sc._pairs[("mexc", "BTC")].books["perp"] = m.book("mexc", 102.0)
        m.m.tick()
    for i in range(1, 601):  # okx goes silent; mexc rises 0.8 % in 10 minutes
        m.clock.advance(1.0)
        m.sc._pairs[("mexc", "BTC")].books["perp"] = m.book("mexc", 102.0 * (1 + 0.008 * i / 600))
        m.m.tick()
    [t] = m.trades()
    assert t["venue"] == "mexc" and t["side"] == "long" and t["status"] == "open"
    assert 0.6 <= t["move_window_pct"] < 0.7  # mexc's own move, not a jump from okx's 100 to 102
    st = m.m.status()[0]
    assert st["venue"] == "mexc" and st["warm_venues"] == 1  # okx restarted its own series only


def test_the_first_configured_venue_that_is_warm_and_fresh_gives_the_signal():
    m = M()
    for i in range(3000):
        m.clock.advance(1.0)
        m.sc._pairs[("okx", "BTC")].books["perp"] = m.book("okx", 100.0)
        m.sc._pairs[("mexc", "BTC")].books["perp"] = m.book("mexc", 102.0)
        m.m.tick()
    for i in range(1, 601):
        m.clock.advance(1.0)
        m.sc._pairs[("okx", "BTC")].books["perp"] = m.book("okx", 100.0 * (1 + 0.008 * i / 600))
        m.sc._pairs[("mexc", "BTC")].books["perp"] = m.book("mexc", 102.0 * (1 + 0.008 * i / 600))
        m.m.tick()
    [t] = m.trades()  # one position per coin, on the first venue of mom_venues
    assert t["venue"] == "okx"


def test_an_open_position_shows_its_result_if_closed_now_and_the_balance():
    m = M()
    warm(m)
    m.path(100.0, 100.8, 600)
    m.path(100.8, 101.0, 30)
    t = m.trades()[0]
    q = t["qty"]
    expected = q * ((101.0 - SPREAD) - t["entry_px"]) - t["fees_quote"] - q * (101.0 - SPREAD) * 0.0005
    assert t["status"] == "open" and t["open_net_quote"] == pytest.approx(expected, abs=1e-6)
    b = m.m.balance()
    assert b["open_n"] == 1 and b["open_quote"] == pytest.approx(expected, abs=1e-6)
    assert b["in_use_quote"] == pytest.approx(q * t["entry_px"])


def test_an_open_position_resumes_after_a_restart_and_its_stop_still_works(tmp_path):
    clock = SimClock()
    db = Database(str(tmp_path / "m.sqlite3"), clock=clock)
    m = M(db=db, clock=clock)
    warm(m)
    m.path(100.0, 100.8, 600)
    entry = m.trades()[0]["entry_px"]
    m.m = Momentum(m.sc, m.s, clock, db=db)  # the bot restarts
    [t] = m.trades()
    assert t["status"] == "open" and t["resumed"]
    m.path(100.8, 99.9, 30)  # through the stop
    t = m.trades()[0]
    assert t["status"] == "closed" and t["exit_reason"].startswith("stop") and t["exit_px"] < entry


def test_a_resumed_position_past_its_max_hold_closes_at_the_first_fresh_price(tmp_path):
    clock = SimClock()
    db = Database(str(tmp_path / "m.sqlite3"), clock=clock)
    m = M(db=db, clock=clock)
    warm(m)
    m.path(100.0, 100.8, 600)
    clock.advance(2 * 3600.0)  # the bot was off for two hours
    m.m = Momentum(m.sc, m.s, clock, db=db)
    m.step(100.7)
    t = m.trades()[0]
    assert t["status"] == "closed" and t["exit_reason"] == "max hold"
