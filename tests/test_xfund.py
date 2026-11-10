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


# ---- maker execution ----------------------------------------------------------------------------------
def maker_x(**over):
    x = X(xfund_exec="maker", maker_wait_s=60.0, **over)
    x.sc.set_fee("okx", "BTC", "perp", 0.0005, 0.0002)   # saves 0.03 %
    x.sc.set_fee("mexc", "BTC", "perp", 0.0005, 0.0004)  # saves 0.01 %: okx (the long leg) is the maker leg
    return x


def test_maker_edge_is_one_maker_and_one_taker_fee_plus_the_hedge_venues_spread():
    x = maker_x()
    x.poll(RATES)
    [c] = x.xf.candidates
    funding = (0.0006 / 8 + 0.0002 / 8) * 100 * 72
    fees = 2 * (0.0002 + 0.0005) * 100
    spread = (100.02 / 100.00 - 1) * 100
    assert c["maker_leg"] == "long" and c["fees_pct"] == pytest.approx(fees)
    assert c["edge_pct"] == pytest.approx(funding - fees - spread)
    t = X()
    t.poll(RATES)
    assert c["edge_pct"] > t.xf.candidates[0]["edge_pct"]


def test_the_long_leg_rests_and_fills_on_a_trade_through_then_the_short_hedges():
    x = maker_x()
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.1)
    [t] = x.trades()
    assert t["status"] == "pending" and t["order"]["leg"] == "long" and t["order"]["px"] == 100.00
    for _ in range(5):
        x.run(1.0)  # the bid stays at our price: a touch, no fill
    assert x.trades()[0]["status"] == "pending" and x.trades()[0]["fees_quote"] == 0
    x.run(1.0, perp={"okx": (99.97, 99.99)})  # okx traded down through our bid: bought as maker
    t = x.trades()[0]
    assert t["qty"]["long"] > 0 and t["entry_px"]["long"] == 100.00 and t["status"] == "pending"
    x.run(0.4, perp={"okx": (99.97, 99.99)})  # mexc sells by market
    t = x.trades()[0]
    q = t["qty"]["long"]
    assert t["status"] == "open" and t["hedged"] and t["entry_px"]["short"] == 100.00
    assert t["fees_quote"] == pytest.approx(q * 100.00 * 0.0002 + q * 100.00 * 0.0005)
    st = x.xf.stats()
    assert st["exec"] == "maker" and st["maker_fills"] == 1 and st["taker_hedges"] == 1


def test_an_unfilled_limit_entry_is_no_trade():
    x = maker_x()
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.1)
    for _ in range(62):
        x.run(1.0)
    assert x.trades() == [] and x.db.list_xfund_trades() == []  # counted, never stored
    st = x.xf.stats()
    assert st["maker_unfilled"] == 1 and st["incidents"] == 0 and st["open"] == 0


# ---- Hyperliquid: a perp-only venue quoted in USDC, hourly funding; leverage -------------------------
def hl_env(**over):
    x = X(venues=("okx",), **over)
    x.venues.append("hyperliquid")
    x.sc.register_perp("hyperliquid", "BTC", "BTC/USDC:USDC")
    x.sc.set_fee("hyperliquid", "BTC", "perp", 0.00045, 0.00015)
    return x


def hl_push(x, hl=PERP, okx=PERP):
    for leg, ba in (("spot", (99.99, 100.01)), ("perp", okx)):
        b = book(x.clock, "okx", leg, *ba)
        b.symbol = "BTC/USDT" if leg == "spot" else "BTC/USDT:USDT"
        x.sc.on_book("okx", leg, b)
    b = book(x.clock, "hyperliquid", "perp", *hl)
    b.symbol = "BTC/USDC:USDC"
    x.sc.on_book("hyperliquid", "perp", b)


def hl_poll(x, okx_rate, hl_rate):
    nxt = x.clock.wall() + 3600.0
    x.sc.set_funding("okx", "BTC", okx_rate, 8.0, nxt)
    x.sc.set_funding("hyperliquid", "BTC", hl_rate, 1.0, nxt)
    hl_push(x)
    x.xf.review()


def test_a_perp_only_pair_gives_no_basis_signal_and_no_stale_counts():
    x = hl_env()
    hl_push(x)
    p = [p for p in x.sc.pairs() if p["venue"] == "hyperliquid"][0]
    assert p["perp_only"] and p["quote_ok"] and p.get("basis_pct") is None and p["reason"] == "perp only"
    before = dict(x.sc.counters)
    hl_push(x)
    assert x.sc.counters["stale_evals"] == before["stale_evals"]
    assert x.sc.counters["evaluations"] == before["evaluations"] + 2  # the okx books only


def test_hourly_and_eight_hour_funding_are_compared_per_hour_and_mixed_quotes_are_marked():
    x = hl_env()
    hl_poll(x, okx_rate=0.0001, hl_rate=0.0001)  # the same number, but hyperliquid pays it every hour
    [c] = x.xf.candidates
    assert c["long"] == "okx" and c["short"] == "hyperliquid" and c["quotes"] == "USDT/USDC"
    assert c["diff_day_pct"] == pytest.approx((0.0001 / 1 - 0.0001 / 8) * 100 * 24)
    assert c["fees_pct"] == pytest.approx(2 * (0.0005 + 0.00045) * 100)


def test_the_price_gap_against_the_pair_is_a_cost():
    x = hl_env()
    hl_poll(x, okx_rate=0.0001, hl_rate=0.0001)
    flat = x.xf.candidates[0]["edge_pct"]
    nxt = x.clock.wall() + 3600.0
    x.sc.set_funding("okx", "BTC", 0.0001, 8.0, nxt)
    x.sc.set_funding("hyperliquid", "BTC", 0.0001, 1.0, nxt)
    hl_push(x, hl=(99.90, 99.92))  # the short venue is 0.1 % cheaper than the long one
    x.xf.review()
    assert x.xf.candidates[0]["edge_pct"] == pytest.approx(flat - (100.01 / 99.91 - 1) * 100, abs=1e-4)


def test_leverage_sets_capital_and_liquidation_and_is_clamped():
    from latbot.config import Settings

    s = Settings(xfund_leverage=50.0)
    s.validate()
    assert s.xfund_leverage == 5.0
    s = Settings(xfund_leverage=0.2)
    s.validate()
    assert s.xfund_leverage == 1.0
    x = opened(xfund_leverage=5.0)
    t = x.trades()[0]
    q = t["qty"]["long"]
    assert t["capital_quote"] == pytest.approx(q * (100.02 + 100.00) / 5)
    assert t["liq_price"]["short"] == pytest.approx(100.00 * (1 + 1 / 5 - 0.005))
    assert t["liq_price"]["long"] == pytest.approx(100.02 * (1 - 1 / 5 + 0.005))
    assert x.xf.stats()["liq_distance_pct"] == pytest.approx(19.5)


def test_two_venues_books_received_apart_but_fresh_give_an_executable_edge():
    x = X()
    nxt = x.clock.wall() + 3600.0
    for (v, a), r in RATES.items():
        x.sc.set_funding(v, a, r, 8.0, nxt)
    b = book(x.clock, "okx", "perp", *PERP)
    b.symbol = "BTC/USDT:USDT"
    x.sc.on_book("okx", "perp", b)
    x.clock.advance(1.2)
    b = book(x.clock, "mexc", "perp", *PERP)
    b.symbol = "BTC/USDT:USDT"
    x.sc.on_book("mexc", "perp", b)
    x.xf.review()
    assert x.xf.candidates[0]["edge_pct"] is not None
    x.clock.advance(1.0)  # okx is now 2.2 s old: stale, no executable edge
    x.xf.review()
    assert x.xf.candidates[0]["edge_pct"] is None


def test_training_pairs_are_off_by_default_and_for_the_owner_config():
    from latbot.config import Settings

    assert Settings().basis_train_all_signals is False
    assert Settings.from_dict({"config_version": 9, "basis_train_all_signals": True}).basis_train_all_signals is False
    assert Settings.from_dict({"config_version": 10, "basis_train_all_signals": True}).basis_train_all_signals is True


# ---- a market order waits for the first fresh book (it was sent; we just could not see the price yet) --
def one_book(x, venue, perp=PERP):
    b = book(x.clock, venue, "perp", *perp)
    b.symbol = "BTC/USDT:USDT"
    x.sc.on_book(venue, "perp", b)


def test_a_leg_whose_book_is_stale_fills_on_the_first_fresh_book():
    x = X(fill_wait_s=30.0)
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.1)  # both legs sent
    x.clock.advance(2.5)  # mexc silent: its book is stale when the legs are due
    one_book(x, "okx")
    x.xf.tick()
    t = x.trades()[0]
    assert t["status"] == "pending" and t["qty"]["long"] > 0 and t["qty"]["short"] == 0  # not an incident yet
    x.clock.advance(3.0)
    one_book(x, "okx")
    one_book(x, "mexc", perp=(100.10, 100.12))  # the price it really got, 5 s later
    x.xf.tick()
    t = x.trades()[0]
    assert t["status"] == "open" and t["hedged"] and t["entry_px"]["short"] == 100.10 and t["incident"] == ""


def test_no_fresh_book_for_the_whole_wait_is_an_incident_and_the_unwind_waits_too():
    x = X(fill_wait_s=30.0)
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.1)
    x.clock.advance(2.5)  # mexc silent: stale when its leg is due
    one_book(x, "okx")
    x.xf.tick()  # the long fills, mexc gives nothing
    for _ in range(32):
        x.clock.advance(1.0)
        x.xf.tick()  # no book from either venue any more
    t = x.trades()[0]
    assert t["status"] == "closing" and t["pending_unwind"]  # one leg: its unwind waits for a price
    for _ in range(32):
        x.clock.advance(1.0)
        x.xf.tick()
    t = x.trades()[0]
    assert t["status"] == "one_leg" and t["unwind_unknown"] and t["net_quote"] is None  # never booked at the entry price
    assert x.xf.stats()["unknown"] == 1 and x.xf.stats()["net_quote"] == 0


def test_the_unwind_of_a_single_leg_uses_the_first_fresh_book():
    x = X(fill_wait_s=30.0)
    x.poll(RATES)
    x.poll(RATES)
    x.run(0.1)
    x.clock.advance(2.5)  # mexc silent: stale when its leg is due
    one_book(x, "okx")
    x.xf.tick()
    for _ in range(32):
        x.clock.advance(1.0)
        x.xf.tick()
    one_book(x, "okx", perp=(99.50, 99.52))  # the long is sold 0.5 % lower: a real, known loss
    x.xf.tick()
    t = x.trades()[0]
    q = 500 / 100.01
    assert t["status"] == "one_leg" and not t["unwind_unknown"]
    assert t["net_quote"] == pytest.approx(q * (99.50 - 100.02) - t["fees_quote"])
