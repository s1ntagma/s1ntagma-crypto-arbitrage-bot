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
    c.poll({"BTC": -0.0001})
    c.run(0.5)
    # slightly negative: 3 payments of 0.01 % (0.03 %) cost less than leaving (2 fees + spread): hold
    assert c.carry()[0]["status"] == "open" and "holding" in c.sc.pairs()[0]["carry_hold_note"]
    c.poll({"BTC": -0.002})  # deeper: the last two polls now cost more than leaving
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
    assert t["liq_price"] is not None and t["capital_quote"] > 0


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


def test_default_venues_are_the_most_traded_and_known_to_the_runner():
    from latbot.basis.runner import VENUES, leg_configs
    from latbot.config import Settings as S

    top = ["binance", "okx", "bybit", "mexc", "gate", "bitget"]
    assert S().basis_venues == top
    assert all(leg_configs(v) is not None for v in top + ["htx", "kraken"])
    assert leg_configs("binance")["perp"].market_type == "swap" and VENUES["mexc"][4] == "USDT"
    # an owner on the old default list moves to the new one; a list he edited himself stays
    assert S.from_dict({"config_version": 7, "basis_venues": ["okx", "bybit", "htx", "kraken"]}).basis_venues == top
    assert S.from_dict({"config_version": 7, "basis_venues": ["htx"]}).basis_venues == ["htx"]


async def test_funding_is_polled_in_one_batch_per_venue_when_the_venue_has_one():
    from latbot.basis.runner import BasisRunner
    from test_basis import FakeLeg

    clock = SimClock()
    single: list[str] = []

    class Batch(FakeLeg):
        async def fetch_funding_rates(self, symbols):
            return {s: {"rate": 0.0003, "interval_h": 4.0, "next_ts": None} for s in symbols if s.startswith("BTC")}

        async def fetch_funding_rate(self, symbol):
            single.append(symbol)
            return {"rate": 0.0001, "interval_h": 8.0, "next_ts": None}

    r = BasisRunner(settings(basis_venues=["okx", "mexc"], basis_assets=["BTC", "ETH"]), clock,
                    leg_factory=lambda cfg, creds: (Batch if cfg.name.startswith("okx") else FakeLeg)(cfg, None, clock))
    for v, legs in r.legs.items():
        await r._start_venue(v, legs, ["BTC", "ETH"])
    assert await r.poll_funding_once() == 4
    assert single == ["ETH/USDT:USDT"]  # okx: BTC came in the batch, ETH asked alone
    assert r.scanner._pairs[("okx", "BTC")].funding_interval_h == 4.0
    assert r.scanner._pairs[("mexc", "ETH")].funding_rate == pytest.approx(0.0002)



def opened(**over):
    c = Carry(**over)
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": 0.0005})
    c.run(0.5)
    return c


def test_capital_is_spot_plus_margin_and_the_apr_is_on_capital():
    c = opened(carry_leverage=2.0)
    t = c.carry()[0]
    assert t["capital_quote"] == pytest.approx(5.0 * 100.01 + 5.0 * 100.04 / 2)
    assert t["liq_price"] == pytest.approx(100.04 * (1 + 0.5 - 0.005))
    assert c.sc.stats()["carry"]["open_capital_quote"] == pytest.approx(t["capital_quote"])


def test_the_liquidation_guard_closes_the_pair_before_the_short_is_liquidated():
    c = opened(carry_leverage=5.0, carry_liq_guard_pct=5.0)  # liq ~ +19.5 %
    c.clock.advance(1.0)
    c.push(spot=(115.99, 116.01), perp=(116.04, 116.06))  # ~3 % to liquidation
    c.sc.tick()
    c.run(0.5)
    t = c.carry()[0]
    assert t["status"] == "closed" and t["exit_reason"].startswith("liquidation guard") and not t["liquidated"]
    assert c.sc.stats()["carry"]["guard_exits"] == 1


def test_a_jump_through_the_liquidation_price_loses_the_margin_and_closes_the_spot():
    c = opened(carry_leverage=5.0, carry_liq_guard_pct=0.0)
    c.clock.advance(1.0)
    c.push(spot=(129.99, 130.01), perp=(130.04, 130.06))
    c.sc.tick()
    c.clock.advance(0.5)
    c.push(spot=(129.99, 130.01), perp=(130.04, 130.06))
    c.sc.tick()
    t = c.carry()[0]
    assert t["liquidated"] and t["status"] == "closed" and t["exit_reason"] == "liquidated"
    margin = 5.0 * 100.04 / 5.0
    # the short lost its whole margin, the spot gained (sold at the bid): net = spot gain - margin - fees
    spot_gain = 5.0 * (129.99 - 100.01)
    assert t["net_quote"] == pytest.approx(spot_gain - margin - t["fees_quote"] + t["funding_quote"], rel=1e-6)
    assert c.sc.stats()["carry"]["liquidations"] == 1


def test_the_rate_estimate_uses_the_settled_history_and_a_spike_does_not_carry_the_edge():
    c = Carry()
    c.sc.set_funding_history("okx", "BTC", [(1_000.0 + i * 28_800, 0.0001) for i in range(21)])
    c.poll({"BTC": 0.003})  # a spike now
    p = c.sc.pairs()[0]
    assert p["carry_rate_est"] == pytest.approx(0.0001) and p["carry_rate_source"] == "history"


def test_coins_whose_funding_flips_often_are_not_confirmed():
    c = Carry()
    c.sc.set_funding_history("okx", "BTC", [(1_000.0 + i * 28_800, 0.002 if i % 2 else -0.0001) for i in range(20)])
    c.poll({"BTC": 0.002})
    c.poll({"BTC": 0.002})
    assert c.carry() == [] and c.sc.pairs()[0]["carry_confirmed"] is False


def test_the_real_funding_interval_comes_from_the_history_when_the_venue_does_not_say():
    c = Carry()
    c.sc.set_funding_history("okx", "BTC", [(1_000.0 + i * 3_600, 0.0001) for i in range(10)])
    assert c.sc._pairs[("okx", "BTC")].funding_interval_h == 1.0
    c.sc.set_funding("okx", "BTC", 0.0001, 8.0)  # the venue's own word wins
    c.sc.set_funding_history("okx", "BTC", [(1_000.0 + i * 3_600, 0.0001) for i in range(10)])
    assert c.sc._pairs[("okx", "BTC")].funding_interval_h == 8.0


def test_polled_rates_survive_a_restart():
    clock = SimClock()
    db = Database(":memory:", clock=clock)
    c = Carry(db=db, clock=clock)
    c.poll({"BTC": 0.0005})
    again = Carry(db=db, clock=clock)
    assert list(again.sc._pairs[("okx", "BTC")].funding_samples) == [0.0005]
    again.poll({"BTC": 0.0005})  # the second positive poll confirms at once
    assert [t["asset"] for t in again.carry()] == ["BTC"]


async def test_the_runner_loads_the_settled_funding_history_of_every_pair():
    from latbot.basis.runner import BasisRunner
    from test_basis import FakeLeg

    clock = SimClock()

    class Hist(FakeLeg):
        async def fetch_funding_history(self, symbol, limit=21):
            return [(1_000.0 + i * 14_400, 0.0002) for i in range(limit)]

    r = BasisRunner(settings(basis_venues=["okx"], basis_assets=["BTC"]), clock,
                    leg_factory=lambda cfg, creds: Hist(cfg, None, clock))
    await r._start_venue("okx", r.legs["okx"], ["BTC"])
    assert await r.poll_history_once() == 1
    p = r.scanner._pairs[("okx", "BTC")]
    assert len(p.funding_history) == 21 and p.funding_interval_h == 4.0


# ---- maker execution: one leg rests as a limit order, the other hedges by market ---------------------
def maker_env(**over):
    c = Carry(carry_exec="maker", maker_wait_s=60.0, **over)
    c.sc.set_fee("okx", "BTC", "spot", 0.001, 0.0008)
    c.sc.set_fee("okx", "BTC", "perp", 0.0005, 0.0002)  # the perp saves more: it is the maker leg
    return c


def approve(c):
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": 0.0005})  # the second poll's last push posts the limit order


def test_maker_edge_counts_one_maker_and_one_taker_fee_and_the_hedge_legs_spread():
    c = maker_env()
    c.poll({"BTC": 0.0005})
    p = c.sc.pairs()[0]
    funding = 0.0005 * 100 * 72 / 8
    fees = 2 * (0.0002 + 0.001) * 100
    spot_spread = (100.01 / 99.99 - 1) * 100
    assert p["carry_maker_leg"] == "perp" and p["carry_fees_pct"] == pytest.approx(fees)
    assert p["carry_edge_pct"] == pytest.approx(funding - fees - spot_spread)
    taker = Carry()
    taker.poll({"BTC": 0.0005})
    assert p["carry_edge_pct"] > taker.sc.pairs()[0]["carry_edge_pct"]


def test_a_touch_is_not_a_fill_and_a_trade_through_fills_then_the_other_leg_hedges():
    c = maker_env()
    approve(c)
    [t] = c.carry()
    assert t["status"] == "pending" and t["orders"]["perp"]["px"] == 100.06 and t["orders"]["perp"]["side"] == -1.0
    for _ in range(5):
        c.run(1.0)  # the ask stays at our price: touched, not traded through
    assert c.carry()[0]["status"] == "pending" and c.carry()[0]["fees_quote"] == 0
    c.run(1.0, )
    c.clock.advance(1.0)
    c.push(perp=(100.07, 100.09))  # the bid went through 100.06: our sell is filled
    c.sc.tick()
    t = c.carry()[0]
    assert t["perp_qty"] == pytest.approx(5.0) and t["entry_perp_vwap"] == 100.06 and t["status"] == "pending"
    c.clock.advance(0.4)
    c.push(perp=(100.07, 100.09))
    c.sc.tick()  # the spot leg hedges by market
    t = c.carry()[0]
    assert t["status"] == "open" and t["hedged"] and t["entry_spot_vwap"] == 100.01
    assert t["fees_quote"] == pytest.approx(5.0 * 100.06 * 0.0002 + 5.0 * 100.01 * 0.001)
    st = c.sc.stats()["carry"]
    assert st["exec"] == "maker" and st["maker_fills"] == 1 and st["taker_hedges"] == 1


def test_an_unfilled_limit_entry_is_cancelled_without_a_trade_or_a_cost():
    c = maker_env()
    approve(c)
    for _ in range(62):
        c.run(1.0)
    assert c.carry() == [] and c.db.list_basis_trades() == []  # counted, never stored
    st = c.sc.stats()["carry"]
    assert st["maker_unfilled"] == 1 and st["incidents"] == 0 and st["open"] == 0


def maker_opened(**over):
    c = maker_env(**over)
    approve(c)
    c.clock.advance(1.0)
    c.push(perp=(100.07, 100.09))
    c.sc.tick()
    c.clock.advance(0.4)
    c.push(perp=(100.07, 100.09))
    c.sc.tick()
    assert c.carry()[0]["status"] == "open"
    return c


def test_a_funding_exit_rests_the_maker_leg_and_falls_back_to_market_after_the_wait():
    c = maker_opened()
    c.poll({"BTC": -0.002})
    c.poll({"BTC": -0.002})
    t = c.carry()[0]
    assert t["status"] == "closing" and t["orders"]["perp"]["side"] == 1.0  # buy the perp back at the bid
    px = t["orders"]["perp"]["px"]
    c.clock.advance(1.0)
    c.push(perp=(px - 0.03, px - 0.01))  # the ask fell through our bid: filled as maker
    c.sc.tick()
    c.clock.advance(0.4)
    c.push(perp=(px - 0.03, px - 0.01))
    c.sc.tick()
    t = c.carry()[0]
    assert t["status"] == "closed" and t["maker_fills"] == 2

    d = maker_opened()
    d.poll({"BTC": -0.002})
    d.poll({"BTC": -0.002})
    for _ in range(62):
        d.run(1.0)
    t = d.carry()[0]
    assert t["status"] == "closed" and t["maker_fills"] == 1  # the exit left by market


def test_the_liquidation_guard_leaves_by_market_even_in_maker_mode():
    c = maker_opened(carry_leverage=5.0, carry_liq_guard_pct=5.0)
    c.clock.advance(1.0)
    c.push(spot=(115.99, 116.01), perp=(116.04, 116.06))
    c.sc.tick()
    assert c.carry()[0]["orders"] == {}
    c.run(0.5)
    t = c.carry()[0]
    assert t["status"] == "closed" and t["exit_reason"].startswith("liquidation guard")


def test_no_maker_saving_means_market_orders():
    c = Carry(carry_exec="maker")  # maker fees unknown: never assumed lower than taker
    c.poll({"BTC": 0.0005})
    c.poll({"BTC": 0.0005})
    c.run(0.5)
    t = c.carry()[0]
    assert t["status"] == "open" and t["maker_fills"] == 0
