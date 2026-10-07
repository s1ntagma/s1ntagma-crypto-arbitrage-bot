"""Offline tests for latbot.execution.risk.RiskManager."""
from __future__ import annotations

import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import Settings
from latbot.events import EventBus
from latbot.execution.risk import RiskManager
from latbot.models import Mode, OppState, TradeRecord
from latbot.storage.db import Database

T0 = 1_700_000_000.0  # 2023-11-14T22:13:20Z
DAY = 86400.0


def make_settings(**over) -> Settings:
    base = dict(
        total_capital_quote=1000.0,
        max_risk_per_trade_pct=10.0,  # -> $100 cap
        max_trade_quote=200.0,
        max_simultaneous_trades=2,
        max_daily_loss_quote=50.0,
        max_exposure_per_exchange_quote=500.0,
        max_exposure_per_asset_quote=300.0,
        min_trade_quote=10.0,
    )
    base.update(over)
    return Settings(**base)


def make_trade(trade_id: str, pnl: float, finished_ts: float, mode: Mode = Mode.PAPER) -> TradeRecord:
    return TradeRecord(id=trade_id, opportunity_id="opp", mode=mode, exchange="bybit", asset="BTC", symbol="BTC/USDT",
                       direction=1, capital_quote=100.0, expected_net_pct=1.0, expected_profit_quote=1.0,
                       expected_entry_price=100.0, expected_exit_price=101.0, state=OppState.CLOSED,
                       realized_pnl_quote=pnl, started_ts=finished_ts - 5, finished_ts=finished_ts)


@pytest.fixture
def clock() -> SimClock:
    return SimClock(wall=T0, mono=1000.0)


@pytest.fixture
def db(clock: SimClock) -> Database:
    d = Database(":memory:", clock=clock)
    yield d
    d.close()


def make_risk(settings: Settings | None = None, db: Database | None = None, clock: SimClock | None = None,
              bus: EventBus | None = None) -> RiskManager:
    return RiskManager(settings or make_settings(), db, clock or SimClock(wall=T0, mono=1000.0), bus)


# --------------------------------------------------------------------------- #
# allowed capital = min(...)
# --------------------------------------------------------------------------- #
def test_check_ok_when_nothing_binds() -> None:
    r = make_risk()
    ok, reason, allowed = r.check("bybit", "BTC", 80.0, Mode.PAPER)
    assert ok and allowed == 80.0 and reason == "OK"


def test_cap_by_max_risk_per_trade() -> None:
    r = make_risk(make_settings(max_risk_per_trade_pct=2.0))  # 1000 * 2 % = 20
    ok, reason, allowed = r.check("bybit", "BTC", 100.0, Mode.PAPER)
    assert ok and allowed == 20.0
    assert "max risk per trade" in reason and "$100.00" in reason and "$20.00" in reason


def test_cap_by_max_trade_quote() -> None:
    r = make_risk(make_settings(max_trade_quote=50.0))
    ok, reason, allowed = r.check("bybit", "BTC", 100.0, "paper")
    assert ok and allowed == 50.0 and "max trade size" in reason


def test_cap_by_exchange_exposure() -> None:
    r = make_risk(make_settings(max_exposure_per_exchange_quote=150.0, max_exposure_per_asset_quote=1000.0, max_simultaneous_trades=5))
    r.register_open("t1", "bybit", "ETH", 100.0)
    ok, reason, allowed = r.check("bybit", "BTC", 100.0, Mode.PAPER)
    assert ok and allowed == 50.0 and "exchange exposure" in reason
    # a different exchange is not affected
    ok, reason, allowed = r.check("kraken", "BTC", 100.0, Mode.PAPER)
    assert ok and allowed == 100.0


def test_cap_by_asset_exposure() -> None:
    r = make_risk(make_settings(max_exposure_per_asset_quote=130.0, max_simultaneous_trades=5))
    r.register_open("t1", "kraken", "BTC", 100.0)
    ok, reason, allowed = r.check("bybit", "BTC", 100.0, Mode.PAPER)
    assert ok and allowed == 30.0 and "asset exposure" in reason
    ok, _, allowed = r.check("bybit", "ETH", 100.0, Mode.PAPER)
    assert ok and allowed == 100.0


def test_below_min_trade_quote_fails() -> None:
    r = make_risk(make_settings(max_exposure_per_exchange_quote=108.0, max_simultaneous_trades=5))
    r.register_open("t1", "bybit", "ETH", 100.0)
    ok, reason, allowed = r.check("bybit", "BTC", 100.0, Mode.PAPER)
    assert not ok
    assert allowed == pytest.approx(8.0)
    assert reason == "Allowed capital $8.00 is below the minimum trade size $10.00 (limited by exchange exposure)"


def test_exposure_fully_used_fails_not_negative() -> None:
    r = make_risk(make_settings(max_exposure_per_asset_quote=100.0, max_simultaneous_trades=5))
    r.register_open("t1", "bybit", "BTC", 150.0)
    ok, reason, allowed = r.check("kraken", "BTC", 50.0, Mode.PAPER)
    assert not ok and allowed == 0.0 and "asset exposure" in reason


def test_non_positive_capital_fails() -> None:
    r = make_risk()
    ok, reason, allowed = r.check("bybit", "BTC", 0.0, Mode.PAPER)
    assert not ok and allowed == 0.0 and "positive" in reason


# --------------------------------------------------------------------------- #
# exposure bookkeeping
# --------------------------------------------------------------------------- #
def test_register_open_close_and_exposure() -> None:
    r = make_risk()
    assert r.open_trades() == 0 and r.exposure() == 0.0
    r.register_open("t1", "bybit", "BTC", 100.0)
    r.register_open("t2", "kraken", "BTC", 40.0)
    r.register_open("t3", "bybit", "ETH", 25.0)
    assert r.open_trades() == 3
    assert r.exposure() == 165.0
    assert r.exposure(exchange="bybit") == 125.0
    assert r.exposure(asset="BTC") == 140.0
    assert r.exposure(exchange="bybit", asset="BTC") == 100.0
    assert r.exposure(exchange="okx") == 0.0
    r.register_close("t1", 3.0)
    assert r.open_trades() == 2 and r.exposure(exchange="bybit") == 25.0
    r.register_close("unknown", -1.0)  # tolerated
    assert r.open_trades() == 2
    assert {p["trade_id"] for p in r.open_positions()} == {"t2", "t3"}


def test_max_simultaneous_trades() -> None:
    r = make_risk(make_settings(max_simultaneous_trades=2))
    r.register_open("t1", "bybit", "BTC", 10.0)
    ok, _, _ = r.check("bybit", "ETH", 50.0, Mode.PAPER)
    assert ok
    r.register_open("t2", "bybit", "ETH", 10.0)
    ok, reason, allowed = r.check("bybit", "SOL", 50.0, Mode.PAPER)
    assert not ok and allowed == 0.0
    assert reason == "Too many simultaneous trades (2 of 2 open)"
    r.register_close("t1", 0.0)
    assert r.check("bybit", "SOL", 50.0, Mode.PAPER)[0]


# --------------------------------------------------------------------------- #
# daily loss
# --------------------------------------------------------------------------- #
def test_daily_pnl_in_memory_and_limit(clock: SimClock) -> None:
    r = make_risk(clock=clock)
    r.register_open("t1", "bybit", "BTC", 100.0)
    r.register_close("t1", -30.0)
    assert r.daily_pnl(Mode.PAPER) == pytest.approx(-30.0)
    assert r.daily_pnl(Mode.LIVE) == 0.0
    assert r.check("bybit", "BTC", 50.0, Mode.PAPER)[0]
    r.register_open("t2", "bybit", "BTC", 100.0)
    r.register_close("t2", -22.1)
    assert r.daily_pnl("paper") == pytest.approx(-52.1)
    ok, reason, allowed = r.check("bybit", "BTC", 50.0, Mode.PAPER)
    assert not ok and allowed == 0.0
    assert reason == "Daily loss limit reached: -52.10 <= -50.00 (paper)"
    # live mode is a separate book
    assert r.check("bybit", "BTC", 50.0, Mode.LIVE)[0]


def test_daily_pnl_combines_db_and_dedupes_by_id(db: Database, clock: SimClock) -> None:
    db.save_trade(make_trade("db1", -20.0, T0 + 5))
    db.save_trade(make_trade("db2", 4.0, T0 + 6))
    db.save_trade(make_trade("old", -100.0, T0 - DAY))  # yesterday: ignored
    db.save_trade(make_trade("live1", -99.0, T0 + 7, mode=Mode.LIVE))
    r = make_risk(db=db, clock=clock)
    assert r.daily_pnl(Mode.PAPER) == pytest.approx(-16.0)
    # the engine persists the trade AND registers the close -> counted once
    r.register_open("db1", "bybit", "BTC", 100.0)
    r.register_close("db1", -20.0)
    assert r.daily_pnl(Mode.PAPER) == pytest.approx(-16.0)
    # a close not (yet) in the db is added
    r.register_close("mem1", -10.0)
    assert r.daily_pnl(Mode.PAPER) == pytest.approx(-26.0)
    assert r.daily_pnl(Mode.LIVE) == pytest.approx(-99.0)


def test_daily_pnl_resets_on_utc_day_boundary(clock: SimClock) -> None:
    r = make_risk(clock=clock)
    r.register_close("t1", -45.0)
    assert r.daily_pnl(Mode.PAPER) == pytest.approx(-45.0)
    clock.advance(2 * 3600)  # 00:13 UTC next day
    assert r.daily_pnl(Mode.PAPER) == 0.0
    assert r.check("bybit", "BTC", 50.0, Mode.PAPER)[0]


# --------------------------------------------------------------------------- #
# emergency
# --------------------------------------------------------------------------- #
async def test_emergency_blocks_and_publishes() -> None:
    bus = EventBus()
    got: list = []
    bus.subscribe(events.EMERGENCY, lambda p: got.append(p))
    r = make_risk(bus=bus)
    assert r.emergency is False and r.emergency_reason == ""
    await r.set_emergency(True, "user pressed stop")
    assert r.emergency is True and r.emergency_reason == "user pressed stop"
    assert got == [{"active": True, "reason": "user pressed stop"}]
    ok, reason, allowed = r.check("bybit", "BTC", 50.0, Mode.PAPER)
    assert not ok and allowed == 0.0
    assert reason == "Emergency stop is active: user pressed stop"
    await r.set_emergency(False)
    assert r.emergency is False and r.emergency_reason == ""
    assert got[-1] == {"active": False, "reason": ""}
    assert r.check("bybit", "BTC", 50.0, Mode.PAPER)[0]


async def test_emergency_without_bus() -> None:
    r = make_risk()
    await r.set_emergency(True)
    assert r.check("bybit", "BTC", 50.0, Mode.PAPER)[1] == "Emergency stop is active"
