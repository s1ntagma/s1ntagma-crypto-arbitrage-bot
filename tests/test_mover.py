"""Offline tests for latbot.engine.mover.MoveDetector."""
from __future__ import annotations

import asyncio

import pytest

from latbot import events
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Settings
from latbot.engine.mover import MoveDetector
from latbot.engine.stats import StatsStore
from latbot.events import EventBus
from latbot.market.state import MarketState
from latbot.models import BookLevel, ExchangeHealth, ExchangeRole, OrderBook, Ticker, Timestamps


def make_settings(**over) -> Settings:
    base = dict(
        exchanges=[
            ExchangeConfig(name="a", role=ExchangeRole.SIGNAL, stream="bbo"),
            ExchangeConfig(name="b", role=ExchangeRole.SIGNAL, stream="bbo"),
            ExchangeConfig(name="c", role=ExchangeRole.EXECUTION),
        ],
        min_mover_volume_quote=0.0,
        min_target_volume_quote=0.0,
        min_movers=2,
        move_threshold_pct=0.35,
        min_lag_pct=0.15,
        move_window_s=5.0,
        price_sample_min_interval_ms=0.0,
        max_data_age_ms=1500.0,
        ticker_poll_interval_s=2.0,
    )
    base.update(over)
    return Settings(**base)


class SpyStats(StatsStore):
    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.calls: list[tuple] = []

    def record_first_mover(self, asset, exchange):
        self.calls.append(("first", asset, exchange))
        super().record_first_mover(asset, exchange)

    def record_lagging(self, asset, exchange, lag_pct):
        self.calls.append(("lag", asset, exchange, round(lag_pct, 6)))
        super().record_lagging(asset, exchange, lag_pct)

    def record_reaction(self, asset, exchange, delay_s, catchup_ratio):
        self.calls.append(("reaction", asset, exchange, round(delay_s, 6), round(catchup_ratio, 6)))
        super().record_reaction(asset, exchange, delay_s, catchup_ratio)

    def record_false_signal(self, asset, exchange):
        self.calls.append(("false", asset, exchange))
        super().record_false_signal(asset, exchange)


class Harness:
    def __init__(self, settings: Settings | None = None, bus: EventBus | None = None) -> None:
        self.settings = settings or make_settings()
        self.clock = SimClock()
        self.state = MarketState(self.settings, self.clock)
        self.stats = SpyStats(self.settings)
        self.det = MoveDetector(self.settings, self.state, self.stats, self.clock, bus)
        self.reactions: list[tuple] = []
        self.det.reaction_listeners.append(lambda *a: self.reactions.append(a))

    def ts(self) -> Timestamps:
        return Timestamps(recv_ts=self.clock.wall(), recv_mono=self.clock.mono())

    def book(self, ex: str, mid: float, symbol: str = "BTC/USDT") -> OrderBook:
        bids = [BookLevel(mid * (1 - 0.0001 * k), 50.0) for k in (1, 2, 3)]
        asks = [BookLevel(mid * (1 + 0.0001 * k), 50.0) for k in (1, 2, 3)]
        return OrderBook(exchange=ex, symbol=symbol, bids=bids, asks=asks, ts=self.ts(), asset="BTC")

    def push(self, ex: str, mid: float, symbol: str = "BTC/USDT"):
        b = self.book(ex, mid, symbol)
        self.state.on_book(b)
        return self.det.on_book(b)

    def push_all(self, prices: dict[str, float]):
        out = []
        for ex, p in prices.items():
            out += self.push(ex, p)
        return out

    def ticker(self, ex: str, mid: float, volume: float | None = None) -> Ticker:
        return Ticker(exchange=ex, symbol="BTC/USDT", bid=mid - 0.01, ask=mid + 0.01, last=mid, ts=self.ts(),
                      asset="BTC", quote_volume=volume)

    def push_ticker(self, ex: str, mid: float, volume: float | None = None, source: str = "poll"):
        t = self.ticker(ex, mid, volume)
        self.state.on_ticker(t, outlier_guard=source != "poll")
        return self.det.on_ticker(t, source=source)

    def warm_up(self) -> None:
        self.push_all({"a": 100.0, "b": 100.0, "c": 100.0})
        self.clock.advance(1.0)
        self.push_all({"a": 100.0, "b": 100.0, "c": 100.0})


def run_lag_scenario(h: Harness):
    """A jumps +1 % at t, B follows at t+2 s, C lags -> one signal."""
    h.warm_up()
    h.clock.advance(1.0)  # t = 1002
    assert h.push("a", 101.0) == []  # single-venue spike: no signal (min_movers=2)
    assert h.det.is_crossed("a", "BTC")
    h.clock.advance(2.0)  # t = 1004
    assert h.push("a", 101.0) == []  # A keeps streaming (still crossed, cross_mono stays 1002)
    assert h.push("c", 100.0) == []
    return h.push("b", 101.0)


def test_lag_signal_for_lagging_execution_exchange():
    h = Harness()
    signals = run_lag_scenario(h)
    assert len(signals) == 1
    sg = signals[0]
    assert sg.asset == "BTC" and sg.target == "c" and sg.first_mover == "a" and sg.direction == 1
    assert sg.lag_pct == pytest.approx(1.0, abs=1e-6)
    assert sg.first_move_pct == pytest.approx(1.0) and sg.target_move_pct == pytest.approx(0.0)
    assert sg.movers_count == 2
    assert sg.catchup_ratio == 0.6 and sg.expected_catchup_pct == pytest.approx(0.6)
    assert sg.target_symbol == "BTC/USDT" and sg.target_quote == "USDT" and sg.source == "ws"
    assert sg.first_move_start_mono == 1002.0 and sg.detected_mono == 1004.0
    assert set(sg.reacted) == {"a", "b", "c"} and sg.median_reaction_delay_s is None
    assert ("first", "BTC", "a") in h.stats.calls
    assert sum(1 for c in h.stats.calls if c[0] == "first") == 1
    assert ("lag", "BTC", "c", 1.0) in h.stats.calls
    assert h.stats.get("BTC", "c").lagging_count == 1
    # same episode: lagging recorded once even though the signal repeats
    h.clock.advance(0.5)
    again = h.push("c", 100.0)
    assert len(again) == 1 and again[0].lag_pct == pytest.approx(1.0)
    assert sum(1 for c in h.stats.calls if c[0] == "lag") == 1


def test_target_catching_up_records_reaction():
    h = Harness()
    run_lag_scenario(h)
    h.clock.advance(1.0)  # t = 1005, first crossed at 1002
    signals = h.push("c", 101.0)
    assert signals == []  # lag gone
    assert ("reaction", "BTC", "c", 3.0, 1.0) in h.stats.calls
    assert h.stats.get("BTC", "c").reaction_delays_s == [3.0]
    assert h.stats.get("BTC", "c").catchup_ratios == [1.0]
    assert h.reactions == [("BTC", "c", 1, 3.0)]
    # partial catch-up ratio: C moves only +0.5 % of a +1 % move
    h2 = Harness()
    run_lag_scenario(h2)
    h2.clock.advance(1.0)
    h2.push("c", 100.5)
    assert ("reaction", "BTC", "c", 3.0, 0.5) in h2.stats.calls
    # no false signal when the move later reverts after the reaction
    h2.clock.advance(0.5)
    h2.push_all({"a": 100.0, "b": 100.0})
    assert not any(c[0] == "false" for c in h2.stats.calls)


def test_reversal_without_reaction_is_false_signal():
    h = Harness()
    run_lag_scenario(h)
    h.clock.advance(1.0)
    h.push("a", 100.0)  # first mover reverts, b still crossed -> episode continues with b
    assert not h.det.is_crossed("a", "BTC") and h.det.is_crossed("b", "BTC")
    assert not any(c[0] == "false" for c in h.stats.calls)
    h.push("b", 100.0)  # last mover reverts -> episode ends
    assert ("false", "BTC", "c") in h.stats.calls
    assert h.stats.get("BTC", "c").false_signal_count == 1
    assert not any(c[0] == "reaction" for c in h.stats.calls)
    # a new move afterwards starts a new episode (first mover recorded again)
    h.clock.advance(1.0)
    h.push_all({"a": 100.0, "b": 100.0, "c": 100.0})
    h.clock.advance(1.0)
    h.push("a", 101.0)
    assert sum(1 for c in h.stats.calls if c[0] == "first") == 2


def test_stale_movers_do_not_count_as_false_signal():
    h = Harness()
    run_lag_scenario(h)
    h.clock.advance(3.0)  # a and b data now older than max_data_age -> unmeasurable
    assert h.push("c", 100.0) == []
    assert not h.det.is_crossed("a", "BTC")
    assert not any(c[0] == "false" for c in h.stats.calls)


def test_hysteresis_prevents_flapping():
    h = Harness(make_settings(min_movers=1))
    h.warm_up()
    h.clock.advance(1.0)
    h.push("a", 100.4)
    assert h.det.is_crossed("a", "BTC") and h.det.crossing_direction("a", "BTC") == 1
    firsts = lambda: sum(1 for c in h.stats.calls if c[0] == "first")  # noqa: E731
    assert firsts() == 1
    h.clock.advance(0.5)
    h.push("a", 100.25)  # below threshold but above threshold/2 -> still crossed
    assert h.det.is_crossed("a", "BTC") and firsts() == 1
    h.clock.advance(0.5)
    h.push("a", 100.1)  # below threshold/2 -> reset
    assert not h.det.is_crossed("a", "BTC")
    h.clock.advance(0.5)
    h.push("a", 100.4)  # crosses again -> new episode
    assert h.det.is_crossed("a", "BTC") and firsts() == 2
    # sign flip beyond the threshold re-crosses in the other direction
    h.clock.advance(0.5)
    h.push("a", 99.5)
    assert h.det.is_crossed("a", "BTC") and h.det.crossing_direction("a", "BTC") == -1


def test_short_only_when_allow_short():
    h = Harness()
    h.warm_up()
    h.clock.advance(1.0)
    h.push("a", 99.0)
    h.push("b", 99.0)
    assert h.push("c", 100.0) == []  # spot target: SHORT not allowed
    assert h.det.allowed_direction("c", 1) and not h.det.allowed_direction("c", -1)
    s = make_settings(
        exchanges=[
            ExchangeConfig(name="a", role=ExchangeRole.SIGNAL),
            ExchangeConfig(name="b", role=ExchangeRole.SIGNAL),
            ExchangeConfig(name="c", role=ExchangeRole.EXECUTION, market_type="swap", allow_short=True),
        ]
    )
    h2 = Harness(s)
    h2.warm_up()
    h2.clock.advance(1.0)
    h2.push("a", 99.0)
    h2.push("b", 99.0)
    signals = h2.push("c", 100.0, symbol="BTC/USDT:USDT")
    assert len(signals) == 1 and signals[0].direction == -1 and signals[0].lag_pct == pytest.approx(1.0)
    assert signals[0].target_quote == "USDT"
    assert h2.det.allowed_direction("c", -1)
    assert not h2.det.allowed_direction("unknown", 1)


def test_stale_or_disabled_target_ignored():
    h = Harness()
    h.state.on_health(ExchangeHealth(exchange="c", ws_connected=True, stale=True))
    assert run_lag_scenario(h) == []
    # no fresh data for the target at all
    h3 = Harness()
    h3.warm_up()
    h3.clock.advance(1.0)
    h3.push("a", 101.0)
    h3.clock.advance(2.0)
    assert h3.push("b", 101.0) == []  # c's last book is 3 s old
    # disabled target
    s = make_settings()
    s.exchange("c").enabled = False
    h4 = Harness(s)
    assert run_lag_scenario(h4) == []


def test_min_movers_and_min_lag_gates():
    h = Harness(make_settings(min_movers=1))
    h.warm_up()
    h.clock.advance(1.0)
    signals = h.push("a", 101.0)
    assert len(signals) == 1 and signals[0].movers_count == 1 and signals[0].target == "c"
    h2 = Harness(make_settings(min_movers=1, min_lag_pct=2.0))
    h2.warm_up()
    h2.clock.advance(1.0)
    assert h2.push("a", 101.0) == []


def test_volume_gate_phase2():
    s = make_settings(min_mover_volume_quote=1_000_000.0, min_target_volume_quote=100_000.0)
    h = Harness(s)
    assert run_lag_scenario(h) == []  # unknown volume -> movers do not count
    h2 = Harness(s)
    h2.state.on_ticker(h2.ticker("a", 100.0, 5_000_000.0))
    h2.state.on_ticker(h2.ticker("b", 100.0, 2_000_000.0))
    h2.state.on_ticker(h2.ticker("c", 100.0, 300_000.0))
    signals = run_lag_scenario(h2)
    assert len(signals) == 1
    assert signals[0].mover_volumes_quote == {"a": 5_000_000.0, "b": 2_000_000.0}
    h3 = Harness(s)
    h3.state.on_ticker(h3.ticker("a", 100.0, 5_000_000.0))
    h3.state.on_ticker(h3.ticker("b", 100.0, 2_000_000.0))
    h3.state.on_ticker(h3.ticker("c", 100.0, 1_000.0))  # illiquid target
    assert run_lag_scenario(h3) == []


def test_learned_catchup_ratio_is_used():
    h = Harness()
    for _ in range(3):
        h.stats.record_reaction("BTC", "c", 2.0, 0.5)
    signals = run_lag_scenario(h)
    assert signals[0].catchup_ratio == 0.5 and signals[0].expected_catchup_pct == pytest.approx(0.5)
    assert signals[0].median_reaction_delay_s == 2.0


def test_signals_sorted_by_lag_and_multiple_targets():
    s = make_settings(
        exchanges=[
            ExchangeConfig(name="a", role=ExchangeRole.SIGNAL),
            ExchangeConfig(name="b", role=ExchangeRole.SIGNAL),
            ExchangeConfig(name="c", role=ExchangeRole.EXECUTION),
            ExchangeConfig(name="d", role=ExchangeRole.EXECUTION),
        ]
    )
    h = Harness(s)
    h.push_all({"a": 100.0, "b": 100.0, "c": 100.0, "d": 100.0})
    h.clock.advance(1.0)
    h.push_all({"a": 100.0, "b": 100.0, "c": 100.0, "d": 100.0})
    h.clock.advance(1.0)
    h.push("a", 101.0)
    h.push("d", 100.3)
    signals = h.push("b", 101.0)
    assert [sg.target for sg in signals] == ["c", "d"]
    assert signals[0].lag_pct == pytest.approx(1.0) and signals[1].lag_pct == pytest.approx(0.7)


def test_polled_tickers_use_wider_freshness_bound():
    h = Harness()
    h.push_ticker("a", 100.0)
    h.push_ticker("b", 100.0)
    h.push_ticker("c", 100.0)
    h.clock.advance(1.0)
    h.push_ticker("a", 100.0)
    h.push_ticker("b", 100.0)
    h.push_ticker("c", 100.0)
    h.clock.advance(3.0)  # c's ticker is now 3 s old
    h.push_ticker("a", 101.0)
    signals = h.push_ticker("b", 101.0)
    assert len(signals) == 1 and signals[0].source == "poll" and signals[0].target == "c"
    # the same data through the strict WS bound: c is not fresh -> nothing
    h2 = Harness()
    h2.push_ticker("a", 100.0, source="ws")
    h2.push_ticker("b", 100.0, source="ws")
    h2.push_ticker("c", 100.0, source="ws")
    h2.clock.advance(1.0)
    h2.push_ticker("a", 100.0, source="ws")
    h2.push_ticker("b", 100.0, source="ws")
    h2.push_ticker("c", 100.0, source="ws")
    h2.clock.advance(3.0)
    h2.push_ticker("a", 101.0, source="ws")
    assert h2.push_ticker("b", 101.0, source="ws") == []


async def test_move_event_published_on_bus():
    bus = EventBus()
    got = []
    bus.subscribe(events.MOVE, lambda ev: got.append(ev))
    h = Harness(make_settings(min_movers=1), bus=bus)
    h.warm_up()
    h.clock.advance(1.0)
    h.push("a", 101.0)
    await asyncio.sleep(0)
    assert len(got) == 1
    ev = got[0]
    assert ev.exchange == "a" and ev.asset == "BTC" and ev.direction == 1
    assert ev.move_pct == pytest.approx(1.0) and ev.start_price == 100.0 and ev.end_price == 101.0
    assert ev.end_mono == h.clock.mono() and ev.velocity_pct_s > 0
    # staying crossed does not re-publish
    h.clock.advance(0.5)
    h.push("a", 101.0)
    await asyncio.sleep(0)
    assert len(got) == 1


def test_ignores_objects_without_asset():
    h = Harness()
    b = OrderBook(exchange="a", symbol="", bids=[], asks=[], ts=h.ts(), asset="")
    assert h.det.on_book(b) == []


def test_persistent_move_keeps_lag_past_window():
    """A genuine, un-reverted move keeps its crossing after the sliding window
    has moved past the jump: the gap on the target is still fully open."""
    h = Harness()
    run_lag_scenario(h)  # a crossed at 1002, b at 1004, c lags at 100 (now = 1004)
    for _ in range(8):  # 1005 .. 1012, well past move_window_s = 5
        h.clock.advance(1.0)
        signals = h.push_all({"a": 101.0, "b": 101.0, "c": 100.0})
        assert h.det.is_crossed("a", "BTC") and h.det.is_crossed("b", "BTC")
        assert signals and all(sg.target == "c" and sg.lag_pct == pytest.approx(1.0) for sg in signals)
        assert signals[-1].first_mover == "a" and signals[-1].first_move_start_mono == 1002.0
        assert signals[-1].first_move_pct == pytest.approx(1.0) and signals[-1].target_move_pct == pytest.approx(0.0)
    assert h.stats.get("BTC", "c").false_signal_count == 0
    assert not any(c[0] == "false" for c in h.stats.calls)
    assert sum(1 for c in h.stats.calls if c[0] == "first") == 1  # one episode, one first mover
    assert h.stats.get("BTC", "c").lagging_count == 1
    # the live view the pipeline refreshes against agrees with the signal
    moves = h.det.current_moves("BTC", h.clock.mono())
    assert moves["a"][0] == pytest.approx(1.0) and moves["b"][0] == pytest.approx(1.0)
    assert moves["c"][0] == pytest.approx(0.0)
    # a further leg of the same move widens the open gap instead of starting a new episode
    h.clock.advance(1.0)
    signals = h.push_all({"a": 102.0, "b": 102.0, "c": 100.0})
    assert signals[-1].lag_pct == pytest.approx(2.0) and signals[-1].first_move_pct == pytest.approx(2.0)
    assert sum(1 for c in h.stats.calls if c[0] == "first") == 1


def test_anchored_reversion_resets_and_records_false_signal():
    """Reversion is decided against the pre-move price, not the sliding window."""
    h = Harness()
    run_lag_scenario(h)
    for _ in range(3):  # the window slides past the jump while the price holds
        h.clock.advance(1.0)
        h.push_all({"a": 101.0, "b": 101.0, "c": 100.0})
    h.clock.advance(1.0)
    h.push("a", 100.1)  # back within thr/2 of the anchor (100.0) -> reverted
    assert not h.det.is_crossed("a", "BTC") and h.det.is_crossed("b", "BTC")
    assert not any(c[0] == "false" for c in h.stats.calls)  # b still carries the episode
    assert h.push("b", 100.1) == []
    assert not h.det.is_crossed("b", "BTC")
    assert ("false", "BTC", "c") in h.stats.calls and h.stats.get("BTC", "c").false_signal_count == 1
    # the reversion is not a new move in the other direction: the price is back where it started
    assert h.push("c", 100.0) == []
    assert not h.det.is_crossed("a", "BTC") and not h.det.is_crossed("b", "BTC")


def test_window_expiry_without_reversion_is_not_false_signal():
    """An episode that merely ages out (max_opp_duration_s) ends without a
    false-signal record; only a price reversion is a false signal."""
    h = Harness(make_settings(max_opp_duration_s=10.0))
    run_lag_scenario(h)  # a crossed at 1002
    for _ in range(7):  # until 1011: crossed for 9 s, the price never came back
        h.clock.advance(1.0)
        assert h.push_all({"a": 101.0, "b": 101.0, "c": 100.0})
    assert h.det.is_crossed("a", "BTC")
    h.clock.advance(1.5)  # 1012.5: the episode is older than max_opp_duration_s -> timed out
    assert h.push_all({"a": 101.0, "b": 101.0, "c": 100.0}) == []
    assert not h.det.is_crossed("a", "BTC") and not h.det.is_crossed("b", "BTC")
    assert not any(c[0] == "false" for c in h.stats.calls)
    assert h.stats.get("BTC", "c").false_signal_count == 0 and h.stats.get("BTC", "c").lagging_count == 1


def test_slow_target_catch_up_is_measured_from_the_episode_anchor():
    """The target's move is measured from its pre-move price too: a slow
    catch-up (never >= threshold inside one window) still shrinks the lag."""
    h = Harness()
    run_lag_scenario(h)  # lag 1.0 % at 1004
    for k, mid in enumerate((100.1, 100.2, 100.3), start=1):
        h.clock.advance(2.0)
        signals = h.push_all({"a": 101.0, "b": 101.0, "c": mid})
        assert signals[-1].target_move_pct == pytest.approx(0.1 * k, abs=1e-6)
        assert signals[-1].lag_pct == pytest.approx(1.0 - 0.1 * k, abs=1e-6)
    h.clock.advance(2.0)
    h.push_all({"a": 101.0, "b": 101.0, "c": 100.5})  # +0.5 % since the move began -> reacted
    assert ("reaction", "BTC", "c", 10.0, 0.5) in h.stats.calls


def test_volume_gate_reports_unknown_vs_low_with_exchange():
    """'volume unknown' is a data gap, 'volume low' is an honest filter: the
    diagnostics name which one and which exchange (and its volume)."""
    s = make_settings(min_mover_volume_quote=1_000_000.0, min_target_volume_quote=0.0)
    h = Harness(s)
    run_lag_scenario(h)  # no tickers at all -> volumes unknown
    assert h.det.diag.rejected["mover_volume_unknown"] >= 1 and h.det.diag.rejected["mover_volume"] == 0
    assert "a:?" in list(h.det.diag.recent["mover_volume_unknown"])[-1]
    h2 = Harness(s)
    h2.state.on_ticker(h2.ticker("a", 100.0, 5_000_000.0))
    h2.state.on_ticker(h2.ticker("b", 100.0, 40_000.0))  # known, too small
    run_lag_scenario(h2)
    assert h2.det.diag.rejected["mover_volume"] >= 1 and h2.det.diag.rejected["mover_volume_unknown"] == 0
    assert "b:40k" in list(h2.det.diag.recent["mover_volume"])[-1]


def test_state_volume_coverage():
    h = Harness()
    h.state.on_ticker(h.ticker("a", 100.0, 5_000_000.0))
    h.state.on_ticker(h.ticker("b", 100.0, None))
    assert h.state.volume_coverage() == {"a": 1}
