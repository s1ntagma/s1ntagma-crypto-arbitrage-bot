"""Offline end-to-end tests for latbot.backtest.engine (replay through the real stack)."""
from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest

from latbot.backtest import engine as bt_engine
from latbot.backtest.__main__ import main
from latbot.backtest.engine import REPORT_KEYS, OppSeen, backtest_settings, fmt_report, run_backtest, run_backtest_async, summarize
from latbot.config import ExchangeConfig, Settings, save_settings
from latbot.models import ExchangeRole, Mode, OppState, TradeRecord

T0 = 1_700_000_000.0
SYMS = {"binance": "BTC/USDT", "kucoin": "BTC/USDT", "kraken": "BTC/EUR"}
LAT_MS = 250.0


def rec(t: float, ex: str, mid: float, size: float = 50.0) -> dict[str, Any]:
    bids = [[mid * (1 - 0.0001 * k), size] for k in (1, 2, 3)]
    asks = [[mid * (1 + 0.0001 * k), size] for k in (1, 2, 3)]
    return {"t": T0 + t, "ex": ex, "asset": "BTC", "symbol": SYMS[ex], "ets": T0 + t - 0.05, "b": bids, "a": asks}


def scenario(catch_up: bool = True, kraken_gap: bool = False) -> list[dict[str, Any]]:
    """binance + kucoin jump +2 % at t=5; kraken (target) lags at 100.0, ticks
    to 100.2 at t=5.2, drifts to 100.5 and catches up to 101.5 at t=8 (or
    never, when ``catch_up`` is False).  ``kraken_gap`` removes the kraken
    books between t=4.5 and t=8 (stale target at entry time)."""
    out: list[dict[str, Any]] = []
    t = 0.0
    while t < 4.6:
        for ex in SYMS:
            out.append(rec(t, ex, 100.0))
        t += 0.5
    out.append(rec(5.0, "binance", 102.0))
    out.append(rec(5.1, "kucoin", 102.0))
    if not kraken_gap:
        out.append(rec(5.2, "kraken", 100.2))
    t = 6.0
    while t < 7.6:
        out.append(rec(t, "binance", 102.0))
        out.append(rec(t, "kucoin", 102.0))
        if not kraken_gap:
            out.append(rec(t, "kraken", 100.5 if catch_up else 100.2))
        t += 0.5
    t = 8.0
    while t < 12.1:
        out.append(rec(t, "binance", 102.0))
        out.append(rec(t, "kucoin", 102.0))
        out.append(rec(t, "kraken", 101.5 if catch_up else 100.2))
        t += 0.5
    return out


def write(path: Path, records: list[dict[str, Any]]) -> Path:
    text = "\n".join(json.dumps(r) for r in records) + "\n"
    if path.suffix == ".gz":
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write(text)
    else:
        path.write_text(text, encoding="utf-8")
    return path


def make_settings(**over: Any) -> Settings:
    base: dict[str, Any] = dict(
        exchanges=[
            ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo", label="Binance"),
            ExchangeConfig(name="kucoin", role=ExchangeRole.SIGNAL, stream="bbo", label="KuCoin"),
            ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, taker_fee=0.001, maker_fee=0.001, label="Kraken"),
        ],
        total_capital_quote=10_000.0,
        price_sample_min_interval_ms=0.0,
        default_api_latency_ms=LAT_MS,
    )
    base.update(over)
    return Settings(**base)


# --------------------------------------------------------------------------- #
# end to end
# --------------------------------------------------------------------------- #
async def test_end_to_end_entry_at_t_plus_latency_and_limit_exit(tmp_path: Path):
    records = scenario(catch_up=True)
    path = write(tmp_path / "day.jsonl", records)
    report = await run_backtest_async([str(path)], make_settings())
    assert all(k in report for k in REPORT_KEYS)
    assert report["records"] == len(records) and report["latency_ms"] == LAT_MS
    assert report["exchanges"] == ["binance", "kraken", "kucoin"] and report["assets"] == ["BTC"]
    assert report["span_s"] == pytest.approx(12.0)
    assert report["opportunities"] == 1
    assert report["executed"] == 1 and report["closed"] == 1 and report["failed"] == 0
    assert report["rejected"] == {}
    assert report["human_executable"] == 1 and report["bot_executable"] == 1
    assert report["false_signals"] == 0 and report["volume_gate"] is False
    trade = report["trades"][0]
    assert trade["state"] == "CLOSED" and trade["side"] == "LONG" and trade["exchange"] == "kraken"
    # detected at t=5.1 on the kraken book of t=4.5 (ask 100.01); executed at t=5.35 on the t=5.2 book (ask 100.21)
    assert trade["entry_price"] == pytest.approx(100.2 * 1.0001)
    assert trade["entry_price"] != pytest.approx(100.01)
    assert trade["exit_type"] == "limit" and trade["exit_price"] == pytest.approx(101.5 * (1 - 0.0001))
    assert trade["expected_net_pct"] == pytest.approx(report["opp_net_mean"])
    assert 0.5 < trade["expected_net_pct"] < 1.5
    assert trade["actual_net_pct"] == pytest.approx(report["realized_net_mean"]) and report["realized_net_median"] == report["realized_net_mean"]
    assert report["realized_net_mean"] > 0 and report["pnl_quote"] > 0
    assert report["pnl_quote"] == pytest.approx(trade["realized_pnl_quote"])
    assert "latency 250 ms" in trade["diff_reason"]
    assert report["by_asset"]["BTC"]["closed"] == 1 and report["by_asset"]["BTC"]["pnl_quote"] == pytest.approx(report["pnl_quote"])
    assert report["by_exchange"]["kraken"]["opportunities"] == 1 and report["by_exchange"]["kraken"]["executed"] == 1
    assert set(report["by_asset"]["BTC"]) >= {"opportunities", "executed", "closed", "expected_net_mean", "realized_net_mean", "pnl_quote"}


def test_timeout_path_exits_at_market_with_negative_net(tmp_path: Path):
    records = scenario(catch_up=False)
    path = write(tmp_path / "flat.jsonl", records)
    report = run_backtest([str(path)], make_settings(sell_timeout_s=20.0))
    assert report["opportunities"] == 1 and report["executed"] == 1 and report["closed"] == 1
    trade = report["trades"][0]
    assert trade["exit_type"] == "market" and trade["state"] == "CLOSED"
    assert trade["execution_time_s"] == pytest.approx(20.0)
    assert trade["exit_price"] == pytest.approx(100.2 * (1 - 0.0001))  # the last kraken book
    assert report["realized_net_mean"] < 0 and report["pnl_quote"] < 0
    assert report["expected_net_mean"] > 0
    assert report["false_signals"] >= 1  # the movers reverted and kraken never caught up
    assert report["expired"] == 0  # the opportunity was executed, not expired


def test_hold_policy_waits_until_max_hold(tmp_path: Path):
    path = write(tmp_path / "flat.jsonl", scenario(catch_up=False))
    report = run_backtest([str(path)], make_settings(sell_timeout_s=20.0, exit_on_timeout="hold", max_hold_s=40.0))
    assert report["closed"] == 1
    assert report["trades"][0]["execution_time_s"] == pytest.approx(40.0)
    assert report["trades"][0]["exit_type"] == "market"


def test_stale_target_book_at_entry_is_not_executed(tmp_path: Path):
    # detected at t=5.1 on the kraken book of t=4.5; with 1 s latency the entry is due at
    # t=6.1 when that book is 1.6 s old (> max_data_age_ms) -> no trade, never a REST "refresh"
    path = write(tmp_path / "gap.jsonl", scenario(catch_up=True, kraken_gap=True))
    report = run_backtest([str(path)], make_settings(), latency_ms=1000.0)
    assert report["latency_ms"] == 1000.0
    assert report["opportunities"] == 1 and report["bot_executable"] == 1
    assert report["executed"] == 0 and report["closed"] == 0
    assert report["rejected"] == {"stale_book_at_entry": 1}
    assert report["pnl_quote"] == 0.0 and report["realized_net_mean"] is None and report["expected_net_mean"] is None


def test_entry_due_after_data_end_is_rejected(tmp_path: Path):
    records = [r for r in scenario(catch_up=True) if r["t"] <= T0 + 5.25]
    path = write(tmp_path / "short.jsonl", records)
    report = run_backtest([str(path)], make_settings(), latency_ms=500.0)
    assert report["opportunities"] == 1 and report["executed"] == 0
    assert report["rejected"] == {"data_ended": 1}


def test_gz_input_and_multiple_files_merge(tmp_path: Path):
    records = scenario(catch_up=True)
    a = write(tmp_path / "part1.jsonl.gz", records[: len(records) // 2])
    b = write(tmp_path / "part2.jsonl", records[len(records) // 2:])
    report = run_backtest([str(b), str(a)], make_settings())
    assert report["records"] == len(records) and report["closed"] == 1


def test_empty_or_malformed_input_gives_empty_report(tmp_path: Path):
    path = tmp_path / "bad.jsonl"
    path.write_text("garbage\n{\"t\": 1}\n", encoding="utf-8")
    report = run_backtest([str(path)], make_settings())
    assert all(k in report for k in REPORT_KEYS)
    assert report["records"] == 0 and report["opportunities"] == 0 and report["executed"] == 0
    assert report["realized_net_mean"] is None and report["by_asset"] == {} and report["pnl_quote"] == 0.0


def test_unknown_exchange_in_data_is_a_signal_source(tmp_path: Path):
    records = scenario(catch_up=True)
    for r in records:
        if r["ex"] == "kucoin":
            r["ex"] = "mystery"
    path = write(tmp_path / "x.jsonl", records)
    settings = make_settings()
    report = run_backtest([str(path)], settings)
    assert "mystery" in report["exchanges"] and report["closed"] == 1
    assert settings.exchange("mystery") is None  # the caller's settings are untouched


# --------------------------------------------------------------------------- #
# settings / deps / report helpers
# --------------------------------------------------------------------------- #
def test_backtest_settings_overrides_do_not_touch_original():
    s = make_settings(auto_confirm=False, confirm_with_rest_book=True, record_market_data=True, ticker_poll_enabled=True)
    bt = backtest_settings(s, latency_ms=900.0)
    assert bt.auto_confirm and bt.paper_mode and not bt.live_mode and bt.mode() is Mode.PAPER
    assert not bt.record_market_data and not bt.ticker_poll_enabled and not bt.confirm_with_rest_book
    assert bt.min_mover_volume_quote == 0.0 and bt.min_target_volume_quote == 0.0
    assert bt.default_api_latency_ms == 900.0 and bt.min_movers == 2
    assert bt.exchange("kraken").taker_fee == 0.001 and [e.name for e in bt.exchanges] == ["binance", "kucoin", "kraken"]
    assert s.auto_confirm is False and s.record_market_data and s.default_api_latency_ms == LAT_MS
    assert backtest_settings(s).default_api_latency_ms == LAT_MS


def test_deps_override_and_missing_dependency(tmp_path: Path, monkeypatch):
    calls: list[str] = []
    from latbot.execution.pnl import finalize as real_finalize

    def spy(trade: TradeRecord, *a: Any, **kw: Any) -> TradeRecord:
        calls.append(trade.id)
        return real_finalize(trade, *a, **kw)

    path = write(tmp_path / "x.jsonl", scenario(catch_up=True))
    report = run_backtest([str(path)], make_settings(), deps={"finalize": spy})
    assert report["closed"] == 1 and calls == [report["trades"][0]["id"]]
    monkeypatch.setitem(bt_engine.DEP_NAMES, "Pipeline", ("latbot.does_not_exist", "Pipeline"))
    with pytest.raises(RuntimeError, match="Pipeline"):
        bt_engine._resolve_deps()
    with pytest.raises(RuntimeError, match="Pipeline"):
        run_backtest([str(path)], make_settings())


def test_summarize_is_pure_and_groups():
    seen = [
        OppSeen(id="o1", asset="BTC", exchange="kraken", direction=1, first_mono=0.0, net_first=1.0, net_max=1.2, human_ok=True, bot_ok=True, final_state="CLOSED"),
        OppSeen(id="o2", asset="ETH", exchange="kraken", direction=1, first_mono=1.0, net_first=0.5, net_max=0.5, human_ok=False, bot_ok=True, final_state="EXPIRED", caught_up=False),
        OppSeen(id="o3", asset="ETH", exchange="bybit", direction=-1, first_mono=2.0, net_first=0.4, net_max=0.4, final_state="EXPIRED", caught_up=True),
    ]

    def trade(tid: str, asset: str, ex: str, state: OppState, expected: float, actual: float | None, pnl: float) -> TradeRecord:
        return TradeRecord(
            id=tid, opportunity_id="o1", mode=Mode.PAPER, exchange=ex, asset=asset, symbol=f"{asset}/EUR", direction=1,
            capital_quote=100.0, expected_net_pct=expected, expected_profit_quote=expected, expected_entry_price=1.0,
            expected_exit_price=1.01, state=state, actual_net_pct=actual, realized_pnl_quote=pnl,
        )

    trades = [
        trade("t1", "BTC", "kraken", OppState.CLOSED, 1.0, 0.8, 0.8),
        trade("t2", "BTC", "kraken", OppState.CLOSED, 1.2, 0.2, 0.2),
        trade("t3", "ETH", "kraken", OppState.FAILED, 0.5, None, -0.1),
    ]
    rep = summarize(seen, trades, records=10, span_s=5.0, exchanges=["bybit", "kraken"], assets=["BTC", "ETH"], latency_ms=250.0,
                    rejected={"net_decayed": 2}, exit_types={"t1": "limit", "t2": "market"})
    assert all(k in rep for k in REPORT_KEYS)
    assert rep["opportunities"] == 3 and rep["executed"] == 3 and rep["closed"] == 2 and rep["failed"] == 1
    assert rep["expected_net_mean"] == pytest.approx(0.9) and rep["expected_net_median"] == pytest.approx(1.0)
    assert rep["realized_net_mean"] == pytest.approx(0.5) and rep["realized_net_median"] == pytest.approx(0.5)
    assert rep["opp_net_mean"] == pytest.approx((1.0 + 0.5 + 0.4) / 3)
    assert rep["human_executable"] == 1 and rep["bot_executable"] == 2
    assert rep["false_signals"] == 1 and rep["expired"] == 2  # EXPIRED without catch-up (no stats given)
    assert rep["pnl_quote"] == pytest.approx(0.9) and rep["rejected"] == {"net_decayed": 2}
    assert rep["by_asset"]["BTC"] == {
        "opportunities": 1, "executed": 2, "closed": 2, "failed": 0, "expected_net_mean": pytest.approx(1.1),
        "realized_net_mean": pytest.approx(0.5), "pnl_quote": pytest.approx(1.0), "human_executable": 1, "bot_executable": 1,
    }
    assert rep["by_asset"]["ETH"]["executed"] == 1 and rep["by_asset"]["ETH"]["realized_net_mean"] is None
    assert set(rep["by_exchange"]) == {"bybit", "kraken"} and rep["by_exchange"]["bybit"]["executed"] == 0
    assert [t["exit_type"] for t in rep["trades"]] == ["limit", "market", None]
    assert summarize(seen, trades, false_signals=7)["false_signals"] == 7
    text = fmt_report(rep)
    assert "opportunities       3" in text and "by asset:" in text and "BTC" in text and "rejected: net_decayed 2" in text


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def test_cli_prints_table_and_json(tmp_path: Path, capsys):
    cfg = tmp_path / "config.json"
    save_settings(make_settings(), cfg)
    data = write(tmp_path / "2023-11-14.jsonl.gz", scenario(catch_up=True))
    assert main([str(tmp_path / "*.jsonl.gz"), "--config", str(cfg), "--latency", "250"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("latbot backtest:") and "records" in out and "closed              1" in out
    assert "by exchange:" in out and "kraken" in out
    assert main([str(data), "--config", str(cfg), "--json", "--capital", "50"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert all(k in report for k in REPORT_KEYS) and report["closed"] == 1
    assert report["trades"][0]["capital_quote"] == pytest.approx(50.0)


def test_cli_exit_codes(tmp_path: Path, capsys):
    assert main([str(tmp_path / "nothing-*.jsonl")]) == 2
    empty = tmp_path / "empty.jsonl"
    empty.write_text("", encoding="utf-8")
    assert main([str(empty), "--config", str(tmp_path / "no-config.json")]) == 2
    assert "no records" in capsys.readouterr().err
