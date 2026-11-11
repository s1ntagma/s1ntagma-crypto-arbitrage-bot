"""Proven pairs: only what the bot's own virtual trades showed to work is offered."""
from __future__ import annotations

from latbot.config import Settings
from latbot.engine.proven import ProvenBook, current_strategy
from latbot.storage.db import Database


def trades(n_win: int, n_loss: int, *, asset="BTC", target="revolutx", strategy="ride", win=0.4, loss=-0.3, start=0):
    out = []
    for i in range(n_win + n_loss):
        out.append({"id": f"{asset}{target}{start + i}", "asset": asset, "target": target, "strategy": strategy,
                    "closed_ts": 1000.0 + start + i, "actual_net_pct": win if i < n_win else loss})
    return out


def book(**kw) -> ProvenBook:
    return ProvenBook(Settings(simple_mode=True, **kw))


def test_strategy_tag_follows_the_mode():
    assert current_strategy(Settings(simple_mode=True)) == "ride"
    assert current_strategy(Settings(simple_mode=True, ride_enabled=False)) == "simple"
    assert current_strategy(Settings()) == "full"


def test_needs_enough_trades_then_a_convincing_record():
    b = book()
    for t in trades(20, 0):
        b.record(t)
    a = b.assess("BTC", "revolutx")
    assert a["status"] == "training" and a["n"] == 20 and "20/30" in a["text"]
    for t in trades(10, 0, start=20):
        b.record(t)
    a = b.assess("BTC", "revolutx")
    assert a["proven"] and a["p10"] > 0.9 and "PROVEN" in a["text"]


def test_a_mediocre_record_is_not_proven_and_a_losing_one_fails():
    b = book()
    for t in trades(18, 12):  # 60 % wins: worst case well below 60 %
        b.record(t)
    assert b.assess("BTC", "revolutx")["status"] == "training"
    for t in trades(5, 25, target="kraken"):
        b.record(t)
    assert b.assess("BTC", "kraken")["status"] == "failing"
    assert [r["status"] for r in b.pairs()] == ["training", "failing"]


def test_positive_win_rate_but_negative_average_is_not_proven():
    b = book()
    for t in trades(27, 3, win=0.05, loss=-1.0):  # many small wins, a few big losses
        b.record(t)
    assert b.assess("BTC", "revolutx")["status"] == "failing"


def test_other_strategies_and_duplicates_do_not_count():
    b = book()
    for t in trades(30, 0, strategy="simple"):
        b.record(t)
    assert b.assess("BTC", "revolutx")["n"] == 0
    t = trades(1, 0, start=100)[0]
    assert b.record(t) and not b.record(t)


def test_loads_history_from_the_database():
    db = Database(":memory:")
    for t in trades(30, 0):
        db.save_shadow(t)
    b = ProvenBook(Settings(simple_mode=True), db=db)
    assert b.assess("BTC", "revolutx")["proven"] and b.counts()["proven"] == 1


def test_gate_is_information_only_outside_simple_mode():
    assert not ProvenBook(Settings()).assess("BTC", "x")["enforced"]
    assert book().assess("BTC", "x")["enforced"]
    assert not book(proven_gate_enabled=False).assess("BTC", "x")["enforced"]
