"""Offline tests for latbot.engine.metalabel (MetaLabelStore + stdlib Beta quantile)."""
from __future__ import annotations

import json
import math
from fractions import Fraction
from math import comb
from types import SimpleNamespace

import pytest

from latbot.clock import SimClock
from latbot.engine.metalabel import (
    MetaLabelStore,
    beta_ppf,
    betainc,
    label_outcome,
    lag_bucket,
    regime_bucket,
)


def _settings(**kw):
    return SimpleNamespace(**kw)


def _store(db=None, **kw) -> MetaLabelStore:
    return MetaLabelStore(_settings(**kw), SimClock(), db=db)


def _outcome(win: bool, *, asset="BTC", target="bybit", fm="binance", lag=0.7, pctile=0.5,
             net=None, grade="B", ts=None, barrier=None) -> dict:
    return {
        "asset": asset, "target": target, "first_mover": fm, "lag_pct": lag,
        "btc_vol_pctile": pctile, "outcome": "win" if win else "loss",
        "barrier": barrier or ("target" if win else "stop"),
        "net_pct": (0.5 if win else -0.4) if net is None else net,
        "quality_score": 70.0, "grade": grade, "ts": ts,
    }


class FakeDb:
    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows: list[dict] = list(rows or [])
        self.limits: list[int] = []

    def save_metalabel_outcome(self, row: dict) -> None:
        self.rows.append(dict(row))

    def load_metalabel_outcomes(self, limit: int) -> list[dict]:
        self.limits.append(limit)
        return [dict(r) for r in reversed(self.rows[-limit:])]  # newest first, like a DESC query


# ---------------------------------------------------------------------- #
# Beta distribution
# ---------------------------------------------------------------------- #
def _exact_int_ppf(q: Fraction, a: int, b: int) -> float:
    """Exact-arithmetic quantile for integer a, b: I_x(a,b) = P(Binomial(a+b-1, x) >= a)."""
    n = a + b - 1

    def cdf(x: Fraction) -> Fraction:
        return sum(comb(n, j) * x ** j * (1 - x) ** (n - j) for j in range(a, n + 1))

    lo, hi = Fraction(0), Fraction(1)
    for _ in range(50):
        mid = (lo + hi) / 2
        if cdf(mid) < q:
            lo = mid
        else:
            hi = mid
    return float((lo + hi) / 2)


def test_beta_ppf_uniform():
    assert beta_ppf(0.1, 1.0, 1.0) == pytest.approx(0.1, abs=1e-4)
    assert beta_ppf(0.5, 1.0, 1.0) == pytest.approx(0.5, abs=1e-9)
    assert beta_ppf(0.9, 1.0, 1.0) == pytest.approx(0.9, abs=1e-9)


def test_beta_ppf_beta_2_8_known_value():
    # reference: exact rational CDF 1 - (1-x)^9 - 9x(1-x)^8 solved to 0.0607690496507...
    assert beta_ppf(0.1, 2.0, 8.0) == pytest.approx(0.0607690, abs=1e-4)
    assert beta_ppf(0.1, 2.0, 8.0) == pytest.approx(_exact_int_ppf(Fraction(1, 10), 2, 8), abs=1e-9)
    assert beta_ppf(0.9, 2.0, 8.0) == pytest.approx(0.3683624, abs=1e-4)


def test_beta_ppf_beta_10_10_symmetric():
    p10 = beta_ppf(0.1, 10.0, 10.0)
    p90 = beta_ppf(0.9, 10.0, 10.0)
    assert p10 == pytest.approx(0.3579299, abs=1e-4)
    assert p10 + p90 == pytest.approx(1.0, abs=1e-9)
    assert beta_ppf(0.5, 10.0, 10.0) == pytest.approx(0.5, abs=1e-9)
    assert p10 == pytest.approx(_exact_int_ppf(Fraction(1, 10), 10, 10), abs=1e-9)


@pytest.mark.parametrize("a,b", [(3, 17), (5, 5), (8, 2), (1, 4)])
def test_beta_ppf_matches_exact_integer_cases(a, b):
    assert beta_ppf(0.1, float(a), float(b)) == pytest.approx(
        _exact_int_ppf(Fraction(1, 10), a, b), abs=1e-9)


@pytest.mark.parametrize("a,b", [(1.0, 3.5), (1.0, 0.4), (2.7, 1.0), (0.3, 1.0)])
def test_beta_ppf_closed_forms_non_integer(a, b):
    # Beta(1, b): F = 1 - (1-x)^b ; Beta(a, 1): F = x^a
    q = 0.1
    expected = 1.0 - (1.0 - q) ** (1.0 / b) if a == 1.0 else q ** (1.0 / a)
    assert beta_ppf(q, a, b) == pytest.approx(expected, abs=1e-9)


def test_betainc_properties_and_degenerate_ppf():
    assert betainc(2.0, 3.0, 0.0) == 0.0
    assert betainc(2.0, 3.0, 1.0) == 1.0
    # symmetry I_x(a, b) = 1 - I_{1-x}(b, a), both continued-fraction branches
    for x in (0.05, 0.3, 0.6, 0.95):
        assert betainc(3.7, 6.3, x) == pytest.approx(1.0 - betainc(6.3, 3.7, 1.0 - x), abs=1e-12)
    # large parameters converge (normal approximation is very close here)
    a = b = 10000.0
    sd = math.sqrt(a * b / ((a + b) ** 2 * (a + b + 1)))
    assert beta_ppf(0.1, a, b) == pytest.approx(0.5 - 1.2815515655 * sd, abs=1e-5)
    assert beta_ppf(0.1, 0.0, 5.0) == 0.0
    assert beta_ppf(0.1, 5.0, 0.0) == 1.0
    with pytest.raises(ValueError):
        beta_ppf(1.5, 1.0, 1.0)
    with pytest.raises(ValueError):
        betainc(0.0, 1.0, 0.5)


# ---------------------------------------------------------------------- #
# buckets / labels / key
# ---------------------------------------------------------------------- #
def test_buckets_and_key():
    assert [lag_bucket(x) for x in (0.0, 0.49, 0.5, 0.99, 1.0, 1.99, 2.0, 7.0)] == [
        "<0.5", "<0.5", "0.5-1", "0.5-1", "1-2", "1-2", ">2", ">2"]
    assert lag_bucket(-1.2) == "1-2"  # magnitude of the lag, direction-agnostic
    assert lag_bucket(None) == "unknown" and lag_bucket(float("nan")) == "unknown"
    assert [regime_bucket(x) for x in (0.0, 0.32, 0.33, 0.5, 0.66, 0.67, 1.0)] == [
        "low", "low", "mid", "mid", "mid", "high", "high"]
    assert regime_bucket(None) == "unknown"
    st = _store()
    assert st.key("BTC", "bybit", "binance", 0.7, 0.9) == ("BTC", "bybit", "binance", "0.5-1", "high")
    assert st.key("ETH", "okx", "kraken", 2.5, None) == ("ETH", "okx", "kraken", ">2", "unknown")


def test_label_outcome_triple_barrier():
    assert label_outcome("target", -0.1) == "win"
    assert label_outcome("stop", 0.3) == "loss"
    assert label_outcome("time", 0.01) == "win"
    assert label_outcome("time", 0.0) == "loss"
    assert label_outcome("time", -0.2) == "loss"
    assert label_outcome("time", None) is None
    assert label_outcome(None, 1.0) is None


def test_record_derives_outcome_and_rejects_missing_data():
    st = _store()
    row = _outcome(True)
    del row["outcome"]
    row["barrier"] = "time"
    row["net_pct"] = -0.05
    assert st.record(row) is True
    assert st.estimate("BTC", "bybit", "binance", 0.7, 0.5)["wins"] == 0
    assert st.record({"asset": "BTC", "target": "bybit", "barrier": "time"}) is False  # no NET -> no label
    assert st.record({"target": "bybit", "outcome": "win"}) is False
    assert st.record({"asset": " ", "target": "bybit", "outcome": "win"}) is False
    assert len(st) == 1
    assert st.snapshot()["rejected"] == 3


# ---------------------------------------------------------------------- #
# estimates / back-off
# ---------------------------------------------------------------------- #
def test_empty_store_estimate_is_prior_at_global():
    st = _store()
    est = st.estimate("BTC", "bybit", "binance", 0.7, 0.5)
    assert est["level"] == "global" and est["key"] == () and est["n"] == 0 and est["wins"] == 0
    assert est["mean"] == pytest.approx(0.5)
    assert est["p10"] == pytest.approx(beta_ppf(0.1, 5.0, 5.0), abs=1e-9)  # kappa 10 around base 0.5


def test_backoff_levels():
    st = _store(metalabel_min_n=20, metalabel_kappa=10)
    # full key (BTC, bybit, binance, 0.5-1, mid): 25 outcomes, 20 wins
    for i in range(25):
        st.record(_outcome(i < 20))
    # same asset/target, other first mover: 10 outcomes, 2 wins
    for i in range(10):
        st.record(_outcome(i < 2, fm="okx"))
    # other target: 15 outcomes, 5 wins
    for i in range(15):
        st.record(_outcome(i < 5, target="gate"))
    # other asset: 5 outcomes, 0 wins
    for _ in range(5):
        st.record(_outcome(False, asset="ETH"))

    base = (20 + 2 + 5 + 0 + 0.5 * 10) / (55 + 10)  # global rate shrunk towards 0.5 with kappa pseudo-trades
    est = st.estimate("BTC", "bybit", "binance", 0.6, 0.5)
    assert est["level"] == "full" and est["key"] == ("BTC", "bybit", "binance", "0.5-1", "mid")
    assert (est["n"], est["wins"]) == (25, 20)
    a, b = 10 * base + 20, 10 * (1 - base) + 5
    assert est["mean"] == pytest.approx(a / (a + b))
    assert est["p10"] == pytest.approx(beta_ppf(0.1, a, b), abs=1e-9)
    assert est["p10"] < est["mean"]
    assert est["base_rate"] == pytest.approx(base)

    # full key with 10 outcomes (< 20) -> (asset, target) has 35
    est = st.estimate("BTC", "bybit", "okx", 0.6, 0.5)
    assert est["level"] == "asset_target" and est["key"] == ("BTC", "bybit") and est["n"] == 35
    assert est["wins"] == 22
    assert [lv["n"] for lv in est["levels"]] == [10, 35, 50, 55]

    # (BTC, gate) has 15 < 20 -> BTC has 50
    est = st.estimate("BTC", "gate", "binance", 0.6, 0.5)
    assert est["level"] == "asset" and est["key"] == ("BTC",) and (est["n"], est["wins"]) == (50, 27)

    # ETH: 5 outcomes everywhere below min_n except global (55)
    est = st.estimate("ETH", "bybit", "binance", 0.6, 0.5)
    assert est["level"] == "global" and est["key"] == () and (est["n"], est["wins"]) == (55, 27)

    # unknown asset, global still trusted
    assert st.estimate("SOL", "okx", "binance", 3.0, None)["level"] == "global"


def test_backoff_most_specific_with_any_data_when_nothing_reaches_min_n():
    st = _store(metalabel_min_n=20)
    for i in range(4):
        st.record(_outcome(i < 3))
    for _ in range(3):
        st.record(_outcome(False, asset="ETH"))
    est = st.estimate("BTC", "bybit", "binance", 0.6, 0.5)
    assert est["level"] == "full" and (est["n"], est["wins"]) == (4, 3)
    est = st.estimate("BTC", "bybit", "binance", 3.0, 0.5)  # other lag bucket
    assert est["level"] == "asset_target" and est["n"] == 4
    est = st.estimate("SOL", "bybit", "binance", 0.6, 0.5)  # never seen -> global
    assert est["level"] == "global" and est["n"] == 7 and est["wins"] == 3


def test_degenerate_base_rate_and_zero_kappa():
    st = _store(metalabel_kappa=10)
    for _ in range(3):
        st.record(_outcome(False))
    est = st.estimate("BTC", "bybit", "binance", 0.6, 0.5)
    # all losses so far: the shrunk base (5/13) keeps the prior alive - never a point mass at 0
    assert est["mean"] == pytest.approx((10 * 5 / 13) / (10 * 5 / 13 + 10 * 8 / 13 + 3)) and est["p10"] > 0.0
    st0 = _store(metalabel_kappa=0)
    est = st0.estimate("BTC", "bybit", "binance", 0.6, 0.5)
    assert est["n"] == 0 and est["mean"] is None and est["p10"] is None  # no data, no estimate


# ---------------------------------------------------------------------- #
# memory bound, persistence, grades, snapshot
# ---------------------------------------------------------------------- #
def test_bounded_memory_evicts_from_aggregates():
    st = _store(metalabel_keep=10, metalabel_min_n=1)
    for i in range(10):
        st.record(_outcome(False, asset="ETH", grade="D", ts=1000.0 + i))
    for i in range(10):
        st.record(_outcome(True, grade="A", ts=2000.0 + i))
    assert len(st) == 10
    assert st.estimate("ETH", "bybit", "binance", 0.6, 0.5)["level"] == "global"
    est = st.estimate("BTC", "bybit", "binance", 0.6, 0.5)
    assert (est["n"], est["wins"]) == (10, 10)
    assert st.by_grade() == {"A": {"n": 10, "win_rate": 1.0, "avg_net_pct": pytest.approx(0.5)}}
    snap = st.snapshot()
    assert snap["n"] == 10 and snap["keys_total"] == 1 and snap["by_barrier"] == {"target": 10}
    assert [o["ts"] for o in st.outcomes(limit=2)] == [2008.0, 2009.0]


def test_persistence_round_trip():
    db = FakeDb()
    st = MetaLabelStore(_settings(metalabel_keep=50), SimClock(wall=5000.0), db=db)
    assert db.limits == [50]
    for i in range(30):
        st.record(_outcome(i % 3 != 0, ts=1000.0 + i, grade="A" if i % 2 else "C"))
    st.record({**_outcome(True), "ts": None})  # stamped with clock.wall()
    assert len(db.rows) == 31
    saved = db.rows[0]
    assert set(saved) == {"id", "asset", "target", "first_mover", "lag_pct", "btc_vol_pctile", "outcome",
                          "barrier", "net_pct", "quality_score", "grade", "ts"}
    assert db.rows[-1]["ts"] == 5000.0
    json.dumps(db.rows)

    st2 = MetaLabelStore(_settings(metalabel_keep=50), SimClock(), db=db)
    assert db.limits == [50, 50]
    assert len(st2) == 31
    assert st2.outcomes() == st.outcomes()  # oldest first although the db returned newest first
    args = ("BTC", "bybit", "binance", 0.7, 0.5)
    assert st2.estimate(*args) == st.estimate(*args)
    assert st2.by_grade() == st.by_grade()
    assert st2.snapshot() == st.snapshot()
    assert len(db.rows) == 31  # loading never re-saves

    # a smaller keep loads only the newest rows
    st3 = MetaLabelStore(_settings(metalabel_keep=5), SimClock(), db=db)
    assert len(st3) == 5 and st3.outcomes()[-1]["ts"] == 5000.0


def test_idempotent_id():
    db = FakeDb()
    st = MetaLabelStore(_settings(metalabel_keep=3), SimClock(), db=db)
    assert st.record({**_outcome(True), "id": "sh-1"}) is True
    assert st.record({**_outcome(False), "id": "sh-1"}) is False  # same shadow trade again
    assert len(st) == 1 and len(db.rows) == 1 and db.rows[0]["id"] == "sh-1"
    assert st.snapshot()["duplicates"] == 1 and st.snapshot()["rejected"] == 0
    for i in range(2, 5):
        st.record({**_outcome(True), "id": f"sh-{i}"})
    assert len(st) == 3  # sh-1 evicted, its id forgotten with it
    assert st.record({**_outcome(True), "id": "sh-1"}) is True
    # loaded rows are deduplicated by id as well (sh-2, sh-3, sh-4, sh-1, sh-1)
    st2 = MetaLabelStore(_settings(metalabel_keep=10), SimClock(), db=FakeDb(db.rows[1:] + db.rows[-1:]))
    assert len(st2) == 4 and st2.snapshot()["duplicates"] == 1


def test_db_failures_are_contained():
    class BrokenDb:
        def load_metalabel_outcomes(self, limit):
            raise RuntimeError("db down")

        def save_metalabel_outcome(self, row):
            raise RuntimeError("db down")

    st = _store(db=BrokenDb())
    assert len(st) == 0
    assert st.record(_outcome(True)) is True
    assert len(st) == 1
    st_nodb = _store(db=object())  # duck typing: no methods -> memory only
    assert st_nodb.record(_outcome(True)) is True


def test_load_skips_invalid_rows():
    db = FakeDb([_outcome(True, ts=1.0), {"asset": "BTC"}, {"junk": 1}, _outcome(False, ts=2.0)])
    st = MetaLabelStore(_settings(), SimClock(), db=db)
    assert len(st) == 2
    assert [o["outcome"] for o in st.outcomes()] == ["win", "loss"]


def test_by_grade_and_snapshot_json_safe():
    st = _store()
    st.record(_outcome(True, grade="A", net=1.0))
    st.record(_outcome(True, grade="A", net=0.6))
    st.record(_outcome(False, grade="A", net=-0.4))
    st.record(_outcome(False, grade="C", net=-0.2))
    st.record(_outcome(True, grade=None, net=0.3, barrier="time"))
    bg = st.by_grade()
    assert list(bg) == ["A", "C", "unknown"]
    assert bg["A"]["n"] == 3
    assert bg["A"]["win_rate"] == pytest.approx(2 / 3)
    assert bg["A"]["avg_net_pct"] == pytest.approx(0.4)
    assert bg["C"] == {"n": 1, "win_rate": 0.0, "avg_net_pct": pytest.approx(-0.2)}
    assert bg["unknown"]["n"] == 1
    snap = st.snapshot()
    json.dumps(snap)
    assert snap["n"] == 5 and snap["wins"] == 3
    assert snap["by_barrier"] == {"stop": 2, "target": 2, "time": 1}
    assert snap["settings"] == {"min_n": 20, "kappa": 10.0, "keep": 20000}
    top = snap["keys"][0]
    assert top["key"] == "BTC|bybit|binance|0.5-1|mid" and top["n"] == 5
    assert 0.0 < top["p10"] < top["mean"] < 1.0


def test_deterministic():
    def build() -> MetaLabelStore:
        st = _store(metalabel_min_n=3)
        for i in range(40):
            st.record(_outcome(i % 4 != 0, asset=("BTC", "ETH")[i % 2], lag=(0.2, 0.7, 1.5, 3.0)[i % 4],
                               pctile=(0.1, 0.5, 0.9, None)[i % 4], ts=float(i)))
        return st
    a, b = build(), build()
    assert a.snapshot() == b.snapshot()
    assert a.estimate("ETH", "bybit", "binance", 0.7, 0.5) == b.estimate("ETH", "bybit", "binance", 0.7, 0.5)
