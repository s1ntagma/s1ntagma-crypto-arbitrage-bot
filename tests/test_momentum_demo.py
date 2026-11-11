"""Momentum on the Bybit DEMO account: real orders through one adapter, with the order-integrity rules.

One order per client id, a timeout is "unknown" and is reconciled (never retried), the risk manager is asked
before the entry, the emergency stop blocks entries, a trade is closed only when the exchange says the
position is flat, a non-demo adapter is refused, and the key never leaves the process."""
from __future__ import annotations

import pytest

from latbot.basis.demo_exec import MomentumDemo
from latbot.config import ExchangeCredentials
from latbot.execution.risk import RiskManager
from latbot.models import Balance, Order, OrderStatus, OrderType, Side
from test_momentum import M, warm

SECRET = "sk-demo-SECRET-123"


class FakeDemo:
    """Stands in for ``CcxtAdapter`` (Bybit Demo, linear perps)."""

    def __init__(self, demo: bool = True, step: float = 0.001, min_amount: float = 0.001) -> None:
        self.name = "bybit.demo"
        self.cfg = type("Cfg", (), {"demo": demo})()
        self.step, self.min_amount = step, min_amount
        self.sent: list[tuple[str, str, float, bool]] = []  # (client id, side, amount, reduce_only)
        self.orders: dict[str, Order] = {}
        self.position = 0.0
        self.price = 100.0
        self.fee_rate = 0.00055
        self.timeout_next = False
        self.reject_next = ""
        self.fill_ratio = 1.0
        self.started = False

    async def start(self, assets, ws_assets=None):
        self.started = True

    async def stop(self):
        pass

    def symbol_for(self, asset):
        return f"{asset}/USDT:USDT"

    def order_amount(self, symbol, amount):
        q = int(amount / self.step + 1e-9) * self.step
        return q if q >= self.min_amount else None

    async def fetch_balances(self):
        return {"USDT": Balance(asset="USDT", free=50_000.0, total=50_000.0)}

    async def fetch_position(self, symbol):
        return self.position

    async def place_order(self, req, *, reduce_only=False):
        assert req.client_id not in self.orders, "the same client id must never be sent twice"
        self.sent.append((req.client_id, req.side.value, req.amount, reduce_only))
        o = Order(id=f"x{len(self.sent)}", client_id=req.client_id, exchange=self.name, symbol=req.symbol,
                  side=req.side, type=OrderType.MARKET, amount=req.amount, price=None, status=OrderStatus.SENT)
        if self.reject_next:
            from latbot.exchanges.base import OrderRejected
            why, self.reject_next = self.reject_next, ""
            raise OrderRejected(why)
        filled = req.amount * self.fill_ratio
        self.position += filled if req.side == Side.BUY else -filled
        done = Order(id=o.id, client_id=o.client_id, exchange=self.name, symbol=req.symbol, side=req.side,
                     type=OrderType.MARKET, amount=req.amount, price=None, status=OrderStatus.FILLED,
                     filled=filled, avg_price=self.price, fee_cost_quote=filled * self.price * self.fee_rate)
        self.orders[req.client_id] = done
        if self.timeout_next:  # the exchange took the order, the answer was lost
            self.timeout_next = False
            o.error = "timeout: status unknown"
            return o
        return done

    async def fetch_order(self, order_id, symbol, *, client_id=None):
        if client_id not in self.orders:
            from latbot.exchanges.base import ExchangeError
            raise ExchangeError("order not found")
        return self.orders[client_id]


def env(creds=True, demo=True, **over):
    over.setdefault("max_risk_per_trade_pct", 10.0)  # 10 % of the 1000 capital: the 100 size is allowed
    m = M(mom_demo_enabled=True, mom_demo_notional_quote=100.0, **over)
    risk = RiskManager(m.s, m.db, m.clock, None)
    fake = FakeDemo(demo=demo)
    c = ExchangeCredentials(name="bybit", api_key="k" if creds else "", api_secret=SECRET if creds else "")
    d = MomentumDemo(m.m, m.s, m.clock, risk, c, adapter=fake)
    return m, d, fake, risk


async def signal(m, d):
    warm(m)
    await d.start()
    m.path(100.0, 100.8, 600)  # the momentum rule fires: a long
    assert m.trades()[0]["status"] == "open"


async def test_entry_is_one_order_with_a_stable_client_id_and_the_exit_is_reduce_only():
    m, d, fake, risk = env()
    await signal(m, d)
    await d.step()
    await d.step()  # a second step never sends a second entry
    assert len(fake.sent) == 1
    cid, side, amount, reduce_only = fake.sent[0]
    assert side == "buy" and not reduce_only and len(cid) <= 36 and amount == pytest.approx(1.0, abs=0.01)
    t = m.trades()[0]
    assert t["demo"]["state"] == "open" and t["demo"]["qty"] == amount and risk.open_trades() == 1
    fake.price = 101.5
    m.path(100.8, 102.0, 300)
    m.path(102.0, 101.4, 60)  # the bot's trailing stop closes the position
    assert m.trades()[0]["status"] == "closed"
    await d.step()
    await d.step()
    assert len(fake.sent) == 2 and fake.sent[1][1] == "sell" and fake.sent[1][3] is True and fake.sent[1][2] == amount
    t = m.trades()[0]
    fees = amount * 100.0 * 0.00055 + amount * 101.5 * 0.00055
    assert t["demo"]["state"] == "closed" and t["demo"]["net"] == pytest.approx(amount * 1.5 - fees)
    assert fake.position == 0 and risk.open_trades() == 0
    assert d.status()["closed"] == 1 and d.status()["net_quote"] == pytest.approx(t["demo"]["net"])


async def test_a_timeout_is_unknown_and_reconciled_never_retried():
    m, d, fake, _ = env()
    await signal(m, d)
    fake.timeout_next = True
    await d.step()
    assert m.trades()[0]["demo"]["state"] == "entry_unknown" and len(fake.sent) == 1
    await d.step()  # fetch_order by client id: it did fill
    t = m.trades()[0]
    assert t["demo"]["state"] == "open" and len(fake.sent) == 1 and t["demo"]["entry_px"] == 100.0


async def test_risk_refusal_emergency_stop_missing_key_and_non_demo_send_nothing():
    m, d, fake, risk = env()
    await signal(m, d)
    risk.emergency, risk.emergency_reason = True, "test"
    await d.step()
    t = m.trades()[0]
    assert fake.sent == [] and t["demo"]["state"] == "skipped" and "Emergency" in t["demo"]["note"]

    m, d, fake, _ = env(max_trade_quote=5.0, min_trade_quote=10.0)
    await signal(m, d)
    await d.step()
    assert fake.sent == [] and m.trades()[0]["demo"]["state"] == "skipped"

    m, d, fake, _ = env(creds=False)
    await signal(m, d)
    await d.step()
    assert fake.sent == [] and not fake.started and "key" in d.status()["error"]

    m, d, fake, _ = env(demo=False)
    await signal(m, d)
    await d.step()
    assert fake.sent == [] and "demo" in d.status()["error"]


async def test_a_rejected_entry_and_a_too_small_size_open_nothing():
    m, d, fake, risk = env()
    await signal(m, d)
    fake.reject_next = "insufficient margin"
    await d.step()
    t = m.trades()[0]
    assert t["demo"]["state"] == "rejected" and "insufficient" in t["demo"]["note"] and risk.open_trades() == 0

    m, d, fake, _ = env()
    fake.min_amount = 50.0
    await signal(m, d)
    await d.step()
    assert fake.sent == [] and "minimum" in m.trades()[0]["demo"]["note"]


async def test_the_trade_is_closed_only_when_the_exchange_position_is_flat():
    m, d, fake, _ = env()
    await signal(m, d)
    await d.step()
    m.path(100.8, 99.9, 30)  # the bot's stop
    fake.fill_ratio = 0.5  # the exchange filled half of the exit
    await d.step()
    t = m.trades()[0]
    assert t["demo"]["state"] == "exit_due" and fake.position > 0 and len(fake.sent) == 2
    fake.fill_ratio = 1.0
    await d.step()  # a NEW order (new client id) for the rest, only after the first one is known
    assert len(fake.sent) == 3 and fake.sent[2][0] != fake.sent[1][0] and fake.sent[2][3] is True
    assert fake.position == pytest.approx(0.0) and m.trades()[0]["demo"]["state"] == "closed"


async def test_after_a_restart_the_position_is_reconciled_from_the_exchange():
    m, d, fake, risk = env()
    await signal(m, d)
    await d.step()
    d2 = MomentumDemo(m.m, m.s, m.clock, risk, ExchangeCredentials(name="bybit", api_key="k", api_secret=SECRET), adapter=fake)
    await d2.start()
    await d2.step()
    assert m.trades()[0]["demo"]["state"] == "open"  # still on the exchange: managed again
    fake.position = 0.0  # closed by hand on the exchange meanwhile
    d3 = MomentumDemo(m.m, m.s, m.clock, risk, ExchangeCredentials(name="bybit", api_key="k", api_secret=SECRET), adapter=fake)
    await d3.start()
    await d3.step()
    t = m.trades()[0]
    assert t["demo"]["state"] == "closed" and t["demo"]["net"] is None and "unknown" in t["demo"]["note"]
    assert len(fake.sent) == 1


async def test_the_key_never_leaves_the_process(caplog):
    m, d, fake, _ = env()
    with caplog.at_level("DEBUG"):
        await signal(m, d)
        await d.step()
    blob = repr(d.status()) + repr(m.trades()) + caplog.text
    assert SECRET not in blob and d.status()["key"] is True


async def test_the_risk_limits_cap_the_demo_size():
    m, d, fake, _ = env(max_risk_per_trade_pct=2.0)  # the default: 2 % of 1000 = 20 per trade
    await signal(m, d)
    await d.step()
    assert fake.sent[0][2] == pytest.approx(0.2, abs=0.01)  # about 20 / 100.6, not 100 / 100.6
