"""Offline tests for latbot.notify.telegram.TelegramNotifier (fake HTTP session)."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from latbot import events
from latbot.clock import SimClock
from latbot.config import ExchangeConfig, Secrets, Settings
from latbot.engine.opportunity import OpportunityManager
from latbot.events import EventBus
from latbot.models import (
    ExchangeHealth,
    ExchangeRole,
    ExecAssessment,
    ExecutionEconomics,
    LagSignal,
    Light,
    Mode,
    Opportunity,
    OppState,
    TradeRecord,
    new_id,
)
from latbot.notify.telegram import EDIT_THROTTLE_S, LINK_GRACE_S, TelegramNotifier, _money, _strip_tags

TOKEN = "123456789:AAFakeTokenForTestsOnly_abcdefghijklmnop"
OWNER = "42"

SPEC_CARD = (
    "🔥 REAL OPPORTUNITY\n"
    "BTC/USDT  LONG\n"
    "TARGET: Bybit\n"
    "NET: +4.82%\n"
    "BUY: $102,140\n"
    "SELL: $107,065\n"
    "CAPITAL: $100\n"
    "EXPECTED PROFIT: +$4.82\n"
    "WINDOW: ~11 sec\n"
    "HUMAN: 🟡 MEDIUM   BOT: 🟢 HIGH"
)


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
class FakeResponse:
    def __init__(self, data: Any) -> None:
        self._data = data

    async def json(self, **_: Any) -> Any:
        return self._data

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False


class FakeSession:
    """Records ``(method, json)`` per call; responses can be scripted per method."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.urls: list[str] = []
        self.responses: dict[str, list[Any]] = {}
        self.fail = False
        self._next_id = 100

    def post(self, url: str, json: dict[str, Any] | None = None, **_: Any) -> FakeResponse:
        method = url.rsplit("/", 1)[-1]
        self.urls.append(url)
        self.calls.append((method, dict(json or {})))
        if self.fail:
            raise RuntimeError(f"boom {url}")
        scripted = self.responses.get(method)
        if scripted:
            return FakeResponse(scripted.pop(0))
        if method in ("sendMessage", "editMessageText"):
            mid = json.get("message_id") if json and method == "editMessageText" else None
            if mid is None:
                mid = self._next_id
                self._next_id += 1
            return FakeResponse({"ok": True, "result": {"message_id": mid, "text": json.get("text") if json else ""}})
        if method == "getUpdates":
            return FakeResponse({"ok": True, "result": []})
        return FakeResponse({"ok": True, "result": True})

    async def close(self) -> None:
        return None

    # helpers
    def of(self, method: str) -> list[dict[str, Any]]:
        return [p for m, p in self.calls if m == method]

    def texts(self, method: str = "sendMessage") -> list[str]:
        return [p.get("text", "") for p in self.of(method)]

    def sent(self) -> list[str]:
        return [t for t in self.texts() if "запущен" not in t and "остановлен" not in t]


class KvDb:
    def __init__(self) -> None:
        self.kv: dict[str, Any] = {}

    def kv_get(self, key: str, default: Any = None) -> Any:
        return self.kv.get(key, default)

    def kv_set(self, key: str, value: Any) -> None:
        self.kv[key] = value


class StubOpps:
    def __init__(self, clock: SimClock) -> None:
        self.clock = clock
        self.items: dict[str, Opportunity] = {}
        self.transitions: list[tuple[str, OppState, str]] = []

    def add(self, opp: Opportunity) -> Opportunity:
        self.items[opp.id] = opp
        return opp

    def get(self, opp_id: str) -> Opportunity | None:
        return self.items.get(opp_id)

    def active(self) -> list[Opportunity]:
        return sorted([o for o in self.items.values() if o.is_active], key=lambda o: o.econ.net_pct, reverse=True)

    def best(self, now_mono: float) -> Opportunity | None:
        for o in self.active():
            if o.human.light is not Light.RED or o.bot.light is not Light.RED:
                return o
        return None

    async def transition(self, opp_id: str, new_state: OppState, reason: str = "") -> Opportunity:
        opp = self.items[opp_id]
        opp.state = new_state
        self.transitions.append((opp_id, new_state, reason))
        return opp


class StubRisk:
    def __init__(self, bus: EventBus) -> None:
        self.bus = bus
        self.emergency = False
        self.emergency_reason = ""

    async def set_emergency(self, active: bool, reason: str = "") -> None:
        self.emergency = active
        self.emergency_reason = reason if active else ""
        await self.bus.publish(events.EMERGENCY, {"active": active, "reason": reason})


class StubExec:
    def __init__(self, risk: StubRisk) -> None:
        self.risk = risk
        self.stops: list[str] = []
        self.resumes = 0

    async def emergency_stop(self, reason: str = "user") -> dict[str, Any]:
        self.stops.append(reason)
        await self.risk.set_emergency(True, reason)
        return {"ok": True}

    async def resume(self) -> None:
        self.resumes += 1
        await self.risk.set_emergency(False, "resume")


# --------------------------------------------------------------------------- #
# builders
# --------------------------------------------------------------------------- #
def make_settings(**over: Any) -> Settings:
    base: dict[str, Any] = dict(
        exchanges=[
            ExchangeConfig(name="bybit", role=ExchangeRole.EXECUTION),
            ExchangeConfig(name="kraken", role=ExchangeRole.EXECUTION, label="Kraken"),
            ExchangeConfig(name="binance", role=ExchangeRole.SIGNAL, stream="bbo"),
        ],
        host="127.0.0.1",
        port=8787,
        public_url="",
    )
    base.update(over)
    return Settings(**base)


def make_econ(**over: Any) -> ExecutionEconomics:
    base: dict[str, Any] = dict(
        direction=1,
        capital_quote=100.0,
        executable_quote=100.0,
        base_amount=100.0 / 102_140.0,
        entry_best=102_100.0,
        entry_vwap=102_140.0,
        exit_best=102_000.0,
        exit_vwap_now=102_000.0,
        exit_target=107_065.0,
        spread_pct=0.1,
        entry_fee_pct=0.1,
        exit_fee_pct=0.1,
        entry_slippage_pct=0.04,
        exit_slippage_pct=0.0,
        execution_loss_pct=0.05,
        gross_pct=5.07,
        total_cost_pct=0.25,
        net_pct=4.82,
        net_quote=4.82,
        liquidity_ok=True,
    )
    base.update(over)
    return ExecutionEconomics(**base)


def make_opp(clock: SimClock, **over: Any) -> Opportunity:
    now = clock.mono()
    direction = over.pop("direction", 1)
    econ = over.pop("econ", make_econ(direction=direction))
    human = over.pop("human", ExecAssessment(light=Light.YELLOW, label="MEDIUM", est_execution_s=7.0, margin_s=4.0, probability=0.5))
    bot = over.pop("bot", ExecAssessment(light=Light.GREEN, label="HIGH", est_execution_s=0.4, margin_s=10.6, probability=0.8))
    target = over.pop("target_exchange", "bybit")
    symbol = over.pop("target_symbol", "BTC/USDT")
    signal = LagSignal(
        asset="BTC",
        direction=direction,
        first_mover="binance",
        first_move_pct=6.0,
        first_move_start_mono=now - 1.0,
        reacted={"binance": 6.0, target: 0.5},
        target=target,
        target_symbol=symbol,
        target_move_pct=0.5,
        lag_pct=5.5,
        expected_catchup_pct=5.0,
        catchup_ratio=0.9,
        detected_mono=now,
        detected_ts=clock.wall(),
        movers_count=2,
        target_quote=symbol.split("/")[1].split(":")[0],
    )
    base: dict[str, Any] = dict(
        id=new_id("opp_"),
        asset="BTC",
        target_exchange=target,
        target_symbol=symbol,
        first_mover="binance",
        direction=direction,
        mode=Mode.PAPER,
        state=OppState.VALID,
        signal=signal,
        econ=econ,
        human=human,
        bot=bot,
        why="Binance moved +6.0%",
        detected_ts=clock.wall(),
        first_seen_mono=now,
        last_valid_mono=now,
        last_update_mono=now,
        estimated_duration_s=11.0,
        quote=symbol.split("/")[1].split(":")[0],
        pair_url="https://www.bybit.com/trade/spot/BTC/USDT",
    )
    base.update(over)
    return Opportunity(**base)


def make_trade(opp: Opportunity, state: OppState, **over: Any) -> TradeRecord:
    base: dict[str, Any] = dict(
        id=over.pop("id", "tr_1"),
        opportunity_id=opp.id,
        mode=Mode.PAPER,
        exchange=opp.target_exchange,
        asset=opp.asset,
        symbol=opp.target_symbol,
        direction=opp.direction,
        capital_quote=100.0,
        expected_net_pct=opp.econ.net_pct,
        expected_profit_quote=opp.econ.net_quote,
        expected_entry_price=opp.econ.entry_vwap,
        expected_exit_price=opp.econ.exit_target,
        state=state,
    )
    base.update(over)
    return TradeRecord(**base)


class Env:
    def __init__(self, chat_id: str = OWNER, db: KvDb | None = None, token: str = TOKEN, config_path: str = "config.json", real_opps: bool = False, **settings_over: Any) -> None:
        self.settings = make_settings(**settings_over)
        self.secrets = Secrets(telegram_bot_token=token, telegram_chat_id=chat_id)
        self.bus = EventBus()
        self.clock = SimClock(wall=1_700_000_000.0, mono=1000.0)
        # ``real_opps``: the production OpportunityManager (state machine enforced) instead of StubOpps
        self.opps: Any = OpportunityManager(self.settings, self.bus, self.clock) if real_opps else StubOpps(self.clock)
        self.risk = StubRisk(self.bus)
        self.execution = StubExec(self.risk)
        self.session = FakeSession()
        self.db = db
        self.notifier = TelegramNotifier(
            self.settings, self.secrets, self.bus, self.opps, self.execution, self.risk, self.clock,
            session=self.session, db=db, config_path=config_path,
        )
        self.settings_events: list[Any] = []
        self.bus.subscribe(events.SETTINGS, lambda p: self.settings_events.append(p))

    async def start(self) -> "Env":
        await self.notifier.start(poll=False)
        return self

    def message(self, text: str, chat: str = OWNER, chat_type: str = "private", date: float | None = None, update_id: int = 1) -> dict[str, Any]:
        return {
            "update_id": update_id,
            "message": {
                "message_id": 7,
                "date": int(self.clock.wall() if date is None else date),
                "chat": {"id": int(chat), "type": chat_type},
                "text": text,
            },
        }

    def callback(self, data: str, chat: str = OWNER, message_id: int = 55, cq_id: str = "cq1") -> dict[str, Any]:
        return {"update_id": 9, "callback_query": {"id": cq_id, "data": data, "message": {"message_id": message_id, "chat": {"id": int(chat), "type": "private"}}}}


# --------------------------------------------------------------------------- #
# formatting
# --------------------------------------------------------------------------- #
def test_format_alert_matches_spec_card():
    env = Env()
    opp = make_opp(env.clock)
    html = env.notifier.format_alert(opp, env.clock.mono())
    assert _strip_tags(html) == SPEC_CARD
    assert html.startswith("🔥 <b>REAL OPPORTUNITY</b>\n<b>BTC/USDT  LONG</b>")
    assert "NET: <b>+4.82%</b>" in html
    assert html.count("<b>") == 3


def test_format_alert_eur_quote_short_and_window_rounding():
    env = Env()
    econ = make_econ(direction=-1, entry_vwap=2.5, exit_target=2.4, executable_quote=99.5, net_quote=-1.234, net_pct=-1.24)
    opp = make_opp(env.clock, direction=-1, econ=econ, target_exchange="kraken", target_symbol="BTC/EUR", pair_url="")
    env.clock.advance(1.6)
    text = _strip_tags(env.notifier.format_alert(opp, env.clock.mono()))
    assert "BTC/EUR  SHORT" in text and "TARGET: Kraken" in text
    # SHORT: BUY = exit_target, SELL = entry_vwap
    assert "BUY: €2.40" in text and "SELL: €2.50" in text
    assert "CAPITAL: €99.50" in text and "EXPECTED PROFIT: -€1.23" in text
    assert "WINDOW: ~9 sec" in text


def test_money_formatting():
    assert _money(102_140) == "$102,140"
    assert _money(4.82, signed=True) == "+$4.82"
    assert _money(100.0) == "$100"
    assert _money(0.5234, "EUR") == "€0.5234"
    assert _money(0.00001234) == "$0.00001234"
    assert _money(-3.5) == "-$3.50"
    assert _money(0.0, signed=True) == "+$0"


# --------------------------------------------------------------------------- #
# should_alert
# --------------------------------------------------------------------------- #
def test_should_alert_rules():
    env = Env()
    n = env.notifier
    opp = make_opp(env.clock)
    assert n.should_alert(opp)
    assert not n.should_alert(make_opp(env.clock, econ=make_econ(net_pct=2.99)))
    env.settings.telegram_profit_threshold_pct = 1.0
    assert n.should_alert(make_opp(env.clock, econ=make_econ(net_pct=1.0)))
    red = ExecAssessment(light=Light.RED, label="NOT EXECUTABLE", est_execution_s=7.0, margin_s=-3.0, probability=0.1)
    # human RED is blocked by alert_human_only even with bot GREEN
    assert not n.should_alert(make_opp(env.clock, human=red))
    env.settings.alert_human_only = False
    assert n.should_alert(make_opp(env.clock, human=red))
    # both RED blocked by telegram_require_executable
    assert not n.should_alert(make_opp(env.clock, human=red, bot=red))
    env.settings.telegram_require_executable = False
    assert n.should_alert(make_opp(env.clock, human=red, bot=red))
    # inactive / already notified
    assert not n.should_alert(make_opp(env.clock, state=OppState.EXPIRED))
    assert not n.should_alert(make_opp(env.clock, telegram_message_id=5))
    # not yet validated on the real book (OPP_NEW is published while DETECTED)
    assert not n.should_alert(make_opp(env.clock, state=OppState.DETECTED))
    assert not n.should_alert(make_opp(env.clock, state=OppState.VALIDATING))


# --------------------------------------------------------------------------- #
# alerts: dedupe, buttons, throttle, final edits
# --------------------------------------------------------------------------- #
async def test_alert_once_per_id_with_buttons_and_message_id():
    env = await Env().start()
    assert env.session.texts() == ["🚀 latbot запущен (PAPER)"]
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    await env.bus.publish(events.OPP_NEW, opp)
    await env.bus.publish(events.OPP_UPDATE, opp)
    alerts = [p for p in env.session.of("sendMessage") if "REAL OPPORTUNITY" in p["text"]]
    assert len(alerts) == 1
    p = alerts[0]
    assert p["chat_id"] == OWNER and p["parse_mode"] == "HTML"
    rows = p["reply_markup"]["inline_keyboard"]
    assert rows[0] == [{"text": "OPEN ON Bybit", "url": "https://www.bybit.com/trade/spot/BTC/USDT"}]
    assert rows[1] == [{"text": "OPEN OPPORTUNITY", "url": f"http://127.0.0.1:8787/#opp/{opp.id}"}]
    assert opp.telegram_message_id == 101 and opp.notified_ts == env.clock.wall()
    assert env.notifier.message_id_for(opp.id) == 101
    assert env.opps.transitions == [(opp.id, OppState.NOTIFIED, "telegram alert sent")]
    assert opp.state is OppState.NOTIFIED


async def test_alert_through_real_state_machine_reaches_notified():
    """Pipeline.evaluate: upsert (OPP_NEW while DETECTED) -> VALIDATING -> VALID.
    The alert must fire once the opp is VALID and leave it NOTIFIED."""
    env = await Env(real_opps=True).start()
    opps: OpportunityManager = env.opps
    proto = make_opp(env.clock)

    def alerts() -> list[dict[str, Any]]:
        return [p for p in env.session.of("sendMessage") if "REAL OPPORTUNITY" in p["text"]]

    opp, is_new = await opps.upsert(proto.signal, proto.econ, proto.human, proto.bot, proto.why, proto.estimated_duration_s, Mode.PAPER)
    assert is_new and opp.state is OppState.DETECTED
    assert alerts() == []  # nothing is validated on the real book yet
    await opps.transition(opp.id, OppState.VALIDATING, "economics computed")
    assert alerts() == []
    await opps.transition(opp.id, OppState.VALID, "net +4.82% on real book")
    assert len(alerts()) == 1
    assert opp.state is OppState.NOTIFIED
    assert [s for _, s, _ in opp.state_history] == ["DETECTED", "VALIDATING", "VALID", "NOTIFIED"]
    assert opp.telegram_message_id == 101 and opp.notified_ts == env.clock.wall()
    assert env.notifier.message_id_for(opp.id) == 101
    # a following update (OPP_UPDATE) edits, never re-sends
    env.clock.advance(EDIT_THROTTLE_S + 0.1)
    same, is_new = await opps.upsert(proto.signal, make_econ(net_pct=4.5, net_quote=4.5), proto.human, proto.bot, proto.why, 11.0, Mode.PAPER)
    assert same is opp and not is_new
    assert len(alerts()) == 1
    assert len(env.session.of("editMessageText")) == 1
    assert opp.state is OppState.NOTIFIED


async def test_below_threshold_at_valid_then_rises_alerts_once_and_notifies():
    """An opp validated at NET 2 % that later climbs over the Telegram
    threshold is alerted from the OPP_UPDATE (and still reaches NOTIFIED)."""
    env = await Env(real_opps=True).start()
    opps: OpportunityManager = env.opps
    proto = make_opp(env.clock)
    opp, _ = await opps.upsert(proto.signal, make_econ(net_pct=2.0, net_quote=2.0), proto.human, proto.bot, proto.why, 11.0, Mode.PAPER)
    await opps.transition(opp.id, OppState.VALIDATING, "economics computed")
    await opps.transition(opp.id, OppState.VALID, "net +2.00% on real book")
    assert [p for p in env.session.of("sendMessage") if "REAL OPPORTUNITY" in p["text"]] == []
    assert opp.state is OppState.VALID
    env.clock.advance(0.5)
    await opps.upsert(proto.signal, make_econ(net_pct=3.4, net_quote=3.4), proto.human, proto.bot, proto.why, 11.0, Mode.PAPER)
    sent = [p for p in env.session.of("sendMessage") if "REAL OPPORTUNITY" in p["text"]]
    assert len(sent) == 1 and "NET: <b>+3.40%</b>" in sent[0]["text"]
    assert opp.state is OppState.NOTIFIED
    assert [s for _, s, _ in opp.state_history] == ["DETECTED", "VALIDATING", "VALID", "NOTIFIED"]


async def test_public_url_used_for_deep_link_and_managed_object_updated():
    env = await Env(public_url="https://bot.example.org/").start()
    managed = env.opps.add(make_opp(env.clock))
    payload = Opportunity(**{**managed.__dict__})  # a different object with the same id (bus copy)
    await env.bus.publish(events.OPP_NEW, payload)
    p = [p for p in env.session.of("sendMessage") if "REAL OPPORTUNITY" in p["text"]][0]
    assert p["reply_markup"]["inline_keyboard"][-1][0]["url"] == f"https://bot.example.org/#opp/{managed.id}"
    assert managed.telegram_message_id == 101 and payload.telegram_message_id == 101


async def test_edit_throttled_to_one_per_three_seconds():
    env = await Env().start()
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    env.clock.advance(1.0)
    opp.econ = make_econ(net_pct=4.5, net_quote=4.5)
    await env.bus.publish(events.OPP_UPDATE, opp)
    assert env.session.of("editMessageText") == []
    env.clock.advance(EDIT_THROTTLE_S - 1.0 + 0.5)
    await env.bus.publish(events.OPP_UPDATE, opp)
    edits = env.session.of("editMessageText")
    assert len(edits) == 1 and edits[0]["message_id"] == 101 and "+4.50%" in edits[0]["text"]
    await env.bus.publish(events.OPP_UPDATE, opp)  # immediately again -> throttled
    assert len(env.session.of("editMessageText")) == 1


async def test_expired_final_edit_once_and_no_more_edits():
    env = await Env().start()
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    env.clock.advance(9.4)
    opp.state = OppState.EXPIRED
    opp.actual_duration_s = 9.4
    await env.bus.publish(events.OPP_STATE, {"opportunity": opp, "from": "NOTIFIED", "to": "EXPIRED", "reason": "window closed"})
    edits = env.session.of("editMessageText")
    assert len(edits) == 1
    assert edits[0]["text"].endswith("\n… EXPIRED after 9.4 s")
    assert _strip_tags(edits[0]["text"]).startswith(SPEC_CARD)
    assert edits[0]["reply_markup"]["inline_keyboard"] == [[{"text": "OPEN OPPORTUNITY", "url": f"http://127.0.0.1:8787/#opp/{opp.id}"}]]
    await env.bus.publish(events.OPP_STATE, {"opportunity": opp, "from": "NOTIFIED", "to": "EXPIRED", "reason": "again"})
    env.clock.advance(10)
    await env.bus.publish(events.OPP_UPDATE, opp)
    assert len(env.session.of("editMessageText")) == 1


async def test_cancelled_state_final_edit_with_reason():
    env = await Env().start()
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    opp.state = OppState.CANCELLED
    await env.bus.publish(events.OPP_STATE, {"opportunity": opp, "from": "VALID", "to": OppState.CANCELLED, "reason": "user"})
    assert env.session.of("editMessageText")[0]["text"].endswith("… CANCELLED: user")


async def test_trade_notices_and_closed_final_edit():
    env = await Env().start()
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    opp.execution_id = "tr_1"
    trade = make_trade(opp, OppState.EXECUTING)
    await env.bus.publish(events.TRADE_UPDATE, trade)
    await env.bus.publish(events.TRADE_UPDATE, trade)  # every step publishes; notice once
    assert env.session.sent()[-1] == "📈 Сделка открыта: BTC LONG на Bybit, $100 (PAPER)"
    assert len([t for t in env.session.sent() if "открыта" in t]) == 1
    # OPP_STATE CLOSED arrives first (engine order) - the final edit waits for the realized number
    opp.state = OppState.CLOSED
    await env.bus.publish(events.OPP_STATE, {"opportunity": opp, "from": "FILLED", "to": "CLOSED", "reason": "realized +3.90"})
    assert env.session.of("editMessageText") == []
    trade.state = OppState.CLOSED
    trade.actual_net_pct = 3.9
    trade.realized_pnl_quote = 3.9
    await env.bus.publish(events.TRADE_UPDATE, trade)
    await env.bus.publish(events.TRADE_UPDATE, trade)
    assert env.session.sent()[-1] == "🟢 Сделка закрыта: BTC LONG на Bybit, NET +3.90% (+$3.90) (PAPER)"
    assert len([t for t in env.session.sent() if "закрыта" in t]) == 1
    edits = env.session.of("editMessageText")
    assert len(edits) == 1 and edits[0]["text"].endswith("\n… CLOSED: realized +3.9%")


async def test_failed_and_cancelled_trade_notices():
    env = await Env().start()
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    opp.execution_id = "tr_2"
    failed = make_trade(opp, OppState.FAILED, id="tr_2", error="entry order failed: rejected", realized_pnl_quote=-0.1)
    await env.bus.publish(events.TRADE_UPDATE, failed)
    assert env.session.sent()[-1].startswith("❌ Сделка не удалась: BTC LONG на Bybit: entry order failed: rejected")
    assert env.session.of("editMessageText")[-1]["text"].endswith("… FAILED: entry order failed: rejected")
    cancelled = make_trade(opp, OppState.CANCELLED, id="tr_3", error="entry not filled")
    await env.bus.publish(events.TRADE_UPDATE, cancelled)
    await env.bus.publish(events.TRADE_UPDATE, cancelled)
    assert len([t for t in env.session.sent() if "отменена" in t]) == 1


async def test_html_rejected_falls_back_to_plain_text():
    env = await Env().start()
    env.session.responses["sendMessage"] = [{"ok": False, "description": "Bad Request: can't parse entities: unsupported start tag"}]
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    calls = [p for p in env.session.of("sendMessage") if "REAL OPPORTUNITY" in p["text"]]
    assert len(calls) == 2
    assert calls[0]["parse_mode"] == "HTML" and "<b>" in calls[0]["text"]
    assert "parse_mode" not in calls[1] and "<" not in calls[1]["text"] and calls[1]["text"] == SPEC_CARD
    assert opp.telegram_message_id == 101


async def test_button_rejected_resends_without_markup():
    env = await Env().start()
    env.session.responses["sendMessage"] = [{"ok": False, "description": "Bad Request: BUTTON_URL_INVALID"}]
    opp = env.opps.add(make_opp(env.clock))
    await env.bus.publish(events.OPP_NEW, opp)
    calls = [p for p in env.session.of("sendMessage") if "REAL OPPORTUNITY" in p["text"]]
    assert len(calls) == 2 and "reply_markup" in calls[0] and "reply_markup" not in calls[1]


# --------------------------------------------------------------------------- #
# owner / backlog / offset
# --------------------------------------------------------------------------- #
async def test_owner_only_first_start_claims_chat_into_kv():
    db = KvDb()
    env = await Env(chat_id="", db=db).start()
    n = env.notifier
    assert n.chat_id is None and env.session.calls == []  # no owner yet -> start notice not sent
    await n.handle_update(env.message("/status", chat="7"))
    assert env.session.calls == []  # not /start -> ignored
    await n.handle_update(env.message("/start", chat="9", chat_type="group"))
    assert n.chat_id is None and env.session.calls == []  # groups never become owner
    await n.handle_update(env.message("/start", chat="7"))
    assert n.chat_id == "7" and db.kv["telegram_chat_id"] == "7"
    assert env.session.of("sendMessage")[0]["chat_id"] == "7" and "/status" in env.session.texts()[0]
    await n.handle_update(env.message("/status", chat="8"))
    assert all(p["chat_id"] == "7" for p in env.session.of("sendMessage")) and len(env.session.of("sendMessage")) == 1


async def test_chat_id_from_kv_when_secret_missing():
    db = KvDb()
    db.kv["telegram_chat_id"] = 77
    db.kv["telegram_offset"] = 12
    env = Env(chat_id="", db=db)
    assert env.notifier.chat_id == "77" and env.notifier.offset == 12


async def test_messages_older_than_start_are_ignored():
    env = await Env().start()
    await env.notifier.handle_update(env.message("/status", date=env.clock.wall() - 120))
    assert env.session.sent() == []
    await env.notifier.handle_update(env.message("/status"))
    assert len(env.session.sent()) == 1


async def test_poll_loop_skips_backlog_persists_offset_and_handles_commands():
    db = KvDb()
    env = Env(db=db)
    env.session.responses["getUpdates"] = [
        {"ok": True, "result": [{"update_id": 5, "message": {"message_id": 1, "date": 1, "chat": {"id": 42, "type": "private"}, "text": "/status"}}]},
        {"ok": True, "result": [env.message("/mode", update_id=6)]},
    ]
    await env.notifier.start(poll=True)
    for _ in range(100):
        await asyncio.sleep(0.02)
        if len(env.session.of("getUpdates")) >= 3:
            break
    await env.notifier.stop()
    polls = env.session.of("getUpdates")
    assert polls[0]["offset"] == -1
    assert polls[1]["offset"] == 6 and polls[1]["timeout"] == 20 and polls[1]["allowed_updates"] == ["message", "callback_query"]
    assert db.kv["telegram_offset"] == 7 and env.notifier.offset == 7
    assert any(p["offset"] == 7 for p in polls[2:])
    texts = env.session.texts()
    assert not any("Активных" in t for t in texts)  # the backlog /status was dropped
    assert any(t.startswith("Режим: PAPER") for t in texts)
    assert texts[-1] == "⏹ latbot остановлен"


async def test_poll_loop_survives_errors(caplog):
    env = Env()
    env.session.responses["getUpdates"] = [
        {"ok": True, "result": []},
        {"ok": True, "result": [{"update_id": 1, "message": "garbage"}, {"update_id": 2, "callback_query": None}]},
    ]
    with caplog.at_level(logging.DEBUG):
        await env.notifier.start(poll=True)
        await asyncio.sleep(0.3)
        await env.notifier.stop()
    assert env.notifier.offset == 3
    assert TOKEN not in caplog.text


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
async def test_status_command_text():
    env = await Env().start()
    opp = env.opps.add(make_opp(env.clock, econ=make_econ(net_pct=1.2, net_quote=1.2)))
    env.opps.add(make_opp(env.clock, econ=make_econ(net_pct=0.4, net_quote=0.4), target_exchange="kraken"))
    h = ExchangeHealth(exchange="bybit", ws_connected=True)
    await env.bus.publish(events.HEALTH, h)
    await env.bus.publish(events.HEALTH, ExchangeHealth(exchange="kraken", ws_connected=False))
    await env.notifier.handle_update(env.message("/status"))
    text = env.session.sent()[-1]
    assert text.startswith("📊 latbot: PAPER")
    assert "EMERGENCY STOP: выключен" in text
    assert "Активных возможностей: 2" in text
    assert "Лучшая: BTC LONG на Bybit NET +1.20% HUMAN 🟡 BOT 🟢" in text
    assert "Биржи: Bybit ✅, Kraken ❌" in text
    assert "Порог Telegram: 3%" in text and "HUMAN_EXECUTION_TIME: 7 с" in text
    assert opp.id not in text


async def test_human_and_threshold_commands_persist(tmp_path: Path):
    cfg = tmp_path / "config.json"
    env = await Env(config_path=str(cfg)).start()
    await env.notifier.handle_update(env.message("/human 9"))
    assert env.settings.human_execution_time_s == 9.0
    assert env.session.sent()[-1] == "HUMAN_EXECUTION_TIME = 9 с"
    assert json.loads(cfg.read_text(encoding="utf-8"))["human_execution_time_s"] == 9.0
    assert env.settings_events and env.settings_events[-1]["human_execution_time_s"] == 9.0
    await env.notifier.handle_update(env.message("/human abc"))
    assert env.session.sent()[-1].startswith("Использование: /human")
    await env.notifier.handle_update(env.message("/human 500"))
    assert env.session.sent()[-1].startswith("Использование: /human") and env.settings.human_execution_time_s == 9.0
    await env.notifier.handle_update(env.message("/threshold 2,5"))
    assert env.settings.telegram_profit_threshold_pct == 2.5
    assert env.session.sent()[-1] == "Порог Telegram = 2.5%"
    assert json.loads(cfg.read_text(encoding="utf-8"))["telegram_profit_threshold_pct"] == 2.5
    await env.notifier.handle_update(env.message("/threshold -1"))
    assert env.session.sent()[-1].startswith("Использование: /threshold")


async def test_mode_help_and_unknown_commands():
    env = await Env().start()
    await env.notifier.handle_update(env.message("/mode"))
    assert env.session.sent()[-1] == "Режим: PAPER (бумажный). Переключение - только в терминале."
    await env.notifier.handle_update(env.message("/help@latbot_bot"))
    assert env.session.sent()[-1].startswith("Команды:") and "/threshold" in env.session.sent()[-1]
    await env.notifier.handle_update(env.message("привет"))
    assert env.session.sent()[-1].startswith("Команды:")
    env.settings.paper_mode = False
    env.settings.live_mode = True
    await env.notifier.handle_update(env.message("/mode"))
    assert env.session.sent()[-1].startswith("Режим: LIVE")


async def test_stop_confirmation_flow_and_resume():
    env = await Env().start()
    await env.notifier.handle_update(env.message("/stop"))
    prompt = env.session.of("sendMessage")[-1]
    assert prompt["text"] == "Остановить все AUTO-сделки и отменить ордера?"
    assert prompt["reply_markup"]["inline_keyboard"] == [[
        {"text": "🛑 Да, остановить", "callback_data": "stop:yes"},
        {"text": "Отмена", "callback_data": "stop:no"},
    ]]
    assert env.execution.stops == []
    # a stranger pressing the button is only answered, never acted on
    await env.notifier.handle_update(env.callback("stop:yes", chat="8", cq_id="cq0"))
    assert env.execution.stops == [] and env.session.of("answerCallbackQuery")[-1] == {"callback_query_id": "cq0"}
    await env.notifier.handle_update(env.callback("stop:yes"))
    assert env.execution.stops == ["telegram"] and env.risk.emergency
    assert env.session.of("answerCallbackQuery")[-1]["callback_query_id"] == "cq1"
    edit = env.session.of("editMessageText")[-1]
    assert edit["message_id"] == 55 and edit["text"] == "🛑 EMERGENCY STOP включён"
    # the EMERGENCY bus event triggered by our own stop is not announced twice
    assert not any("EMERGENCY STOP включён" in t for t in env.session.sent())
    await env.notifier.handle_update(env.callback("stop:no", message_id=56))
    assert env.session.of("editMessageText")[-1] == {
        "chat_id": OWNER, "message_id": 56, "text": "Отменено", "parse_mode": "HTML", "disable_web_page_preview": True,
    }
    await env.notifier.handle_update(env.callback("other:thing"))
    assert env.session.of("answerCallbackQuery")[-1] == {"callback_query_id": "cq1"}
    await env.notifier.handle_update(env.message("/resume"))
    assert env.execution.resumes == 1 and not env.risk.emergency
    assert env.session.sent()[-1] == "▶️ Возобновлено"
    assert not any("снят" in t for t in env.session.sent())


async def test_stop_without_execution_uses_risk():
    env = Env()
    env.notifier._execution = None  # execution engine not wired (e.g. early start)
    await env.notifier.start(poll=False)
    await env.notifier.handle_update(env.callback("stop:yes"))
    assert env.risk.emergency and env.risk.emergency_reason == "telegram"


# --------------------------------------------------------------------------- #
# emergency / links / lifecycle
# --------------------------------------------------------------------------- #
async def test_emergency_notices_deduped():
    env = await Env().start()
    await env.bus.publish(events.EMERGENCY, {"active": True, "reason": "daily loss"})
    await env.bus.publish(events.EMERGENCY, {"active": True, "reason": "daily loss"})
    await env.bus.publish(events.EMERGENCY, {"active": False, "reason": "resume"})
    await env.bus.publish(events.EMERGENCY, {"active": False, "reason": "resume"})
    assert env.session.sent() == ["🛑 EMERGENCY STOP включён: daily loss", "✅ EMERGENCY STOP снят"]


async def test_link_lost_and_restored_with_grace():
    env = await Env().start()
    h = ExchangeHealth(exchange="kraken", ws_connected=True)
    await env.bus.publish(events.HEALTH, h)
    h.ws_connected = False
    await env.bus.publish(events.HEALTH, h)
    env.clock.advance(10)
    await env.bus.publish(events.HEALTH, h)
    assert await env.notifier.check_links(env.clock.mono()) == []
    assert env.session.sent() == []
    env.clock.advance(LINK_GRACE_S - 10 + 1)
    assert await env.notifier.check_links(env.clock.mono()) == ["kraken"]
    await env.bus.publish(events.HEALTH, h)
    assert env.session.sent() == ["⚠️ Связь с биржей Kraken потеряна"]
    h.ws_connected = True
    await env.bus.publish(events.HEALTH, h)
    await env.bus.publish(events.HEALTH, h)
    assert env.session.sent() == ["⚠️ Связь с биржей Kraken потеряна", "✅ Связь с биржей Kraken восстановлена"]
    # a short blip inside the grace period is silent
    h.ws_connected = False
    await env.bus.publish(events.HEALTH, h)
    env.clock.advance(5)
    h.ws_connected = True
    await env.bus.publish(events.HEALTH, h)
    env.clock.advance(60)
    assert await env.notifier.check_links(env.clock.mono()) == []
    assert len(env.session.sent()) == 2


async def test_stale_feed_counts_as_link_down():
    env = await Env().start()
    h = ExchangeHealth(exchange="bybit", ws_connected=True, stale=True)
    await env.bus.publish(events.HEALTH, h)
    env.clock.advance(LINK_GRACE_S + 1)
    await env.bus.publish(events.HEALTH, h)
    assert env.session.sent() == ["⚠️ Связь с биржей Bybit потеряна"]


async def test_no_token_is_a_no_op():
    env = Env(token="")
    await env.notifier.start()
    assert not env.notifier.enabled
    assert await env.notifier.send("hi") is None
    await env.bus.publish(events.OPP_NEW, make_opp(env.clock))
    await env.notifier.stop()
    assert env.session.calls == []


async def test_start_stop_subscriptions_and_notices():
    env = await Env().start()
    await env.notifier.start(poll=False)  # idempotent
    assert env.session.texts() == ["🚀 latbot запущен (PAPER)"]
    await env.notifier.stop()
    assert env.session.texts()[-1] == "⏹ latbot остановлен"
    await env.bus.publish(events.OPP_NEW, env.opps.add(make_opp(env.clock)))
    assert len(env.session.texts()) == 2  # unsubscribed
    await env.notifier.stop()  # idempotent
    assert len(env.session.texts()) == 2


async def test_failures_are_logged_without_the_token(caplog):
    env = await Env().start()
    env.session.fail = True
    with caplog.at_level(logging.DEBUG):
        assert await env.notifier.send("x") is None
        await env.bus.publish(events.OPP_NEW, env.opps.add(make_opp(env.clock)))
        await env.notifier.handle_update(env.message("/status"))
    assert "sendMessage" in caplog.text
    assert TOKEN not in caplog.text and "api.telegram.org" not in caplog.text
    assert env.session.urls and all(url.startswith(f"https://api.telegram.org/bot{TOKEN}/") for url in env.session.urls)


async def test_handler_exceptions_never_propagate():
    env = await Env().start()
    await env.bus.publish(events.OPP_STATE, "not a dict")
    await env.bus.publish(events.EMERGENCY, None)
    await env.bus.publish(events.HEALTH, {"exchange": "x"})
    await env.notifier.handle_update({"message": {"chat": {"id": 42}, "text": None}})
    await env.notifier.handle_update({"callback_query": "bad"})
    await env.notifier.handle_update({"weird": 1})
    assert env.session.sent() == []


def test_api_url_uses_method_name_only_in_logs(caplog):
    env = Env()
    with caplog.at_level(logging.WARNING):
        env.session.responses["sendMessage"] = [{"ok": False, "description": "Forbidden: bot was blocked by the user"}]
        assert asyncio.run(env.notifier.send("hi")) is None
    assert "sendMessage: Forbidden" in caplog.text and TOKEN not in caplog.text
