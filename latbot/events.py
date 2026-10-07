"""Tiny async pub/sub event bus.

Topics (string constants below).  Payloads are plain objects from models.py or
dicts; subscribers must never mutate them.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

# ---- topics ----------------------------------------------------------------
BOOK = "book"  # OrderBook
TRADE = "trade"  # Trade
TICKER = "ticker"  # Ticker
HEALTH = "health"  # ExchangeHealth
MOVE = "move"  # MoveEvent
SIGNAL = "signal"  # LagSignal
OPP_NEW = "opp.new"  # Opportunity
OPP_UPDATE = "opp.update"  # Opportunity
OPP_STATE = "opp.state"  # {"opportunity": Opportunity, "from": str, "to": str, "reason": str}
TRADE_UPDATE = "trade.update"  # TradeRecord
ORDER_UPDATE = "order.update"  # Order
BALANCE = "balance"  # {"exchange": str, "mode": str, "balances": {asset: Balance}}
SETTINGS = "settings"  # Settings (public dict)
EMERGENCY = "emergency"  # {"active": bool, "reason": str}
DASHBOARD = "dashboard"  # dict of counters
LOG = "log"  # {"level": str, "msg": str, "ts": float}

Handler = Callable[[Any], Awaitable[None] | None]


class EventBus:
    def __init__(self) -> None:
        self._subs: dict[str, list[Handler]] = defaultdict(list)
        self._all: list[Callable[[str, Any], Awaitable[None] | None]] = []

    def subscribe(self, topic: str, handler: Handler) -> Callable[[], None]:
        self._subs[topic].append(handler)

        def _unsub() -> None:
            try:
                self._subs[topic].remove(handler)
            except ValueError:
                pass

        return _unsub

    def subscribe_all(self, handler: Callable[[str, Any], Awaitable[None] | None]) -> Callable[[], None]:
        self._all.append(handler)

        def _unsub() -> None:
            try:
                self._all.remove(handler)
            except ValueError:
                pass

        return _unsub

    async def publish(self, topic: str, payload: Any) -> None:
        """Deliver to all handlers sequentially (awaiting coroutines).
        Handler exceptions are logged and never propagate."""
        for h in list(self._subs.get(topic, ())):
            try:
                r = h(payload)
                if inspect.isawaitable(r):
                    await r
            except Exception:  # noqa: BLE001
                log.exception("event handler failed topic=%s handler=%r", topic, h)
        for h in list(self._all):
            try:
                r = h(topic, payload)
                if inspect.isawaitable(r):
                    await r
            except Exception:  # noqa: BLE001
                log.exception("event handler failed topic=%s handler=%r", topic, h)

    def publish_nowait(self, topic: str, payload: Any) -> None:
        """Schedule publish on the running loop (for use from sync code)."""
        loop = asyncio.get_event_loop()
        loop.create_task(self.publish(topic, payload))
