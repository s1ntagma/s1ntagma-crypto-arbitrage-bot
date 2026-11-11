"""Market context: macro calendar, news / Telegram channels, TradFi markets,
derivatives (funding, open interest, liquidations), minute bars / regime and
historical reactions to events.  Everything here is a MEASUREMENT with a
source; a collector without data says so (``Factor.available = False``).
Nothing in this package can create a signal on its own - it only scores the
signals the latency engine already found (see ``engine.quality``)."""
from .base import (
    AiohttpGetter,
    Factor,
    HttpGetter,
    HttpResponse,
    MacroEvent,
    NewsItem,
    PollingCollector,
    SourceStatus,
)

__all__ = [
    "AiohttpGetter",
    "Factor",
    "HttpGetter",
    "HttpResponse",
    "MacroEvent",
    "NewsItem",
    "PollingCollector",
    "SourceStatus",
]
