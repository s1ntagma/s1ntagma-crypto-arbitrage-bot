"""Direct links to a trading pair page on each exchange (for the "open pair"
button in Telegram and in the UI).  Templates use {BASE}/{QUOTE} (upper) and
{base}/{quote} (lower).  ExchangeConfig.pair_url overrides the default."""
from __future__ import annotations

PAIR_URLS: dict[str, str] = {
    "kraken": "https://pro.kraken.com/app/trade/{base}-{quote}",
    "coinbase": "https://www.coinbase.com/advanced-trade/spot/{BASE}-{QUOTE}",
    "bitvavo": "https://account.bitvavo.com/markets/{BASE}-{QUOTE}",
    "bitstamp": "https://www.bitstamp.net/markets/{base}/{quote}/",
    "cryptocom": "https://crypto.com/exchange/trade/{BASE}_{QUOTE}",
    "okx": "https://www.okx.com/trade-spot/{base}-{quote}",
    "myokx": "https://www.okx.com/trade-spot/{base}-{quote}",
    "bybit": "https://www.bybit.com/trade/spot/{BASE}/{QUOTE}",
    "gate": "https://www.gate.io/trade/{BASE}_{QUOTE}",
    "binance": "https://www.binance.com/en/trade/{BASE}_{QUOTE}",
    "kucoin": "https://www.kucoin.com/trade/{BASE}-{QUOTE}",
    "mexc": "https://www.mexc.com/exchange/{BASE}_{QUOTE}",
    "bitget": "https://www.bitget.com/spot/{BASE}{QUOTE}",
    "htx": "https://www.htx.com/trade/{base}_{quote}",
    "revolutx": "https://x.revolut.com/",  # web app has no public per-pair URL
}


def split_symbol(symbol: str) -> tuple[str, str]:
    """'BTC/EUR' -> ('BTC', 'EUR'); 'BTC/USDT:USDT' -> ('BTC', 'USDT')."""
    core = symbol.split(":", 1)[0]
    if "/" in core:
        base, quote = core.split("/", 1)
    else:
        base, quote = core, ""
    return base.upper(), quote.upper()


def pair_url(exchange: str, symbol: str, template: str | None = None) -> str:
    """Return a link to the pair page, or '' when no template is known."""
    tpl = template or PAIR_URLS.get(exchange, "")
    if not tpl or not symbol:
        return ""
    base, quote = split_symbol(symbol)
    if not quote:
        return ""
    return tpl.format(BASE=base, QUOTE=quote, base=base.lower(), quote=quote.lower())
