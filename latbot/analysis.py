"""Trade review with Claude (vision): a screenshot of the exchange screen plus
the trade's own numbers -> a short Russian review of what went right / wrong.

Only used when ``ANTHROPIC_API_KEY`` is in ``.env``.  Without it ``available``
is False and the UI shows the feature as NOT AVAILABLE - never a fake result.
The key never leaves ``Secrets`` (never logged: ``register_secret``).
"""
from __future__ import annotations

import base64
import logging
from typing import Any

log = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Ты разбираешь сделки межбиржевого лага (покупка и продажа на одной бирже, пока она догоняет рынок). "
    "Тебе дают факты сделки из журнала бота и скриншот экрана биржи. Ответь по-русски, кратко, по пунктам: "
    "1) что видно на скриншоте (цены, стакан, ордера, время) и сходится ли это с журналом; "
    "2) где потерялся NET: комиссии, проскальзывание, задержка, недодогон, ранний/поздний выход; "
    "3) что сделать иначе в следующий раз. Не выдумывай цифр, которых нет ни на скриншоте, ни в журнале; "
    "если чего-то не видно - так и скажи. Это разбор, а не финансовая рекомендация."
)
MAX_IMAGE_BYTES = 8 * 1024 * 1024
MEDIA_TYPES = ("image/png", "image/jpeg", "image/webp", "image/gif")


def available(secrets: Any) -> bool:
    return bool(getattr(secrets, "anthropic_api_key", ""))


def trade_facts(trade: dict[str, Any]) -> str:
    keys = (
        "id", "mode", "asset", "exchange", "side", "state", "capital_quote", "expected_net_pct", "actual_net_pct",
        "expected_profit_quote", "realized_pnl_quote", "actual_slippage_pct", "execution_time_s", "entry_price",
        "exit_price", "diff_reason", "error", "started_ts", "finished_ts",
    )
    lines = [f"{k}: {trade[k]}" for k in keys if trade.get(k) not in (None, "", [])]
    return "\n".join(lines) or "(нет данных о сделке)"


async def analyze_screenshot(
    secrets: Any, model: str, trade: dict[str, Any], image: bytes, media_type: str, note: str = "",
    client: Any | None = None,
) -> str:
    """Return the review text.  Raises RuntimeError with a user-facing message
    (no secrets) when the key is missing or the API call fails."""
    if not available(secrets):
        raise RuntimeError("ANTHROPIC_API_KEY не задан в .env")
    if media_type not in MEDIA_TYPES:
        raise RuntimeError(f"неподдерживаемый тип изображения: {media_type}")
    if len(image) > MAX_IMAGE_BYTES:
        raise RuntimeError("скриншот больше 8 МБ")
    if client is None:
        import anthropic  # local import: optional dependency at runtime

        client = anthropic.AsyncAnthropic(api_key=secrets.anthropic_api_key)
    text = "Факты сделки из журнала:\n" + trade_facts(trade)
    if note.strip():
        text += "\n\nЗаметка трейдера:\n" + note.strip()
    text += "\n\nРазбери эту сделку по скриншоту."
    content = [
        {"type": "image", "source": {"type": "base64", "media_type": media_type,
                                      "data": base64.standard_b64encode(image).decode("ascii")}},
        {"type": "text", "text": text},
    ]
    try:
        response = await client.messages.create(
            model=model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": content}],
        )
    except Exception as exc:  # noqa: BLE001 - the message must not carry the key
        name = type(exc).__name__
        log.warning("trade analysis failed: %s", name)
        raise RuntimeError(f"ошибка Claude API ({name})") from exc
    if getattr(response, "stop_reason", None) == "refusal":
        raise RuntimeError("модель отказалась разбирать это изображение")
    parts = [getattr(b, "text", "") for b in getattr(response, "content", []) or [] if getattr(b, "type", "") == "text"]
    out = "\n".join(p for p in parts if p).strip()
    if not out:
        raise RuntimeError("пустой ответ модели")
    return out


__all__ = ["available", "analyze_screenshot", "trade_facts", "MEDIA_TYPES", "MAX_IMAGE_BYTES"]
