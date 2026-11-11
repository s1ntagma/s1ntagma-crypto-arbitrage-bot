---
name: market-data-validation
description: Проверка качества рыночных данных latbot - свежесть стаканов по WebSocket, задержки, обрывы, staleness, расхождения timestamp бирж. Используй, когда бот молчит, сигналы выглядят странно, BOT всегда красный «stale book», или меняются exchanges/ccxt_adapter.py, market/state.py, engine/pipeline.py (sweep).
---

# Проверка рыночных данных

Только анализ и открытые данные. Ордеров не отправляет, `.env` не читает.

## Что бот считает плохими данными

- стакан старше `max_data_age_ms` (1500 мс) - биржа не используется ни как источник сигнала, ни как target;
- нет сообщений от биржи дольше `stale_exchange_s` (10 с) - `ExchangeHealth.stale`, адаптер переподключается;
- `ws_connected=False`, `maintenance`, `rate_limited_until` - биржа не пригодна для исполнения (`health.is_usable`);
- пустая сторона стакана, bid >= ask, глубина меньше `min_liquidity_quote` - нет экономики, нет сигнала;
- `exchange_ts` далеко от локального времени - латентность не считается (отбрасывается > 60 с и < -5 с).

Без данных бот не анализирует: карточек нет, в полосе здоровья бирж видно, кто отвалился.

## Как проверить

```
python -m pytest -q tests/test_market_state.py tests/test_ccxt_adapter.py tests/test_pipeline.py -k "stale or fresh or reconnect or health"
curl -s localhost:8787/api/health
findstr /c:"reconnect" /c:"stale" logs\console.log
```

- Полоса здоровья в UI: `ws`, `ws_latency_ms`, `order_latency_ms`, `error_count` по каждой бирже.
- Много «stale» подряд у одной биржи - связь или лимиты, не рынок.
- Часы ПК должны совпадать с биржевыми с точностью до секунд, иначе подписанные запросы отклоняются.

## Чего не делать

- Не увеличивать `max_data_age_ms`, `stale_exchange_s` или `max_slippage_pct`, чтобы «сигналов стало больше».
- Не подменять недостающие данные: нет стакана - нет возможности.
