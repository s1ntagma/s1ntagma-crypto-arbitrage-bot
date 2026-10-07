---
name: order-integrity
description: Проверка, что latbot не отправит два ордера по одной возможности, не исполнит устаревший снимок и не потеряет сделку. Используй при любом изменении execution/engine.py (revalidate, execute), exchanges/*/place_order, engine/opportunity.py (переходы состояний), а также если владелец сообщает о повторной или «потерянной» сделке.
---

# Целостность ордеров

Только анализ. Ничего не отправляет на биржу, `.env` не читает.

## Что должно быть правдой всегда

1. Ордера отправляет только `ExchangeAdapter.place_order`, и вызывает его только `ExecutionEngine`
   (`execute`, `emergency_close_positions`). Больше нигде `place_order` не вызывается.
2. У каждого ордера есть `OrderRequest.client_id` (идемпотентный ключ, уходит на биржу как clientOrderId/orderLinkId).
3. Перед входом и перед выходом - `revalidate` по свежему стакану. Expected NET 4.8 %, сейчас 1.2 % - отмена.
4. Таймаут или обрыв при отправке = статус неизвестен: повторной отправки нет, `fetch_order` сверяет с биржей.
5. Возможность в состоянии EXECUTING/PARTIALLY_FILLED/FILLED принадлежит движку исполнения:
   `OpportunityManager.sweep` её не истекает; CLOSED - только когда биржа подтвердила выход.
6. Переходы состояний только через `OpportunityManager.transition` и только по таблице `models.TRANSITIONS`.
7. При EMERGENCY STOP новые ордера не уходят; pending отменяются; история сохраняется.

## Как проверить

```
python -m pytest -q tests/test_execution.py tests/test_opportunity.py -k "revalidate or stale or partial or timeout or emergency or transition or duplicate"
grep -rn "place_order(" latbot | grep -v "def place_order"
```

- Вызовы `place_order(` должны быть только в `latbot/execution/engine.py`, адаптерах и тестах.
- В `GET /api/trades` у каждой сделки есть `entry_order.client_id`; два ордера с одним `client_id` - баг.
- Сверь сделки с биржей глазами владельца (история ордеров Bybit Demo) или запросами только на чтение.

## Чего не делать

- Не отправлять «проверочный» ордер. AUTO запускает только владелец кнопкой (или `auto_confirm`, который он включил сам).
- Не ослаблять тесты: падение теста на повтор ордера или на исполнение устаревшего снимка - это найденный баг.
