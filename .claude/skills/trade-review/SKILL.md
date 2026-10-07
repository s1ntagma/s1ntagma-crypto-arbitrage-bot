---
name: trade-review
description: Разбор закрытых сделок latbot - ожидалось vs получилось, где потерялся NET (slippage, задержка, недодогон, комиссии), статус ордеров и partial fill. Используй, когда владелец просит разобрать сделку или серию, или когда меняется execution/pnl.py и закрытие сделки в execution/engine.py.
---

# Разбор сделок

Только чтение базы (`GET /api/trades`, `data/latbot.sqlite3`). Ордеров не отправляет, `.env` не читает.

## У каждой сделки есть

`expected_net_pct`, `expected_entry_price`, `expected_exit_price`, `entry_order`/`exit_order` (статусы, fills,
`latency_ms`), `actual_entry_price`, `actual_exit_price`, `actual_fees_quote`, `actual_slippage_pct`,
`actual_net_pct`, `realized_pnl_quote`, `execution_time_s`, `diff_reason`, `events` (журнал шагов), `state`.

## Порядок разбора одной сделки

1. Итог: `actual_net_pct` против `expected_net_pct`; состояние CLOSED / FAILED / CANCELLED и почему (`error`, `events`).
2. Исполнение: slippage входа до 0.05 % - хорошо, до 0.15 % - приемлемо, выше - плохо; partial fill - какая доля.
3. Время: `execution_time_s` и `entry_latency_ms` против оценки BOT в карточке (`bot.est_execution_s`).
4. Догон: дошла ли цена до `expected_exit_price`; если выход по таймауту рынком - недодогон, смотри `diff_reason`.
5. Вывод: одна фраза, что сработало или нет. Без подгонки объяснения под результат.

## Разбор серии

Одна сделка ничего не доказывает. Для серии - `GET /api/stats` и skill `strategy-performance`:
меньше 20-30 сделок - выводы делать рано.

## Чего не делать

- Не менять настройки из-за одной убыточной сделки.
- Не править записи в базе: это единственный след того, что бот решил и почему.
