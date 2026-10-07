---
name: opportunity-analysis
description: Разбор, почему latbot видит (или не видит) возможность по монете - first mover, отстающая биржа, лаг, экономика после издержек, окно, светофор HUMAN/BOT. Используй, когда владелец спрашивает «почему по BTC нет сигнала», «почему HUMAN красный», или когда меняются engine/mover.py, engine/economics.py, engine/executability.py.
---

# Разбор возможности

Только анализ. Ничего не покупает, `.env` не читает. Ответ владельцу - не финансовая рекомендация.

## Как считает бот (latbot/engine)

1. `MoveDetector` (mover.py): движение каждой биржи за `move_window_s` (5 с) по mid-цене.
   Биржа «пересекла порог», если |движение| >= `move_threshold_pct` (0.35 %). First mover - кто пересёк раньше.
   Отстающая (target) - биржа с ролью EXECUTION, чьё движение меньше на `min_lag_pct` (0.15 %) и больше.
2. `compute_economics` (economics.py): обход стакана target на размер `capital_quote`:
   вход по VWAP (ask для LONG), выход по VWAP (bid) * (1 + ожидаемый догон), минус комиссии обеих ног
   и `execution_loss_pct`. Размер = то, что стакан реально держит в пределах `max_slippage_pct`.
   Главное число - NET %. Если стакан держит $7 из $100 - карточка покажет $7.
3. `DurationEstimator` (duration.py): медиана прошлых окон по (монета, биржа) + живой спад NET.
4. `assess_human` / `assess_bot` (executability.py): HUMAN зелёный, если окно >= `human_execution_time_s` * 1.5;
   BOT зелёный, если окно >= оценка задержки API * 3, стакан свежий (`max_data_age_ms`), ликвидности хватает,
   NET >= `min_net_pct_bot`.
5. Возможность показывается при NET >= `min_net_pct_show` (0.1 %). 3 % - это порог Telegram, не показа.

## Как посмотреть

- Карточка в UI: раскрыть «WHY?» - там first mover, лаг, глубина, издержки, светофор.
- `GET /api/opportunities?state=history` - что истекло и почему (`expire_reason`).
- `GET /api/stats` - медианные задержки реакции, доля false signal, catch-up ratio по биржам.
- Тесты: `python -m pytest -q tests/test_mover.py tests/test_economics.py tests/test_executability.py`.

## Как отвечать владельцу

- Назови first mover, лаг, ожидаемый догон, NET после издержек, окно и оба светофора с причинами из `reasons`.
- Если сигнала нет - скажи, какое условие не выполнено (нет движения, нет лага, стакан тонкий, данные старые).
- Не обещай движение цены. Размер сделки считает только `RiskManager`.
