---
name: trading-risk-audit
description: Аудит правил риска latbot - размер сделки, лимиты на биржу и монету, одновременные сделки, дневной убыток, баланс, EMERGENCY STOP. Используй при изменении execution/risk.py, execution/engine.py (revalidate), настроек риска в config.json, и когда владелец спрашивает, почему AUTO отклонён.
---

# Аудит риска

Только анализ. Ордеров не отправляет, `.env` не читает (настройки видны в `GET /api/settings`).

## Правила, которые нельзя ослаблять

| Правило | Где | По умолчанию |
|---|---|---|
| Размер сделки не больше `max_trade_quote` | `RiskManager.check` | $200 |
| Риск на сделку как доля капитала | `max_risk_per_trade_pct` x `total_capital_quote` | 2 % от $1000 |
| Одновременных сделок | `max_simultaneous_trades` | 2 |
| Дневной убыток | `max_daily_loss_quote` - дальше AUTO блокируется | $50 |
| Экспозиция на биржу / на монету | `max_exposure_per_exchange_quote`, `max_exposure_per_asset_quote` | $500 / $300 |
| Баланс | перед AUTO `fetch_balances`; нет $100 - сделки на $100 нет (`allow_downsize` - до доступного) | |
| Финальная ревалидация | NET сейчас >= expected x `revalidate_tolerance` и >= `min_net_pct_auto` | 0.5 / 0.5 % |
| EMERGENCY STOP | блокирует новые ордера, отменяет pending, сохраняет историю | |

`RiskManager.check` выполняется перед каждым ордером и не отключается никаким флагом.
PAPER и LIVE - отдельные балансы; UI всегда показывает, какой режим активен.

## Как проверить

```
python -m pytest -q tests/test_risk.py tests/test_execution.py -k "risk or daily or exposure or balance or emergency or downsize"
curl -s localhost:8787/api/balances
```

Для каждой открытой сделки пересчитай руками: `capital_quote` не больше лимитов выше на момент входа.

## Чего не делать

- Не повышать лимиты «временно» и не добавлять обходные флаги.
- Не считать PAPER-результат обещанием прибыли на реальном счёте.
- Не просить у биржи права на вывод средств.
