# Полная экономика товара: финансовый отчёт OZON

## Ключевые отличия от WB

| | WB | OZON |
|---|---|---|
| Гранулярность финансовых данных | Строка на каждый заказ (raw_sale) | **День** в целом + день × SKU (без суммы к выплате по SKU) |
| «К выплате» по SKU | forPay на каждую строку | Нет — только за день суммарно (`for_pay`) |
| Название «скидки WB» | СПП | **avg_spp** — средневзвешенный % скидки по дню |
| Логистика | deliveryRub на строку | Сумма за день из транзакционного API |

---

## Источники данных

| API-эндпоинт | Что возвращает | Куда сохраняем |
|---|---|---|
| `POST /v3/finance/transaction/list` | Все транзакции за период: начисления, комиссии, услуги, итог | `ozon_daily_summary` |
| `POST /v1/finance/accrual/by-day` | Комиссия и логистика по каждому SKU за день | `ozon_sku_day_finance` |
| `POST /v2/posting/fbo/list` + `financial_data=true` | `old_price` (цена продавца) и `price` (цена клиента) по каждому отправлению | расчёт `avg_spp` → `ozon_sku_day_analytics` |
| `GET /v1/analytics/data` | Заказы, выкупы, выручка по дням/SKU | `ozon_sku_day_analytics` |

---

## Цепочка цен

```
old_price (financial_data)      — Цена продавца (original, до всех скидок)
  − скидки продавца (акции)
  − субсидии OZON (Premium cashback, промо)
= customer_price (products[].price) — Цена клиента (фактически оплачена покупателем)

avg_spp % = (old_price − customer_price) / old_price × 100  — средний % скидки за день
```

> **Важно:** OZON не имеет аналога СПП WB. `avg_spp` — это агрегированный показатель за день,
> рассчитанный из постингов FBO. По нему нельзя восстановить точную цену клиента для каждой транзакции.

---

## Финансовые поля (уровень дня)

Таблица `ozon_daily_summary`. **База для %** = `accruals_for_sale`.

| Поле | Название | Знак | Пример, ₽ | % от accruals |
|---|---|:---:|---:|---:|
| `accruals_for_sale` | **Начисление за выкупы** (сумма выкупов, факт) | + | **322 308** | **100%** |
| `sale_commission` | Комиссия OZON | − | −139 692 | −43,3% |
| `delivery_charge` | Логистика до покупателя | − | −36 822 | −11,4% |
| `return_delivery` | Логистика возвратов | − | −4 651 | −1,4% |
| `return_accruals` | Сторно начислений за возвраты | − | −7 624 | −2,4% |
| `other_services` ¹ | Прочие услуги OZON (хранение, обработка, кешбэк и др.) | − | −18 714 | −5,8% |
| **`for_pay`** | **К выплате продавцу** | | **114 806** | **35,6%** |

¹ `other_services` — **не хранится** отдельным полем, это разница `for_pay − (accruals + commission + delivery + return_delivery + return_accruals)`. Включает: хранение, кросс-докинг, обработку возвратов, коофинансирование кешбэка, маркировку и др.

---

## Проверочная формула

```
for_pay = accruals_for_sale
        + sale_commission         (отрицательное)
        + delivery_charge         (отрицательное)
        + return_delivery         (отрицательное)
        + return_accruals         (отрицательное)
        + other_services          (обычно отрицательное, не хранится отдельно)
```

`for_pay` берётся из транзакционного API как сумма поля `amount` по всем операциям дня — **это единственная абсолютно точная цифра**.

**Проверка:**
```
322 308 − 139 692 − 36 822 − 4 651 − 7 624 − 18 714 = 114 806 ✓
```

### SQL-проверка

```sql
SELECT
  day,
  ROUND(accruals_for_sale, 0) AS accruals,
  ROUND(sale_commission, 0)   AS commission,
  ROUND(delivery_charge, 0)   AS delivery,
  ROUND(return_delivery, 0)   AS return_delivery,
  ROUND(return_accruals, 0)   AS return_accruals,
  -- residual: прочие услуги (хранение, обработка, кешбэк и др.)
  ROUND(for_pay
        - accruals_for_sale
        - sale_commission
        - delivery_charge
        - return_delivery
        - return_accruals, 0) AS other_services,
  ROUND(for_pay, 0)           AS for_pay,
  ROUND(for_pay / accruals_for_sale * 100, 1) AS forpay_pct
FROM ozon_daily_summary
WHERE accruals_for_sale > 0
ORDER BY day DESC;
```

---

## Реальные данные (5 дней)

| День | Начисления | Комиссия | % | Логистика | % | Возвраты | Прочие услуги | К выплате | % |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-05-28 | 91 586 | −39 630 | −43,3% | −12 083 | −13,2% | −4 306 | −480 | 35 087 | 38,3% |
| 2026-05-27 | 322 308 | −139 692 | −43,3% | −36 822 | −11,4% | −12 275 | −18 714 | 114 806 | 35,6% |
| 2026-05-26 | 304 804 | −131 506 | −43,1% | −35 390 | −11,6% | −10 015 | −20 895 | 106 998 | 35,1% |
| 2026-05-25 | 335 961 | −146 096 | −43,5% | −36 897 | −11,0% | −8 140 | −24 474 | 120 354 | 35,8% |
| 2026-05-24 | 240 095 | −104 362 | −43,5% | −27 391 | −11,4% | −16 561 | −26 618 | 65 162 | 27,1% |

> Комиссия OZON стабильна ~43%. Логистика ~11–13%. «К выплате» = 27–38% в зависимости от дня (влияют возвраты и прочие услуги).

---

## Финансовые поля (уровень SKU × день)

Таблица `ozon_sku_day_finance` — данные из `POST /v1/finance/accrual/by-day`.

| Поле | Доступно | Описание |
|---|:---:|---|
| `sale_commission` | ✅ | Комиссия OZON по SKU за день |
| `delivery_charge` | ✅ | Логистика до покупателя по SKU за день |
| `accruals_for_sale` | ❌ | Нет — только в `ozon_daily_summary` |
| `return_delivery` | ❌ | Нет на уровне SKU |
| `return_accruals` | ❌ | Нет на уровне SKU |
| `other_services` | ❌ | Нет на уровне SKU |
| `for_pay` | ❌ | Нет — только суммарно за день |

### Как считать начисления за выкупы по SKU

```sql
-- accruals_for_sale на SKU × день = delivered_qty × средняя цена заказа
SELECT
  f.sku,
  f.day,
  s.orders_revenue / NULLIF(s.orders_qty, 0)                     AS avg_order_price,
  s.avg_spp,
  ROUND(s.delivered_qty
        * s.orders_revenue / NULLIF(s.orders_qty, 0), 2)          AS accruals_est,
  f.sale_commission,
  ROUND(ABS(f.sale_commission)
        / NULLIF(s.delivered_qty
                 * s.orders_revenue / NULLIF(s.orders_qty, 0), 0)
        * 100, 1)                                                  AS comm_pct,
  f.delivery_charge,
  ROUND(ABS(f.delivery_charge)
        / NULLIF(s.delivered_qty
                 * s.orders_revenue / NULLIF(s.orders_qty, 0), 0)
        * 100, 1)                                                  AS delivery_pct
FROM ozon_sku_day_finance f
JOIN ozon_sku_day_analytics s
  ON s.sku = f.sku AND s.day = f.day
WHERE f.sale_commission <> 0
  AND s.orders_qty > 0;
```

### Пример данных (1 SKU за день)

| SKU | День | Ср. цена | avg_spp% | Начисл. (расч.) | Комиссия | % | Логистика | % |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 241260883 | 2026-05-27 | 2 010 ₽ | 230% | 6 030 ₽ | −2 593 ₽ | −43,0% | −297 ₽ | −4,9% |
| 170056158 | 2026-05-27 | 2 487 ₽ | 1979% | 7 461 ₽ | −3 360 ₽ | −45,0% | −974 ₽ | −13,1% |
| 272328144 | 2026-05-27 | 2 010 ₽ | 5904% | 10 050 ₽ | −4 063 ₽ | −40,4% | −645 ₽ | −6,4% |

> `avg_spp` > 100% означает неточность расчёта (заказы и выкупы приходятся на разные дни). Использовать только как ориентир.

---

## Что включают «Прочие услуги»

`other_services` = `for_pay − accruals_for_sale − sale_commission − delivery_charge − return_delivery − return_accruals`

Состав (OZON API service names из `services[]`):

| Услуга OZON | Описание |
|---|---|
| `MarketplaceServiceItemStorageLarge` | Хранение крупногабарита |
| `MarketplaceServiceItemStorageFreeCharge` | Хранение (стандарт) |
| `MarketplaceServiceItemPickup` | Забор товара (FBS) |
| `MarketplaceServiceItemDropoffSC` | Сдача в сортировочный центр |
| `MarketplaceServiceItemDropoffFF` | Сдача на фулфилмент |
| `MarketplaceServiceItemDirectFlowTrans` | Транзит |
| `MarketplaceServicePremiumCashback` | Коофинансирование кешбэка Premium |
| `MarketplaceServiceCashback` | Коофинансирование кешбэка |
| `MarketplaceServiceItemProcessing` | Обработка |
| `MarketplaceServiceItemPackagingItems` | Упаковка |

Эти услуги приходят в поле `services[]` каждой транзакции, но в текущей системе они **не разбиваются** и попадают в разницу при сверке.

---

## Сводное сравнение WB и OZON

| Показатель | WB (raw_sale) | OZON (ozon_daily_summary) |
|---|---|---|
| Цена продавца | `retailPriceWithDisc` | `old_price` из FBO-постингов |
| Цена клиента | `retailAmount` | `customer_price` из FBO-постингов |
| Скидка площадки | `spp` % и ₽ (на строку) | `avg_spp` % (за день, приблизительно) |
| Начисление за продажу | `retailAmount` (= цена клиента) | `accruals_for_sale` |
| Комиссия площадки | `ppvzSalesCommission` (~5–15%) | `sale_commission` (~43–45%) |
| Логистика | `deliveryRub` (~1–3%) | `delivery_charge` (~11–13%) |
| Прочие удержания | `penalty` + `deduction` (обычно 0) | `other_services` (~0–11%, не детализируется) |
| К выплате | `forPay` (на каждую строку) | `for_pay` (только за день суммарно) |
| % к выплате от начислений | ~60–65% | ~27–38% (широкий разброс) |

> OZON берёт значительно бо́льшую комиссию (~43% vs ~5–15% у WB), но «цена клиента» в `accruals_for_sale` уже уменьшена на скидку площадки — это делает сравнение напрямую некорректным без учёта avg_spp.
