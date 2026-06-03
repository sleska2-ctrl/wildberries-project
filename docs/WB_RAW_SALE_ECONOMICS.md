# Полная экономика товара: финансовый отчёт WB (raw_sale)

## Источник данных

API: `GET /api/v5/supplier/reportDetailByPeriod`  
Таблица в SQLite: `raw_sales`  
Фильтр по строкам: `docTypeName = 'Продажа'` | `'Возврат'`  
При возвратах все суммы меняют знак (quantity × -1).

---

## Цепочка цен — от цены продавца до цены клиента

```
retailPrice          — Базовая цена продавца (установлена в кабинете)
  − productDiscountForReport %  — Скидка продавца (участие в акциях WB)
  − sellerPromo %               — Промо скидка продавца
  − sellerPromoDiscount %       — Скидка из промо
  − loyaltyDiscount %           — Скидка по программе лояльности
= retailPriceWithDisc           — ЦЕНА ПРОДАВЦА (= base для % в этом документе)

  − spp %                       — СПП: скидка постоянного покупателя (WB платит за покупателя)
= retailAmount                  — ЦЕНА КЛИЕНТА (по этой сумме WB отчитывается о продаже)
```

| Поле API | Название | Пример |
|---|---|---|
| `retailPrice` | Базовая цена продавца | 500,03 ₽ |
| `retailPriceWithDisc` | Цена продавца (после скидок продавца, до СПП) | 500,03 ₽ |
| `spp` | СПП, % | 28,50% |
| `retailPriceWithDisc × spp / 100` | СПП в рублях | 142,51 ₽ |
| `retailAmount` | Цена клиента | 357,54 ₽ |

> **Формула:** `retailAmount = retailPriceWithDisc × (1 − spp / 100)`

---

## Комиссии и платежи по строке продажи

> Все % считаются **от цены продавца** (`retailPriceWithDisc`).

| Поле API | Название | Знак | Пример, ₽ | % от цены продавца |
|---|---|:---:|---:|---:|
| `retailPriceWithDisc` | **Цена продавца** | | **500,03** | **100%** |
| `spp` (рублей) | СПП (субсидия WB покупателю) | − | 142,49 | 28,50% |
| `retailAmount` | **Цена клиента** | | **357,54** | 71,50% |
| `ppvzSalesCommission` | Комиссия WB | − | 22,55 | 4,51% |
| `acquiringFee` | Эквайринг (оплата картой) | − | 8,99 | 1,80% |
| `deliveryRub` | Логистика (FBS/FBO) | − | 4,96 | 0,99% |
| `paidStorage` | Хранение на складе WB | − | 0 | 0% |
| `paidAcceptance` | Приёмка на склад WB | − | 0 | 0% |
| `penalty` | Штрафы | − | 0 | 0% |
| `deduction` | Прочие удержания WB | − | 0 | 0% |
| `additionalPayment` | Доп. выплаты WB продавцу | + | 0 | 0% |
| **`forPay`** | **К выплате продавцу** | | **321,04** | **64,20%** |

> **Сумма** = 100%: СПП% + комиссия% + эквайринг% + логистика% + ... + forPay% = 100% от retailPriceWithDisc

---

## Проверочная формула

```
forPay = retailAmount
       − ppvzSalesCommission
       − acquiringFee
       − deliveryRub
       − paidStorage
       − paidAcceptance
       − penalty
       − deduction
       + additionalPayment
```

**Проверка на примере:**
```
357,54 − 22,55 − 8,99 − 4,96 − 0 − 0 − 0 − 0 + 0 = 321,04 ✓
```

---

## Справочник всех полей строки

### Идентификация

| Поле | Описание |
|---|---|
| `rrdId` | Уникальный ID строки отчёта |
| `reportId` | ID отчётного периода |
| `orderId` / `orderUid` | ID заказа |
| `srid` | ID доставки (shipment) |
| `nmId` | Артикул WB |
| `vendorCode` | Артикул продавца |
| `sku` | SKU |
| `subjectName` | Категория товара |
| `brandName` | Бренд |
| `techSize` | Размер |
| `docTypeName` | Тип документа: `Продажа` / `Возврат` |
| `saleDt` | Дата продажи/возврата |
| `orderDt` | Дата заказа |

### Цены и скидки

| Поле | Описание |
|---|---|
| `retailPrice` | Базовая цена продавца |
| `salePercent` | Суммарный % скидки продавца |
| `productDiscountForReport` | Скидка продавца для отчёта, % |
| `sellerPromo` | Промо скидка продавца, % |
| `sellerPromoDiscount` | Сумма скидки от промо, ₽ |
| `loyaltyDiscount` | Скидка по программе лояльности, ₽ |
| `retailPriceWithDisc` | Цена продавца после своих скидок (до СПП) |
| `spp` | СПП — скидка постоянного покупателя, % |
| `retailAmount` | Цена клиента = сумма продажи |

### Структура комиссии WB

| Поле | Описание |
|---|---|
| `commissionPercent` | Общая комиссия WB по категории, % |
| `kvwBase` | Базовая комиссия за логистику/выполнение, % |
| `kvw` | `kvwBase − spp` — эффективная ставка (может быть отрицательной) |
| `ppvzSalesCommission` | Итоговая комиссия WB по транзакции, ₽ (**ключевое поле**) |
| `ppvzReward` | Вознаграждение ПВЗ (входит в ppvzSalesCommission) |

> Когда `spp > kvwBase`, поле `ppvzSalesCommission` становится **отрицательным** — WB доплачивает продавцу за субсидирование СПП.

### Эквайринг

| Поле | Описание |
|---|---|
| `acquiringFee` | Комиссия за приём оплаты картой, ₽ |
| `acquiringPercent` | Ставка эквайринга, % (от `retailPrice`) |
| `acquiringBank` | Банк-эквайер |
| `paymentProcessing` | Вид обработки платежа |

### Логистика

| Поле | Описание |
|---|---|
| `deliveryRub` | Стоимость доставки, ₽ (часто = 0 в API → вычисляется) |
| `deliveryAmount` | Количество доставок |
| `returnAmount` | Количество возвратов |
| `deliveryService` | Служба доставки |
| `deliveryMethod` | Метод доставки |
| `rebillLogisticCost` | Перевыставление логистики (при пересчёте), ₽ |
| `officeName` | Название склада/ПВЗ |
| `ppvzOfficeName` / `ppvzOfficeId` | ПВЗ партнёра |

### Прочие удержания и выплаты

| Поле | Описание |
|---|---|
| `paidStorage` | Платное хранение, ₽ |
| `paidAcceptance` | Платная приёмка, ₽ |
| `penalty` | Штрафы, ₽ |
| `deduction` | Прочие удержания, ₽ |
| `additionalPayment` | Дополнительные выплаты от WB продавцу, ₽ |

### Кешбэк и рассрочка (новые поля)

| Поле | Описание |
|---|---|
| `cashbackAmount` | Сумма кешбэка покупателю, ₽ |
| `cashbackDiscount` | Скидка от кешбэка, ₽ |
| `cashbackCommissionChange` | Изменение комиссии из-за кешбэка, ₽ |
| `wibesDiscountPercent` | Скидка WBes, % |
| `installmentCofinancingAmount` | Участие WB в рассрочке, ₽ |

### Итог

| Поле | Описание |
|---|---|
| `forPay` | **К выплате продавцу по данной строке** |
| `vw` | Промежуточная выплата (вспомогательное) |
| `vwNds` | НДС на vw (вспомогательное) |

---

## Особенности

### deliveryRub часто отсутствует в API

WB не всегда отдаёт `deliveryRub` в детализации. Когда поле = 0, логистику вычисляют как остаток:

```python
delivery_rub = (
    retailAmount
    - ppvzSalesCommission
    - acquiringFee
    - paidStorage
    - paidAcceptance
    - penalty
    - deduction
    + additionalPayment
    - forPay
)
```

Значение может быть отрицательным (WB эффективно доплачивает за сделку) — это нормально при высоком СПП.

### Строки без продажи

В отчёте встречаются строки с `docTypeName` ≠ 'Продажа' / 'Возврат' (пустые строки) — в них могут быть только `paidStorage`, `penalty`, `deduction`, `additionalPayment`. Они относятся к кабинету в целом, не к конкретному заказу.

### Возвраты

`docTypeName = 'Возврат'` — все поля идентичны, но `quantity` и финансовые суммы должны интерпретироваться со знаком минус к исходной продаже. В коде: `sale_sign = −1`.

---

## Полный числовой пример (5 реальных продаж)

| Цена продавца | СПП% | СПП ₽ | Цена клиента | Комиссия WB | Эквайринг | Логистика | forPay | forPay% |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 500,03 | 28,50% | 142,49 | 357,54 | 22,55 (4,51%) | 8,99 (1,80%) | 4,96 (0,99%) | 321,04 | 64,20% |
| 479,00 | 40,71% | 195,00 | 284,00 | −26,34 (−5,50%) | 11,36 (2,37%) | −5,80 (−1,21%) | 304,78 | 63,63% |
| 1 481,00 | 32,34% | 479,00 | 1 002,00 | 20,11 (1,36%) | 40,08 (2,71%) | 4,43 (0,30%) | 937,38 | 63,29% |
| 400,00 | 34,75% | 139,00 | 261,00 | −2,46 (−0,61%) | 12,74 (3,19%) | −0,54 (−0,14%) | 251,26 | 62,81% |
| 802,00 | 28,43% | 228,00 | 574,00 | 36,62 (4,57%) | 22,96 (2,86%) | 8,06 (1,00%) | 506,36 | 63,14% |

> Строки 2 и 4: комиссия WB **отрицательная** — СПП больше базовой комиссии, WB доплачивает продавцу.

---

## SQL-запрос для расчёта экономики

```sql
SELECT
  nmId,
  vendorCode,
  saleDt,
  CAST(retailPriceWithDisc AS REAL)                                        AS seller_price,
  CAST(spp AS REAL)                                                         AS spp_pct,
  ROUND(CAST(retailPriceWithDisc AS REAL) - CAST(retailAmount AS REAL), 2) AS spp_rub,
  CAST(retailAmount AS REAL)                                                AS client_price,
  CAST(ppvzSalesCommission AS REAL)                                         AS wb_commission_rub,
  ROUND(CAST(ppvzSalesCommission AS REAL)
        / CAST(retailPriceWithDisc AS REAL) * 100, 2)                      AS wb_commission_pct,
  CAST(acquiringFee AS REAL)                                                AS acquiring_rub,
  CAST(acquiringPercent AS REAL)                                            AS acquiring_pct,
  -- deliveryRub часто = 0, считаем обратно:
  ROUND(CAST(retailAmount AS REAL)
        - CAST(ppvzSalesCommission AS REAL)
        - CAST(acquiringFee AS REAL)
        - CAST(paidStorage AS REAL)
        - CAST(paidAcceptance AS REAL)
        - CAST(penalty AS REAL)
        - CAST(deduction AS REAL)
        + CAST(additionalPayment AS REAL)
        - CAST(forPay AS REAL), 2)                                         AS delivery_rub,
  CAST(paidStorage AS REAL)                                                 AS storage_rub,
  CAST(paidAcceptance AS REAL)                                              AS acceptance_rub,
  CAST(penalty AS REAL)                                                     AS penalty_rub,
  CAST(deduction AS REAL)                                                   AS deduction_rub,
  CAST(additionalPayment AS REAL)                                           AS additional_rub,
  CAST(forPay AS REAL)                                                      AS for_pay,
  ROUND(CAST(forPay AS REAL)
        / CAST(retailPriceWithDisc AS REAL) * 100, 2)                      AS for_pay_pct,
  -- Проверка: должна быть = 0
  ROUND(
    CAST(retailAmount AS REAL)
    - CAST(ppvzSalesCommission AS REAL)
    - CAST(acquiringFee AS REAL)
    - COALESCE(NULLIF(CAST(deliveryRub AS REAL), 0),
        CAST(retailAmount AS REAL)
        - CAST(ppvzSalesCommission AS REAL)
        - CAST(acquiringFee AS REAL)
        - CAST(paidStorage AS REAL)
        - CAST(paidAcceptance AS REAL)
        - CAST(penalty AS REAL)
        - CAST(deduction AS REAL)
        + CAST(additionalPayment AS REAL)
        - CAST(forPay AS REAL)
      )
    - CAST(paidStorage AS REAL)
    - CAST(paidAcceptance AS REAL)
    - CAST(penalty AS REAL)
    - CAST(deduction AS REAL)
    + CAST(additionalPayment AS REAL)
    - CAST(forPay AS REAL)
  , 4)                                                                      AS check_diff
FROM raw_sales
WHERE docTypeName = 'Продажа';
```
