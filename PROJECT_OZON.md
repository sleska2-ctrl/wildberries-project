# Аналитический пайплайн Ozon: страница выкупов

## Назначение

Страница «Выкупы» — ежедневный сводный отчёт по кабинету Ozon.
Одна строка = один кабинет + один день.
Данные собираются из нескольких независимых источников и складываются в одну предагрегированную таблицу (`daily_summary`).
Pivot читает только из неё + отдельно из таблицы воронки (`plugin_analytics`).

**Текущий день не показывается.** Данные всегда за вчера и старше.

---

## Таблица `daily_summary`

Уникальный ключ: `(cabinet_prefix, day)`

```
cabinet_prefix      str       # ID кабинета, короткий код
day                 date      # дата

-- Analytics API
orders_revenue      float     # выручка от заказов, ₽
orders_qty          int       # количество заказов, шт
delivered_qty       int       # выкупы в эту дату, шт
returns_qty         int       # возвраты, шт
cancellations_qty   int       # отмены, шт

-- Finance API
accruals_for_sale   float     # сумма выкупов продавца (до вычетов), ₽
sale_commission     float     # комиссия Ozon, ₽
delivery_charge     float     # логистика до покупателя, ₽
return_delivery     float     # логистика возвратов, ₽
return_accruals     float     # сумма возвратов продавцу (отрицательная), ₽
for_pay             float     # к выплате = accruals − комиссия − логистика + возвраты

-- Postings API (по дате заказа, не доставки)
avg_spp             float     # средний СПП/Соинвест, %

-- Performance API (агрегация из статистики кампаний)
ad_spend            float     # расход на рекламу, ₽
ad_impressions      int       # показы рекламных объявлений
ad_clicks           int       # клики по рекламе

synced_at           datetime  # последнее обновление строки
```

Каждый источник пишет только свои поля через upsert по ключу `(cabinet_prefix, day)`.
Если один источник упал — данные остальных за этот день сохраняются.

---

## Источники данных

### 1. Analytics API → заказы и выкупы

**Эндпоинт:** `POST /v1/analytics/data`

**Запрос:**
```json
{
  "date_from": "2026-05-15",
  "date_to":   "2026-05-21",
  "dimension": [{"name": "day"}],
  "metrics":   ["revenue", "ordered_units", "delivered_units", "returns", "cancellations"],
  "limit": 1000
}
```

**Ответ:** массив объектов:
```json
{
  "dimensions": [{"id": "2026-05-21", "name": "2026-05-21"}],
  "metrics":    [выручка, заказы, выкупы, возвраты, отмены]
}
```

**Маппинг в daily_summary:**

| Поле ответа | Поле таблицы |
|---|---|
| `metrics[0]` (revenue) | `orders_revenue` |
| `metrics[1]` (ordered_units) | `orders_qty` |
| `metrics[2]` (delivered_units) | `delivered_qty` |
| `metrics[3]` (returns) | `returns_qty` |
| `metrics[4]` (cancellations) | `cancellations_qty` |

**Окно ресинка:** последние 7 дней.
Данные Analytics API появляются с задержкой ~24ч — вчера и позавчера всегда ресинкаются.

---

### 2. Finance API → финансовые операции

**Эндпоинт:** `POST /v3/finance/transaction/list`

**Параметры:**
```json
{
  "filter": {
    "date": {"from": "2026-05-01T00:00:00.000Z", "to": "2026-05-31T23:59:59.999Z"},
    "transaction_type": "all"
  },
  "page": 1,
  "page_size": 1000
}
```

Ограничение: не более 1 месяца за запрос. Грузить циклом по месячным чанкам.

**Логика агрегации по дням** (группировка по `operation_date[:10]`):

| Тип операции | Что суммируем |
|---|---|
| `orders` | `accruals_for_sale`, `sale_commission`, `delivery_charge` |
| `returns` | `return_accruals` (отрицательные), `return_delivery` |

**Формула:**
```
for_pay = accruals_for_sale − sale_commission − delivery_charge + return_accruals − return_delivery
```

**Окно ресинка:** последние 14 дней.
Возвраты могут прийти с опозданием на несколько дней.

> **Оговорка:** закрывающий отчёт о реализации выходит 6-го числа следующего месяца — он финален.
> До этой даты финансовые данные месяца считаются предварительными.

---

### 3. Postings API → СПП/Соинвест

**Цель:** посчитать среднюю скидку Ozon (`avg_spp`) по дате заказа.

**Ключевое правило:** группировать по `in_process_at[:10]` — **дата заказа**, не дата доставки.
Без этого СПП появляется в дни без новых заказов.

**Эндпоинты:**
- Список FBO: `POST /v3/posting/fbo/list` (параметр `status = "delivered"`)
- Детали FBO (с `customer_price`): `GET /v2/posting/fbo/get`
- Список FBS: `POST /v3/posting/fbs/list`
- Детали FBS: `GET /v3/posting/fbs/get`

**Алгоритм:**
```
1. Запросить постинги: status=delivered, окно = [today−60d, today]
   (широкое окно: доставка может занять 3–14 дней после заказа)

2. Для каждого постинга → GET детали → customer_price

3. Для каждого товара в постинге:
   day_key = posting.in_process_at[:10]         # дата заказа
   price_sum[day_key]    += item.price × item.quantity
   coinvest_sum[day_key] += (item.price − item.customer_price) × item.quantity

4. avg_spp[day] = coinvest_sum[day] / price_sum[day] × 100

5. Upsert в daily_summary: поле avg_spp для каждого (cabinet_prefix, day)
```

**Если `customer_price` = 0 — постинг пропускается.**

**Формула СПП/Соинвест:**
```
avg_spp = SUM((price − customer_price) × qty) / SUM(price × qty) × 100
```

**Окно:** запрашиваем постинги за 60 дней назад.
Запись падает по дате заказа → upsert безопасен, пересчёт не ломает данные.

> Этот шаг медленный: тысячи индивидуальных GET-запросов на детали постингов.
> При нескольких кабинетах — 20–40 минут.

---

### 4. Performance API → рекламная статистика

Нового API-запроса нет — агрегация из уже накопленной таблицы `campaign_stats`.

**SQL-агрегация:**
```sql
SELECT
    cabinet_prefix,
    DATE(stat_date) AS day,
    SUM(money_spent)  AS ad_spend,
    SUM(views)        AS ad_impressions,
    SUM(clicks)       AS ad_clicks
FROM campaign_stats
WHERE cabinet_prefix = :prefix
  AND stat_date BETWEEN :from AND :to
GROUP BY cabinet_prefix, DATE(stat_date)
```

Результат → upsert в `daily_summary`.

**Окно ресинка:** последние 7 дней.

---

### 5. Chrome-плагин → воронка (показы, карточка, корзина)

Ozon не отдаёт данные воронки через публичный API без платного тарифа Analytics Premium.
Воронка собирается браузерным расширением Chrome, которое перехватывает запросы в личном кабинете продавца.

**Что собирается:**

| Поле | Описание |
|---|---|
| `hits_view` | Показы в каталоге/поиске |
| `hits_view_pdp` | Переходы в карточку товара |
| `hits_tocart_pdp` | Добавления в корзину из карточки |
| `ordered_units` | Заказы (альтернатива API-данным) |

Данные пишутся в отдельную таблицу `plugin_analytics` с ключом `(cabinet_prefix, day)`.

**Pivot читает воронку отдельным запросом**, не из `daily_summary`.
Если плагин молчит — вся группа воронки в pivot = NULL.

---

## Расписание ETL

| Время (MSK) | Шаг | Окно |
|---|---|---|
| 06:00 | Analytics API → orders/delivered/returns | −7 дней |
| 06:00 | Finance API → финансы | −14 дней |
| 06:00 | Performance API → реклама (из campaign_stats) | −7 дней |
| 07:30 | Postings API → SPP/Соинвест | постинги за −60 дней, запись по дате заказа |
| 09:00 | Healthcheck | строка за вчера должна быть |

Шаги 1–3 и 4 запускаются параллельно (независимые источники).
Шаг Postings — последний, потому что самый долгий.

**Healthcheck:** если в `daily_summary` нет строки за вчера для кабинета X → Telegram-алерт.

---

## Метрики pivot — полный список

### Суммы

| Метрика | Источник / Формула |
|---|---|
| Сумма заказов, ₽ | `orders_revenue` |
| Сумма выкупов (факт), ₽ | `accruals_for_sale` |

### Штуки

| Метрика | Источник |
|---|---|
| Заказы, шт | `orders_qty` |
| Выкупы в эту дату, шт | `delivered_qty` |

### Средние показатели

| Метрика | Формула |
|---|---|
| Средний чек, ₽ | `orders_revenue / orders_qty` |
| Средний СПП/Соинвест, % | `avg_spp` (по дате заказа) |

### Воронка (источник: plugin_analytics)

Если плагин не работает — вся группа NULL.

| Метрика | Поле / Формула |
|---|---|
| Показы | `hits_view` |
| Переходы в карточку | `hits_view_pdp` |
| CTR органический, % | `hits_view_pdp / hits_view × 100` |
| В корзину | `hits_tocart_pdp` |
| CR в корзину, % | `hits_tocart_pdp / hits_view_pdp × 100` |
| Заказы (плагин) | `ordered_units` |
| CR в заказ, % | `ordered_units / hits_tocart_pdp × 100` |
| % выкупа | `delivered_qty / orders_qty × 100` (не более 100%) |

### Реклама

| Метрика | Поле / Формула |
|---|---|
| Показы рекламы | `ad_impressions` |
| Клики | `ad_clicks` |
| CTR рекламы, % | `ad_clicks / ad_impressions × 100` |
| Расход, ₽ | `ad_spend` |
| CPM (цена 1000 показов) | `ad_spend / ad_impressions × 1000` |
| CPC (цена клика) | `ad_spend / ad_clicks` |
| ДРР рекламный (ACOS), % | `ad_spend / accruals_for_sale × 100` |
| Общий ДРР (TACOS), % | `ad_spend / orders_revenue × 100` |

### Стоимость воронки

| Метрика | Формула |
|---|---|
| CPB (цена корзины) | `ad_spend / hits_tocart_pdp` |
| CPO (цена заказа) | `ad_spend / orders_qty` |

### Доходность

Показывается только если задана себестоимость хотя бы части товаров И `for_pay > 0`.

| Метрика | Формула |
|---|---|
| Маржинальная прибыль, ₽ | `for_pay − cost_total − ad_spend` |
| Маржинальность, % | `Маржинальная прибыль / accruals_for_sale × 100` |

`cost_total = SUM(delivered_qty_per_sku × cost_price_per_sku)` — считается отдельно, не хранится в `daily_summary`.

---

## Логика чтения pivot

```python
# Основные данные — одна таблица
summary_rows = db.query(
    cabinet_prefix IN (...) AND day BETWEEN date_from AND date_to
).order_by(day)

# Воронка — только из плагина (отдельный запрос)
plugin_rows = db.query(
    cabinet_prefix IN (...) AND day BETWEEN date_from AND date_to
)

# Мерж по (cabinet_prefix, day)
for day in days:
    slot[day] = merge(summary[day], plugin[day])  # plugin перекрывает только свои поля
```

---

## Ограничения и важные детали

- **Finance API**: не более 1 месяца за запрос → цикл по месячным чанкам.
- **Postings API**: медленный из-за индивидуальных GET на каждый постинг.
- **Analytics API**: данные появляются с задержкой ~24ч. Вчера + позавчера всегда ресинкаются.
- **Никаких удалений строк** в `daily_summary`. Таблица только пополняется.
- **Первичная загрузка истории** — ручной скрипт, 60 дней назад. Один раз при внедрении.
- **Закрывающий документ Ozon** (отчёт о реализации) выходит 6-го числа следующего месяца. До него финансовые данные предварительные.
- **Хранить всё как числа** (float/int), не как текст. Ozon API возвращает строки в некоторых полях — приводить при записи.
