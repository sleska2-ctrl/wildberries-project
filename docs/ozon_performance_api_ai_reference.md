# Ozon Performance API 2.0 - AI-friendly reference

Источник: документация Ozon Performance API 2.0 из загруженного PDF.  
Назначение файла: быстрый справочник для ИИ-агентов и разработчиков: методы, назначение, параметры, ограничения и примеры запросов.

> Важно: это справочник по документации, а не официальный SDK. Перед продакшеном сверяйте спорные поля с актуальной документацией Ozon.

---

## 1. Базовые сведения

### Base URL

```text
https://api-performance.ozon.ru
```

Старый хост `performance.ozon.ru` не используется; документация указывает переход на `api-performance.ozon.ru`.

### Авторизация

Performance API использует Bearer-токен.

#### Получить токен через API-ключ

```http
POST /api/client/token HTTP/1.1
Host: api-performance.ozon.ru
Content-Type: application/json
Accept: application/json
```

```json
{
  "client_id": "XYZ@advertising.performance.ozon.ru",
  "client_secret": "CLIENT_SECRET",
  "grant_type": "client_credentials"
}
```

Ответ:

```json
{
  "access_token": "ACCESS_TOKEN",
  "expires_in": 1800,
  "token_type": "Bearer"
}
```

#### Использование токена

```http
Authorization: Bearer ACCESS_TOKEN
Content-Type: application/json
Accept: application/json
```

#### OAuth-токен

OAuth-токен доступен только продавцам из России. Его можно использовать с методами Performance API в рамках уровней доступа, назначенных приложению.

---

## 2. Глобальные ограничения и правила

### Общие лимиты

| Ограничение | Значение |
|---|---:|
| Общий лимит запросов | 100 000 запросов в сутки |
| Максимальный период статистической выгрузки | 62 дня |
| Максимум кампаний в одном статистическом отчёте | 10 |
| Одновременные выгрузки с аккаунта | 1 |
| Выгрузки за 24 часа с аккаунта | 2000 |
| Одновременные выгрузки по организации | 5 |
| Выгрузки за 24 часа по организации | 2000 |

Лимит статистических выгрузок считается так: `количество активных кампаний * 240`, но не больше табличного лимита. Одна кампания в запросе = одна выгрузка. Если передано несколько кампаний, они считаются как несколько выгрузок.

### Деньги и ставки

В большинстве campaign-методов бюджеты передаются в миллионных долях рубля:

```text
1 000 000 = 1 рубль
```

Исключение: часть методов лимитов ставок возвращает значения в рублях как `number`.

### Статистика

Даты в отчётах рекламного кабинета группируются по московскому времени.

Многие отчёты асинхронные:

1. Запустить генерацию отчёта.
2. Получить `UUID`.
3. Проверять статус через `GET /api/client/statistics/{UUID}` или vendor-аналог.
4. Скачать отчёт через `GET /api/client/statistics/report?UUID=...`.

Формат отчёта:

- CSV - если в запросе одна кампания.
- ZIP - если кампаний несколько; внутри один CSV-файл на кампанию.
- Для части методов доступен JSON-вариант через добавление `/json` к endpoint.

---

## 3. Общие enum-значения

### Тип рекламируемой кампании `advObjectType`

| Значение | Описание |
|---|---|
| `SKU` | Оплата за клик |
| `BANNER` | Баннерная рекламная кампания |
| `SEARCH_PROMO` | Оплата за заказ |
| `VIDEO_BANNER` | Видеобаннер |

### Тип оплаты `paymentType`

| Значение | Описание |
|---|---|
| `CPC` | Оплата за клики |
| `CPM` | Оплата за показы / 1000 показов |
| `CPO` | Оплата за заказы |

### Состояние кампании `state`

| Значение | Описание |
|---|---|
| `CAMPAIGN_STATE_RUNNING` | Активная кампания |
| `CAMPAIGN_STATE_PLANNED` | Сроки проведения ещё не наступили |
| `CAMPAIGN_STATE_STOPPED` | Приостановлена из-за нехватки бюджета |
| `CAMPAIGN_STATE_INACTIVE` | Остановлена владельцем |
| `CAMPAIGN_STATE_ARCHIVED` | Архивная кампания |
| `CAMPAIGN_STATE_MODERATION_DRAFT` | Черновик до отправки на модерацию |
| `CAMPAIGN_STATE_MODERATION_IN_PROGRESS` | На модерации |
| `CAMPAIGN_STATE_MODERATION_FAILED` | Модерация не пройдена |
| `CAMPAIGN_STATE_FINISHED` | Кампания завершена; изменить нельзя, можно клонировать или создать новую |

### Размещение `placement`

| Значение | Описание |
|---|---|
| `PLACEMENT_INVALID` | Не определено |
| `PLACEMENT_PDP` | Карточка товара; доступно только для ручного управления |
| `PLACEMENT_SEARCH_AND_CATEGORY` | Поиск и рекомендации |
| `PLACEMENT_TOP_PROMOTION` | Поиск |
| `PLACEMENT_OVERTOP` | Поиск и главная, спецразмещение |
| `PLACEMENT_TAKEOVER` | Одновременный показ на первых 4 плитках; встречается в исторических/устаревших описаниях |

### Стратегия `productAutopilotStrategy`

| Значение | Описание |
|---|---|
| `MAX_VIEWS` | Максимум показов |
| `MAX_CLICKS` | Автостратегия для Поиска и рекомендаций |
| `TOP_MAX_CLICKS` | Автостратегия для Поиска |
| `TARGET_BIDS` | Средняя стоимость клика |
| `TOP_PROMOTION` | Вывод в топ |
| `TARGET_CIR` | Целевой расход |
| `NO_AUTO_STRATEGY` | Автостратегия не используется |
| `TAKEOVER` | Спецразмещение для Поиска; встречается в исторических/устаревших описаниях |

### Группировка отчётов `groupBy`

| Значение | Описание |
|---|---|
| `NO_GROUP_BY` | Без группировки |
| `DATE` | По дням |
| `START_OF_WEEK` | По неделям |
| `START_OF_MONTH` | По месяцам |

### Состояние отчёта

| Значение | Описание |
|---|---|
| `NOT_STARTED` | Запрос ожидает выполнения |
| `IN_PROGRESS` | Выполняется |
| `OK` | Успешно выполнен |
| `ERROR` | Завершился ошибкой |
| `TIMEOUT` | Истекло время ожидания; используется в vendor-отчётах |
| `CANCEL` | Отменено; используется в vendor-отчётах |

---

## 4. Индекс методов

| Группа | Метод | Назначение |
|---|---|---|
| Auth | `POST /api/client/token` | Получить Bearer-токен |
| Кампании | `GET /api/client/campaign` | Список кампаний |
| Кампании | `GET /api/client/campaign/{campaignId}/objects` | Список продвигаемых объектов |
| Лимиты | `GET /api/client/limits/list` | Лимиты ставок |
| Лимиты | `POST /api/client/min/sku` | Минимальная ставка по SKU |
| Бонусы | `GET /api/client/products_with_bonuses` | SKU с бонусами |
| Статистика | `POST /api/client/statistics` | Асинхронная статистика по кампании |
| Статистика | `POST /api/client/statistics/video` | Отчёт по видеобаннерам |
| Статистика | `POST /api/client/statistics/attribution` | Отчёт по заказам на баннеры |
| Статистика | `GET /api/client/statistics/{UUID}` | Статус отчёта |
| Статистика | `GET /api/client/statistics/list` | Отчёты из интерфейса кабинета |
| Статистика | `GET /api/client/statistics/externallist` | Отчёты, сгенерированные через API |
| Статистика | `GET /api/client/statistics/report` | Скачать отчёт |
| Статистика | `GET /api/client/statistics/campaign/media` | Статистика по медийным кампаниям |
| Статистика | `GET /api/client/statistics/campaign/product` | Статистика по Оплате за клик |
| Статистика | `GET /api/client/statistics/expense` | Статистика расходов |
| Статистика | `GET /api/client/statistics/daily` | Дневная статистика |
| Статистика | `POST /api/client/statistic/orders/generate` | CPO-отчёт по заказам, выбранные товары |
| Статистика | `POST /api/client/statistic/products/generate` | CPO-отчёт по товарам, выбранные товары |
| Статистика | `GET /api/client/statistics/all_sku_promo/orders/generate` | CPO-отчёт по заказам, все товары |
| Статистика | `GET /api/client/statistics/all_sku_promo/products/generate` | CPO-отчёт по товарам, все товары |
| Статистика | `POST /api/client/statistics/phrases` | Отчёт по поисковым запросам |
| CPC | `POST /api/client/campaign/cpc/v2/product` | Создать кампанию с оплатой за клик |
| CPC | `POST /api/client/campaign/{campaignId}/activate` | Активировать кампанию |
| CPC | `POST /api/client/campaign/{campaignId}/deactivate` | Выключить кампанию |
| CPC | `PATCH /api/client/campaign/{campaignId}` | Изменить параметры кампании |
| CPC товары | `POST /api/client/campaign/{campaignId}/products` | Добавить товары в кампанию |
| CPC товары | `PUT /api/client/campaign/{campaignId}/products` | Обновить ставки товаров |
| CPC товары | `GET /api/client/campaign/{campaignId}/v2/products` | Список товаров кампании |
| CPC товары | `POST /api/client/campaign/{campaignId}/products/delete` | Удалить товары из кампании |
| CPC товары | `GET /api/client/campaign/{campaignId}/products/bids/competitive` | Конкурентные ставки |
| CPO | `POST /api/client/campaign/search_promo/v2/products` | Список товаров в оплате за заказ |
| CPO | `POST /api/client/search_promo/bids/recommendation` | Рекомендованные CPO-ставки; deprecated |
| CPO | `POST /api/client/campaign/search_promo/v2/bids/set` | Установить ставку; deprecated |
| CPO | `POST /api/client/search_promo/get_cpo_min_bids` | Фиксированные ставки для товаров |
| CPO | `POST /api/client/search_promo/product/enable` | Включить продвижение товара |
| CPO | `POST /api/client/search_promo/product/disable` | Отключить продвижение товара |
| CPO | `POST /api/client/campaign/search_promo/v2/bids/delete` | Удалить товар из CPO-продвижения |
| CPO all SKU | `GET /api/client/campaign/all_sku_promo/activate` | Включить CPO для всех товаров |
| CPO all SKU | `GET /api/client/campaign/all_sku_promo/deactivate` | Выключить CPO для всех товаров |
| Морковск | `POST /api/client/campaign/search_promo/carrots/enable` | Добавить товары в акцию «Морковск» |
| Морковск | `POST /api/client/campaign/search_promo/carrots/disable` | Удалить товары из акции «Морковск» |
| Vendor | `POST /api/client/vendors/statistics` | Запустить отчёт внешнего трафика |
| Vendor | `GET /api/client/vendors/statistics/list` | Список vendor-отчётов |
| Vendor | `GET /api/client/vendors/statistics/{UUID}` | Информация о vendor-отчёте |
| Vendor | `GET /api/client/organisation/vendor_tag` | UTM-префикс организации |
| Deprecated | `POST /external/api/dynamic_budget` | Расчёт минимального бюджета; deprecated |

---

## 5. Методы подробно

### 5.1 `GET /api/client/campaign` - список кампаний

**Назначение:** получить список рекламных кампаний с фильтрами.

**Query parameters:**

| Параметр | Тип | Обяз. | Описание |
|---|---|---:|---|
| `campaignIds` | array<string uint64> | Нет | ID кампаний. Если пусто, вернутся все кампании. |
| `advObjectType` | string | Нет | `SKU`, `BANNER`, `SEARCH_PROMO`, `VIDEO_BANNER`. |
| `state` | string | Нет | Состояние кампании. |
| `page` | int64 | Нет | Номер страницы, с 1. |
| `pageSize` | int64 | Нет | Размер страницы. |

**Ответ 200:** `list[]` с полями `id`, `paymentType`, `title`, `state`, `advObjectType`, `fromDate`, `toDate`, `autostopStatus`, `isAutocreated`, `budget`, `dailyBudget`, `weeklyBudget`, `placement`, `productAutopilotStrategy`, `autopilot`, `createdAt`, `updatedAt`, `productCampaignMode`.

**Пример:**

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/campaign?advObjectType=SKU&state=CAMPAIGN_STATE_RUNNING&page=1&pageSize=100' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json'
```

---

### 5.2 `GET /api/client/campaign/{campaignId}/objects` - продвигаемые объекты

**Назначение:** получить список продвигаемых объектов в кампаниях «Оплата за клик», «Баннеры» и «Видеобаннеры».

**Ограничение/заметка:** для товаров в кампании «Оплата за заказ» используйте `POST /api/client/campaign/search_promo/v2/products`.

**Path parameters:**

| Параметр | Тип | Описание |
|---|---|---|
| `campaignId` | string uint64 | ID кампании |

**Ответ 200:** `list[]`, где `id` - SKU для товарной рекламы или числовой ID для баннерных кампаний.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/campaign/48852/objects' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Accept: application/json'
```

---

### 5.3 `GET /api/client/limits/list` - лимиты ставок

**Назначение:** получить минимальные и максимальные ставки для инструментов продвижения.

**Покрывает:**

- Оплата за заказ: min/max ставка инструмента, min ставки по категориям.
- Спецразмещение и Оплата за клик: min/max ставка за клик, min ставки по категориям.

**Ответ 200:**

| Поле | Тип | Описание |
|---|---|---|
| `limits[].categories[]` | array | Минимальная ставка для категории второго уровня |
| `limits[].maxBid` | number | Максимальная ставка в рублях |
| `limits[].minBid` | number | Минимальная ставка в рублях |
| `limits[].objectType` | string | `SKU` или `SEARCH_PROMO` |
| `limits[].paymentMethod` | string | `CPO`, `CPC`, `CPM` |
| `limits[].placement` | string | `CAMPAIGN_PLACEMENT_SEARCH_AND_CATEGORY`, `CAMPAIGN_PLACEMENT_TOP_PROMOTION`, `CAMPAIGN_PLACEMENT_OVERTOP` |

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/limits/list' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Accept: application/json'
```

---

### 5.4 `POST /api/client/min/sku` - минимальная ставка для товаров по SKU

**Назначение:** узнать минимальную ставку для одного или нескольких товаров.

**Body:**

| Параметр | Тип | Обяз. | Описание |
|---|---|---:|---|
| `marketplaceId` | string | Да | `MARKETPLACE_ID_RU`, `MARKETPLACE_ID_KZ`, `MARKETPLACE_ID_BY` |
| `paymentType` | string | Да | `CPO`, `CPC`, `CPC_TOP` |
| `sku` | array<string uint64> | Да | Ozon ID или SKU |

**Ответ 200:** `minBids[]` с полями `bid`, `sku`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/min/sku' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "marketplaceId": "MARKETPLACE_ID_RU",
    "paymentType": "CPO",
    "sku": ["123456789"]
  }'
```

---

### 5.5 `GET /api/client/products_with_bonuses` - товары с бонусами

**Назначение:** получить SKU товаров, на которые начислены бонусы. За клик сначала тратятся бонусы товара, затем общий бюджет кампании.

**Ответ 200:** `skus[]`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/products_with_bonuses' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Accept: application/json'
```

---

## 6. Статистика и отчёты

### 6.1 `POST /api/client/statistics` - статистика по кампании

**Назначение:** запустить асинхронную генерацию отчёта по кампаниям.

**Формат:** CSV; ZIP, если кампаний несколько. JSON-вариант: `POST /api/client/statistics/json`.

**Body:**

| Параметр | Тип | Обяз. | Описание |
|---|---|---:|---|
| `campaigns` | array<string uint64> | Да | ID кампаний. Максимум 10 кампаний в отчёте. |
| `from` | date-time RFC3339 | Нет | Начало периода, максимум 62 дня |
| `to` | date-time RFC3339 | Нет | Конец периода, максимум 62 дня |
| `dateFrom` | string `YYYY-MM-DD` | Нет | Начало периода |
| `dateTo` | string `YYYY-MM-DD` | Нет | Конец периода |
| `groupBy` | string | Нет | `NO_GROUP_BY`, `DATE`, `START_OF_WEEK`, `START_OF_MONTH` |

Если заполнены `from`, `to`, `dateFrom`, `dateTo`, используется период из `dateFrom` и `dateTo`.

**Ответ 200:** `UUID`, `vendor`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/statistics' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "campaigns": ["123456"],
    "dateFrom": "2026-05-01",
    "dateTo": "2026-05-31",
    "groupBy": "DATE"
  }'
```

---

### 6.2 `POST /api/client/statistics/video` - статистика по видеобаннерам

**Назначение:** асинхронный отчёт по показам видеобаннера.

**JSON-вариант:** `POST /api/client/statistics/video/json`.

**Отчёт содержит:** название, ID кампании, период, ID баннера, показы, клики, охват, CTR, долю видимых показов, досмотры 25/50/75/100%, долю досмотров, просмотры со звуком, заказы, расход.

**Body:** `campaigns[]`, `dateFrom`, `dateTo`, `groupBy`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/statistics/video' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "campaigns": ["123456"],
    "dateFrom": "2026-05-01",
    "dateTo": "2026-05-31",
    "groupBy": "DATE"
  }'
```

---

### 6.3 `POST /api/client/statistics/attribution` - отчёт по заказам на баннеры

**Назначение:** асинхронный отчёт по заказам на баннеры.

**JSON-вариант:** `POST /api/client/statistics/attribution/json`.

**Отчёт содержит:** название, ID кампании, период, SKU, названия товаров, количество заказов, выручку, количество заказов модели, выручку с заказов модели, тип атрибуции.

**Body:** `campaigns[]`, `from`, `to`, `dateFrom`, `dateTo`, `groupBy`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/statistics/attribution' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "campaigns": ["123456"],
    "dateFrom": "2026-05-01",
    "dateTo": "2026-05-31",
    "groupBy": "NO_GROUP_BY"
  }'
```

---

### 6.4 `GET /api/client/statistics/{UUID}` - статус отчёта

**Назначение:** проверить статус асинхронного отчёта.

**Path:** `UUID` - идентификатор запроса.

**Ответ 200:** `UUID`, `state`, `createdAt`, `updatedAt`, `request`, `error`, `link`, `kind`.

`kind`: `STATS`, `SEARCH_PHRASES`, `ATTRIBUTION`, `VIDEO`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/statistics/0c159c60-ab92-...' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Accept: application/json'
```

---

### 6.5 `GET /api/client/statistics/list` - отчёты из интерфейса

**Назначение:** получить список отчётов, сгенерированных через интерфейс рекламного кабинета.

**Query:** `page`, `pageSize`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/statistics/list?page=1&pageSize=50' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 6.6 `GET /api/client/statistics/externallist` - отчёты через API

**Назначение:** получить список отчётов, сгенерированных через API сервисными аккаунтами.

**Query:** `page`, `pageSize`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/statistics/externallist?page=1&pageSize=50' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 6.7 `GET /api/client/statistics/report` - скачать отчёт

**Назначение:** скачать готовый отчёт по `UUID`.

**Query:** `UUID`.

**Ответ:** `text/csv` или ZIP; формат зависит от исходного запроса.

```bash
curl -L -X GET 'https://api-performance.ozon.ru/api/client/statistics/report?UUID=0c159c60-ab92-...' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -o report.csv
```

---

### 6.8 `GET /api/client/statistics/campaign/media` - статистика по медийным кампаниям

**Назначение:** синхронный отчёт по медийным кампаниям.

**JSON-вариант:** `GET /api/client/statistics/campaign/media/json`.

**Query:** `campaignIds[]`, `from`, `to`, `dateFrom`, `dateTo`.

**Отчёт содержит:** ID кампании, название, формат, статус, дневной бюджет, бюджет, расход, показы, клики, CPM, CTR, CPC, заказы, заказы после просмотра, сумма заказов после просмотра, ДРР, сумма заказов, тип оплаты, тип бюджета.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/statistics/campaign/media?campaignIds=123456&dateFrom=2026-05-01&dateTo=2026-05-31' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 6.9 `GET /api/client/statistics/campaign/product` - статистика по Оплате за клик

**Назначение:** синхронный отчёт по кампаниям с оплатой за клик.

**JSON-вариант:** `GET /api/client/statistics/campaign/product/json`.

**Query:** `campaignIds[]`, `from`, `to`, `dateFrom`, `dateTo`.

**Отчёт содержит:** ID кампании, название, тип объекта, статус, недельный бюджет, бюджет, расход, показы, клики, добавления в корзину, CTR, CPC, заказы, сумма заказов, ДРР, тип продвижения, места размещения для спецразмещения.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/statistics/campaign/product?campaignIds=123456&dateFrom=2026-05-01&dateTo=2026-05-31' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 6.10 `GET /api/client/statistics/expense` - статистика по расходу

**Назначение:** отчёт по расходам кампаний. Если `dateFrom` и `dateTo` не указаны, возвращаются последние 7 дней.

**JSON-вариант:** `GET /api/client/statistics/expense/json`.

**Query:** `campaignIds[]`, `dateFrom`, `dateTo`.

**Отчёт содержит:** ID кампании, дата, название, расход, расход с абонентского счёта, расход бонусов.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/statistics/expense?dateFrom=2026-05-01&dateTo=2026-05-07' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 6.11 `GET /api/client/statistics/daily` - дневная статистика

**Назначение:** дневная статистика по кампаниям. Если даты не указаны, возвращаются последние 7 дней.

**JSON-вариант:** `GET /api/client/statistics/daily/json`.

**Query:** `campaignIds[]`, `dateFrom`, `dateTo`.

**Отчёт содержит:** ID кампании, название, дата, показы, клики, расход, заказы в штуках, заказы в рублях.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/statistics/daily?campaignIds=123456&dateFrom=2026-05-01&dateTo=2026-05-07' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 6.12 `POST /api/client/statistic/orders/generate` - CPO-заказы, выбранные товары

**Назначение:** асинхронный отчёт по заказам в оплате за заказ для выбранных товаров.

**JSON-вариант:** `POST /api/client/statistic/orders/generate/json`.

**Body:** `from`, `to` в RFC3339.

**Отчёт содержит:** период, дату, ID заказа, номер заказа, SKU, SKU продвигаемого товара, артикул, наименование, источник заказа, количество, стоимость, ставку %, ставку ₽, расход ₽.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/statistic/orders/generate' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "from": "2026-05-01T00:00:00Z",
    "to": "2026-05-31T23:59:59Z"
  }'
```

---

### 6.13 `POST /api/client/statistic/products/generate` - CPO-товары, выбранные товары

**Назначение:** асинхронный отчёт по товарам в оплате за заказ для выбранных товаров.

**JSON-вариант:** `POST /api/client/statistic/products/generate/json`.

**Body:** `from`, `to` в RFC3339.

**Отчёт содержит:** период, SKU, артикул, наименование, категорию, продвижение, цену, ставку %, ставку ₽, количество заказов, сумму заказов, расход, продажи/расход/заказы по Оплате за клик, добавления в корзину, ДРР, последнее изменение.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/statistic/products/generate' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "from": "2026-05-01T00:00:00Z",
    "to": "2026-05-31T23:59:59Z"
  }'
```

---

### 6.14 `GET /api/client/statistics/all_sku_promo/orders/generate` - CPO-заказы, все товары

**Назначение:** асинхронный отчёт по заказам в оплате за заказ для всех товаров.

**JSON-вариант:** `GET /api/client/statistics/all_sku_promo/orders/generate/json`.

**Query:** `timeBounds.from`, `timeBounds.to` в RFC3339.

```bash
curl -G 'https://api-performance.ozon.ru/api/client/statistics/all_sku_promo/orders/generate' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  --data-urlencode 'timeBounds.from=2026-05-01T00:00:00Z' \
  --data-urlencode 'timeBounds.to=2026-05-31T23:59:59Z'
```

---

### 6.15 `GET /api/client/statistics/all_sku_promo/products/generate` - CPO-товары, все товары

**Назначение:** асинхронный отчёт по товарам в оплате за заказ для всех товаров.

**JSON-вариант:** `GET /api/client/statistics/all_sku_promo/products/generate/json`.

**Query:** `timeBounds.from`, `timeBounds.to` в RFC3339.

```bash
curl -G 'https://api-performance.ozon.ru/api/client/statistics/all_sku_promo/products/generate' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  --data-urlencode 'timeBounds.from=2026-05-01T00:00:00Z' \
  --data-urlencode 'timeBounds.to=2026-05-31T23:59:59Z'
```

---

### 6.16 `POST /api/client/statistics/phrases` - отчёт по поисковым запросам

**Назначение:** сформировать отчёт по поисковым запросам.

**Статус:** метод находится на стадии тестирования.

**Ограничение:** отчёт можно сформировать только для кампаний с оплатой за клик с `placement = PLACEMENT_TOP_PROMOTION`. Дата начала не может быть раньше 1 февраля 2025 года.

**JSON-вариант:** `POST /api/client/statistics/phrases/json`.

**Body:** `campaigns[]`, `dateFrom`, `dateTo`, `from`, `to`, `groupBy`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/statistics/phrases' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "campaigns": ["123456"],
    "dateFrom": "2026-05-01",
    "dateTo": "2026-05-31",
    "groupBy": "DATE"
  }'
```

---

## 7. Оплата за клик: кампании

### 7.1 `POST /api/client/campaign/cpc/v2/product` - создать CPC-кампанию

**Назначение:** создать товарную рекламную кампанию с оплатой за клики.

**Ответ 200:** `campaignId`.

**Body:**

| Параметр | Тип | Обяз. | Описание |
|---|---|---:|---|
| `title` | string | Да | Название кампании |
| `fromDate` | string | Нет | Дата начала по Москве. Если пусто, старт - начало текущего дня |
| `toDate` | string | Нет | Дата окончания; не учитывается для CPC-кампаний в автоматическом режиме |
| `dailyBudget` | string uint64 | Нет | Deprecated; дневной бюджет в миллионных долях рубля |
| `weeklyBudget` | string uint64 | Нет | Недельный бюджет в миллионных долях рубля |
| `placement` | string | Да | `PLACEMENT_TOP_PROMOTION`, `PLACEMENT_INVALID`, `PLACEMENT_SEARCH_AND_CATEGORY` |
| `productAutopilotStrategy` | string | Да | `MAX_CLICKS`, `TOP_MAX_CLICKS`, `TARGET_BIDS`, `TOP_PROMOTION`, `TARGET_CIR` |

**Ограничение:** после создания нельзя переключить тип бюджета с дневного на недельный и наоборот.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/cpc/v2/product' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "CPC campaign example",
    "fromDate": "2026-06-01",
    "toDate": "2026-06-30",
    "weeklyBudget": "700000000",
    "placement": "PLACEMENT_SEARCH_AND_CATEGORY",
    "productAutopilotStrategy": "TARGET_BIDS"
  }'
```

---

### 7.2 `POST /api/client/campaign/{campaignId}/activate` - активировать кампанию

**Назначение:** активировать кампанию.

**Body:** пустой объект `{}`.

**Ответ:** объект кампании.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/123456/activate' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{}'
```

---

### 7.3 `POST /api/client/campaign/{campaignId}/deactivate` - выключить кампанию

**Назначение:** деактивировать кампанию.

**Body:** пустой объект `{}`.

**Ответ:** объект кампании.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/123456/deactivate' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{}'
```

---

### 7.4 `PATCH /api/client/campaign/{campaignId}` - изменить параметры кампании

**Назначение:** изменить даты и бюджетные параметры кампании.

**Body:**

| Параметр | Тип | Описание |
|---|---|---|
| `autopilot` | object | Обязателен, если включена автостратегия |
| `fromDate` | string | Дата начала по Москве, не раньше текущей даты |
| `toDate` | string | Дата окончания, не раньше даты начала |
| `budget` | string uint64 | Общий бюджет; для автоматических кампаний брендов и агентств |
| `dailyBudget` | string uint64 | Deprecated; дневной бюджет |
| `weeklyBudget` | string uint64 | Недельный бюджет |

**Ограничение:** после создания нельзя изменить тип бюджета с дневного на недельный и наоборот. Чтобы убрать общий бюджет, передайте `budget: "0"`.

```bash
curl -X PATCH 'https://api-performance.ozon.ru/api/client/campaign/123456' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "fromDate": "2026-06-01",
    "toDate": "2026-06-30",
    "weeklyBudget": "700000000"
  }'
```

---

### 7.5 `POST /external/api/dynamic_budget` - расчёт минимального бюджета, deprecated

**Статус:** deprecated; метод должен был быть отключён 1 сентября 2025 года. В документации указано, что минимальный бюджет теперь рассчитывается по формуле:

```text
2 000 рублей * 1 SKU
```

**Не рекомендуется использовать в новых интеграциях.**

---

## 8. Оплата за клик: товары

### 8.1 `POST /api/client/campaign/{campaignId}/products` - добавить товары в кампанию

**Назначение:** добавить товары в CPC-кампанию.

**Ограничения:**

- В кампанию можно добавить не более 500 товаров.
- Для добавления передаются `sku` и `bid`.
- Если `bid` не указан, автоматически задаётся конкурентная ставка.
- `bid` учитывается только для кампании со стратегией «Средняя стоимость клика».
- Для кампании с включённой стратегией можно добавлять товары только из категории `autopilot.categoryId`.

**Body:** `bids[]` с полями `sku`, `bid`, `targetCir`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/123456/products' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "bids": [
      {
        "sku": "987654321",
        "bid": "1500000",
        "targetCir": 15
      }
    ]
  }'
```

---

### 8.2 `PUT /api/client/campaign/{campaignId}/products` - обновить ставки товаров

**Назначение:** обновить ставки товаров в кампании.

**Важное поведение:** метод перезаписывает список стоп-слов и фразы со ставками; исходные стоп-слова и фразы со ставками удаляются.

**Body:** `bids[]` с полями `sku`, `bid`, `targetCir`.

```bash
curl -X PUT 'https://api-performance.ozon.ru/api/client/campaign/123456/products' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "bids": [
      {
        "sku": "987654321",
        "bid": "1700000",
        "targetCir": 20
      }
    ]
  }'
```

---

### 8.3 `GET /api/client/campaign/{campaignId}/v2/products` - список товаров кампании

**Query:** `page`, `pageSize`.

**Ответ:** `products[]` с полями `sku`, `bid`, `targetCir`, `title`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/campaign/123456/v2/products?page=1&pageSize=100' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 8.4 `POST /api/client/campaign/{campaignId}/products/delete` - удалить товары из кампании

**Body:** `sku[]`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/123456/products/delete' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "sku": ["987654321"]
  }'
```

---

### 8.5 `GET /api/client/campaign/{campaignId}/products/bids/competitive` - конкурентные ставки

**Назначение:** получить конкурентные ставки для товара.

**Ограничение:** в одном запросе можно передать до 200 товаров. Если товар не добавлен в кампанию, `bid = 0`.

**Query:** `sku[]`.

```bash
curl -G 'https://api-performance.ozon.ru/api/client/campaign/123456/products/bids/competitive' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  --data-urlencode 'sku=987654321' \
  --data-urlencode 'sku=123456789'
```

---

## 9. Оплата за заказ / Search Promo

### 9.1 `POST /api/client/campaign/search_promo/v2/products` - список товаров в CPO

**Назначение:** получить список товаров из оплаты за заказ.

**Body:**

| Параметр | Тип | Описание |
|---|---|---|
| `page` | int64 | Номер страницы, с 1 |
| `pageSize` | int64 | Размер страницы |

**Ответ:** `products[]`, `total`. В товаре могут быть поля `bid`, `bidPrice`, `bidWithoutAdditive`, `carrotsAdditive`, `carrotsStatus`, `hint`, `imageUrl`, `isSearchPromoAvailable`, `previousBid`, `previousVisibilityIndex`, `price`, `searchPromoStatus`, `sku`, `sourceSku`, `title`, `views`, `visibilityIndex`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/search_promo/v2/products' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "page": 1,
    "pageSize": 100
  }'
```

---

### 9.2 `POST /api/client/search_promo/bids/recommendation` - рекомендованные ставки, deprecated

**Статус:** deprecated.

**Назначение:** получить рекомендованные ставки для товаров из оплаты за заказ.

**Ограничение:** до 200 SKU за запрос.

**Body:** `skus[]`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/search_promo/bids/recommendation' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"skus": ["987654321"]}'
```

---

### 9.3 `POST /api/client/campaign/search_promo/v2/bids/set` - установить ставку, deprecated

**Статус:** deprecated.

**Назначение:** установить ставку на товар. Если товар ещё не добавлен в продвижение, система добавит его автоматически.

**Ограничение:** максимум 1000 товаров в одном запросе.

**Body:** `bids[]` с `sku`, `bid`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/search_promo/v2/bids/set' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "bids": [
      {"sku": "987654321", "bid": 10.5}
    ]
  }'
```

---

### 9.4 `POST /api/client/search_promo/get_cpo_min_bids` - фиксированные ставки для товаров

**Назначение:** получить фиксированные ставки для товаров.

**Ограничение:** до 200 SKU за запрос.

**Body:** `skus[]`.

**Ответ:** `bids[]` с `bid`, `sku`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/search_promo/get_cpo_min_bids' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"skus": ["987654321", "123456789"]}'
```

---

### 9.5 `POST /api/client/search_promo/product/enable` - включить продвижение товара

**Назначение:** включить оплату за заказ для товара.

**Ограничение:** максимум 1000 товаров в одном запросе.

**Body:** `skus[]`.

**Ответ:** `response[]` с `bid`, `error`, `sku`, `update`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/search_promo/product/enable' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"skus": ["987654321"]}'
```

---

### 9.6 `POST /api/client/search_promo/product/disable` - отключить продвижение товара

**Назначение:** отключить оплату за заказ для товара.

**Ограничение:** максимум 1000 товаров в одном запросе.

**Body:** `skus[]`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/search_promo/product/disable' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"skus": ["987654321"]}'
```

---

### 9.7 `POST /api/client/campaign/search_promo/v2/bids/delete` - удалить товар из CPO-продвижения

**Назначение:** удалить товар из продвижения в оплате за заказ.

**Ограничение:** максимум 1000 товаров в одном запросе.

**Body:** `sku[]`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/search_promo/v2/bids/delete' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"sku": ["987654321"]}'
```

---

### 9.8 `GET /api/client/campaign/all_sku_promo/activate` - включить CPO для всех товаров

**Назначение:** включить продвижение в оплате за заказ для всех товаров.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/campaign/all_sku_promo/activate' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 9.9 `GET /api/client/campaign/all_sku_promo/deactivate` - выключить CPO для всех товаров

**Назначение:** выключить продвижение в оплате за заказ для всех товаров.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/campaign/all_sku_promo/deactivate' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

## 10. Акция «Морковск»

### 10.1 `POST /api/client/campaign/search_promo/carrots/enable` - включить товары в акции

**Назначение:** добавить товары в акцию «Морковск».

**Body:** `skus[]`.

**Ответ:** `skuToInfo`, где ключ - SKU, значение содержит `error`, `isDisabled`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/search_promo/carrots/enable' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"skus": ["987654321"]}'
```

---

### 10.2 `POST /api/client/campaign/search_promo/carrots/disable` - отключить товары от акции

**Назначение:** удалить товары из акции «Морковск».

**Body:** `skus[]`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/campaign/search_promo/carrots/disable' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"skus": ["987654321"]}'
```

---

## 11. Аналитика внешнего трафика

### 11.1 `POST /api/client/vendors/statistics` - запустить vendor-отчёт

**Назначение:** запустить формирование отчёта с аналитикой внешнего трафика.

**Ограничения периода:** `dateFrom` не ранее 1 января 2022 года. Разница между `dateFrom` и `dateTo` должна быть не больше 3 месяцев. Если больше, отчёт сформируется за 3 месяца от `dateFrom`.

**Body:**

| Параметр | Тип | Обяз. | Описание |
|---|---|---:|---|
| `dateFrom` | string | Да | Начало периода |
| `dateTo` | string | Да | Конец периода |
| `type` | string | Да | `TRAFFIC_SOURCES` или `ORDERS` |

**Ответ:** `UUID`, `vendor: true`.

```bash
curl -X POST 'https://api-performance.ozon.ru/api/client/vendors/statistics' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{
    "dateFrom": "2026-05-01",
    "dateTo": "2026-05-31",
    "type": "TRAFFIC_SOURCES"
  }'
```

---

### 11.2 `GET /api/client/vendors/statistics/list` - список vendor-отчётов

**Query:** `page`, `pageSize`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/vendors/statistics/list?page=1&pageSize=50' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 11.3 `GET /api/client/vendors/statistics/{UUID}` - информация о vendor-отчёте

**Path:** `UUID`.

**Query:** `vendor=true` - признак, что запрашивается отчёт с аналитикой внешнего трафика.

**Ответ:** `UUID`, `createdAt`, `error`, `link`, `request`, `state`, `updatedAt`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/vendors/statistics/0c159c60-ab92-...?vendor=true' \
  -H 'Authorization: Bearer ACCESS_TOKEN'
```

---

### 11.4 `GET /api/client/organisation/vendor_tag` - метка организации

**Назначение:** вернуть префикс для UTM_CAMPAIGN по идентификатору организации.

**Query:** `orgId`.

**Ответ:** `tag`, например `vendor_org_42`.

```bash
curl -X GET 'https://api-performance.ozon.ru/api/client/organisation/vendor_tag?orgId=42' \
  -H 'Authorization: Bearer ACCESS_TOKEN' \
  -H 'Accept: application/json'
```

---

## 12. Быстрые сценарии для ИИ-агента

### Сценарий A: получить статистику кампании за месяц

1. Вызвать `POST /api/client/statistics` с `campaigns`, `dateFrom`, `dateTo`.
2. Сохранить `UUID`.
3. Каждые N секунд вызывать `GET /api/client/statistics/{UUID}`.
4. Если `state = OK`, взять `link` или вызвать `GET /api/client/statistics/report?UUID=...`.
5. Если `state = ERROR`, вернуть пользователю `error`.

### Сценарий B: создать CPC-кампанию и добавить товары

1. Вызвать `POST /api/client/campaign/cpc/v2/product`.
2. Получить `campaignId`.
3. Добавить товары через `POST /api/client/campaign/{campaignId}/products`.
4. Активировать через `POST /api/client/campaign/{campaignId}/activate`.
5. Проверить кампанию в `GET /api/client/campaign?campaignIds=...`.

### Сценарий C: включить оплату за заказ для SKU

1. При необходимости получить фиксированные ставки через `POST /api/client/search_promo/get_cpo_min_bids`.
2. Включить SKU через `POST /api/client/search_promo/product/enable`.
3. Проверить товары через `POST /api/client/campaign/search_promo/v2/products`.

### Сценарий D: vendor-аналитика внешнего трафика

1. Запустить `POST /api/client/vendors/statistics`.
2. Проверять `GET /api/client/vendors/statistics/{UUID}?vendor=true`.
3. После `state = OK` скачать файл по `link`.

---

## 13. Машиночитаемый краткий каталог

```yaml
api:
  name: Ozon Performance API
  version: "2.0"
  base_url: https://api-performance.ozon.ru
  auth:
    type: bearer
    token_endpoint: POST /api/client/token
    token_ttl_example_seconds: 1800
  global_limits:
    requests_per_day: 100000
    report_max_days: 62
    report_max_campaigns: 10
    concurrent_exports_per_account: 1
    exports_per_24h_per_account: 2000
    concurrent_exports_per_org: 5
    exports_per_24h_per_org: 2000
  methods:
    - {method: POST, path: /api/client/token, group: auth, async: false, description: Получить Bearer-токен}
    - {method: GET, path: /api/client/campaign, group: campaigns, async: false, description: Список кампаний}
    - {method: GET, path: /api/client/campaign/{campaignId}/objects, group: campaigns, async: false, description: Продвигаемые объекты}
    - {method: GET, path: /api/client/limits/list, group: limits, async: false, description: Лимиты ставок}
    - {method: POST, path: /api/client/min/sku, group: limits, async: false, description: Минимальная ставка по SKU}
    - {method: GET, path: /api/client/products_with_bonuses, group: bonuses, async: false, description: Товары с бонусами}
    - {method: POST, path: /api/client/statistics, group: statistics, async: true, json_variant: /api/client/statistics/json, description: Статистика по кампании}
    - {method: POST, path: /api/client/statistics/video, group: statistics, async: true, json_variant: /api/client/statistics/video/json, description: Статистика по видеобаннерам}
    - {method: POST, path: /api/client/statistics/attribution, group: statistics, async: true, json_variant: /api/client/statistics/attribution/json, description: Отчёт по заказам на баннеры}
    - {method: GET, path: /api/client/statistics/{UUID}, group: statistics, async: false, description: Статус отчёта}
    - {method: GET, path: /api/client/statistics/list, group: statistics, async: false, description: Отчёты из интерфейса}
    - {method: GET, path: /api/client/statistics/externallist, group: statistics, async: false, description: Отчёты через API}
    - {method: GET, path: /api/client/statistics/report, group: statistics, async: false, description: Скачать отчёт}
    - {method: GET, path: /api/client/statistics/campaign/media, group: statistics, async: false, json_variant: /api/client/statistics/campaign/media/json, description: Медийная статистика}
    - {method: GET, path: /api/client/statistics/campaign/product, group: statistics, async: false, json_variant: /api/client/statistics/campaign/product/json, description: Статистика Оплаты за клик}
    - {method: GET, path: /api/client/statistics/expense, group: statistics, async: false, json_variant: /api/client/statistics/expense/json, description: Расходы}
    - {method: GET, path: /api/client/statistics/daily, group: statistics, async: false, json_variant: /api/client/statistics/daily/json, description: Дневная статистика}
    - {method: POST, path: /api/client/statistic/orders/generate, group: cpo_statistics, async: true, json_variant: /api/client/statistic/orders/generate/json, description: CPO заказы выбранных товаров}
    - {method: POST, path: /api/client/statistic/products/generate, group: cpo_statistics, async: true, json_variant: /api/client/statistic/products/generate/json, description: CPO товары выбранных товаров}
    - {method: GET, path: /api/client/statistics/all_sku_promo/orders/generate, group: cpo_statistics, async: true, json_variant: /api/client/statistics/all_sku_promo/orders/generate/json, description: CPO заказы всех товаров}
    - {method: GET, path: /api/client/statistics/all_sku_promo/products/generate, group: cpo_statistics, async: true, json_variant: /api/client/statistics/all_sku_promo/products/generate/json, description: CPO товары всех товаров}
    - {method: POST, path: /api/client/statistics/phrases, group: statistics, async: true, status: testing, json_variant: /api/client/statistics/phrases/json, description: Поисковые запросы}
    - {method: POST, path: /api/client/campaign/cpc/v2/product, group: cpc_campaigns, async: false, description: Создать CPC-кампанию}
    - {method: POST, path: /api/client/campaign/{campaignId}/activate, group: cpc_campaigns, async: false, description: Активировать кампанию}
    - {method: POST, path: /api/client/campaign/{campaignId}/deactivate, group: cpc_campaigns, async: false, description: Выключить кампанию}
    - {method: PATCH, path: /api/client/campaign/{campaignId}, group: cpc_campaigns, async: false, description: Изменить параметры кампании}
    - {method: POST, path: /api/client/campaign/{campaignId}/products, group: cpc_products, async: false, limit: max_500_products_add, description: Добавить товары}
    - {method: PUT, path: /api/client/campaign/{campaignId}/products, group: cpc_products, async: false, description: Обновить ставки товаров}
    - {method: GET, path: /api/client/campaign/{campaignId}/v2/products, group: cpc_products, async: false, description: Список товаров кампании}
    - {method: POST, path: /api/client/campaign/{campaignId}/products/delete, group: cpc_products, async: false, description: Удалить товары}
    - {method: GET, path: /api/client/campaign/{campaignId}/products/bids/competitive, group: cpc_products, async: false, limit: max_200_skus, description: Конкурентные ставки}
    - {method: POST, path: /api/client/campaign/search_promo/v2/products, group: cpo, async: false, description: Список CPO-товаров}
    - {method: POST, path: /api/client/search_promo/bids/recommendation, group: cpo, async: false, status: deprecated, limit: max_200_skus, description: Рекомендованные ставки}
    - {method: POST, path: /api/client/campaign/search_promo/v2/bids/set, group: cpo, async: false, status: deprecated, limit: max_1000_products, description: Установить ставку}
    - {method: POST, path: /api/client/search_promo/get_cpo_min_bids, group: cpo, async: false, limit: max_200_skus, description: Фиксированные ставки}
    - {method: POST, path: /api/client/search_promo/product/enable, group: cpo, async: false, limit: max_1000_products, description: Включить товар}
    - {method: POST, path: /api/client/search_promo/product/disable, group: cpo, async: false, limit: max_1000_products, description: Отключить товар}
    - {method: POST, path: /api/client/campaign/search_promo/v2/bids/delete, group: cpo, async: false, limit: max_1000_products, description: Удалить товар}
    - {method: GET, path: /api/client/campaign/all_sku_promo/activate, group: cpo_all_sku, async: false, description: Включить CPO для всех товаров}
    - {method: GET, path: /api/client/campaign/all_sku_promo/deactivate, group: cpo_all_sku, async: false, description: Выключить CPO для всех товаров}
    - {method: POST, path: /api/client/campaign/search_promo/carrots/enable, group: carrots, async: false, description: Включить Морковск}
    - {method: POST, path: /api/client/campaign/search_promo/carrots/disable, group: carrots, async: false, description: Отключить Морковск}
    - {method: POST, path: /api/client/vendors/statistics, group: vendor, async: true, max_period: 3_months, description: Vendor-отчёт}
    - {method: GET, path: /api/client/vendors/statistics/list, group: vendor, async: false, description: Список vendor-отчётов}
    - {method: GET, path: /api/client/vendors/statistics/{UUID}, group: vendor, async: false, description: Информация о vendor-отчёте}
    - {method: GET, path: /api/client/organisation/vendor_tag, group: vendor, async: false, description: UTM-префикс организации}
```

---

## 14. Чек-лист перед вызовом API

- Токен свежий: `expires_in` в ответе token endpoint обычно 1800 секунд.
- Используется хост `api-performance.ozon.ru`.
- Для отчётов не превышен период 62 дня и максимум 10 кампаний.
- Для CPO bulk-операций не больше 1000 товаров в запросе.
- Для конкурентных ставок и CPO min/recommendation - не больше 200 SKU.
- Для добавления товаров в CPC-кампанию - не больше 500 товаров.
- Для бюджетов campaign-методов сумма указана в миллионных долях рубля.
- Для JSON-отчётов добавлен суффикс `/json`, если он поддерживается методом.
- Для асинхронных отчётов реализован polling по `UUID` и обработка `ERROR`.
