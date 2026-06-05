# Мониторинг цен WB через агент на ноуте

## Зачем такая схема

WB с 2024 года заблокировал публичный API `card.wb.ru` для всех датацентровых IP.
Новый endpoint `wildberries.ru/__internal/u-card/cards/v4/detail` работает только с
браузерными куками сессии WB — без них возвращает 498.

Решение: на ноуте запущен HTTP-агент `scripts/wb_price_agent.py`. Он сам управляет Chrome через Chrome DevTools Protocol (CDP), выполняет `fetch()` прямо в контексте браузера с WB-cookies и жилым IP, а сервер обращается к агенту по Tailscale.

В `scripts/wb_price_monitor.py` CDP-туннель оставлен как fallback: сначала используется HTTP-агент, а если он недоступен, монитор пробует старую схему через SSH/CDP-туннель.

---

## Схема работы

```
┌─────────────────────────────────────────────────────────┐
│                        СЕРВЕР (VPS)                      │
│                                                          │
│  wb_price_monitor.py                                     │
│       │                                                  │
│       ├─1─▶ HTTP GET http://100.65.13.99:8100/prices  │  │
│       │                                               │  │
│       ├─2─▶ fallback: CDP-туннель, если агент недоступен │
│       │                                               │  │
│       └─3─▶ Записывает цены в data/wb_prices.db       │  │
│                                                       │  │
└───────────────────────────────────────────────────────┘  │
                                                           │
┌──────────────────────────────────────────────────────────┘
│                    НОУТ (Windows 11)
│
│  wb_price_agent.py, порт 8100
│       │
│       ├─ Chrome с портом CDP 9224 и профилем profile-wb
│       │
│       ├─ fetch("wildberries.ru/__internal/...&nm=332756469;542415681;...")
│       │       credentials: "include"  ← куки WB передаются автоматически
│       │
│       └─▶ WB API отвечает: цены, остатки, рейтинг
│
│  Tailscale IP: 100.65.13.99
└──────────────────────────────────────────────────────────
```

---

## Агент и отдельный браузер для WB

На ноуте уже работают два профиля Chromium под Ozon (порты 9222 и 9223).
Для WB-мониторинга нужен **третий независимый экземпляр** — отдельный профиль и порт **9224**.

`scripts/wb_price_agent.py` запускается на ноуте, слушает порт `8100`, проверяет/поднимает Chrome на порту `9224` и отдает два endpoint:

- `GET /health` - состояние агента и Chrome.
- `GET /prices?nm=123;456` - цены, старая цена, остатки, рейтинг и отзывы по списку `nmId`.

### Опциональный bat-файл для ручного запуска Chrome

Обычно агент сам запускает Chrome. Bat-файл нужен только для ручной диагностики профиля или cookies.

```bat
@echo off
title WB Price Monitor Browser
start "" "C:\ozon-collector\chrome-win\chrome.exe" ^
  --remote-debugging-port=9224 ^
  --user-data-dir=C:\ozon-collector\profile-wb ^
  https://www.wildberries.ru
```

- `profile-wb` — отдельная папка, не пересекается с profile-1 и profile-2
- Порт `9224` — не конфликтует с 9222 и 9223
- При первом запуске нужно вручную войти на WB в этом браузере (куки сохранятся в profile-wb)
- Браузер можно держать свёрнутым — CDP работает в фоне

### Запуск агента на ноуте

```bat
cd C:\ozon-collector
python wb_price_agent.py
```

Лог агента пишется в `C:\ozon-collector\log-wb-agent.log`.

---

## Требования

### На сервере
- Tailscale подключён к той же сети (sleska2@)
  ```bash
  tailscale status   # должен видеть noir (100.65.13.99)
  ```
- HTTP-доступ к агенту: `http://100.65.13.99:8100/health`

### На ноуте (Windows 11)
- Tailscale установлен и залогинен через тот же Google-аккаунт
- `scripts/wb_price_agent.py` скопирован/доступен как `C:\ozon-collector\wb_price_agent.py`
- Python с пакетом `websockets`
- Chrome-профиль `C:\ozon-collector\profile-wb` залогинен на WB
- агент запущен и отвечает на `http://100.65.13.99:8100/health`

---

## Диагностика агента

### Проверить HTTP-агент

```bash
curl http://100.65.13.99:8100/health
curl "http://100.65.13.99:8100/prices?nm=542415681"
```

### Проверить доступность ноута
```bash
ping -c 2 100.65.13.99           # должен пройти ~66ms
```

### CDP fallback

CDP fallback нужен только если HTTP-агент недоступен, но Chrome на ноуте работает. Для него нужны пакет `websockets` на сервере и SSH-доступ к ноуту.

```bash
ssh -o StrictHostKeyChecking=no -f -N \
    -L 19224:127.0.0.1:9224 "ии@100.65.13.99"
```
После этого `http://127.0.0.1:19224/json/list` возвращает список вкладок Chrome.

### Chrome DevTools на ноуте
Агент запускает Chrome с профилем `profile-wb` и портом `9224`.
Через SSH-туннель на сервере CDP доступен как `http://127.0.0.1:19224`.

---

## WB API endpoint

### URL
```
GET https://www.wildberries.ru/__internal/u-card/cards/v4/detail
    ?appType=1
    &curr=rub
    &dest=-1257786
    &spp=30
    &hide_vflags=4294967296
    &hide_dtype=15
    &lang=ru
    &ab_testing=false
    &nm=332756469;542415681;398218192   ← до 50 nmId через ;
```

### Авторизация
Не требует токена — нужны браузерные куки сессии WB (`_wbauid`, `x_wbaas_token`).
Без кук: **HTTP 498** (с датацентровых IP и с ноута без браузера).
С куками через `fetch(..., {credentials: "include"})`: **HTTP 200**.

### Структура ответа
```json
{
  "products": [
    {
      "id": 542415681,
      "name": "Стиральная машина автомат IWSC 6105 (CIS) 6 кг",
      "brand": "Indesit",
      "supplier": "Маркет Бытовой техники",
      "supplierId": 12345,
      "rating": 5,
      "feedbacks": 383,
      "sizes": [
        {
          "price": {
            "basic":   3015900,   // цена до скидки в копейках → / 100 = 30 159 ₽
            "product": 2099000    // финальная цена в копейках  → / 100 = 20 990 ₽
          },
          "stocks": [
            {"wh": 50177962, "qty": 30}   // остаток на складе
          ]
        }
      ]
    }
  ]
}
```

### Парсинг цены и остатка
```python
for s in product["sizes"]:
    price       = s["price"]["product"] // 100   # финальная цена ₽
    price_basic = s["price"]["basic"]   // 100   # до скидки ₽
    stock      += sum(st["qty"] for st in s.get("stocks", []))
```

### Пакетный запрос
Один запрос — до **50 nmId** через `;`. WB сам делает пакетные запросы по 29 штук.
Мы используем 50 — на практике работает стабильно.

---

## Скрипт мониторинга

**Файл:** `scripts/wb_price_monitor.py`

### Запуск
```bash
# Тест без записи в БД (один кабинет)
python3 scripts/wb_price_monitor.py --cabinet ewb --dry-run

# Все WB-кабинеты, запись в БД
python3 scripts/wb_price_monitor.py

# Конкретный кабинет
python3 scripts/wb_price_monitor.py --cabinet hld
```

### Cron (2 раза в день: 8:00 и 20:00)
```bash
crontab -e
```
Добавить строку:
```
0 8,20 * * * cd /opt/wildberries/app && python3 scripts/wb_price_monitor.py >> logs/price_monitor.log 2>&1
```

### Что делает скрипт
1. Читает WB-кабинеты из платформенной базы и `nmId` из `data/cabs/{cabinet_id}.db`.
2. Берет товары в наличии на последнюю дату `raw_stocks`.
3. Бьет на батчи по 50 `nmId`, пауза 1.5 сек между батчами.
4. Для каждого батча сначала вызывает HTTP-агент `http://100.65.13.99:8100/prices`.
5. Если агент недоступен, пробует fallback через CDP-туннель.
6. Парсит `price.product`, `price.basic`, `stocks[].qty`, рейтинг и отзывы.
7. Пишет в `data/wb_prices.db` → таблицу `price_history`.
8. Для кабинета `hld` дополнительно читает `competitor_products` и пишет цены конкурентов в `competitor_prices`.

### Скорость
- 1 200 товаров ÷ 50 = 24 батча × 1.5 сек = **~36 секунд** на полный прогон
- 2 000 товаров = **~60 секунд**

---

## База данных цен

**Файл:** `data/wb_prices.db`

### Схема
```sql
CREATE TABLE price_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT,           -- timestamp ISO: "2026-06-03T08:00:00"
    date        TEXT,           -- дата: "2026-06-03"
    cabinet_id  TEXT,           -- "ewb", "hld", "mipao", ...
    nm_id       INTEGER,        -- WB артикул
    name        TEXT,           -- название товара
    brand       TEXT,
    seller      TEXT,
    price       INTEGER,        -- финальная цена ₽
    price_basic INTEGER,        -- цена до скидки ₽
    rating      REAL,
    feedbacks   INTEGER,
    stock       INTEGER         -- суммарный остаток по всем складам
);
```

Дополнительные таблицы контроля конкурентов:

```sql
-- Заполняет scripts/wb_competitor_finder.py
competitor_products (
    cabinet_id,
    our_nm_id,
    our_name,
    comp_nm_id,
    comp_name,
    comp_brand,
    comp_seller,
    comp_seller_id,
    subject_id,
    subject_name,
    url,
    found_at
);

-- Заполняет scripts/wb_price_monitor.py
competitor_prices (
    ts,
    date,
    cabinet_id,
    our_nm_id,
    comp_nm_id,
    price,
    price_basic,
    stock,
    rating,
    feedbacks
);
```

### Пример запросов
```sql
-- Последние цены по кабинету
SELECT nm_id, name, price, stock
FROM price_history
WHERE cabinet_id='ewb' AND date=date('now')
ORDER BY price;

-- История цены одного товара
SELECT date, price, stock
FROM price_history
WHERE nm_id=542415681
ORDER BY date;

-- Товары с изменением цены за последние 2 дня
SELECT p1.nm_id, p1.name, p2.price as price_prev, p1.price as price_now,
       p1.price - p2.price as delta
FROM price_history p1
JOIN price_history p2 ON p1.nm_id=p2.nm_id AND p1.cabinet_id=p2.cabinet_id
WHERE p1.date=date('now') AND p2.date=date('now','-1 day')
  AND p1.price != p2.price
ORDER BY abs(delta) DESC;
```

## Поиск конкурентов

**Файл:** `scripts/wb_competitor_finder.py`

Скрипт работает для кабинета `hld`: берет товары в наличии из `raw_stocks`, название и `subject_id` из `wb_cards` или `raw_sales`, извлекает модель из названия, ищет похожие карточки через MPSTATS и сохраняет связи в `competitor_products`.

Запуск:

```bash
python3 scripts/wb_competitor_finder.py --test
python3 scripts/wb_competitor_finder.py
```

Для работы нужен `MPSTATS_TOKEN` в окружении или в `/root/.claude/skills/mpstats/config/.env`.

## Web UI

Страница `/analytics/competitor-prices` показывает:

- список наших товаров;
- текущую цену и поле новой цены;
- среднюю/минимальную/максимальную цену конкурентов за последние 7 дней;
- детализацию конкурентов по выбранному товару;
- live-цены через агент;
- расчет маржинальности по текущей и новой цене.

API:

- `/api/competitor-prices/summary?cabinet_id=hld`;
- `/api/competitor-prices/detail?cabinet_id=hld&nm_id=...`;
- `/api/competitor-prices/live-prices?nm=123;456`;
- `/api/competitor-prices/costs?cabinet_id=hld`.

---

## Возможные проблемы

| Проблема | Причина | Решение |
|---|---|---|
| `агент недоступен` | `wb_price_agent.py` не запущен или Tailscale не видит ноут | Запустить агент, проверить `/health` |
| `CDP недоступен` | Fallback-туннель упал или Chrome закрыт | Проверить агент; fallback чинить только если нужен |
| `0 товаров` | WB-вкладка не открыта | Открыть любую страницу wildberries.ru в Chrome |
| `HTTP 498` | Нет WB-кук в браузере | Зайти на WB в Chrome, подождать сессии |
| `SSH timeout` | Fallback: Tailscale/SSH не работает | Для основного режима проверить агент; для fallback проверить `tailscale status` |
| `Permission denied` | Fallback: SSH-ключ не добавлен | Нужно только для CDP fallback |
| `0 nmId найдено` | В SKU нет реальных WB-артикулов | Обновить SKU-файл кабинета с nmId |
