# Wildberries Analytics Sync: Technical Notes

Этот файл - рабочая техническая памятка по проекту. Он нужен, чтобы быстро понимать архитектуру, точки входа, потоки данных и осторожные места перед будущими правками.

Подробное вводное описание проекта, источников данных и типового процесса синхронизации лежит в `docs/PROJECT_OVERVIEW.md`.

## Назначение

Проект синхронизирует данные Wildberries и Ozon в локальные SQLite-базы и показывает аналитику через web UI. Внешние таблицы не используются.

Проект собирает:

- продажи и финансовый отчет WB;
- заказы WB;
- рекламную статистику;
- воронку продаж;
- агрегированный `daily_pnl`;
- расчетное планирование заказов, выкупов, выручки и остатков;
- управление рекламными кампаниями WB через биддер start/pause;
- контроль цен наших товаров и конкурентов;
- задачник для операционных работ;
- вспомогательные дашборды и сводные таблицы в SQLite/web UI.

Есть два основных интерфейса:

- CLI: `python -m wb_gsheets.main` или `python run_sync.py`;
- веб-страница: `python web_app.py`, затем открыть `http://127.0.0.1:8765`.

В мультикабинетном режиме данные кабинетов лежат в `data/cabs/{cabinet_id}.db`. Общие платформенные настройки, выбранный кабинет и часть служебных таблиц обслуживаются `web_app.py`.

## Стек

- Python 3.13 в Dockerfile.
- HTTP-клиент: `requests`.
- Конфиг из `.env`: `python-dotenv`.
- Веб-интерфейс на стандартной библиотеке: `http.server.ThreadingHTTPServer`, Server-Sent Events для логов.

## Структура

- `src/wb_gsheets/config.py` - загрузка настроек из окружения.
- `src/wb_gsheets/main.py` - основной сценарий синхронизации WB -> SQLite.
- `src/wb_gsheets/wb_client.py` - клиент Wildberries API.
- `src/wb_gsheets/transform.py` - фильтрация, маппинг, агрегация PnL, подготовка строк для SQLite.
- `src/wb_gsheets/utils.py` - Decimal, даты, chunk/window helpers.
- `web_app.py` - локальный веб-интерфейс запуска синхронизации, аналитики и планирования.
- `run_sync.py` - тонкая обертка над `wb_gsheets.main`.
- `scripts/ozon_sync.py` - Ozon-синхронизация и Ozon Performance API.
- `scripts/wb_price_monitor.py` - серверный сбор цен WB.
- `scripts/wb_price_agent.py` - агент на ноуте для live-цен WB через Chrome/CDP.
- `scripts/wb_competitor_finder.py` - поиск конкурентов через MPSTATS.
- `scripts/*.py` - остальные вспомогательные скрипты проекта.
- `scripts/*.sh` - локальный запуск/остановка web UI, автозапуск, деплой.
- `Dockerfile`, `docker-compose.yml` - контейнерный запуск web UI.

## Конфигурация

Обязательные переменные:

- `DEFAULT_DATE_FROM`, `DEFAULT_DATE_TO` - даты по умолчанию для CLI.
- `WB_FINANCE_TOKEN` или общий `WB_API_TOKEN`.
- `WB_ADV_TOKEN` или общий `WB_API_TOKEN`.

Опциональные переменные:

- `RAW_SALES_TABLE`, по умолчанию `raw_sales`.
- `RAW_ORDERS_TABLE`, по умолчанию `raw_orders`.
- `RAW_ADS_TABLE`, по умолчанию `raw_ads`.
- `DAILY_PNL_TABLE`, по умолчанию `daily_pnl`.
- `SKU_TABLE`, по умолчанию `SKU`.
- `FUNNEL_ANALYTICS_TABLE`, по умолчанию `funnel_analytics`.
- `ARTICLE_FILTER_TYPE`, по умолчанию `nmId`, допустимо `nmId` или `vendorCode`.
- `ARTICLE_FILTER_VALUES`, CSV-список артикулов. Если пусто, фильтры берутся из листа `SKU`.
- `WEB_APP_HOST`, по умолчанию `127.0.0.1`.
- `WEB_APP_PORT`, по умолчанию `8765`.
- `WEB_PUBLIC_PORT` для docker-compose, по умолчанию используется в compose как порт `80`.

## Основной поток синхронизации

`src/wb_gsheets/main.py`:

1. Читает настройки и даты.
2. Создает `WildberriesClient` и `SQLiteStore`.
3. Читает `SKU`/COGS только из SQLite.
4. Получает фильтры артикулов:
   - `ARTICLE_FILTER_VALUES`, если заданы;
   - иначе значения из `SKU` через `extract_filter_values`.
5. Загружает продажи через finance API:
   - `fetch_sales_details(date_from, date_to, period="daily")`;
   - фильтрует через `filter_sales_rows`.
6. Загружает заказы через statistics API:
   - `fetch_orders(date_from)`;
   - фильтрует по артикулам и диапазону дат через `filter_orders_rows`.
7. Если не указан `--skip-ads`, подбирает рекламные кампании по `nmId`, грузит статистику и разворачивает ее в строки.
8. Считает `daily_pnl` через `aggregate_daily_pnl`.
9. Пишет листы:
   - `raw_sales`, key `rrdId`;
   - `raw_orders`, key `srid`;
   - `raw_ads`, key `date`, `advertId`, `appType`, `nmId`;
   - `daily_pnl`, key `date`, `article_type`, `article`.
10. Если не указан `--skip-funnel`, грузит воронку и upsert-ит `funnel_analytics` с обновлением существующих строк и расширением колонок.

## Wildberries API

`WildberriesClient` использует четыре базовых URL:

- finance: `https://finance-api.wildberries.ru`;
- ads: `https://advert-api.wildberries.ru`;
- statistics: `https://statistics-api.wildberries.ru`;
- analytics: `https://seller-analytics-api.wildberries.ru`.

Важные детали:

- finance token используется для finance/statistics/analytics;
- adv token используется для advertising API;
- запросы ретраятся при `429`, временных `500/502/503/504`, сетевых ошибках, timeout и SSL-ошибках;
- рекламная статистика грузится чанками по 50 кампаний и окнами по 31 день;
- между запросами рекламы есть `time.sleep(20)`, поэтому загрузка рекламы может быть долгой;
- воронка грузится чанками по 20 `nmId`; при `400` чанк дробится на одиночные `nmId`.

## Daily PnL

`aggregate_daily_pnl` группирует продажи по `(date, article)` и добавляет рекламу по `(date, article)`.

Ключевые поля результата:

- `orders_amount` - количество с учетом продаж/возвратов;
- `sales_amount` - сумма продаж с СПП, по `retailAmount`;
- `sales_without_spp` - по `retailPriceWithDisc * quantity`;
- `wb_commission`, `acquiring_fee`, `storage_fee`, `acceptance_fee`;
- `penalties`, `deductions`, `additional_payments`;
- `delivery_fee`;
- `ad_spend`;
- `cogs_amount`;
- `net_profit`;
- `margin_pct`.

Продажи имеют знак по `docTypeName`:

- `Продажа` -> `+1`;
- `Возврат` -> `-1`;
- остальные типы учитываются только если есть нефинансовые/операционные начисления.

Если `deliveryRub` отсутствует или равен нулю, логистика считается остатком до `forPay`.

## Лист SKU / COGS

Код поддерживает несколько вариантов заголовков.

Для фильтров заказов:

- supplier article: `Артикул поставщика`;
- WB article: `Артикул WB` или `nmId`.

Для себестоимости:

- новый формат: `article`, `article_type`, `cogs`;
- альтернативно: `SKU`/`sku` + `себестоимость`/`cost_price`;
- альтернативно: `Артикул поставщика` + `себестоимость`.

Для маппинга `nmId -> SKU`:

- `SKU`/`sku` + `nmId`/`nm_id`;
- или `Артикул поставщика` + `Артикул WB`.

## Web UI

`web_app.py`:

- отдает HTML на `/`;
- запускает синхронизацию на `/stream`;
- передает логи через Server-Sent Events;
- внутри запускает `python -m wb_gsheets.main` с `PYTHONPATH=src`;
- имеет быстрые диапазоны: вчера, сегодня, последние 7 дней;
- умеет выставить "воронка за последний месяц": включает `skip_ads`, выключает `skip_funnel`;
- показывает расчетную страницу `/analytics/planning`;
- отдает JSON планирования через `/api/analytics/planning`;
- отдает Excel-выгрузку планирования через `/api/analytics/planning/export`.
- показывает биддер на `/ads/bidder`;
- показывает контроль цен на `/analytics/competitor-prices`;
- показывает задачник на `/tasks`.

## Планирование

Планирование живет в `web_app.py` и не пишет отдельные таблицы в SQLite. Это расчетная витрина: API каждый раз собирает данные из существующих таблиц и возвращает итоговый блок плюс строки SKU.

Основные функции:

- `_fetch_planning` - собирает данные, фильтры, базовые периоды и строки прогноза.
- `_simulate_planning_orders` - ограничивает прогноз доступным остатком, учитывает возврат невыкупленных товаров и приход складского остатка.
- `_calc_elasticity` / `_calc_demand_model` - оценивают влияние цены и рекламы на спрос по истории заказов.

Входные таблицы:

- `SKU` - товары, предмет, стратегия, категория, себестоимость, комиссия WB и поле `склад`.
- `raw_stocks` - текущий остаток WB по `quantity`.
- `buyout_order_day` - скорость заказов и сумма заказов.
- `funnel_analytics` - WB-выкуп, отмены, выкупленная выручка и ABC-категория.
- `raw_orders` - цены, СПП и дневные заказы для эластичности.
- `raw_ads` - рекламные расходы для ДРР и рекламной эластичности.
- `funnel_impressions_upload` - опциональная таблица с показами/открытиями/корзинами для CTR/CR1/CR2.

Базовые периоды:

- скорость заказов и средний чек: 7 полных дней до последней даты в `buyout_order_day`; последняя дата исключается;
- `% выкупа WB`: с 44-го по 14-й день до последней даты, чтобы не брать незавершенные свежие выкупы;
- ДРР и рекламная база: 14 дней, заканчивая за 2 дня до последней даты;
- эластичность: до 60 дней истории `raw_orders` и `raw_ads`.

Важные поля строки планирования:

- `stock` - текущий остаток WB.
- `warehouse_stock` - складской остаток из `SKU.склад`, приходит в расчет через `transit_days`.
- `base_speed` - базовая скорость заказов в день.
- `buyout_percent` - WB-выкуп: `buyoutCount / (buyoutCount + cancelCount)`.
- `average_check` - средний чек заказа за базовый период.
- `elasticity` - ценовая эластичность: насколько меняется спрос при изменении цены.
- `ads_elasticity` - рекламная эластичность: насколько меняется спрос при изменении рекламы.
- `base_drr_pct`, `base_ctr`, `base_cr1`, `base_cr2` - базовые рекламные и воронковые показатели.
- `potential_orders` - спрос без ограничения остатком.
- `forecast_orders` - прогноз заказов с учетом остатка, возвратов и склада.
- `forecast_buyouts` - ожидаемые выкупы.
- `forecast_revenue` - ожидаемая выручка.
- `lost_orders`, `lost_revenue` - недопродажи из-за нехватки остатка.
- `stockout_date` - дата, когда товар закончится.

Сценарии изменения цены, ДРР и конверсий сохраняются в браузере через `localStorage`. Это удобно для работы на странице, но не является серверным хранилищем и не переносится между браузерами.

## WB-биддер

Биддер живет в `web_app.py` и использует кабинетный WB advertising token. Это не управление ставками, а управление состоянием кампаний: `active` или `paused`.

Основные функции:

- `_ensure_ad_bidding_tables` - создает таблицы настроек, кэша, логов, действий и lock.
- `_fetch_ad_articles` - ищет товары для выбора на странице.
- `_fetch_ad_campaigns` - собирает кампании по `nmid`, метрики из `raw_ads`, настройки и последние действия.
- `_save_ad_campaign_setting` - сохраняет бюджет, автостоп и интервал паузы по кампании.
- `_run_ad_bidding_once_for_cabinet` - один проход исполнителя по кабинету.
- `_run_ad_bidding_once_all` - проход по всем кабинетам с WB-рекламным токеном.
- `_ad_executor_loop` / `_start_ad_executor` - daemon-thread, который просыпается каждый час.

Таблицы:

- `ad_bidding_campaign_settings` - основной источник настроек кампаний: `enabled`, `daily_budget`, `auto_pause`, `today_spend`, `pause_start`, `pause_end`.
- `ad_campaign_cache` - кэш кампаний WB: название, статус, тип, связанные `nmId`.
- `ad_bidding_actions` - история отправленных действий start/pause/noop/error.
- `ad_bidding_state` - последнее желаемое состояние и последняя ошибка по `advert_id`.
- `ad_bidding_log` - журнал для интерфейса.
- `ad_bidding_executor_lock` - защита от параллельного исполнения.
- `ad_bidding_global_settings`, `ad_bidding_rules`, `ad_bidding_rule_campaigns` - совместимый слой недельных правил; текущий экран в основном работает с `ad_bidding_campaign_settings`.

Исполнитель:

1. Берет кампании, где `enabled=1` или `auto_pause=1`.
2. При необходимости синхронизирует расход за сегодня через `adv/v3/fullstats`; обычный интервал - не чаще 1 раза в час.
3. Если включен автостоп и расход достиг дневного бюджета, ставит желаемое состояние `paused`.
4. Если активен интервал паузы, ставит `paused`.
5. Если расписание включено, пауза не активна и бюджет не превышен, ставит `active`.
6. Не отправляет повторный start/pause, если `ad_bidding_state.last_desired_state` уже совпадает.
7. Пишет действие и человекочитаемый лог.

WB API:

- `GET /adv/v1/promotion/count` - список кампаний.
- `GET /api/advert/v2/adverts` - детали кампаний.
- `GET /adv/v3/fullstats` - расход за день.
- `GET /adv/v0/start?id=...` - запуск кампании.
- `GET /adv/v0/pause?id=...` - пауза кампании.

HTTP API web UI:

- `GET /api/ads/articles`.
- `GET /api/ads/campaigns`.
- `GET /api/ads/logs`.
- `GET/POST /api/ads/settings`.
- `POST /api/ads/campaign-settings`.
- `GET/POST /api/ads/rules`.
- `POST /api/ads/rules/{id}/toggle`.
- `POST /api/ads/executor/run-once`.

## Контроль цен

Цены WB не собираются из датацентра напрямую: WB internal endpoint требует браузерные cookies. Текущая схема такая:

1. На ноуте запущен `scripts/wb_price_agent.py`.
2. Агент слушает `http://100.65.13.99:8100`, сам поднимает Chrome на CDP-порту `9224` и выполняет `fetch()` в контексте страницы WB.
3. Серверный `scripts/wb_price_monitor.py` сначала вызывает агент `GET /prices?nm=...`.
4. Если агент недоступен, монитор использует fallback: SSH/CDP-туннель на `LOCAL_CDP`.
5. Результат пишется в `data/wb_prices.db`.

Скрипты:

- `scripts/wb_price_agent.py` - ноут, HTTP `/health` и `/prices`.
- `scripts/wb_price_monitor.py` - сервер, батчи до 50 `nmId`, пауза между батчами, `--cabinet`, `--dry-run`, `--test`, `--cdp-port`.
- `scripts/wb_competitor_finder.py` - MPSTATS-поиск конкурентов для `hld`; использует `raw_stocks`, `raw_sales` и при наличии `wb_cards`.

Таблицы `data/wb_prices.db`:

- `price_history` - история цен наших товаров: `cabinet_id`, `nm_id`, `price`, `price_basic`, `rating`, `feedbacks`, `stock`.
- `competitor_products` - связка `our_nm_id -> comp_nm_id`, название, бренд, продавец, subject и URL.
- `competitor_prices` - история цен конкурентов: `our_nm_id`, `comp_nm_id`, `price`, `price_basic`, `stock`, `rating`, `feedbacks`.

Web UI:

- `/analytics/competitor-prices` - сводка по нашим товарам и конкурентам.
- `/api/competitor-prices/summary` - агрегаты конкурентов за последние 7 дней.
- `/api/competitor-prices/detail` - список конкурентов по товару.
- `/api/competitor-prices/live-prices` - live-запрос цен через агента.
- `/api/competitor-prices/costs` - себестоимость и экономические параметры из данных кабинета.

## Задачник

Задачник живет в `web_app.py`, страница `/tasks`.

Таблица `tasks`:

- `id`;
- `title`;
- `why`;
- `result`;
- `due_date`;
- `status`: `backlog`, `in_progress`, `review`, `done`;
- `created_at`, `updated_at`.

API:

- `GET /api/tasks`.
- `POST /api/tasks`.
- `POST /api/tasks/update`.
- `POST /api/tasks/delete`.

Локальные helper-скрипты:

- `scripts/start_web_ui.sh` - запускает web UI из `.venv`, пишет PID и лог в корень, открывает браузер через `open`.
- `scripts/stop_web_ui.sh` - останавливает процесс из PID-файла.
- `scripts/install_autostart.sh` - macOS LaunchAgent.
- `scripts/uninstall_autostart.sh` - удаление автозапуска.

## Docker / deploy

`Dockerfile`:

- базовый образ `python:3.13-slim`;
- устанавливает зависимости из `requirements.txt`;
- копирует `src`, `web_app.py`, `run_sync.py`;
- запускает `python web_app.py`;
- выставляет `PYTHONPATH=/app/src`.

`docker-compose.yml`:

- сервис `wb-sync-web`;
- `network_mode: host`;
- читает `.env`;
- монтирует `./secrets` в `/run/secrets:ro`;
- на сервере `WEB_APP_HOST=127.0.0.1`;
- на сервере `WEB_APP_PORT=8765`;
- внешний доступ идет через Caddy, который проксирует `ewb.prprod.ru` на `127.0.0.1:8765`.

`scripts/deploy_server.sh`:

- собирает tar-архив проекта;
- копирует архив по SSH;
- на сервере запускает `docker compose up -d --build`.

## Команды

Установка локально:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Запуск CLI:

```bash
PYTHONPATH=src python -m wb_gsheets.main --date-from 2026-05-01 --date-to 2026-05-07
```

Без рекламы:

```bash
PYTHONPATH=src python -m wb_gsheets.main --date-from 2026-05-01 --date-to 2026-05-07 --skip-ads
```

Без воронки:

```bash
PYTHONPATH=src python -m wb_gsheets.main --date-from 2026-05-01 --date-to 2026-05-07 --skip-funnel
```

Web UI:

```bash
source .venv/bin/activate
PYTHONPATH=src python web_app.py
```

Мониторинг цен:

```bash
python3 scripts/wb_price_monitor.py --test
python3 scripts/wb_price_monitor.py --cabinet hld
```

Поиск конкурентов:

```bash
python3 scripts/wb_competitor_finder.py --test
```

Docker:

```bash
docker compose up -d --build
```

## Осторожные места

- В корне проекта нет `.git`, поэтому перед крупными изменениями лучше вручную проверять, какие файлы меняются.
- Реальные API-ключи не должны храниться в проекте. Каталог `secrets/` игнорируется git и подходит только для локальных временных секретов.
- `__pycache__` сейчас лежат в дереве проекта, но не являются исходниками.
- API рекламы медленный из-за лимитов и `sleep(20)`.
- `raw_sales` и `raw_orders` пишутся append/upsert-логикой; повторные запуски не должны дублировать строки при корректных ключах.
- `daily_pnl` по умолчанию только добавляет новые ключи. Для пересчета уже существующих дат может понадобиться очистка листа или изменение режима записи.
- Биддер делает реальные запросы start/pause в WB. Перед включением автостопа проверять `daily_budget`, `pause_start`, `pause_end` и актуальность рекламного токена.
- `ad_bidding_state` подавляет повторные действия. Если руками изменить состояние кампании в кабинете WB, кэш/состояние может временно расходиться до следующего refresh или ручного прогона.
- Live-контроль цен зависит от ноутбука, Tailscale, Chrome-профиля и cookies WB. При `agent_unavailable` страница может показать исторические цены, но не живые.
- `wb_price_monitor.py` пишет в отдельную базу `data/wb_prices.db`, а не в кабинетные `data/cabs/*.db`.
