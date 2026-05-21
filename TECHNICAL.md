# Wildberries Google Sheets Sync: Technical Notes

Этот файл - рабочая техническая памятка по проекту. Он нужен, чтобы быстро понимать архитектуру, точки входа, потоки данных и осторожные места перед будущими правками.

Подробное вводное описание проекта, источников данных и типового процесса синхронизации лежит в `docs/PROJECT_OVERVIEW.md`.

## Назначение

Проект синхронизирует данные Wildberries в Google Sheets:

- продажи и финансовый отчет WB;
- заказы WB;
- рекламную статистику;
- воронку продаж;
- агрегированный `daily_pnl`;
- вспомогательные дашборды и сводные листы в Google Sheets.

Есть два основных интерфейса:

- CLI: `python -m wb_gsheets.main` или `python run_sync.py`;
- веб-страница: `python web_app.py`, затем открыть `http://127.0.0.1:8765`.

## Стек

- Python 3.13 в Dockerfile.
- HTTP-клиент: `requests`.
- Google Sheets API: `google-api-python-client`, `google-auth`.
- Конфиг из `.env`: `python-dotenv`.
- Веб-интерфейс на стандартной библиотеке: `http.server.ThreadingHTTPServer`, Server-Sent Events для логов.

## Структура

- `src/wb_gsheets/config.py` - загрузка настроек из окружения.
- `src/wb_gsheets/main.py` - основной сценарий синхронизации WB -> Google Sheets.
- `src/wb_gsheets/wb_client.py` - клиент Wildberries API.
- `src/wb_gsheets/google_sheets.py` - клиент Google Sheets, создание листов, replace/upsert.
- `src/wb_gsheets/transform.py` - фильтрация, маппинг, агрегация PnL, подготовка строк для Sheets.
- `src/wb_gsheets/utils.py` - Decimal, даты, chunk/window helpers.
- `web_app.py` - локальный веб-интерфейс запуска синхронизации с live-логом.
- `run_sync.py` - тонкая обертка над `wb_gsheets.main`.
- `scripts/*.py` - пересборка дашбордов, сводных и формульных листов.
- `scripts/*.sh` - локальный запуск/остановка web UI, автозапуск, деплой.
- `Dockerfile`, `docker-compose.yml` - контейнерный запуск web UI.

## Конфигурация

Обязательные переменные:

- `GOOGLE_SERVICE_ACCOUNT_FILE` - путь к JSON сервисного аккаунта.
- `GOOGLE_SPREADSHEET_ID` - id Google Spreadsheet.
- `DEFAULT_DATE_FROM`, `DEFAULT_DATE_TO` - даты по умолчанию для CLI.
- `WB_FINANCE_TOKEN` или общий `WB_API_TOKEN`.
- `WB_ADV_TOKEN` или общий `WB_API_TOKEN`.

Опциональные переменные:

- `GOOGLE_RAW_SALES_SHEET`, по умолчанию `raw_sales`.
- `GOOGLE_RAW_ORDERS_SHEET`, по умолчанию `raw_orders`.
- `GOOGLE_RAW_ADS_SHEET`, по умолчанию `raw_ads`.
- `GOOGLE_DAILY_PNL_SHEET`, по умолчанию `daily_pnl`.
- `GOOGLE_COGS_SHEET`, по умолчанию `SKU`.
- `GOOGLE_FUNNEL_ANALYTICS_SHEET`, по умолчанию `funnel_analytics`.
- `ARTICLE_FILTER_TYPE`, по умолчанию `nmId`, допустимо `nmId` или `vendorCode`.
- `ARTICLE_FILTER_VALUES`, CSV-список артикулов. Если пусто, фильтры берутся из листа `SKU`.
- `WEB_APP_HOST`, по умолчанию `127.0.0.1`.
- `WEB_APP_PORT`, по умолчанию `8765`.
- `WEB_PUBLIC_PORT` для docker-compose, по умолчанию используется в compose как порт `80`.

## Основной поток синхронизации

`src/wb_gsheets/main.py`:

1. Читает настройки и даты.
2. Создает `WildberriesClient` и `GoogleSheetsClient`.
3. Читает лист `SKU`/COGS из Google Sheets.
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
- запросы ретраятся при `429`, сетевых ошибках, timeout и SSL-ошибках;
- рекламная статистика грузится чанками по 50 кампаний и окнами по 31 день;
- между запросами рекламы есть `time.sleep(20)`, поэтому загрузка рекламы может быть долгой;
- воронка грузится чанками по 20 `nmId`; при `400` чанк дробится на одиночные `nmId`.

## Google Sheets

`GoogleSheetsClient` умеет:

- создавать лист, если его нет: `ensure_sheet`;
- полностью пересоздавать лист: `recreate_sheet`;
- читать значения: `get_values`;
- полностью заменять лист: `replace_sheet`;
- добавлять/обновлять строки по ключам: `upsert_sheet`;
- выполнять batchUpdate: `batch_update`.

Особенности `upsert_sheet`:

- по умолчанию дописывает только новые строки;
- если `update_existing=True`, обновляет найденные строки по ключам непустыми новыми значениями;
- если `allow_new_columns=True`, объединяет старый и новый header;
- если лист пустой, сначала пишет header;
- если payload пустой или содержит только `[[]]`, фактически ничего не пишет.

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

- supplier article: `Артикул поставщика` или `НАШ`;
- WB article: `Артикул WB` или `nmId`.

Для себестоимости:

- новый формат: `article`, `article_type`, `cogs`;
- альтернативно: `SKU`/`sku` + `себестоимость`/`cost_price`;
- альтернативно: `НАШ`/`Артикул поставщика` + `себестоимость`.

Для маппинга `nmId -> SKU`:

- `SKU`/`sku` + `nmId`/`nm_id`;
- или `НАШ`/`Артикул поставщика` + `Артикул WB`.

## Web UI

`web_app.py`:

- отдает HTML на `/`;
- запускает синхронизацию на `/stream`;
- передает логи через Server-Sent Events;
- внутри запускает `python -m wb_gsheets.main` с `PYTHONPATH=src`;
- имеет быстрые диапазоны: вчера, сегодня, последние 7 дней;
- умеет выставить "воронка за последний месяц": включает `skip_ads`, выключает `skip_funnel`.

Локальные helper-скрипты:

- `scripts/start_web_ui.sh` - запускает web UI из `.venv`, пишет PID и лог в корень, открывает браузер через `open`.
- `scripts/stop_web_ui.sh` - останавливает процесс из PID-файла.
- `scripts/install_autostart.sh` - macOS LaunchAgent.
- `scripts/uninstall_autostart.sh` - удаление автозапуска.

## Дашборды и служебные скрипты

- `scripts/build_dashboard.py` пересоздает `dashboard` и строит KPI/таблицы на основе `daily_pnl`.
- `scripts/build_pivot_sheets.py` пересоздает `свод_артикулы` и `свод_дни`.
- `scripts/build_management_viz.py` пересоздает `management_viz`; логика сейчас жестко заточена на май 2026.
- `scripts/rebuild_formula_only.py` и `scripts/fix_formula_sheets.py` создают формульные листы `formula_pnl`, `dashboard_formula`, `keys`; даты там также жестко заданы на `2026-05-01`..`2026-05-02`.

Важно: часть формул в этих скриптах зависит от русской локали Google Sheets и использует `;` как разделитель аргументов.

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
- `WEB_APP_HOST=0.0.0.0`;
- `WEB_APP_PORT=${WEB_PUBLIC_PORT:-80}`.

`scripts/deploy_server.sh`:

- собирает tar-архив проекта;
- переписывает `GOOGLE_SERVICE_ACCOUNT_FILE` на `/run/secrets/google-service-account.json`;
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

Docker:

```bash
docker compose up -d --build
```

Дашборды:

```bash
PYTHONPATH=src python scripts/build_dashboard.py
PYTHONPATH=src python scripts/build_pivot_sheets.py
PYTHONPATH=src python scripts/build_management_viz.py
```

## Осторожные места

- В корне проекта нет `.git`, поэтому перед крупными изменениями лучше вручную проверять, какие файлы меняются.
- В проекте есть `secrets/google-service-account.json`; не печатать и не коммитить секреты.
- `__pycache__` сейчас лежат в дереве проекта, но не являются исходниками.
- API рекламы медленный из-за лимитов и `sleep(20)`.
- `raw_sales` и `raw_orders` пишутся append/upsert-логикой; повторные запуски не должны дублировать строки при корректных ключах.
- `daily_pnl` по умолчанию только добавляет новые ключи. Для пересчета уже существующих дат может понадобиться очистка листа или изменение режима записи.
- Некоторые дашбордные скрипты содержат жестко заданные даты мая 2026 и ссылки на конкретные имена листов.
- Формулы в Google Sheets завязаны на текущие названия колонок и локаль таблицы.
