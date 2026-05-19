from __future__ import annotations

import argparse

from .analytics import build_analytics_tables
from .config import load_settings
from .google_sheets import GoogleSheetsClient
from .sqlite_store import SQLiteStore
from .transform import (
    ads_to_sheet_rows,
    aggregate_daily_pnl,
    build_buyout_order_day_rows,
    build_cogs_map,
    build_nm_mapping,
    extract_orders_filters,
    extract_filter_values,
    filter_orders_rows,
    filter_sales_rows,
    flatten_ads_rows,
    funnel_to_sheet_rows,
    orders_to_sheet_rows,
    pnl_to_sheet_rows,
    sales_to_sheet_rows,
    sheet_values_to_dicts,
    stocks_to_sheet_rows,
)
from .wb_client import WildberriesClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Wildberries data to SQLite")
    parser.add_argument("--date-from", dest="date_from")
    parser.add_argument("--date-to", dest="date_to")
    parser.add_argument("--skip-ads", action="store_true")
    parser.add_argument("--skip-funnel", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = load_settings()

    date_from = args.date_from or settings.default_date_from
    date_to = args.date_to or settings.default_date_to
    print(f"[INFO] Синхронизация: период {date_from}..{date_to}, реклама={'нет' if args.skip_ads else 'да'}, воронка={'нет' if args.skip_funnel else 'да'}")

    wb_client = WildberriesClient(
        finance_token=settings.wb_finance_token,
        adv_token=settings.wb_adv_token,
    )
    store = SQLiteStore(settings.sqlite_db_path)

    cogs_values = store.get_values(settings.cogs_sheet)
    if not cogs_values and settings.google_service_account_file and settings.google_spreadsheet_id:
        sheets_client = GoogleSheetsClient(
            service_account_file=settings.google_service_account_file,
            spreadsheet_id=settings.google_spreadsheet_id,
        )
        cogs_values = sheets_client.get_values(settings.cogs_sheet)
        if cogs_values:
            store.replace_table(settings.cogs_sheet, cogs_values)
            print(f"[INFO] Кэширован лист {settings.cogs_sheet} из Google Sheets в SQLite")

    nm_mapping = build_nm_mapping(cogs_values, article_filter_type=settings.article_filter_type)
    article_filter_values = settings.article_filter_values or extract_filter_values(
        cogs_values,
        article_filter_type=settings.article_filter_type,
    )
    orders_supplier_articles, orders_nm_ids = extract_orders_filters(cogs_values)

    print("[INFO] Загружаем продажи")
    sales_rows = wb_client.fetch_sales_details(date_from=date_from, date_to=date_to, period="daily")
    print(f"[INFO] Продажи: получено строк {len(sales_rows)}")
    if article_filter_values or nm_mapping:
        filtered_sales_rows = filter_sales_rows(
            sales_rows,
            article_filter_type=settings.article_filter_type,
            article_filter_values=article_filter_values,
            nm_id_filter_values=set(nm_mapping.keys()),
        )
    else:
        filtered_sales_rows = sales_rows
    print(f"[INFO] Продажи: после фильтра строк {len(filtered_sales_rows)}")

    print("[INFO] Загружаем заказы")
    orders_rows = wb_client.fetch_orders(date_from=date_from)
    print(f"[INFO] Заказы: получено строк {len(orders_rows)}")
    if orders_supplier_articles or orders_nm_ids:
        filtered_orders_rows = filter_orders_rows(
            orders_rows,
            supplier_articles=orders_supplier_articles,
            nm_ids=orders_nm_ids,
            date_from=date_from,
            date_to=date_to,
        )
    else:
        filtered_orders_rows = orders_rows
    print(f"[INFO] Заказы: после фильтра строк {len(filtered_orders_rows)}")

    try:
        print("[INFO] Загружаем остатки")
        stock_rows = wb_client.fetch_stocks()
        if orders_supplier_articles or orders_nm_ids:
            allowed_nm_ids = {str(value).strip() for value in orders_nm_ids if str(value).strip()}
            filtered_stock_rows = [
                row for row in stock_rows
                if str(row.get("nmId") or "").strip() in allowed_nm_ids
            ]
        else:
            filtered_stock_rows = stock_rows
    except Exception as exc:
        print(f"[WARN] Не удалось загрузить остатки: {exc}")
        filtered_stock_rows = []

    ads_rows = []
    if not args.skip_ads:
        try:
            relevant_nm_ids = [
                int(nm_id)
                for nm_id in nm_mapping.keys()
                if str(nm_id).isdigit()
            ]
            print(f"[INFO] Реклама: ищем кампании по {len(relevant_nm_ids)} nmId")
            advert_ids = wb_client.fetch_relevant_campaign_ids(relevant_nm_ids)
            print(f"[INFO] Реклама: найдено кампаний {len(advert_ids)}")
            ads_payload = wb_client.fetch_campaign_stats(
                advert_ids=advert_ids,
                date_from=date_from,
                date_to=date_to,
            )
            ads_rows = flatten_ads_rows(ads_payload)
            print(f"[INFO] Реклама: получено строк {len(ads_rows)}")
        except Exception as exc:
            print(f"[WARN] Не удалось загрузить рекламу: {exc}")
            ads_rows = []

    print("[INFO] Собираем производные таблицы")
    cogs_map = build_cogs_map(
        sheet_values=cogs_values,
        article_filter_type=settings.article_filter_type,
    )
    daily_pnl = aggregate_daily_pnl(
        sales_rows=filtered_sales_rows,
        ads_rows=ads_rows,
        cogs_map=cogs_map,
        article_filter_type=settings.article_filter_type,
        nm_mapping=nm_mapping,
    )

    analytics_tables = build_analytics_tables(
        sales_rows=filtered_sales_rows,
        ads_rows=ads_rows,
        cogs_map=cogs_map,
        article_filter_type=settings.article_filter_type,
        nm_mapping=nm_mapping,
        date_from=date_from,
        date_to=date_to,
    )

    store.upsert_table(
        settings.raw_sales_sheet,
        sales_to_sheet_rows(filtered_sales_rows),
        key_columns=("rrdId",),
    )
    store.upsert_table(
        settings.raw_orders_sheet,
        orders_to_sheet_rows(filtered_orders_rows),
        key_columns=("srid",),
    )
    store.upsert_table(
        settings.raw_ads_sheet,
        ads_to_sheet_rows(ads_rows),
        key_columns=("date", "advertId", "appType", "nmId"),
    )
    print("[INFO] Сохраняем исходные таблицы в SQLite")
    store.replace_table("raw_stocks", stocks_to_sheet_rows(filtered_stock_rows))
    print(f"[INFO] Остатки: загружено строк {len(filtered_stock_rows)}")
    store.upsert_table(
        settings.daily_pnl_sheet,
        pnl_to_sheet_rows(daily_pnl),
        key_columns=("date", "article_type", "article"),
        update_existing=True,
    )

    stored_sales_rows = sheet_values_to_dicts(store.get_values(settings.raw_sales_sheet))
    if article_filter_values or nm_mapping:
        stored_sales_rows = filter_sales_rows(
            stored_sales_rows,
            article_filter_type=settings.article_filter_type,
            article_filter_values=article_filter_values,
            nm_id_filter_values=set(nm_mapping.keys()),
        )

    stored_orders_rows = sheet_values_to_dicts(store.get_values(settings.raw_orders_sheet))
    if orders_supplier_articles or orders_nm_ids:
        stored_orders_rows = filter_orders_rows(
            stored_orders_rows,
            supplier_articles=orders_supplier_articles,
            nm_ids=orders_nm_ids,
            date_from=date_from,
            date_to=date_to,
        )

    # Fetch funnel before building buyout_order_day so funnel prices populate orders_sum/orders_qty.
    funnel_data: list[dict] = []
    if not args.skip_funnel:
        try:
            target_nm_ids = [
                int(nm_id)
                for nm_id in nm_mapping.keys()
                if str(nm_id).isdigit()
            ]
            if not target_nm_ids:
                target_nm_ids = sorted({
                    int(nm_id)
                    for nm_id in orders_nm_ids
                    if str(nm_id).isdigit()
                })
            if target_nm_ids:
                print(f"[INFO] Воронка: загружаем по {len(target_nm_ids)} nmId")
                funnel_data = wb_client.fetch_funnel_history(date_from, date_to, nm_ids=target_nm_ids)
                funnel_rows = funnel_to_sheet_rows(funnel_data)
                store.upsert_table(
                    settings.funnel_analytics_sheet,
                    funnel_rows,
                    key_columns=("date", "nmId"),
                    update_existing=True,
                    allow_new_columns=True,
                )
                print(f"[INFO] Воронка: загружено строк {max(0, len(funnel_rows) - 1)}")
            else:
                print("[WARN] Воронка: нет nmId для запроса (проверь лист SKU / маппинг nmId)")
        except Exception as exc:
            print(f"[WARN] Не удалось загрузить воронку: {exc}")

    # Supplement funnel_data with stored funnel_analytics for any (date, nmId) the API didn't return.
    # buyout_order_day is rebuilt as a full table below, so stored funnel rows from every date
    # must be included; otherwise order sums/counts disappear outside the current sync window.
    api_funnel_keys: set[tuple[str, str]] = {
        (str(h.get("date", "")), str((item.get("product") or {}).get("nmId", "")))
        for item in funnel_data
        for h in (item.get("history") or [])
    }
    stored_funnel_rows = sheet_values_to_dicts(store.get_values(settings.funnel_analytics_sheet))
    allowed_funnel_nm_ids = {
        str(nm_id).strip()
        for nm_id in (set(nm_mapping.keys()) or orders_nm_ids)
        if str(nm_id).strip()
    }
    extra_by_nm: dict[str, dict] = {}
    for row in stored_funnel_rows:
        d = str(row.get("date", "") or "").strip()
        nm = str(row.get("nmId", "") or "").strip()
        if not d or not nm:
            continue
        if allowed_funnel_nm_ids and nm not in allowed_funnel_nm_ids:
            continue
        if (d, nm) in api_funnel_keys:
            continue
        entry = extra_by_nm.setdefault(nm, {
            "product": {"nmId": nm, "vendorCode": str(row.get("supplierArticle", "") or "")},
            "history": [],
        })
        entry["history"].append({
            "date": d,
            "orderSum": row.get("orderSum") or 0,
            "orderCount": row.get("orderCount") or 0,
            "buyoutCount": row.get("buyoutCount") or 0,
            "buyoutSum": row.get("buyoutSum") or 0,
        })
    if extra_by_nm:
        funnel_data = funnel_data + list(extra_by_nm.values())
        print(f"[INFO] Воронка (из хранилища): добавлено {sum(len(e['history']) for e in extra_by_nm.values())} строк за даты вне API-окна")

    print("[INFO] Пересобираем buyout_order_day")
    buyout_order_day_rows = build_buyout_order_day_rows(
        orders_rows=stored_orders_rows,
        sales_rows=stored_sales_rows,
        ads_rows=sheet_values_to_dicts(store.get_values(settings.raw_ads_sheet)),
        nm_mapping=nm_mapping,
        funnel_data=funnel_data,
    )
    store.replace_table("buyout_order_day", buyout_order_day_rows)
    print(f"[INFO] Таблица buyout_order_day: строк {max(0, len(buyout_order_day_rows) - 1)}")

    # Derived analytics should preserve old dates and only refresh the keys
    # that were recalculated in the current sync run.
    print("[INFO] Обновляем аналитические таблицы")
    store.upsert_table(
        "finance_article_day_detail",
        analytics_tables["finance_article_day_detail"],
        key_columns=("Дата", "Артикул"),
        overwrite_existing=True,
        allow_new_columns=True,
    )
    store.upsert_table(
        "analytics_article_day",
        analytics_tables["analytics_article_day"],
        key_columns=("Артикул", "Дата"),
        overwrite_existing=True,
        allow_new_columns=True,
    )
    store.upsert_table(
        "analytics_day",
        analytics_tables["analytics_day"],
        key_columns=("Дата",),
        overwrite_existing=True,
        allow_new_columns=True,
    )
    store.upsert_table(
        "analytics_article_period",
        analytics_tables["analytics_article_period"],
        key_columns=("Период с", "Период по", "Артикул"),
        overwrite_existing=True,
        allow_new_columns=True,
    )
    print("[INFO] Синхронизация завершена")


def calculate_preliminary_economics(sku, date, orders_count, orders_sum, commission_rate, advertising_cost, additional_expense_rate):
    commission = orders_sum * commission_rate
    acquiring = orders_sum * 0.02
    additional_expenses = orders_sum * additional_expense_rate
    preliminary_profit = orders_sum - commission - acquiring - advertising_cost - additional_expenses

    return {
        "sku": sku,
        "date": date,
        "orders_count": orders_count,
        "orders_sum": orders_sum,
        "commission": commission,
        "acquiring": acquiring,
        "advertising": advertising_cost,
        "additional_expenses": additional_expenses,
        "preliminary_profit": preliminary_profit
    }


def link_orders_with_advertising(sku, date):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # Fetch orders data
        cursor.execute("SELECT orders_count, orders_sum FROM orders WHERE sku = ? AND date = ?", (sku, date))
        orders_data = cursor.fetchone()

        # Fetch advertising data
        cursor.execute("SELECT advertising_cost FROM advertising WHERE sku = ? AND date = ?", (sku, date))
        advertising_data = cursor.fetchone()

        if orders_data and advertising_data:
            orders_count, orders_sum = orders_data
            advertising_cost = advertising_data[0]
            return {
                "sku": sku,
                "date": date,
                "orders_count": orders_count,
                "orders_sum": orders_sum,
                "advertising_cost": advertising_cost
            }
        else:
            return None


def calculate_financial_metrics(row):
    commission = row['orders_sum'] * 0.1  # Example commission rate
    acquiring = row['orders_sum'] * 0.02
    additional_expenses = row['orders_sum'] * 0.05  # Example additional expense rate
    preliminary_profit = row['orders_sum'] - commission - acquiring - row['advertising_cost'] - additional_expenses

    return {
        "sku": row['sku'],
        "date": row['date'],
        "orders_count": row['orders_count'],
        "orders_sum": row['orders_sum'],
        "commission": commission,
        "acquiring": acquiring,
        "advertising": row['advertising_cost'],
        "additional_expenses": additional_expenses,
        "preliminary_profit": preliminary_profit
    }


def calculate_average_additional_expenses():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT AVG(additional_expenses / orders_sum) FROM preliminary_economics WHERE orders_sum > 0")
        result = cursor.fetchone()
        return result[0] if result and result[0] else 0.05  # Default to 5% if no data


if __name__ == "__main__":
    main()
