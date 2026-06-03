"""Migrate last 90 days of legacy cabinet SQLite DBs into per-cabinet DBs."""
from __future__ import annotations

import shutil
import sqlite3
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
CABS_DIR = DATA_DIR / "cabs"

# Cabinet definitions: (cabinet_id, source_db_path, has_ozon)
SOURCES = [
    ("hld",   ROOT.parent / "app-hld" / "data" / "wb_sync_hld.db", False),
    ("mipao", ROOT.parent / "mipao" / "data" / "mipao_sync.db",    True),
    ("dutex", ROOT.parent / "dutex" / "data" / "dutex_sync.db",    True),
]

# WB tables where date column name varies
WB_TABLES = {
    # table_name: date_column (None = copy all rows, no date filter)
    "SKU": None,
    "raw_sales": "saleDt",
    "raw_orders": "date",
    "raw_ads": "date",
    "raw_stocks": "dateFrom",
    "daily_pnl": "date",
    "finance_article_day_detail": "Дата",
    "analytics_article_day": "Дата",
    "analytics_day": "Дата",
    "analytics_article_period": None,  # period-based, copy all
    "buyout_order_day": "Дата заказа",
    "funnel_analytics": "date",
    "funnel_impressions_upload": "date",
    "manager_comments": None,
    "preliminary_order_economics": "date",
    "app_settings": None,
}

OZON_TABLES = {
    "ozon_raw_orders": "date",
    "ozon_raw_sales": "date",
    "ozon_raw_stocks": None,
    "ozon_raw_ads": "date",
    "ozon_funnel_analytics": "date",
    "ozon_analytics_day": "day",
    "ozon_daily_summary": "day",
    "ozon_plugin_analytics": "day",
    "ozon_sku_day_analytics": "day",
    "ozon_sku_day_finance": "day",
    "ozon_sku_day_ad_spend": "day",
    "ozon_sku_period_ad_spend": None,
    "ozon_stock_on_warehouses": None,
    "ozon_product_day_analytics": None,
}


def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {r[0] for r in rows}


def _existing_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [r[1] for r in rows]


def _copy_table(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection,
                table: str, date_col: str | None, cutoff: str) -> int:
    src_tables = _existing_tables(src_conn)
    if table not in src_tables:
        return 0

    src_cols = _existing_columns(src_conn, table)
    if not src_cols:
        return 0

    # Read from source with optional date filter
    if date_col and date_col in src_cols:
        rows = src_conn.execute(
            f'SELECT * FROM "{table}" WHERE substr("{date_col}", 1, 10) >= ?',
            (cutoff,),
        ).fetchall()
    else:
        rows = src_conn.execute(f'SELECT * FROM "{table}"').fetchall()

    if not rows:
        return 0

    # Ensure destination table exists
    dst_tables = _existing_tables(dst_conn)
    if table not in dst_tables:
        col_defs = ", ".join(f'"{c}" TEXT' for c in src_cols)
        dst_conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
    else:
        dst_cols = set(_existing_columns(dst_conn, table))
        for col in src_cols:
            if col not in dst_cols:
                dst_conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')

    placeholders = ", ".join("?" for _ in src_cols)
    col_list = ", ".join(f'"{c}"' for c in src_cols)
    stmt = f'INSERT OR IGNORE INTO "{table}" ({col_list}) VALUES ({placeholders})'

    inserted = 0
    for row in rows:
        try:
            dst_conn.execute(stmt, row)
            inserted += 1
        except Exception:
            pass
    return inserted


def migrate_cabinet(cabinet_id: str, src_db: Path, has_ozon: bool, cutoff: str) -> None:
    if not src_db.exists():
        print(f"  [SKIP] {src_db} не найден")
        return

    dst_db = CABS_DIR / f"{cabinet_id}.db"
    CABS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Кабинет: {cabinet_id}")
    print(f"  Источник: {src_db}")
    print(f"  Назначение: {dst_db}")
    print(f"  Дата отсечки: {cutoff}")

    src_conn = sqlite3.connect(str(src_db))
    src_conn.row_factory = sqlite3.Row

    dst_conn = sqlite3.connect(str(dst_db))
    dst_conn.execute("PRAGMA journal_mode=WAL")

    total = 0
    for table, date_col in WB_TABLES.items():
        n = _copy_table(src_conn, dst_conn, table, date_col, cutoff)
        if n:
            print(f"  WB {table}: {n} строк")
        total += n

    if has_ozon:
        for table, date_col in OZON_TABLES.items():
            n = _copy_table(src_conn, dst_conn, table, date_col, cutoff)
            if n:
                print(f"  Ozon {table}: {n} строк")
            total += n

    dst_conn.commit()
    dst_conn.close()
    src_conn.close()

    print(f"  Итого перенесено: {total} строк")

    # Spot-check counts
    check_conn = sqlite3.connect(str(dst_db))
    src_check = sqlite3.connect(str(src_db))
    for table in ["raw_sales", "analytics_article_day", "ozon_daily_summary"] if has_ozon else ["raw_sales", "analytics_article_day"]:
        dst_tables_set = _existing_tables(check_conn)
        src_tables_set = _existing_tables(src_check)
        if table in dst_tables_set and table in src_tables_set:
            dst_cnt = check_conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            print(f"  Проверка {table}: {dst_cnt} строк в новой БД")
    check_conn.close()
    src_check.close()


def main() -> None:
    cutoff = (date.today() - timedelta(days=90)).isoformat()
    print(f"Миграция данных за последние 90 дней (с {cutoff})")
    print(f"Целевая директория: {CABS_DIR}")

    # Backup platform.db if exists
    platform_db = DATA_DIR / "platform.db"
    if platform_db.exists():
        backup = DATA_DIR / "platform.db.backup"
        shutil.copy2(str(platform_db), str(backup))
        print(f"\nРезервная копия platform.db → {backup}")

    for cabinet_id, src_db, has_ozon in SOURCES:
        migrate_cabinet(cabinet_id, src_db, has_ozon, cutoff)

    print(f"\n{'='*60}")
    print("Миграция завершена.")
    print("\nДальнейшие шаги:")
    print("1. Зайди на /_admin и создай 4 кабинета (ewb, hld, mipao, dutex)")
    print("2. Заполни API ключи в /_admin для каждого кабинета")
    print("3. Проверь данные через /db в каждом кабинете")


if __name__ == "__main__":
    main()
