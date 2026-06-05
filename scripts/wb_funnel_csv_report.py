#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
import sys
import time
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import requests

from wb_gsheets.main import _stored_funnel_data
from wb_gsheets.sqlite_store import SQLiteStore
from wb_gsheets.transform import build_buyout_order_day_rows, build_nm_mapping, sheet_values_to_dicts


TZ = ZoneInfo("Europe/Moscow")
BASE_URL = "https://seller-analytics-api.wildberries.ru"
CREATE_PATH = "/api/v2/nm-report/downloads"
LIST_PATH = "/api/v2/nm-report/downloads"
FILE_PATH = "/api/v2/nm-report/downloads/file/{download_id}"


def log(message: str) -> None:
    stamp = datetime.now(TZ).isoformat(timespec="seconds")
    print(f"[{stamp}] {message}", flush=True)


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row["name"]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def load_cabinet(platform_db: Path, cabinet_id: str) -> dict[str, str]:
    with connect(platform_db) as conn:
        row = conn.execute(
            """
            SELECT c.article_filter_type,
                   COALESCE(NULLIF(cr.wb_finance_token, ''), NULLIF(cr.wb_api_token, ''), '') AS wb_token
            FROM cabinets c
            LEFT JOIN cabinet_credentials cr USING(cabinet_id)
            WHERE c.cabinet_id = ?
            """,
            (cabinet_id,),
        ).fetchone()
    if row is None:
        raise SystemExit(f"Cabinet {cabinet_id} not found")
    if not str(row["wb_token"] or "").strip():
        raise SystemExit(f"Cabinet {cabinet_id} has no WB token")
    return {key: str(row[key] or "") for key in row.keys()}


def numeric(value: object) -> int | None:
    text = str(value or "").strip()
    if text.isdigit() and int(text) > 0:
        return int(text)
    return None


def collect_nm_ids(db_path: Path, date_from: str, date_to: str) -> list[int]:
    result: set[int] = set()
    with connect(db_path) as conn:
        if "nmId" in table_columns(conn, "raw_sales"):
            rows = conn.execute(
                'SELECT DISTINCT "nmId" AS value FROM raw_sales WHERE substr("dateFrom", 1, 10) BETWEEN ? AND ?',
                (date_from, date_to),
            ).fetchall()
            result.update(value for row in rows if (value := numeric(row["value"])) is not None)
        if "nmId" in table_columns(conn, "raw_orders"):
            rows = conn.execute(
                'SELECT DISTINCT "nmId" AS value FROM raw_orders WHERE substr("date", 1, 10) BETWEEN ? AND ?',
                (date_from, date_to),
            ).fetchall()
            result.update(value for row in rows if (value := numeric(row["value"])) is not None)
        for table, columns in {
            "raw_stocks": ("nmId", "nmID"),
            "wb_cards": ("nmID", "nmId"),
            "SKU": ("nmId", "nmID", "Артикул WB"),
        }.items():
            existing = table_columns(conn, table)
            for column in columns:
                if column not in existing:
                    continue
                rows = conn.execute(f'SELECT DISTINCT "{column}" AS value FROM "{table}"').fetchall()
                result.update(value for row in rows if (value := numeric(row["value"])) is not None)
    return sorted(result)


def first(row: dict[str, str], names: tuple[str, ...]) -> str:
    by_lower = {key.lower(): value for key, value in row.items()}
    for name in names:
        if name in row and str(row[name]).strip():
            return str(row[name]).strip()
        value = by_lower.get(name.lower())
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def load_product_info(db_path: Path) -> dict[str, dict[str, str]]:
    store = SQLiteStore(str(db_path))
    info: dict[str, dict[str, str]] = {}
    for table in ("raw_stocks", "raw_orders", "raw_sales", "wb_cards", "SKU"):
        for row in sheet_values_to_dicts(store.get_values(table)):
            nm_id = first(row, ("nmId", "nmID", "Артикул WB"))
            if not nm_id:
                continue
            entry = info.setdefault(nm_id, {})
            supplier = first(row, ("supplierArticle", "vendorCode", "Артикул продавца", "Артикул"))
            title = first(row, ("productName", "title", "Название", "name"))
            brand = first(row, ("brand", "brandName", "Бренд"))
            if supplier and not entry.get("supplierArticle"):
                entry["supplierArticle"] = supplier
            if title and not entry.get("productName"):
                entry["productName"] = title
            if brand and not entry.get("brand"):
                entry["brand"] = brand
    return info


def wait_for_limit(response: requests.Response, cooldown: int, attempt: int, max_attempts: int) -> None:
    retry_after = response.headers.get("Retry-After")
    seconds = int(retry_after) if retry_after and retry_after.isdigit() else cooldown
    log(f"WB 429; wait {seconds}s ({attempt}/{max_attempts})")
    time.sleep(seconds)


def request_with_retry(call, cooldown: int, max_attempts: int) -> requests.Response:
    response: requests.Response | None = None
    for attempt in range(1, max_attempts + 1):
        response = call()
        if response.status_code != 429:
            return response
        wait_for_limit(response, cooldown, attempt, max_attempts)
    assert response is not None
    return response


def create_report(
    session: requests.Session,
    token: str,
    report_id: str,
    nm_ids: list[int],
    date_from: str,
    date_to: str,
    timeout: int,
    cooldown: int,
    max_attempts: int,
) -> None:
    payload = {
        "id": report_id,
        "reportType": "DETAIL_HISTORY_REPORT",
        "userReportName": f"funnel {date_from}..{date_to}",
        "params": {
            "nmIDs": nm_ids,
            "subjectIds": [],
            "brandNames": [],
            "tagIds": [],
            "startDate": date_from,
            "endDate": date_to,
            "timezone": "Europe/Moscow",
            "aggregationLevel": "day",
            "skipDeletedNm": False,
        },
    }
    response = request_with_retry(
        lambda: session.post(
            f"{BASE_URL}{CREATE_PATH}",
            headers={"Authorization": token, "Accept": "application/json"},
            json=payload,
            timeout=timeout,
        ),
        cooldown,
        max_attempts,
    )
    if response.status_code not in {200, 201}:
        raise RuntimeError(f"Create report failed: HTTP {response.status_code}: {response.text[:1000]}")
    log(f"Report creation requested: {report_id}")


def extract_reports(payload: object) -> list[dict]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def report_id_of(row: dict) -> str:
    for key in ("id", "downloadId", "downloadID"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def report_status(row: dict) -> str:
    return str(row.get("status") or row.get("state") or "").strip().upper()


def poll_report(
    session: requests.Session,
    token: str,
    report_id: str,
    timeout: int,
    cooldown: int,
    poll_interval: int,
    max_polls: int,
    max_attempts: int,
) -> dict:
    params = {"filter[downloadIds]": report_id}
    for poll in range(1, max_polls + 1):
        response = request_with_retry(
            lambda: session.get(f"{BASE_URL}{LIST_PATH}", headers={"Authorization": token}, params=params, timeout=timeout),
            cooldown,
            max_attempts,
        )
        if response.status_code != 200:
            raise RuntimeError(f"List reports failed: HTTP {response.status_code}: {response.text[:1000]}")
        reports = extract_reports(response.json())
        report = next((item for item in reports if report_id_of(item) == report_id), None)
        status = report_status(report or {})
        log(f"Report {report_id}: status={status or 'UNKNOWN'} poll={poll}/{max_polls}")
        if status == "SUCCESS":
            return report or {}
        if status in {"FAILED", "FAIL", "ERROR", "CANCELED", "CANCELLED"}:
            raise RuntimeError(f"Report failed: {json.dumps(report, ensure_ascii=False)}")
        time.sleep(poll_interval)
    raise RuntimeError(f"Report {report_id} was not ready")


def download_report(
    session: requests.Session,
    token: str,
    report_id: str,
    timeout: int,
    cooldown: int,
    max_attempts: int,
) -> bytes:
    response = request_with_retry(
        lambda: session.get(
            f"{BASE_URL}{FILE_PATH.format(download_id=report_id)}",
            headers={"Authorization": token},
            timeout=timeout,
        ),
        cooldown,
        max_attempts,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Download failed: HTTP {response.status_code}: {response.text[:1000]}")
    return response.content


def read_csv_from_zip(content: bytes, save_dir: Path, report_id: str) -> list[dict[str, str]]:
    zip_path = save_dir / f"{report_id}.zip"
    zip_path.write_bytes(content)
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("Report ZIP does not contain CSV")
        raw = archive.read(csv_names[0])
    csv_path = save_dir / f"{report_id}.csv"
    csv_path.write_bytes(raw)
    text = raw.decode("utf-8-sig")
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    rows = list(csv.DictReader(io.StringIO(text), dialect=dialect))
    log(f"Downloaded rows={len(rows)}, zip={zip_path}, csv={csv_path}")
    return rows


def decimal_text(value: object) -> str:
    return str(value or "").strip().replace(",", ".")


def csv_to_funnel_rows(csv_rows: list[dict[str, str]], product_info: dict[str, dict[str, str]]) -> list[list[object]]:
    headers = [
        "nmId",
        "supplierArticle",
        "productName",
        "brand",
        "currency",
        "date",
        "addToCartConversion",
        "addToWishlistCount",
        "buyoutCount",
        "buyoutPercent",
        "buyoutSum",
        "cancelCount",
        "cancelSum",
        "cartCount",
        "cartToOrderConversion",
        "openCount",
        "orderCount",
        "orderSum",
    ]
    rows: list[list[object]] = [headers]
    for row in csv_rows:
        nm_id = first(row, ("nmID", "nmId", "nm_id"))
        date = first(row, ("dt", "date", "Дата"))
        if not nm_id or not date:
            continue
        info = product_info.get(nm_id, {})
        rows.append(
            [
                nm_id,
                info.get("supplierArticle", ""),
                info.get("productName", ""),
                info.get("brand", ""),
                first(row, ("currency",)) or "RUB",
                date[:10],
                decimal_text(first(row, ("addToCartConversion",))),
                decimal_text(first(row, ("addToWishlist", "addToWishlistCount"))),
                decimal_text(first(row, ("buyoutsCount", "buyoutCount"))),
                decimal_text(first(row, ("buyoutPercent",))),
                decimal_text(first(row, ("buyoutsSumRub", "buyoutSum"))),
                decimal_text(first(row, ("cancelCount", "cancellationsCount"))),
                decimal_text(first(row, ("cancelSumRub", "cancelSum"))),
                decimal_text(first(row, ("addToCartCount", "cartCount"))),
                decimal_text(first(row, ("cartToOrderConversion",))),
                decimal_text(first(row, ("openCardCount", "openCount"))),
                decimal_text(first(row, ("ordersCount", "orderCount"))),
                decimal_text(first(row, ("ordersSumRub", "orderSum"))),
            ]
        )
    return rows


def delete_period(db_path: Path, table: str, date_column: str, date_from: str, date_to: str) -> None:
    with connect(db_path) as conn:
        if not table_exists(conn, table):
            return
        conn.execute(
            f'DELETE FROM "{table}" WHERE substr("{date_column}", 1, 10) BETWEEN ? AND ?',
            (date_from, date_to),
        )
        conn.commit()


def rebuild_buyout_order_day(db_path: Path, article_filter_type: str) -> int:
    store = SQLiteStore(str(db_path))
    cogs_values = store.get_values("SKU")
    nm_mapping = build_nm_mapping(cogs_values, article_filter_type=article_filter_type or "nmId")
    rows = build_buyout_order_day_rows(
        orders_rows=sheet_values_to_dicts(store.get_values("raw_orders")),
        sales_rows=sheet_values_to_dicts(store.get_values("raw_sales")),
        ads_rows=sheet_values_to_dicts(store.get_values("raw_ads")),
        nm_mapping=nm_mapping,
        funnel_data=_stored_funnel_data(store, "funnel_analytics"),
    )
    store.replace_table("buyout_order_day", rows)
    return max(0, len(rows) - 1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load WB DETAIL_HISTORY_REPORT CSV into funnel_analytics")
    parser.add_argument("--cabinet", required=True)
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--platform-db", default=str(ROOT / "data" / "platform.db"))
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--save-dir", default=str(ROOT / "data" / "wb_reports"))
    parser.add_argument("--download-id", default="")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--cooldown", type=int, default=3600)
    parser.add_argument("--poll-interval", type=int, default=120)
    parser.add_argument("--max-polls", type=int, default=120)
    parser.add_argument("--max-attempts", type=int, default=24)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    platform_db = Path(args.platform_db)
    data_dir = Path(args.data_dir)
    db_path = data_dir / "cabs" / f"{args.cabinet}.db"
    save_dir = Path(args.save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    cabinet = load_cabinet(platform_db, args.cabinet)
    token = cabinet["wb_token"]
    nm_ids = collect_nm_ids(db_path, args.date_from, args.date_to)
    if not nm_ids:
        raise SystemExit("No nmIDs found for report")
    log(f"Cabinet {args.cabinet}: {args.date_from}..{args.date_to}, nmIDs={len(nm_ids)}")

    session = requests.Session()
    report_id = args.download_id.strip() or str(uuid.uuid4())
    if not args.download_id.strip():
        create_report(
            session,
            token,
            report_id,
            nm_ids,
            args.date_from,
            args.date_to,
            args.timeout,
            args.cooldown,
            args.max_attempts,
        )
    poll_report(
        session,
        token,
        report_id,
        args.timeout,
        args.cooldown,
        args.poll_interval,
        args.max_polls,
        args.max_attempts,
    )
    content = download_report(session, token, report_id, args.timeout, args.cooldown, args.max_attempts)
    csv_rows = read_csv_from_zip(content, save_dir, report_id)
    funnel_rows = csv_to_funnel_rows(csv_rows, load_product_info(db_path))
    log(f"Prepared funnel rows={max(0, len(funnel_rows) - 1)}")

    delete_period(db_path, "funnel_analytics", "date", args.date_from, args.date_to)
    SQLiteStore(str(db_path)).upsert_table(
        "funnel_analytics",
        funnel_rows,
        key_columns=("date", "nmId"),
        update_existing=True,
        allow_new_columns=True,
    )
    buyout_rows = rebuild_buyout_order_day(db_path, cabinet.get("article_filter_type") or "nmId")
    log(f"Imported funnel rows={max(0, len(funnel_rows) - 1)}; buyout_order_day rows={buyout_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
