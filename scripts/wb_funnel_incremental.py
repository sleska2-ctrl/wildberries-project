#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import requests

from wb_gsheets.sqlite_store import SQLiteStore
from wb_gsheets.transform import funnel_to_sheet_rows


TZ = ZoneInfo("Europe/Moscow")
API_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products/history"


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _log(message: str) -> None:
    stamp = datetime.now(TZ).isoformat(timespec="seconds")
    print(f"[{stamp}] {message}", flush=True)


def _load_tokens(platform_db: Path, cabinet_id: str) -> tuple[str, str]:
    with _connect(platform_db) as conn:
        row = conn.execute(
            """
            SELECT COALESCE(NULLIF(cr.wb_finance_token, ''), NULLIF(cr.wb_api_token, ''), '') AS token,
                   COALESCE(NULLIF(cr.wb_adv_token, ''), NULLIF(cr.wb_api_token, ''), '') AS adv_token
            FROM cabinets c
            LEFT JOIN cabinet_credentials cr USING(cabinet_id)
            WHERE c.cabinet_id = ?
            """,
            (cabinet_id,),
        ).fetchone()
    if row is None or not str(row["token"] or "").strip():
        raise SystemExit(f"No WB token for cabinet {cabinet_id}")
    return str(row["token"]), str(row["adv_token"] or row["token"])


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row["name"]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def _numeric_values(conn: sqlite3.Connection, table: str, column: str) -> set[int]:
    if column not in _columns(conn, table):
        return set()
    rows = conn.execute(
        f'SELECT DISTINCT "{column}" AS value FROM "{table}" '
        f'WHERE "{column}" IS NOT NULL AND TRIM("{column}") <> ""'
    ).fetchall()
    result: set[int] = set()
    for row in rows:
        value = str(row["value"] or "").strip()
        if value.isdigit() and int(value) > 0:
            result.add(int(value))
    return result


def _collect_nm_ids(db_path: Path, date_from: str, date_to: str) -> list[int]:
    result: set[int] = set()
    with _connect(db_path) as conn:
        if "nmId" in _columns(conn, "raw_sales"):
            rows = conn.execute(
                'SELECT DISTINCT "nmId" AS value FROM "raw_sales" '
                'WHERE substr("dateFrom", 1, 10) BETWEEN ? AND ?',
                (date_from, date_to),
            ).fetchall()
            result.update(int(str(row["value"]).strip()) for row in rows if str(row["value"]).strip().isdigit() and int(str(row["value"]).strip()) > 0)
        if "nmId" in _columns(conn, "raw_orders"):
            rows = conn.execute(
                'SELECT DISTINCT "nmId" AS value FROM "raw_orders" '
                'WHERE substr("date", 1, 10) BETWEEN ? AND ?',
                (date_from, date_to),
            ).fetchall()
            result.update(int(str(row["value"]).strip()) for row in rows if str(row["value"]).strip().isdigit() and int(str(row["value"]).strip()) > 0)
        for column in ("nmId", "nmID", "Артикул WB"):
            result.update(_numeric_values(conn, "raw_stocks", column))
            result.update(_numeric_values(conn, "SKU", column))
    return sorted(result)


def _progress_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wb_funnel_backfill_progress (
            date_from TEXT NOT NULL,
            date_to TEXT NOT NULL,
            nmId INTEGER NOT NULL,
            status TEXT NOT NULL,
            rows INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_status INTEGER,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (date_from, date_to, nmId)
        )
        """
    )
    conn.commit()


def _is_done(conn: sqlite3.Connection, date_from: str, date_to: str, nm_id: int) -> bool:
    if not _table_exists(conn, "funnel_analytics"):
        return False
    row = conn.execute(
        'SELECT COUNT(*) AS cnt FROM "funnel_analytics" '
        'WHERE "nmId" = ? AND substr("date", 1, 10) BETWEEN ? AND ?',
        (str(nm_id), date_from, date_to),
    ).fetchone()
    return int(row["cnt"] or 0) > 0


def _mark(conn: sqlite3.Connection, date_from: str, date_to: str, nm_id: int, status: str, rows: int, attempts: int, last_status: int | None) -> None:
    conn.execute(
        """
        INSERT INTO wb_funnel_backfill_progress
        (date_from, date_to, nmId, status, rows, attempts, last_status, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date_from, date_to, nmId) DO UPDATE SET
            status=excluded.status,
            rows=excluded.rows,
            attempts=excluded.attempts,
            last_status=excluded.last_status,
            updated_at=excluded.updated_at
        """,
        (date_from, date_to, nm_id, status, rows, attempts, last_status, datetime.now(TZ).isoformat(timespec="seconds")),
    )
    conn.commit()


def _fetch_one(session: requests.Session, token: str, date_from: str, date_to: str, nm_id: int, timeout: int) -> requests.Response:
    payload = {
        "selectedPeriod": {"start": date_from, "end": date_to},
        "nmIds": [nm_id],
        "skipDeletedNm": False,
        "aggregationLevel": "day",
    }
    return session.post(API_URL, headers={"Authorization": token}, json=payload, timeout=timeout)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Incrementally backfill WB funnel by one nmId at a time")
    parser.add_argument("--cabinet", required=True)
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--platform-db", default=str(ROOT / "data" / "platform.db"))
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--interval", type=int, default=61)
    parser.add_argument("--cooldown", type=int, default=1800)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--max-429", type=int, default=24)
    parser.add_argument("--limit", type=int, default=0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    platform_db = Path(args.platform_db)
    db_path = Path(args.data_dir) / "cabs" / f"{args.cabinet}.db"
    token, _adv_token = _load_tokens(platform_db, args.cabinet)
    nm_ids = _collect_nm_ids(db_path, args.date_from, args.date_to)
    if args.limit > 0:
        nm_ids = nm_ids[: args.limit]
    _log(f"Cabinet {args.cabinet}: {args.date_from}..{args.date_to}, nmIds={len(nm_ids)}, db={db_path}")

    store = SQLiteStore(str(db_path))
    session = requests.Session()
    last_request = 0.0
    completed = 0
    skipped_existing = 0

    with _connect(db_path) as conn:
        _progress_table(conn)

    for index, nm_id in enumerate(nm_ids, start=1):
        with _connect(db_path) as conn:
            if _is_done(conn, args.date_from, args.date_to, nm_id):
                skipped_existing += 1
                _mark(conn, args.date_from, args.date_to, nm_id, "already_done", 0, 0, None)
                continue

        attempts_429 = 0
        while True:
            delay = args.interval - (time.monotonic() - last_request)
            if delay > 0:
                time.sleep(delay)
            last_request = time.monotonic()
            _log(f"[{index}/{len(nm_ids)}] nmId {nm_id}: request")
            try:
                response = _fetch_one(session, token, args.date_from, args.date_to, nm_id, args.timeout)
            except requests.RequestException as exc:
                _log(f"nmId {nm_id}: network error {exc}; wait {args.cooldown}s")
                time.sleep(args.cooldown)
                continue

            if response.status_code == 429:
                attempts_429 += 1
                with _connect(db_path) as conn:
                    _mark(conn, args.date_from, args.date_to, nm_id, "rate_limited", 0, attempts_429, 429)
                if attempts_429 > args.max_429:
                    _log(f"nmId {nm_id}: max 429 retries reached, stopping")
                    return 2
                _log(f"nmId {nm_id}: 429; wait {args.cooldown}s (attempt {attempts_429}/{args.max_429})")
                time.sleep(args.cooldown)
                continue

            if response.status_code >= 500:
                _log(f"nmId {nm_id}: HTTP {response.status_code}; wait {args.cooldown}s")
                time.sleep(args.cooldown)
                continue

            if response.status_code != 200:
                _log(f"nmId {nm_id}: HTTP {response.status_code}; {response.text[:300]}")
                with _connect(db_path) as conn:
                    _mark(conn, args.date_from, args.date_to, nm_id, "http_error", 0, attempts_429, response.status_code)
                break

            payload = response.json()
            if not isinstance(payload, list):
                payload = []
            rows = funnel_to_sheet_rows(payload)
            row_count = max(0, len(rows) - 1)
            if row_count > 0:
                store.upsert_table("funnel_analytics", rows, key_columns=("date", "nmId"), update_existing=True, allow_new_columns=True)
            with _connect(db_path) as conn:
                _mark(conn, args.date_from, args.date_to, nm_id, "done" if row_count else "empty", row_count, attempts_429, 200)
            completed += 1
            _log(f"nmId {nm_id}: saved rows={row_count}")
            break

    _log(f"Finished: completed={completed}, skipped_existing={skipped_existing}, total={len(nm_ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
