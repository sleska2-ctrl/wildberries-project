#!/usr/bin/env python3
"""Run and validate scheduled syncs for all platform cabinets."""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


TZ_NAME = "Europe/Moscow"
ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PLATFORM_DB = ROOT / "data" / "platform.db"
DEFAULT_DATA_DIR = ROOT / "data"
DEFAULT_LOG_DIR = ROOT / "data" / "sync_logs"


@dataclass(slots=True)
class Cabinet:
    cabinet_id: str
    name: str
    marketplace: str
    article_filter_type: str
    disable_scope_filter: bool
    wb_api_token: str
    wb_finance_token: str
    wb_adv_token: str
    ozon_client_id: str
    ozon_api_key: str
    ozon_performance_client_id: str
    ozon_performance_client_secret: str


class RunLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def write(self, message: str) -> None:
        stamp = datetime.now(ZoneInfo(TZ_NAME)).isoformat(timespec="seconds")
        line = f"[{stamp}] {message}"
        print(line, flush=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def _today_msk() -> date:
    return datetime.now(ZoneInfo(TZ_NAME)).date()


def _default_period() -> tuple[str, str]:
    today = _today_msk()
    return (today - timedelta(days=2)).isoformat(), (today - timedelta(days=1)).isoformat()


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def load_cabinets(platform_db: Path, selected: set[str]) -> list[Cabinet]:
    with _connect(platform_db) as conn:
        rows = conn.execute(
            """
            SELECT c.cabinet_id, c.name, c.marketplace, c.article_filter_type,
                   c.disable_scope_filter,
                   COALESCE(cr.wb_api_token, '') AS wb_api_token,
                   COALESCE(cr.wb_finance_token, '') AS wb_finance_token,
                   COALESCE(cr.wb_adv_token, '') AS wb_adv_token,
                   COALESCE(cr.ozon_client_id, '') AS ozon_client_id,
                   COALESCE(cr.ozon_api_key, '') AS ozon_api_key,
                   COALESCE(cr.ozon_performance_client_id, '') AS ozon_performance_client_id,
                   COALESCE(cr.ozon_performance_client_secret, '') AS ozon_performance_client_secret
            FROM cabinets c
            LEFT JOIN cabinet_credentials cr USING(cabinet_id)
            ORDER BY c.created_at, c.cabinet_id
            """
        ).fetchall()
    cabinets = []
    for row in rows:
        cabinet_id = str(row["cabinet_id"])
        if selected and cabinet_id not in selected:
            continue
        cabinets.append(
            Cabinet(
                cabinet_id=cabinet_id,
                name=str(row["name"] or cabinet_id),
                marketplace=str(row["marketplace"] or "wb").lower(),
                article_filter_type=str(row["article_filter_type"] or "nmId"),
                disable_scope_filter=bool(row["disable_scope_filter"]),
                wb_api_token=str(row["wb_api_token"] or ""),
                wb_finance_token=str(row["wb_finance_token"] or ""),
                wb_adv_token=str(row["wb_adv_token"] or ""),
                ozon_client_id=str(row["ozon_client_id"] or ""),
                ozon_api_key=str(row["ozon_api_key"] or ""),
                ozon_performance_client_id=str(row["ozon_performance_client_id"] or ""),
                ozon_performance_client_secret=str(row["ozon_performance_client_secret"] or ""),
            )
        )
    return cabinets


def run_command(cmd: list[str], env: dict[str, str], log: RunLog) -> int:
    log.write("CMD: " + " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        log.write(line.rstrip())
    proc.wait()
    log.write(f"Exit code: {proc.returncode}")
    return int(proc.returncode)


def _base_env() -> dict[str, str]:
    env = dict(os.environ)
    env["TZ"] = TZ_NAME
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT / "src")
    return env


def cabinet_db_path(data_dir: Path, cabinet_id: str) -> Path:
    path = data_dir / "cabs" / f"{cabinet_id}.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def run_wb(cabinet: Cabinet, db_path: Path, date_from: str, date_to: str, log: RunLog) -> bool:
    token = cabinet.wb_api_token
    if not token:
        log.write(f"WB {cabinet.cabinet_id}: skipped, no WB token")
        return False

    env = _base_env()
    env.update(
        {
            "SQLITE_DB_PATH": str(db_path),
            "WB_API_TOKEN": token,
            "WB_FINANCE_TOKEN": cabinet.wb_finance_token or token,
            "WB_ADV_TOKEN": cabinet.wb_adv_token or token,
            "ARTICLE_FILTER_TYPE": cabinet.article_filter_type or "nmId",
            "DISABLE_SCOPE_FILTER": "1" if cabinet.disable_scope_filter else "",
            "DEFAULT_DATE_FROM": date_from,
            "DEFAULT_DATE_TO": date_to,
        }
    )
    base_cmd = [
        sys.executable,
        "-u",
        "-m",
        "wb_gsheets.main",
        "--date-from",
        date_from,
        "--date-to",
        date_to,
        "--slim",
    ]

    log.write(f"WB {cabinet.cabinet_id}: full attempt 1")
    if run_command(base_cmd, env, log) == 0:
        return True

    log.write(f"WB {cabinet.cabinet_id}: full attempt 2")
    if run_command(base_cmd, env, log) == 0:
        return True

    log.write(f"WB {cabinet.cabinet_id}: fallback without ads")
    return run_command(base_cmd + ["--skip-ads"], env, log) == 0


def run_ozon(cabinet: Cabinet, db_path: Path, date_from: str, date_to: str, log: RunLog) -> bool:
    required = [
        cabinet.ozon_client_id,
        cabinet.ozon_api_key,
        cabinet.ozon_performance_client_id,
        cabinet.ozon_performance_client_secret,
    ]
    if not all(required):
        log.write(f"OZON {cabinet.cabinet_id}: skipped, missing OZON credentials")
        return False

    env = _base_env()
    env.update(
        {
            "OZON_CLIENT_ID": cabinet.ozon_client_id,
            "OZON_API_KEY": cabinet.ozon_api_key,
            "OZON_PERFORMANCE_CLIENT_ID": cabinet.ozon_performance_client_id,
            "OZON_PERFORMANCE_CLIENT_SECRET": cabinet.ozon_performance_client_secret,
        }
    )
    base_cmd = [
        sys.executable,
        "-u",
        str(ROOT / "scripts" / "ozon_sync.py"),
        "--date-from",
        date_from,
        "--date-to",
        date_to,
        "--db",
        str(db_path),
        "--cabinet",
        cabinet.cabinet_id,
    ]

    log.write(f"OZON {cabinet.cabinet_id}: full attempt 1")
    if run_command(base_cmd, env, log) == 0:
        return True

    log.write(f"OZON {cabinet.cabinet_id}: full attempt 2")
    if run_command(base_cmd, env, log) == 0:
        return True

    log.write(f"OZON {cabinet.cabinet_id}: fallback without ads")
    return run_command(base_cmd + ["--skip-ads"], env, log) == 0


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _count_between(conn: sqlite3.Connection, table: str, column: str, date_from: str, date_to: str) -> int | None:
    if not _table_exists(conn, table):
        return None
    row = conn.execute(
        f'SELECT COUNT(*) AS cnt FROM "{table}" WHERE substr("{column}", 1, 10) BETWEEN ? AND ?',
        (date_from, date_to),
    ).fetchone()
    return int(row["cnt"] or 0)


def _max_date(conn: sqlite3.Connection, table: str, column: str) -> str:
    if not _table_exists(conn, table):
        return ""
    row = conn.execute(f'SELECT MAX(substr("{column}", 1, 10)) AS max_date FROM "{table}"').fetchone()
    return str(row["max_date"] or "")


def _expected_days(date_from: str, date_to: str) -> list[str]:
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    days = []
    current = start
    while current <= end:
        days.append(current.isoformat())
        current += timedelta(days=1)
    return days


def _days_with_rows(conn: sqlite3.Connection, table: str, column: str, date_from: str, date_to: str) -> set[str] | None:
    if not _table_exists(conn, table):
        return None
    rows = conn.execute(
        f'SELECT substr("{column}", 1, 10) AS day, COUNT(*) AS cnt '
        f'FROM "{table}" WHERE substr("{column}", 1, 10) BETWEEN ? AND ? '
        f'GROUP BY substr("{column}", 1, 10)',
        (date_from, date_to),
    ).fetchall()
    return {str(row["day"]) for row in rows if int(row["cnt"] or 0) > 0}


def validate_wb(db_path: Path, date_from: str, date_to: str, log: RunLog, cabinet_id: str) -> bool:
    required = [
        ("raw_sales", "dateFrom"),
        ("raw_orders", "date"),
        ("raw_ads", "date"),
        ("funnel_analytics", "date"),
        ("buyout_order_day", "Дата"),
    ]
    ok = True
    with _connect(db_path) as conn:
        for table, column in required:
            count = _count_between(conn, table, column, date_from, date_to)
            max_date = _max_date(conn, table, column)
            if count is None:
                log.write(f"CHECK WB {cabinet_id}: {table} missing")
                ok = False
            elif count <= 0:
                log.write(f"CHECK WB {cabinet_id}: {table} has 0 rows for {date_from}..{date_to}; max={max_date}")
                ok = False
            else:
                log.write(f"CHECK WB {cabinet_id}: {table} rows={count}, max={max_date}")
    return ok


def validate_ozon(db_path: Path, date_from: str, date_to: str, log: RunLog, cabinet_id: str) -> bool:
    required = [
        ("ozon_daily_summary", "day"),
        ("ozon_plugin_analytics", "day"),
        ("ozon_sku_day_analytics", "day"),
    ]
    ok = True
    expected_days = set(_expected_days(date_from, date_to))
    with _connect(db_path) as conn:
        for table, column in required:
            count = _count_between(conn, table, column, date_from, date_to)
            max_date = _max_date(conn, table, column)
            days = _days_with_rows(conn, table, column, date_from, date_to)
            synced_at = ""
            if _table_exists(conn, table):
                try:
                    row = conn.execute(f'SELECT MAX(synced_at) AS synced_at FROM "{table}"').fetchone()
                    synced_at = str(row["synced_at"] or "")
                except sqlite3.OperationalError:
                    synced_at = ""
            if count is None:
                log.write(f"CHECK OZON {cabinet_id}: {table} missing")
                ok = False
            elif count <= 0:
                log.write(f"CHECK OZON {cabinet_id}: {table} has 0 rows for {date_from}..{date_to}; max={max_date}, synced_at={synced_at}")
                ok = False
            elif days is not None and days != expected_days:
                missing = ", ".join(sorted(expected_days - days))
                log.write(
                    f"CHECK OZON {cabinet_id}: {table} rows={count}, but missing days: "
                    f"{missing}; max={max_date}, synced_at={synced_at}"
                )
                ok = False
            else:
                log.write(f"CHECK OZON {cabinet_id}: {table} rows={count}, max={max_date}, synced_at={synced_at}")

        ad_count = _count_between(conn, "ozon_sku_day_ad_spend", "day", date_from, date_to)
        ad_max_date = _max_date(conn, "ozon_sku_day_ad_spend", "day")
        ad_days = _days_with_rows(conn, "ozon_sku_day_ad_spend", "day", date_from, date_to)
        ad_synced_at = ""
        if _table_exists(conn, "ozon_sku_day_ad_spend"):
            row = conn.execute('SELECT MAX(synced_at) AS synced_at FROM "ozon_sku_day_ad_spend"').fetchone()
            ad_synced_at = str(row["synced_at"] or "")
        daily_ad_sum = 0.0
        daily_ad_by_day: dict[str, float] = {}
        sku_ad_by_day: dict[str, float] = {}
        if _table_exists(conn, "ozon_daily_summary"):
            rows = conn.execute(
                """
                SELECT day, COALESCE(SUM(CAST(ad_spend AS REAL)), 0) AS ad_sum
                FROM ozon_daily_summary
                WHERE day BETWEEN ? AND ?
                GROUP BY day
                """,
                (date_from, date_to),
            ).fetchall()
            daily_ad_by_day = {str(row["day"]): float(row["ad_sum"] or 0) for row in rows}
            daily_ad_sum = sum(daily_ad_by_day.values())
        if _table_exists(conn, "ozon_sku_day_ad_spend"):
            rows = conn.execute(
                """
                SELECT day, COALESCE(SUM(CAST(ad_spend AS REAL)), 0) AS ad_sum
                FROM ozon_sku_day_ad_spend
                WHERE day BETWEEN ? AND ?
                GROUP BY day
                """,
                (date_from, date_to),
            ).fetchall()
            sku_ad_by_day = {str(row["day"]): float(row["ad_sum"] or 0) for row in rows}
        if ad_count is None:
            log.write("CHECK OZON %s: ozon_sku_day_ad_spend missing" % cabinet_id)
            ok = False
        elif ad_count <= 0 and daily_ad_sum > 0:
            log.write(
                f"CHECK OZON {cabinet_id}: ozon_sku_day_ad_spend has 0 rows for "
                f"{date_from}..{date_to}, but daily ad_spend={daily_ad_sum}; max={ad_max_date}, synced_at={ad_synced_at}"
            )
            ok = False
        elif ad_count <= 0:
            log.write(
                f"CHECK OZON {cabinet_id}: ozon_sku_day_ad_spend rows=0, daily ad_spend=0 "
                f"(warning only); max={ad_max_date}, synced_at={ad_synced_at}"
            )
        elif ad_days is not None and ad_days != expected_days:
            missing_ad_days = sorted(expected_days - ad_days)
            missing_with_spend = [day for day in missing_ad_days if daily_ad_by_day.get(day, 0.0) > 0]
            if missing_with_spend:
                log.write(
                    f"CHECK OZON {cabinet_id}: ozon_sku_day_ad_spend rows={ad_count}, "
                    f"but missing days with daily ad_spend: {', '.join(missing_with_spend)}; "
                    f"max={ad_max_date}, synced_at={ad_synced_at}"
                )
                ok = False
            else:
                log.write(
                    f"CHECK OZON {cabinet_id}: ozon_sku_day_ad_spend rows={ad_count}, "
                    f"missing days {', '.join(missing_ad_days)} have daily ad_spend=0 "
                    f"(warning only); max={ad_max_date}, synced_at={ad_synced_at}"
                )
        else:
            log.write(
                f"CHECK OZON {cabinet_id}: ozon_sku_day_ad_spend rows={ad_count}, "
                f"max={ad_max_date}, synced_at={ad_synced_at}"
            )
        for day in sorted(set(daily_ad_by_day) | set(sku_ad_by_day)):
            daily_spend = float(daily_ad_by_day.get(day, 0.0) or 0.0)
            sku_spend = float(sku_ad_by_day.get(day, 0.0) or 0.0)
            if daily_spend <= 0:
                continue
            diff = abs(daily_spend - sku_spend)
            tolerance = max(1.0, daily_spend * 0.02)
            if diff > tolerance:
                log.write(
                    f"CHECK OZON {cabinet_id}: sku ad_spend differs on {day} "
                    f"(warning only): "
                    f"daily={daily_spend:.2f}, sku={sku_spend:.2f}, diff={diff:.2f}"
                )
    return ok


def build_parser() -> argparse.ArgumentParser:
    default_from, default_to = _default_period()
    parser = argparse.ArgumentParser(description="Sync all Wildberries/OZON platform cabinets")
    parser.add_argument("--date-from", default=default_from)
    parser.add_argument("--date-to", default=default_to)
    parser.add_argument("--platform-db", default=str(DEFAULT_PLATFORM_DB))
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    parser.add_argument("--cabinet", action="append", default=[], help="Run only this cabinet; can be repeated")
    parser.add_argument("--only", choices=("all", "wb", "ozon"), default="all")
    parser.add_argument("--validate-only", action="store_true")
    return parser


def main() -> int:
    os.environ["TZ"] = TZ_NAME
    args = build_parser().parse_args()
    platform_db = Path(args.platform_db)
    data_dir = Path(args.data_dir)
    selected = {str(c).strip() for c in args.cabinet if str(c).strip()}
    started = datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y%m%d_%H%M%S")
    log = RunLog(Path(args.log_dir) / f"auto_all_{started}_{args.date_from}_{args.date_to}.log")

    log.write(f"Starting all-project sync: {args.date_from}..{args.date_to}, only={args.only}, validate_only={args.validate_only}")
    cabinets = load_cabinets(platform_db, selected)
    if not cabinets:
        log.write("No cabinets found")
        return 1

    success = True
    for cabinet in cabinets:
        db_path = cabinet_db_path(data_dir, cabinet.cabinet_id)
        log.write(f"Cabinet {cabinet.cabinet_id} ({cabinet.name}), marketplace={cabinet.marketplace}, db={db_path}")

        if args.only in ("all", "wb") and cabinet.marketplace in ("wb", "both"):
            if not args.validate_only:
                success = run_wb(cabinet, db_path, args.date_from, args.date_to, log) and success
            success = validate_wb(db_path, args.date_from, args.date_to, log, cabinet.cabinet_id) and success

        if args.only in ("all", "ozon") and cabinet.marketplace in ("ozon", "both"):
            if not args.validate_only:
                success = run_ozon(cabinet, db_path, args.date_from, args.date_to, log) and success
            success = validate_ozon(db_path, args.date_from, args.date_to, log, cabinet.cabinet_id) and success

    log.write("All-project sync finished: " + ("OK" if success else "FAILED"))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
