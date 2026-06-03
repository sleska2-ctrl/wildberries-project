from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence


_NAME_RE = re.compile(r"[^0-9A-Za-z_]+")


def _sanitize_identifier(name: str) -> str:
    sanitized = _NAME_RE.sub("_", name.strip())
    if not sanitized:
        raise ValueError("Identifier cannot be empty")
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalize_columns(raw_header: list[object]) -> list[str]:
    normalized: list[str] = []
    used: dict[str, int] = {}
    for index, value in enumerate(raw_header, start=1):
        base = str(value).strip() or f"col_{index}"
        count = used.get(base, 0) + 1
        used[base] = count
        if count == 1:
            normalized.append(base)
        else:
            normalized.append(f"{base}_{count}")
    return normalized


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_name(self, name: str) -> str:
        return _sanitize_identifier(name)

    def _existing_columns(self, conn: sqlite3.Connection, table_name: str) -> list[str]:
        query = f"PRAGMA table_info({_quote_ident(table_name)})"
        rows = conn.execute(query).fetchall()
        return [str(row["name"]) for row in rows]

    def _ensure_table(self, conn: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
        if not columns:
            return
        quoted_columns = ", ".join(f"{_quote_ident(c)} TEXT" for c in columns)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {_quote_ident(table_name)} ({quoted_columns})")

    def _ensure_columns(self, conn: sqlite3.Connection, table_name: str, required_columns: list[str]) -> list[str]:
        existing = self._existing_columns(conn, table_name)
        existing_set = set(existing)
        for column in required_columns:
            if column in existing_set:
                continue
            conn.execute(
                f"ALTER TABLE {_quote_ident(table_name)} ADD COLUMN {_quote_ident(column)} TEXT"
            )
            existing.append(column)
            existing_set.add(column)
        return existing

    def _ensure_unique_index(self, conn: sqlite3.Connection, table_name: str, key_columns: Sequence[str]) -> None:
        if not key_columns:
            return
        idx_suffix = "_".join(key_columns)
        index_name = _sanitize_identifier(f"idx_{table_name}_{idx_suffix}_uniq")
        cols = ", ".join(_quote_ident(col) for col in key_columns)
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_ident(index_name)} "
            f"ON {_quote_ident(table_name)} ({cols})"
        )

    def get_values(self, table_name: str) -> list[list[str]]:
        resolved = self._table_name(table_name)
        with self._connect() as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (resolved,),
            ).fetchone()
            if not tables:
                return []

            columns = self._existing_columns(conn, resolved)
            if not columns:
                return []

            select_cols = ", ".join(_quote_ident(c) for c in columns)
            rows = conn.execute(
                f"SELECT {select_cols} FROM {_quote_ident(resolved)}"
            ).fetchall()

        result: list[list[str]] = [columns]
        for row in rows:
            result.append(["" if row[col] is None else str(row[col]) for col in columns])
        return result

    def upsert_table(
        self,
        table_name: str,
        rows: Iterable[list[object]],
        *,
        key_columns: Sequence[str],
        update_existing: bool = False,
        overwrite_existing: bool = False,
        allow_new_columns: bool = False,
    ) -> None:
        payload = [list(row) for row in rows]
        if not payload or not payload[0]:
            return

        header = _normalize_columns(payload[0])
        if not header:
            return

        resolved_table = self._table_name(table_name)
        key_columns_norm = [str(key).strip() for key in key_columns]
        missing_keys = [key for key in key_columns_norm if key not in header]
        if missing_keys:
            raise ValueError(f"Missing key columns in payload for {table_name}: {missing_keys}")

        with self._connect() as conn:
            self._ensure_table(conn, resolved_table, header)
            existing = self._existing_columns(conn, resolved_table)
            if allow_new_columns:
                existing = self._ensure_columns(conn, resolved_table, header)
            column_order = existing

            self._ensure_unique_index(conn, resolved_table, key_columns_norm)

            rows_data: list[dict[str, str]] = []
            for row in payload[1:]:
                mapped: dict[str, str] = {}
                for idx, col in enumerate(header):
                    value = row[idx] if idx < len(row) else ""
                    mapped[col] = "" if value is None else str(value)
                rows_data.append(mapped)

            insert_columns = ", ".join(_quote_ident(c) for c in column_order)
            placeholders = ", ".join("?" for _ in column_order)

            if (update_existing or overwrite_existing) and key_columns_norm:
                conflict_cols = ", ".join(_quote_ident(c) for c in key_columns_norm)
                updatable = [c for c in column_order if c not in key_columns_norm]
                if updatable:
                    if overwrite_existing:
                        # For derived analytics tables we recalculate the same key fully,
                        # so the fresh row should replace the previous snapshot as-is.
                        update_sql = ", ".join(
                            f"{_quote_ident(c)} = excluded.{_quote_ident(c)}"
                            for c in updatable
                        )
                    else:
                        update_sql = ", ".join(
                            f"{_quote_ident(c)} = CASE "
                            f"WHEN excluded.{_quote_ident(c)} <> '' THEN excluded.{_quote_ident(c)} "
                            f"ELSE {_quote_ident(c)} END"
                            for c in updatable
                        )
                    stmt = (
                        f"INSERT INTO {_quote_ident(resolved_table)} ({insert_columns}) VALUES ({placeholders}) "
                        f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_sql}"
                    )
                else:
                    stmt = (
                        f"INSERT OR IGNORE INTO {_quote_ident(resolved_table)} "
                        f"({insert_columns}) VALUES ({placeholders})"
                    )
            else:
                stmt = (
                    f"INSERT OR IGNORE INTO {_quote_ident(resolved_table)} "
                    f"({insert_columns}) VALUES ({placeholders})"
                )

            for row in rows_data:
                values = [row.get(col, "") for col in column_order]
                if key_columns_norm and any(not row.get(k, "").strip() for k in key_columns_norm):
                    # For keyed tables, skip malformed rows with empty key parts.
                    # Otherwise UNIQUE indexes can fail on repeated empty-string keys.
                    continue
                conn.execute(stmt, values)
            conn.commit()

    def replace_table(self, table_name: str, rows: Iterable[list[object]]) -> None:
        payload = [list(row) for row in rows]
        if not payload or not payload[0]:
            return

        header = _normalize_columns(payload[0])
        if not header:
            return

        resolved_table = self._table_name(table_name)
        with self._connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {_quote_ident(resolved_table)}")
            self._ensure_table(conn, resolved_table, header)

            insert_columns = ", ".join(_quote_ident(c) for c in header)
            placeholders = ", ".join("?" for _ in header)
            stmt = f"INSERT INTO {_quote_ident(resolved_table)} ({insert_columns}) VALUES ({placeholders})"

            for row in payload[1:]:
                values = []
                for idx in range(len(header)):
                    value = row[idx] if idx < len(row) else ""
                    values.append("" if value is None else str(value))
                conn.execute(stmt, values)
            conn.commit()

    def ensure_analytics_indexes(self) -> None:
        """Create performance indexes on key analytics tables after sync."""
        _table_indexes: dict[str, list[tuple[str, str]]] = {
            "buyout_order_day": [("Дата", "idx_bod_date"), ("nmId", "idx_bod_nmid")],
            "funnel_analytics": [("date", "idx_funnel_date"), ("nmId", "idx_funnel_nmid")],
            "raw_ads": [("date", "idx_raw_ads_date"), ("nmId", "idx_raw_ads_nmid")],
            "raw_sales": [("saleDt", "idx_raw_sales_dt"), ("nmId", "idx_raw_sales_nmid")],
            "raw_stocks": [("nmId", "idx_raw_stocks_nmid")],
            "analytics_article_day": [("Дата", "idx_aad_date"), ("Артикул", "idx_aad_article")],
            "finance_article_day_detail": [("Дата", "idx_fadd_date")],
        }
        with self._connect() as conn:
            existing_tables = {
                str(r["name"]) for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            for table, cols in _table_indexes.items():
                if table not in existing_tables:
                    continue
                existing_cols = set(self._existing_columns(conn, table))
                for col, idx_name in cols:
                    if col not in existing_cols:
                        continue
                    safe_col = col.replace('"', '""')
                    try:
                        conn.execute(
                            f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ("{safe_col}")'
                        )
                    except Exception:
                        pass
            conn.commit()

    def list_tables(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        return [str(row["name"]) for row in rows]

    def fetch_table_page(self, table_name: str, limit: int, offset: int) -> tuple[list[str], list[list[str]], int]:
        resolved = self._table_name(table_name)
        with self._connect() as conn:
            columns = self._existing_columns(conn, resolved)
            if not columns:
                return [], [], 0

            total = int(
                conn.execute(f"SELECT COUNT(*) AS cnt FROM {_quote_ident(resolved)}").fetchone()["cnt"]
            )
            select_cols = ", ".join(_quote_ident(c) for c in columns)
            rows = conn.execute(
                f"SELECT {select_cols} FROM {_quote_ident(resolved)} LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()

        page_rows = [["" if row[col] is None else str(row[col]) for col in columns] for row in rows]
        return columns, page_rows, total
