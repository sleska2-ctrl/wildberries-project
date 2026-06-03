"""Multi-cabinet platform store: manages cabinets, credentials, and sessions."""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path


SESSION_SECRET = os.getenv("SESSION_SECRET", "changeme-please-set-in-env")
ADMIN_PIN = os.getenv("ADMIN_PIN", "")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

_local = threading.local()


def set_request_cabinet(cabinet: dict | None) -> None:
    _local.cabinet = cabinet


def get_request_cabinet() -> dict | None:
    return getattr(_local, "cabinet", None)


def _hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode("utf-8")).hexdigest()


class PlatformStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_platform_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_platform_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS cabinets (
                    cabinet_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    marketplace TEXT NOT NULL DEFAULT 'wb',
                    pin_hash TEXT NOT NULL DEFAULT '',
                    article_filter_type TEXT DEFAULT 'nmId',
                    disable_scope_filter INTEGER DEFAULT 0,
                    created_at TEXT
                );
                CREATE TABLE IF NOT EXISTS cabinet_credentials (
                    cabinet_id TEXT PRIMARY KEY,
                    wb_api_token TEXT DEFAULT '',
                    wb_finance_token TEXT DEFAULT '',
                    wb_adv_token TEXT DEFAULT '',
                    ozon_client_id TEXT DEFAULT '',
                    ozon_api_key TEXT DEFAULT '',
                    ozon_performance_client_id TEXT DEFAULT '',
                    ozon_performance_client_secret TEXT DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS cabinet_sessions (
                    token TEXT PRIMARY KEY,
                    cabinet_id TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
            """)
            self._migrate_credentials_to_unified_store(conn)

    def _migrate_credentials_to_unified_store(self, conn: sqlite3.Connection) -> None:
        # Ensure every cabinet has a credentials row.
        conn.execute(
            """
            INSERT OR IGNORE INTO cabinet_credentials (cabinet_id)
            SELECT cabinet_id FROM cabinets
            """
        )

        cabinets_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(cabinets)").fetchall()
        }
        legacy_fields = [
            "wb_api_token",
            "wb_finance_token",
            "wb_adv_token",
            "ozon_client_id",
            "ozon_api_key",
            "ozon_performance_client_id",
            "ozon_performance_client_secret",
        ]
        existing_legacy_fields = [field for field in legacy_fields if field in cabinets_columns]
        if not existing_legacy_fields:
            return

        # Copy non-empty legacy values from cabinets to cabinet_credentials if target is empty.
        for field in existing_legacy_fields:
            conn.execute(
                f"""
                UPDATE cabinet_credentials
                SET {field} = (
                    SELECT c.{field}
                    FROM cabinets c
                    WHERE c.cabinet_id = cabinet_credentials.cabinet_id
                )
                WHERE COALESCE({field}, '') = ''
                  AND EXISTS (
                    SELECT 1
                    FROM cabinets c
                    WHERE c.cabinet_id = cabinet_credentials.cabinet_id
                      AND COALESCE(c.{field}, '') <> ''
                  )
                """
            )

    def get_cabinet(self, cabinet_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT c.cabinet_id, c.name, c.marketplace, c.pin_hash, "
                "c.article_filter_type, c.disable_scope_filter, c.created_at, "
                "cr.wb_api_token, cr.wb_finance_token, cr.wb_adv_token, "
                "cr.ozon_client_id, cr.ozon_api_key, "
                "cr.ozon_performance_client_id, cr.ozon_performance_client_secret "
                "FROM cabinets c "
                "LEFT JOIN cabinet_credentials cr USING(cabinet_id) "
                "WHERE c.cabinet_id = ?",
                (cabinet_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_cabinets(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT cabinet_id, name, marketplace, article_filter_type, "
                "disable_scope_filter, created_at "
                "FROM cabinets ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def create_cabinet(self, config: dict) -> None:
        pin = str(config.get("pin", "321"))
        now = datetime.now().isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cabinets "
                "(cabinet_id, name, marketplace, pin_hash, article_filter_type, "
                "disable_scope_filter, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    config["cabinet_id"],
                    config["name"],
                    config.get("marketplace", "wb"),
                    _hash_pin(pin),
                    config.get("article_filter_type", "nmId"),
                    int(bool(config.get("disable_scope_filter", False))),
                    now,
                ),
            )
            conn.execute(
                "INSERT OR REPLACE INTO cabinet_credentials "
                "(cabinet_id, wb_api_token, wb_finance_token, wb_adv_token, "
                "ozon_client_id, ozon_api_key, "
                "ozon_performance_client_id, ozon_performance_client_secret) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    config["cabinet_id"],
                    config.get("wb_api_token", ""),
                    config.get("wb_finance_token", ""),
                    config.get("wb_adv_token", ""),
                    config.get("ozon_client_id", ""),
                    config.get("ozon_api_key", ""),
                    config.get("ozon_performance_client_id", ""),
                    config.get("ozon_performance_client_secret", ""),
                ),
            )

    def update_cabinet(self, cabinet_id: str, config: dict) -> None:
        with self._connect() as conn:
            meta_fields = ["name", "marketplace", "article_filter_type", "disable_scope_filter"]
            meta_sets = [(f, config[f]) for f in meta_fields if f in config]
            if meta_sets:
                sql = "UPDATE cabinets SET " + ", ".join(f"{f} = ?" for f, _ in meta_sets)
                sql += " WHERE cabinet_id = ?"
                conn.execute(sql, [v for _, v in meta_sets] + [cabinet_id])

            if config.get("pin"):
                conn.execute(
                    "UPDATE cabinets SET pin_hash = ? WHERE cabinet_id = ?",
                    (_hash_pin(str(config["pin"])), cabinet_id),
                )

            cred_fields = [
                "wb_api_token", "wb_finance_token", "wb_adv_token",
                "ozon_client_id", "ozon_api_key",
                "ozon_performance_client_id", "ozon_performance_client_secret",
            ]
            cred_sets = [(f, config[f]) for f in cred_fields if f in config]
            if cred_sets:
                # Ensure credentials row exists
                conn.execute(
                    "INSERT OR IGNORE INTO cabinet_credentials (cabinet_id) VALUES (?)",
                    (cabinet_id,),
                )
                sql = "UPDATE cabinet_credentials SET " + ", ".join(f"{f} = ?" for f, _ in cred_sets)
                sql += " WHERE cabinet_id = ?"
                conn.execute(sql, [v for _, v in cred_sets] + [cabinet_id])

    def delete_cabinet(self, cabinet_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cabinet_credentials WHERE cabinet_id = ?", (cabinet_id,))
            conn.execute("DELETE FROM cabinet_sessions WHERE cabinet_id = ?", (cabinet_id,))
            conn.execute("DELETE FROM cabinets WHERE cabinet_id = ?", (cabinet_id,))

    def verify_pin(self, cabinet_id: str, pin: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT pin_hash FROM cabinets WHERE cabinet_id = ?", (cabinet_id,)
            ).fetchone()
        if not row:
            return False
        expected = _hash_pin(pin)
        return hmac.compare_digest(expected, str(row["pin_hash"]))

    def verify_admin_pin(self, pin: str) -> bool:
        if not ADMIN_PIN:
            return False
        return hmac.compare_digest(pin.strip(), ADMIN_PIN.strip())

    def create_session(self, cabinet_id: str) -> str:
        token = secrets.token_urlsafe(32)
        expires_at = (datetime.now() + timedelta(hours=24)).isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO cabinet_sessions (token, cabinet_id, expires_at) VALUES (?, ?, ?)",
                (token, cabinet_id, expires_at),
            )
        return token

    def get_session_cabinet(self, token: str) -> dict | None:
        if not token:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cabinet_id, expires_at FROM cabinet_sessions WHERE token = ?",
                (token,),
            ).fetchone()
        if not row:
            return None
        try:
            if datetime.fromisoformat(row["expires_at"]) < datetime.now():
                return None
        except ValueError:
            return None
        return self.get_cabinet(row["cabinet_id"])

    def delete_session(self, token: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cabinet_sessions WHERE token = ?", (token,))

    def cleanup_expired_sessions(self) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM cabinet_sessions WHERE expires_at < ?",
                (datetime.now().isoformat(),),
            )

    def initialize_cabinet_db(self, cabinet_id: str, data_dir: Path) -> Path:
        """Create per-cabinet SQLite with WAL mode. Tables are created on first sync."""
        cabs_dir = data_dir / "cabs"
        cabs_dir.mkdir(parents=True, exist_ok=True)
        db_path = cabs_dir / f"{cabinet_id}.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.close()
        return db_path
