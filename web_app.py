"""Simple web UI for triggering Wildberries data sync."""
from __future__ import annotations

import base64
import binascii
import queue
import json
import sqlite3
import subprocess
import sys
import threading
import time
import tempfile
from decimal import Decimal, InvalidOperation
from html import escape
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from math import ceil
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urlparse

ROOT = Path(__file__).parent
PYTHON = sys.executable
DB_PATH = os.getenv("SQLITE_DB_PATH", str(ROOT / "data" / "wb_sync.db")).strip() or str(ROOT / "data" / "wb_sync.db")
SITE_PASSWORD = os.getenv("WB_SITE_PASSWORD", "321")
CORE_TABLES = [
  "finance_article_day_detail",
  "analytics_article_day",
  "analytics_day",
  "analytics_article_period",
  "buyout_order_day",
  "raw_stocks",
  "preliminary_order_economics",
]


def db_store():
  src_path = str(ROOT / "src")
  if src_path not in sys.path:
    sys.path.insert(0, src_path)
  from wb_gsheets.sqlite_store import SQLiteStore

  return SQLiteStore(DB_PATH)


REPORT_NAV_HTML = (
  '<a href="/">Обмен</a> · '
  '<a href="/analytics/day">Сводка по дням</a> · '
  '<a href="/analytics/period">Период по артикулам</a> · '
  '<a href="/analytics/article-day">Дни по артикулу</a> · '
  '<a href="/analytics/buyout-order-day">Выкупы по датам заказов</a> · '
  '<a href="/analytics/buyout-order-week">Выкупы по неделям</a> · '
  '<a href="/analytics/funnel-upload">Загрузка воронки</a> · '
  '<a href="/analytics/preliminary-economics">Предэко по дням</a> · '
  '<a href="/analytics/preliminary-economics-summary">Предэко за период</a> · '
  '<a href="/db?table=raw_stocks&page=1">Остатки</a> · '
  '<a href="/db">SQLite</a>'
)


REPORT_LINKS_HTML = (
  '<a href="/analytics/day">Сводка по дням</a>'
  '<a href="/analytics/period">Период по артикулам</a>'
  '<a href="/analytics/article-day">Дни по артикулу</a>'
  '<a href="/analytics/buyout-order-day">Выкупы по датам заказов</a>'
  '<a href="/analytics/buyout-order-week">Выкупы по неделям</a>'
  '<a href="/analytics/funnel-upload">Загрузка воронки</a>'
  '<a href="/analytics/preliminary-economics">Предэко по дням</a>'
  '<a href="/analytics/preliminary-economics-summary">Предэко за период</a>'
  '<a href="/db?table=raw_stocks&page=1">Остатки</a>'
  '<a href="/db">SQLite</a>'
)


def _db_connect() -> sqlite3.Connection:
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  return conn


def _to_float(value: object) -> float:
  if value is None:
    return 0.0
  text = str(value).strip().replace(" ", "").replace(",", ".")
  if not text:
    return 0.0
  try:
    return float(text)
  except ValueError:
    return 0.0


def _format_metric(column: str, value: object) -> str:
  if value is None:
    return ""
  text = str(value)
  if not text.strip():
    return ""

  if column in {"Артикул", "Дата", "Период с", "Период по"}:
    return text

  numeric = _to_float(value)
  if column in {"ДРР", "% маржи"}:
    return f"{int(round(numeric))}%"

  return f"{numeric:,.0f}".replace(",", " ")


def _format_percent(value: float) -> str:
  return f"{int(round(value))}%"


def _safe_percent(numerator: float, denominator: float, min_abs_den: float = 100.0) -> str:
  """Return percent string, or '—' when denominator is too close to zero to be meaningful."""
  if abs(denominator) < min_abs_den:
    return "—"
  return _format_percent(numerator / denominator * 100.0)


def _fetch_period_analytics(date_from: str, date_to: str, article_query: str = "") -> list[dict[str, str]]:
  where = ['"Дата" >= ?', '"Дата" <= ?']
  params: list[str] = [date_from, date_to]
  article_query = article_query.strip()
  if article_query:
    where.append('"Артикул" = ?')
    params.append(article_query)

  where_sql = f"WHERE {' AND '.join(where)}"

  with _db_connect() as conn:
    rows = conn.execute(
      (
        "SELECT \"Артикул\", "
        "SUM(\"Продажи по нашей цене\") AS \"Продажи по нашей цене\", "
        "SUM(\"Реклама\") AS \"Реклама\", "
        "SUM(\"Чистая прибыль\") AS \"Чистая прибыль\", "
        "CASE WHEN SUM(\"Продажи по нашей цене\") != 0 "
        "THEN SUM(\"Реклама\") / SUM(\"Продажи по нашей цене\") * 100 ELSE 0 END AS \"ДРР\", "
        "CASE WHEN SUM(\"Продажи по нашей цене\") != 0 "
        "THEN SUM(\"Чистая прибыль\") / SUM(\"Продажи по нашей цене\") * 100 ELSE 0 END AS \"% маржи\" "
        f"FROM analytics_article_day {where_sql} GROUP BY \"Артикул\" ORDER BY \"Артикул\" ASC LIMIT 5000"
      ),
      params,
    ).fetchall()

  if not rows:
    return []

  return [
    {column: _format_metric(column, row[idx]) for idx, column in enumerate(rows[0].keys())}
    for row in rows
  ]


def _fetch_article_day_analytics(article_query: str, date_from: str | None, date_to: str | None) -> list[dict[str, str]]:
  where = []
  params: list[str] = []
  article_query = article_query.strip()

  if article_query:
    where.append('"Артикул" = ?')
    params.append(article_query)
  if date_from:
    where.append('"Дата" >= ?')
    params.append(date_from)
  if date_to:
    where.append('"Дата" <= ?')
    params.append(date_to)

  where_sql = f"WHERE {' AND '.join(where)}" if where else ""
  sql = (
    "SELECT \"Артикул\", \"Дата\", \"Продажи по нашей цене\", \"Реклама\", \"Чистая прибыль\", \"ДРР\", \"% маржи\" "
    f"FROM analytics_article_day {where_sql} ORDER BY \"Дата\" DESC, \"Артикул\" ASC LIMIT 500"
  )

  with _db_connect() as conn:
    rows = conn.execute(sql, params).fetchall()
    if article_query and not rows:
      fallback_where = []
      fallback_params: list[str] = []
      fallback_where.append('"Артикул" LIKE ?')
      fallback_params.append(f"%{article_query}%")
      if date_from:
        fallback_where.append('"Дата" >= ?')
        fallback_params.append(date_from)
      if date_to:
        fallback_where.append('"Дата" <= ?')
        fallback_params.append(date_to)
      fallback_sql = (
        "SELECT \"Артикул\", \"Дата\", \"Продажи по нашей цене\", \"Реклама\", \"Чистая прибыль\", \"ДРР\", \"% маржи\" "
        f"FROM analytics_article_day WHERE {' AND '.join(fallback_where)} ORDER BY \"Дата\" DESC, \"Артикул\" ASC LIMIT 500"
      )
      rows = conn.execute(fallback_sql, fallback_params).fetchall()

  payload: list[dict[str, str]] = []
  for row in rows:
    payload.append(
      {
        "Артикул": _format_metric("Артикул", row["Артикул"]),
        "Дата": _format_metric("Дата", row["Дата"]),
        "Продажи по нашей цене": _format_metric("Продажи по нашей цене", row["Продажи по нашей цене"]),
        "Реклама": _format_metric("Реклама", row["Реклама"]),
        "Чистая прибыль": _format_metric("Чистая прибыль", row["Чистая прибыль"]),
        "ДРР": _format_metric("ДРР", row["ДРР"]),
        "% маржи": _format_metric("% маржи", row["% маржи"]),
      }
    )
  return payload


def _fetch_day_analytics(date_from: str, date_to: str) -> dict[str, object]:
  with _db_connect() as conn:
    rows = conn.execute(
      """
      SELECT
        "Дата",
        "Продажи по нашей цене, р",
        "Реклама, р",
        "Чистая прибыль",
        "ДРР",
        "% маржи"
      FROM analytics_day
      WHERE "Дата" >= ? AND "Дата" <= ?
      ORDER BY "Дата" DESC
      """,
      (date_from, date_to),
    ).fetchall()

  payload_rows: list[dict[str, str]] = []
  total_sales = 0.0
  total_ads = 0.0
  total_profit = 0.0

  for row in rows:
    sales = _to_float(row["Продажи по нашей цене, р"])
    ads = _to_float(row["Реклама, р"])
    profit = _to_float(row["Чистая прибыль"])
    total_sales += sales
    total_ads += ads
    total_profit += profit
    payload_rows.append(
      {
        "Дата": _format_metric("Дата", row["Дата"]),
        "Продажи": _format_metric("Продажи", sales),
        "Реклама": _format_metric("Реклама", ads),
        "Чистая прибыль": _format_metric("Чистая прибыль", profit),
        "ДРР": _format_metric("ДРР", row["ДРР"]),
        "% маржи": _format_metric("% маржи", row["% маржи"]),
      }
    )

  total_drr = (total_ads / total_sales * 100.0) if total_sales else 0.0
  total_margin = (total_profit / total_sales * 100.0) if total_sales else 0.0
  summary = {
    "days": str(len(payload_rows)),
    "sales": _format_metric("Продажи", total_sales),
    "ads": _format_metric("Реклама", total_ads),
    "profit": _format_metric("Чистая прибыль", total_profit),
    "drr": _format_metric("ДРР", total_drr),
    "margin": _format_metric("% маржи", total_margin),
  }
  return {"rows": payload_rows, "summary": summary}


def _date_range_limited(date_from: str, date_to: str, max_days: int = 30) -> tuple[list[str], str, str]:
  start = date.fromisoformat(date_from)
  end = date.fromisoformat(date_to)
  if start > end:
    start, end = end, start
  if (end - start).days + 1 > max_days:
    start = end - timedelta(days=max_days - 1)
  dates = [(start + timedelta(days=idx)).isoformat() for idx in range((end - start).days + 1)]
  return dates, start.isoformat(), end.isoformat()


def _week_label(day: str, effective_from: str, effective_to: str) -> str:
  current = date.fromisoformat(day)
  week_start = current - timedelta(days=current.weekday())
  week_end = week_start + timedelta(days=6)
  start = max(week_start, date.fromisoformat(effective_from))
  end = min(week_end, date.fromisoformat(effective_to))
  return f"{start.isoformat()}..{end.isoformat()}"


def _aggregate_daily_dicts_by_week(rows: list[dict[str, object]], effective_from: str, effective_to: str) -> list[dict[str, object]]:
  buckets: dict[str, dict[str, object]] = {}
  for row in rows:
    label = _week_label(str(row.get("Дата", "")), effective_from, effective_to)
    bucket = buckets.setdefault(label, {"Дата": label})
    for key, value in row.items():
      if key == "Дата":
        continue
      bucket[key] = _to_float(bucket.get(key, 0.0)) + _to_float(value)
  return [buckets[key] for key in sorted(buckets)]


def _aggregate_daily_values_by_week(
  values_by_date: dict[str, float],
  daily_dates: list[str],
  effective_from: str,
  effective_to: str,
) -> dict[str, float]:
  result: dict[str, float] = {}
  for day in daily_dates:
    label = _week_label(day, effective_from, effective_to)
    result[label] = result.get(label, 0.0) + float(values_by_date.get(day, 0.0))
  return result


def _aggregate_nested_daily_values_by_week(
  values_by_date: dict[str, dict[str, float]],
  daily_dates: list[str],
  effective_from: str,
  effective_to: str,
) -> dict[str, dict[str, float]]:
  result: dict[str, dict[str, float]] = {}
  for day in daily_dates:
    label = _week_label(day, effective_from, effective_to)
    bucket = result.setdefault(label, {})
    for key, value in values_by_date.get(day, {}).items():
      bucket[key] = bucket.get(key, 0.0) + float(value)
  return result


def _buyout_subject_join() -> str:
  return (
    "LEFT JOIN ("
    "SELECT "
    "\"Артикул WB\" AS nmid, "
    "MIN(\"Предмет\") AS subject "
    "FROM sku WHERE TRIM(COALESCE(\"Артикул WB\", '')) != '' GROUP BY \"Артикул WB\""
    ") sku_subject ON sku_subject.nmid = buyout_order_day.\"nmId\""
  )


def _buyout_cogs_join(conn: sqlite3.Connection) -> str:
  columns = _table_columns(conn, "sku")
  nmid_col = _first_existing(columns, ["Артикул WB", "nmId", "nm_id"])
  cogs_col = _first_existing(columns, ["себестоимость", "cost_price", "cogs"])
  if nmid_col and cogs_col:
    return (
      "LEFT JOIN ("
      f"SELECT {_sql_ident(nmid_col)} AS nmid, "
      f"MAX(CAST(REPLACE(REPLACE({_sql_ident(cogs_col)}, ' ', ''), ',', '.') AS REAL)) AS cogs "
      "FROM sku "
      f"WHERE TRIM(COALESCE({_sql_ident(nmid_col)}, '')) != '' "
      f"GROUP BY {_sql_ident(nmid_col)}"
      ") cogs_data ON cogs_data.nmid = buyout_order_day.\"nmId\""
    )
  return "LEFT JOIN (SELECT NULL AS nmid, 0 AS cogs WHERE 0) cogs_data ON 1 = 0"


def _stocks_join(conn: sqlite3.Connection) -> str:
  columns = _table_columns(conn, "raw_stocks")
  if {"nmId", "quantity"}.issubset(columns):
    return (
      "LEFT JOIN ("
      "SELECT \"nmId\" AS nmid, "
      "SUM(CAST(REPLACE(REPLACE(\"quantity\", ' ', ''), ',', '.') AS REAL)) AS stock "
      "FROM raw_stocks WHERE TRIM(COALESCE(\"nmId\", '')) != '' GROUP BY \"nmId\""
      ") stock_data ON stock_data.nmid = buyout_order_day.\"nmId\""
    )
  return "LEFT JOIN (SELECT NULL AS nmid, NULL AS stock WHERE 0) stock_data ON 1 = 0"


def _buyout_nm_join(conn: sqlite3.Connection) -> str:
  columns = _table_columns(conn, "sku")
  article_col = _first_existing(columns, ["НАШ", "Артикул поставщика", "supplierArticle", "SKU", "sku"])
  nmid_col = _first_existing(columns, ["Артикул WB", "nmId", "nm_id"])
  if article_col and nmid_col:
    # Берем nmId из SKU-таблицы, чтобы построить прямую ссылку на карточку WB.
    return (
      "LEFT JOIN ("
      f"SELECT {_sql_ident(nmid_col)} AS nmid, MIN({_sql_ident(article_col)}) AS article "
      "FROM sku "
      f"WHERE TRIM(COALESCE({_sql_ident(nmid_col)}, '')) != '' "
      f"GROUP BY {_sql_ident(nmid_col)}"
      ") nm_data ON nm_data.nmid = buyout_order_day.\"nmId\""
    )
  return "LEFT JOIN (SELECT NULL AS article, NULL AS nmid WHERE 0) nm_data ON 1 = 0"


def _fetch_buyout_subjects(date_from: str, date_to: str) -> list[str]:
  dates, effective_from, effective_to = _date_range_limited(date_from, date_to, max_days=30)
  with _db_connect() as conn:
    rows = conn.execute(
      (
        "SELECT sku_subject.subject AS subject, SUM(CAST(buyout_order_day.\"Сумма заказов\" AS REAL)) AS orders_sum "
        "FROM buyout_order_day "
        f"{_buyout_subject_join()} "
        "WHERE buyout_order_day.\"Дата\" >= ? AND buyout_order_day.\"Дата\" <= ? "
        "AND TRIM(COALESCE(sku_subject.subject, '')) != '' "
        "GROUP BY sku_subject.subject "
        "ORDER BY orders_sum DESC, sku_subject.subject ASC "
        "LIMIT 1000"
      ),
      (effective_from, effective_to),
    ).fetchall()
  return [str(row["subject"]) for row in rows if str(row["subject"]).strip()]


def _fetch_buyout_articles(date_from: str, date_to: str, subject: str = "") -> list[dict[str, object]]:
  dates, effective_from, effective_to = _date_range_limited(date_from, date_to, max_days=30)
  where = ['buyout_order_day."Дата" >= ?', 'buyout_order_day."Дата" <= ?']
  params: list[str] = [effective_from, effective_to]
  subject = subject.strip()
  if subject:
    where.append("sku_subject.subject = ?")
    params.append(subject)
  with _db_connect() as conn:
    stocks_join = _stocks_join(conn)
    nm_join = _buyout_nm_join(conn)
    rows = conn.execute(
      (
        "WITH revenue AS ("
        "SELECT \"Артикул\" AS article, SUM(CAST(\"Продажи по нашей цене\" AS REAL)) AS revenue "
        "FROM analytics_article_day WHERE \"Дата\" >= ? AND \"Дата\" <= ? GROUP BY \"Артикул\""
        ") "
        "SELECT "
        "buyout_order_day.\"nmId\" AS nmid, "
        "MAX(buyout_order_day.\"Артикул\") AS \"Артикул\", "
        "MAX(stock_data.stock) AS stock, "
        "COALESCE(MAX(revenue.revenue), 0) AS revenue, "
        "SUM(CAST(buyout_order_day.\"Сумма заказов\" AS REAL)) AS orders_sum "
        "FROM buyout_order_day "
        f"{_buyout_subject_join()} "
        f"{stocks_join} "
        f"{nm_join} "
        "LEFT JOIN revenue ON revenue.article = buyout_order_day.\"Артикул\" "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY buyout_order_day.\"nmId\" "
        "ORDER BY revenue DESC, orders_sum DESC, buyout_order_day.\"Артикул\" ASC "
        "LIMIT 1000"
      ),
      [effective_from, effective_to, *params],
    ).fetchall()
  result = []
  for row in rows:
    article = str(row["Артикул"]).strip()
    if not article:
      continue
    result.append({
      "article": article,
      "nmid": str(row["nmid"]).strip() if row["nmid"] is not None else "",
      "stock": _to_float(row["stock"]) if row["stock"] is not None else None,
      "revenue": _to_float(row["revenue"]),
    })
  return result


def _fetch_buyout_order_day_pivot(
  date_from: str,
  date_to: str,
  article_query: str = "",
  articles: list[str] | None = None,
  subject: str = "",
  granularity: str = "day",
) -> dict[str, object]:
  dates, effective_from, effective_to = _date_range_limited(date_from, date_to, max_days=93 if granularity == "week" else 30)
  daily_dates = list(dates)
  where = ['buyout_order_day."Дата" >= ?', 'buyout_order_day."Дата" <= ?']
  params: list[str] = [effective_from, effective_to]
  article_query = article_query.strip()
  subject = subject.strip()
  selected_articles = [article.strip() for article in (articles or []) if article.strip()]
  has_article_filter = bool(selected_articles or article_query or subject)
  if selected_articles:
    placeholders = ", ".join("?" for _ in selected_articles)
    where.append(f'buyout_order_day."nmId" IN ({placeholders})')
    params.extend(selected_articles)
  elif article_query:
    if article_query.isdigit():
      where.append('buyout_order_day."nmId" = ?')
      params.append(article_query)
    else:
      where.append('buyout_order_day."Артикул" = ?')
      params.append(article_query)
  elif subject:
    where.append("sku_subject.subject = ?")
    params.append(subject)

  where_sql = f"WHERE {' AND '.join(where)}"
  with _db_connect() as conn:
    spp_without_by_date: dict[str, float] = {}
    spp_with_by_date: dict[str, float] = {}
    cogs_join = _buyout_cogs_join(conn)

    raw_sales_columns = _table_columns(conn, "raw_sales")
    if {
      "saleDt",
      "docTypeName",
      "quantity",
      "retailPriceWithDisc",
      "retailAmount",
      "nmId",
    }.issubset(raw_sales_columns):
      raw_where = [
        "substr(raw_sales.saleDt, 1, 10) >= ?",
        "substr(raw_sales.saleDt, 1, 10) <= ?",
        "raw_sales.docTypeName IN ('Продажа', 'Возврат')",
      ]
      raw_params: list[str] = [effective_from, effective_to]
      if selected_articles:
        placeholders = ", ".join("?" for _ in selected_articles)
        raw_where.append(f"raw_sales.nmId IN ({placeholders})")
        raw_params.extend(selected_articles)
      elif article_query:
        if article_query.isdigit():
          raw_where.append("raw_sales.nmId = ?")
          raw_params.append(article_query)
        else:
          raw_where.append("raw_sales.vendorCode = ?")
          raw_params.append(article_query)
      elif subject:
        raw_where.append(
          "raw_sales.nmId IN ("
          "SELECT TRIM(\"Артикул WB\") FROM SKU "
          "WHERE TRIM(COALESCE(\"Артикул WB\", '')) != '' AND \"Предмет\" = ?"
          ")"
        )
        raw_params.append(subject)
      else:
        raw_where.append(
          "raw_sales.nmId IN ("
          "SELECT TRIM(\"Артикул WB\") FROM SKU WHERE TRIM(COALESCE(\"Артикул WB\", '')) != ''"
          ")"
        )
      spp_rows = conn.execute(
        (
          "SELECT "
          "substr(raw_sales.saleDt, 1, 10) AS sale_date, "
          "SUM(CASE raw_sales.docTypeName WHEN 'Возврат' THEN -1 ELSE 1 END "
          "* CAST(raw_sales.retailPriceWithDisc AS REAL) * CAST(raw_sales.quantity AS REAL)) AS without_spp, "
          "SUM(CASE raw_sales.docTypeName WHEN 'Возврат' THEN -1 ELSE 1 END "
          "* CAST(raw_sales.retailAmount AS REAL)) AS with_spp "
          "FROM raw_sales "
          f"WHERE {' AND '.join(raw_where)} "
          "GROUP BY sale_date ORDER BY sale_date ASC"
        ),
        raw_params,
      ).fetchall()
      spp_without_by_date = {
        str(row["sale_date"]): _to_float(row["without_spp"])
        for row in spp_rows
      }
      spp_with_by_date = {
        str(row["sale_date"]): _to_float(row["with_spp"])
        for row in spp_rows
      }

    funnel_where = ["date >= ?", "date <= ?"]
    funnel_params: list[str] = [effective_from, effective_to]
    if selected_articles:
      placeholders = ", ".join("?" for _ in selected_articles)
      funnel_where.append(f"nmId IN ({placeholders})")
      funnel_params.extend(selected_articles)
    elif article_query:
      if article_query.isdigit():
        funnel_where.append("nmId = ?")
        funnel_params.append(article_query)
      else:
        funnel_where.append("supplierArticle = ?")
        funnel_params.append(article_query)
    elif subject:
      funnel_where.append(
        "nmId IN ("
        "SELECT TRIM(\"Артикул WB\") FROM SKU "
        "WHERE TRIM(COALESCE(\"Артикул WB\", '')) != '' AND \"Предмет\" = ?"
        ")"
      )
      funnel_params.append(subject)
    else:
      funnel_where.append(
        "nmId IN ("
        "SELECT TRIM(\"Артикул WB\") FROM SKU WHERE TRIM(COALESCE(\"Артикул WB\", '')) != ''"
        ")"
      )

    funnel_rows = conn.execute(
      (
        "SELECT date, "
        "SUM(CAST(openCount AS REAL)) AS open_count, "
        "SUM(CAST(cartCount AS REAL)) AS cart_count, "
        "SUM(CAST(orderCount AS REAL)) AS order_count, "
        "SUM(CAST(buyoutCount AS REAL)) AS buyout_count "
        "FROM funnel_analytics "
        f"WHERE {' AND '.join(funnel_where)} "
        "GROUP BY date ORDER BY date ASC"
      ),
      funnel_params,
    ).fetchall()
    funnel_by_date = {
      str(row["date"]): {
        "open_count": _to_float(row["open_count"]),
        "cart_count": _to_float(row["cart_count"]),
        "order_count": _to_float(row["order_count"]),
        "buyout_count": _to_float(row["buyout_count"]),
      }
      for row in funnel_rows
    }

    impression_by_date: dict[str, float] = {}
    uploaded_open_by_date: dict[str, float] = {}
    if "funnel_impressions_upload" in {
      str(row["name"])
      for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }:
      impression_where = ["date >= ?", "date <= ?"]
      impression_params: list[str] = [effective_from, effective_to]
      if selected_articles:
        placeholders = ", ".join("?" for _ in selected_articles)
        impression_where.append(f"nmId IN ({placeholders})")
        impression_params.extend(selected_articles)
      elif article_query:
        if article_query.isdigit():
          impression_where.append("nmId = ?")
          impression_params.append(article_query)
        else:
          impression_where.append("supplierArticle = ?")
          impression_params.append(article_query)
      elif subject:
        impression_where.append(
          "nmId IN ("
          "SELECT TRIM(\"Артикул WB\") FROM SKU "
          "WHERE TRIM(COALESCE(\"Артикул WB\", '')) != '' AND \"Предмет\" = ?"
          ")"
        )
        impression_params.append(subject)
      else:
        impression_where.append(
          "nmId IN ("
          "SELECT TRIM(\"Артикул WB\") FROM SKU WHERE TRIM(COALESCE(\"Артикул WB\", '')) != ''"
          ")"
        )
      impression_rows = conn.execute(
        (
          "SELECT date, "
          "SUM(CAST(impressions AS REAL)) AS impressions, "
          "SUM(CAST(openCount AS REAL)) AS open_count "
          "FROM funnel_impressions_upload "
          f"WHERE {' AND '.join(impression_where)} "
          "GROUP BY date ORDER BY date ASC"
        ),
        impression_params,
      ).fetchall()
      impression_by_date = {
        str(row["date"]): _to_float(row["impressions"])
        for row in impression_rows
      }
      uploaded_open_by_date = {
        str(row["date"]): _to_float(row["open_count"])
        for row in impression_rows
      }

    rows = conn.execute(
      (
        "SELECT "
        "buyout_order_day.\"Дата\", "
        "SUM(CAST(buyout_order_day.\"Сумма заказов\" AS REAL)) AS orders_sum, "
        "SUM(CAST(buyout_order_day.\"Сумма выкупов в эту дату\" AS REAL)) AS event_buyout_sum, "
        "SUM(CAST(buyout_order_day.\"Сумма выкупов товаров, заказанных в эту дату\" AS REAL)) AS ordered_buyout_sum, "
        "SUM(CAST(buyout_order_day.\"К перечислению от выкупов в эту дату\" AS REAL)) AS event_for_pay_sum, "
        "SUM(CAST(buyout_order_day.\"К перечислению от выкупов товаров, заказанных в эту дату\" AS REAL)) AS ordered_for_pay_sum, "
        "SUM(CAST(buyout_order_day.\"Реклама\" AS REAL)) AS ads, "
        "SUM(CAST(buyout_order_day.\"Выкупы в эту дату, шт\" AS REAL)) AS event_buyout_qty, "
        "SUM(CAST(buyout_order_day.\"Выкупы товаров, заказанных в эту дату, шт\" AS REAL)) AS ordered_buyout_qty, "
        "SUM(COALESCE(cogs_data.cogs, 0) * CAST(buyout_order_day.\"Выкупы в эту дату, шт\" AS REAL)) AS event_cogs_sum, "
        "SUM(COALESCE(cogs_data.cogs, 0) * CAST(buyout_order_day.\"Выкупы товаров, заказанных в эту дату, шт\" AS REAL)) AS ordered_cogs_sum, "
        "SUM(CAST(buyout_order_day.\"Заказы, шт\" AS REAL)) AS orders_qty, "
        "SUM(CAST(buyout_order_day.\"Продажи до возвратов, шт\" AS REAL)) AS gross_sale_qty, "
        "SUM(CAST(buyout_order_day.\"Сумма продаж до возвратов\" AS REAL)) AS gross_sale_sum, "
        "SUM(CAST(buyout_order_day.\"Возвраты, шт\" AS REAL)) AS return_qty, "
        "SUM(CAST(buyout_order_day.\"Сумма возвратов\" AS REAL)) AS return_sum "
        "FROM buyout_order_day "
        f"{_buyout_subject_join()} "
        f"{cogs_join} "
        f"{where_sql} GROUP BY buyout_order_day.\"Дата\" ORDER BY buyout_order_day.\"Дата\" ASC"
      ),
      params,
    ).fetchall()

    if article_query and not selected_articles and not rows:
      fallback_where = ['buyout_order_day."Дата" >= ?', 'buyout_order_day."Дата" <= ?']
      fallback_params = [effective_from, effective_to]
      if article_query.isdigit():
        fallback_where.append('buyout_order_day."nmId" = ?')
        fallback_params.append(article_query)
      else:
        fallback_where.append('buyout_order_day."Артикул" LIKE ?')
        fallback_params.append(f"%{article_query}%")
      rows = conn.execute(
        (
          "SELECT "
          "buyout_order_day.\"Дата\", "
          "SUM(CAST(buyout_order_day.\"Сумма заказов\" AS REAL)) AS orders_sum, "
          "SUM(CAST(buyout_order_day.\"Сумма выкупов в эту дату\" AS REAL)) AS event_buyout_sum, "
          "SUM(CAST(buyout_order_day.\"Сумма выкупов товаров, заказанных в эту дату\" AS REAL)) AS ordered_buyout_sum, "
          "SUM(CAST(buyout_order_day.\"К перечислению от выкупов в эту дату\" AS REAL)) AS event_for_pay_sum, "
          "SUM(CAST(buyout_order_day.\"К перечислению от выкупов товаров, заказанных в эту дату\" AS REAL)) AS ordered_for_pay_sum, "
          "SUM(CAST(buyout_order_day.\"Реклама\" AS REAL)) AS ads, "
          "SUM(CAST(buyout_order_day.\"Выкупы в эту дату, шт\" AS REAL)) AS event_buyout_qty, "
          "SUM(CAST(buyout_order_day.\"Выкупы товаров, заказанных в эту дату, шт\" AS REAL)) AS ordered_buyout_qty, "
          "SUM(COALESCE(cogs_data.cogs, 0) * CAST(buyout_order_day.\"Выкупы в эту дату, шт\" AS REAL)) AS event_cogs_sum, "
          "SUM(COALESCE(cogs_data.cogs, 0) * CAST(buyout_order_day.\"Выкупы товаров, заказанных в эту дату, шт\" AS REAL)) AS ordered_cogs_sum, "
          "SUM(CAST(buyout_order_day.\"Заказы, шт\" AS REAL)) AS orders_qty, "
          "SUM(CAST(buyout_order_day.\"Продажи до возвратов, шт\" AS REAL)) AS gross_sale_qty, "
          "SUM(CAST(buyout_order_day.\"Сумма продаж до возвратов\" AS REAL)) AS gross_sale_sum, "
          "SUM(CAST(buyout_order_day.\"Возвраты, шт\" AS REAL)) AS return_qty, "
          "SUM(CAST(buyout_order_day.\"Сумма возвратов\" AS REAL)) AS return_sum "
          "FROM buyout_order_day "
          f"{cogs_join} "
          f"WHERE {' AND '.join(fallback_where)} GROUP BY buyout_order_day.\"Дата\" ORDER BY buyout_order_day.\"Дата\" ASC"
        ),
        fallback_params,
      ).fetchall()

  row_dicts = [dict(row) for row in rows]
  if granularity == "week":
    dates = []
    for day in daily_dates:
      label = _week_label(day, effective_from, effective_to)
      if not dates or dates[-1] != label:
        dates.append(label)
    row_dicts = _aggregate_daily_dicts_by_week(row_dicts, effective_from, effective_to)
    spp_without_by_date = _aggregate_daily_values_by_week(spp_without_by_date, daily_dates, effective_from, effective_to)
    spp_with_by_date = _aggregate_daily_values_by_week(spp_with_by_date, daily_dates, effective_from, effective_to)
    impression_by_date = _aggregate_daily_values_by_week(impression_by_date, daily_dates, effective_from, effective_to)
    uploaded_open_by_date = _aggregate_daily_values_by_week(uploaded_open_by_date, daily_dates, effective_from, effective_to)
    funnel_by_date = _aggregate_nested_daily_values_by_week(funnel_by_date, daily_dates, effective_from, effective_to)

  by_date = {str(row["Дата"]): row for row in row_dicts}
  def day_value(day: str, key: str) -> float:
    row = by_date.get(day)
    return _to_float(row[key]) if row else 0.0

  def format_number(raw: float) -> str:
    return f"{int(round(raw)):,}".replace(",", " ")

  def format_money(label: str, raw: float) -> str:
    return _format_metric(label, raw)

  def format_percent(raw: float) -> str:
    return _format_percent(raw)

  def format_ratio(numerator: float, denominator: float) -> str:
    return _format_percent(numerator / denominator * 100.0) if denominator else "—"

  def format_unit_cost(cost: float, count: float) -> str:
    return f"{(cost / count):,.2f}".replace(",", " ").replace(".", ",") if count else "—"

  metrics = [
    ("Суммы", "Сумма заказов", "orders_sum", "money", "income"),
    ("Суммы", "Сумма выкупов в эту дату", "event_buyout_sum", "money", "income"),
    ("Суммы", "Сумма выкупов товаров, заказанных в эту дату", "ordered_buyout_sum", "money", "income"),
    ("Штуки", "Заказы, шт", "orders_qty", "number", "income"),
    ("Штуки", "Выкупы в эту дату, шт", "event_buyout_qty", "number", "income"),
    ("Штуки", "Выкупы товаров, заказанных в эту дату, шт", "ordered_buyout_qty", "number", "income"),
  ]
  pivot_rows: list[dict[str, object]] = []
  for group, label, key, kind, color_kind in metrics:
    values = []
    raw_values = []
    total = 0.0
    for day in dates:
      raw = day_value(day, key)
      total += raw
      raw_values.append(raw)
      if kind == "number":
        values.append(format_number(raw))
      else:
        values.append(format_money(label, raw))
    total_value = (
      format_number(total) if kind == "number"
      else format_money(label, total)
    )
    pivot_rows.append({"group": group, "metric": label, "total": total_value, "values": values, "raw_values": raw_values, "kind": color_kind})

  average_check_values = []
  total_orders_sum = 0.0
  total_orders_qty_for_average = 0.0
  for day in dates:
    orders_sum = day_value(day, "orders_sum")
    orders_qty = day_value(day, "orders_qty")
    total_orders_sum += orders_sum
    total_orders_qty_for_average += orders_qty
    average_check = orders_sum / orders_qty if orders_qty else 0.0
    average_check_values.append(format_money("Средний чек", average_check))
  average_check_raw = []
  for day in dates:
    orders_sum_v = day_value(day, "orders_sum")
    orders_qty_v = day_value(day, "orders_qty")
    average_check_raw.append(orders_sum_v / orders_qty_v if orders_qty_v else 0.0)
  pivot_rows.append({
    "group": "Средние показатели",
    "metric": "Средний чек",
    "total": format_money(
      "Средний чек",
      total_orders_sum / total_orders_qty_for_average if total_orders_qty_for_average else 0.0,
    ),
    "values": average_check_values,
    "raw_values": average_check_raw,
    "kind": "income",
    "color_mode": "absolute",
    "color_threshold": 10,
  })

  spp_values = []
  spp_raw = []
  total_without_spp = 0.0
  total_with_spp = 0.0
  for day in dates:
    without_spp = spp_without_by_date.get(day, 0.0)
    with_spp = spp_with_by_date.get(day, 0.0)
    total_without_spp += without_spp
    total_with_spp += with_spp
    spp_percent = (without_spp - with_spp) / without_spp * 100.0 if without_spp else 0.0
    spp_values.append(format_percent(spp_percent))
    spp_raw.append(spp_percent)
  total_spp_percent = (
    (total_without_spp - total_with_spp) / total_without_spp * 100.0
    if total_without_spp else 0.0
  )
  pivot_rows.append({
    "group": "Средние показатели",
    "metric": "СПП",
    "total": format_percent(total_spp_percent),
    "values": spp_values,
    "raw_values": spp_raw,
    "kind": "percent_expense",
    "color_threshold": 3,
  })

  def funnel_value(day: str, key: str) -> float:
    return float(funnel_by_date.get(day, {}).get(key, 0.0))

  def open_value(day: str) -> float:
    uploaded_value = uploaded_open_by_date.get(day)
    if uploaded_value is not None:
      return uploaded_value
    return funnel_value(day, "open_count")

  funnel_specs = [
    ("Воронка", "Показы", lambda day: impression_by_date.get(day, 0.0), "number", "income"),
    ("Воронка", "Переходы", lambda day: open_value(day), "number", "income"),
    ("Воронка", "CTR", lambda day: (open_value(day), impression_by_date.get(day, 0.0)), "percent_ratio", "percent_income"),
    ("Воронка", "Корзины", lambda day: funnel_value(day, "cart_count"), "number", "income"),
    ("Воронка", "CR1", lambda day: (funnel_value(day, "cart_count"), open_value(day)), "percent_ratio", "percent_income"),
    ("Воронка", "Заказы", lambda day: funnel_value(day, "order_count"), "number", "income"),
    ("Воронка", "CR2", lambda day: (funnel_value(day, "order_count"), funnel_value(day, "cart_count")), "percent_ratio", "percent_income"),
    ("Воронка", "% выкупа", lambda day: (funnel_value(day, "buyout_count"), funnel_value(day, "order_count")), "percent_ratio", "percent_income"),
    ("Стоимость воронки", "Цена показа", lambda day: (day_value(day, "ads"), impression_by_date.get(day, 0.0)), "money_ratio", "expense"),
    ("Стоимость воронки", "Цена клика", lambda day: (day_value(day, "ads"), open_value(day)), "money_ratio", "expense"),
    ("Стоимость воронки", "Цена корзины", lambda day: (day_value(day, "ads"), funnel_value(day, "cart_count")), "money_ratio", "expense"),
    ("Стоимость воронки", "Цена заказа", lambda day: (day_value(day, "ads"), funnel_value(day, "order_count")), "money_ratio", "expense"),
  ]
  for group, label, value_fn, value_kind, color_kind in funnel_specs:
    values = []
    raw_values = []
    total_numerator = 0.0
    total_denominator = 0.0
    total_value_raw = 0.0
    for day in dates:
      raw = value_fn(day)
      if value_kind in {"percent_ratio", "money_ratio"}:
        numerator, denominator = raw
        if denominator:
          total_numerator += numerator
          total_denominator += denominator
        raw_value = numerator / denominator * (100.0 if value_kind == "percent_ratio" else 1.0) if denominator else None
        raw_values.append(raw_value)
        values.append(format_ratio(numerator, denominator) if value_kind == "percent_ratio" else format_unit_cost(numerator, denominator))
      else:
        total_value_raw += raw
        raw_values.append(raw)
        values.append(format_number(raw))
    if value_kind == "percent_ratio":
      total_value = format_ratio(total_numerator, total_denominator)
    elif value_kind == "money_ratio":
      total_value = format_unit_cost(total_numerator, total_denominator)
    else:
      total_value = format_number(total_value_raw)
    pivot_rows.append({"group": group, "metric": label, "total": total_value, "values": values, "raw_values": raw_values, "kind": color_kind})

  expense_metrics = [
    ("Расходы", "Реклама", "ads", "money", "expense"),
  ]
  for group, label, key, kind, color_kind in expense_metrics:
    values = []
    raw_values = []
    total = 0.0
    for day in dates:
      raw = day_value(day, key)
      total += raw
      raw_values.append(raw)
      values.append(format_number(raw) if kind == "number" else format_money(label, raw))
    total_value = format_number(total) if kind == "number" else format_money(label, total)
    pivot_rows.append({"group": group, "metric": label, "total": total_value, "values": values, "raw_values": raw_values, "kind": color_kind})

  derived_metrics = [
    (
      "Расходы",
      "ДРР от заказов",
      lambda row, ads: ads,
      lambda total_num, total_den: _safe_percent(total_num, total_den),
      lambda num, den: _safe_percent(num, den),
      "orders_sum",
      "percent_expense",
    ),
    (
      "Расходы",
      "ДРР от выкупов в эту дату",
      lambda row, ads: ads,
      lambda total_num, total_den: _safe_percent(total_num, total_den),
      lambda num, den: _safe_percent(num, den),
      "event_buyout_sum",
      "percent_expense",
    ),
    (
      "Расходы",
      "ДРР от выкупов заказанных товаров",
      lambda row, ads: ads,
      lambda total_num, total_den: _safe_percent(total_num, total_den),
      lambda num, den: _safe_percent(num, den),
      "ordered_buyout_sum",
      "percent_expense",
    ),
    (
      "Доходность",
      "Прибыль от выкупов в эту дату",
      lambda row, ads: _to_float(row["event_for_pay_sum"]) - ads - _to_float(row["event_cogs_sum"]) if row else -ads,
      lambda total_num, total_den: format_money("Прибыль от выкупов в эту дату", total_num),
      lambda num, den: format_money("Прибыль от выкупов в эту дату", num),
      "",
      "income",
    ),
    (
      "Доходность",
      "Прибыль от выкупов по заказам",
      lambda row, ads: _to_float(row["ordered_for_pay_sum"]) - ads - _to_float(row["ordered_cogs_sum"]) if row else -ads,
      lambda total_num, total_den: format_money("Прибыль от выкупов по заказам", total_num),
      lambda num, den: format_money("Прибыль от выкупов по заказам", num),
      "",
      "income",
    ),
    (
      "Доходность",
      "Маржинальность от выкупов за дату",
      lambda row, ads: (_to_float(row["event_for_pay_sum"]) - ads - _to_float(row["event_cogs_sum"])) if row else -ads,
      lambda total_num, total_den: _safe_percent(total_num, total_den),
      lambda num, den: _safe_percent(num, den),
      "event_buyout_sum",
      "percent_income",
    ),
    (
      "Доходность",
      "Маржинальность от выкупов по заказам",
      lambda row, ads: (_to_float(row["ordered_for_pay_sum"]) - ads - _to_float(row["ordered_cogs_sum"])) if row else -ads,
      lambda total_num, total_den: _safe_percent(total_num, total_den),
      lambda num, den: _safe_percent(num, den),
      "ordered_buyout_sum",
      "percent_income",
    ),
  ]

  for metric in derived_metrics:
    group, label, numerator_fn, total_formatter, value_formatter, denominator_name, color_kind = metric
    values = []
    raw_values = []
    total_numerator = 0.0
    total_denominator = 0.0
    for day in dates:
      row = by_date.get(day)
      ads = day_value(day, "ads")
      numerator = numerator_fn(row, ads)
      denominator = day_value(day, denominator_name) if denominator_name else 0.0
      total_numerator += numerator
      total_denominator += denominator
      raw_values.append(numerator if not denominator_name else (numerator / denominator * 100.0 if abs(denominator) >= 100.0 else None))
      values.append(value_formatter(numerator, denominator))
    total_value = total_formatter(total_numerator, total_denominator)
    pivot_rows.append({"group": group, "metric": label, "total": total_value, "values": values, "raw_values": raw_values, "kind": color_kind})

  return {
    "dates": dates,
    "rows": pivot_rows,
    "effective_from": effective_from,
    "effective_to": effective_to,
    "max_days": 93 if granularity == "week" else 30,
    "granularity": granularity,
  }


def _sql_ident(name: str) -> str:
  return '"' + name.replace('"', '""') + '"'


def _first_existing(columns: set[str], candidates: list[str]) -> str | None:
  for candidate in candidates:
    if candidate in columns:
      return candidate
  return None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
  safe_table = table_name.replace('"', '""')
  rows = conn.execute(f'PRAGMA table_info("{safe_table}")').fetchall()
  return {str(row["name"]) for row in rows}


def _sum_expr(columns: set[str], candidates: list[str]) -> tuple[str, list[str]]:
  used = [candidate for candidate in candidates if candidate in columns]
  if not used:
    return "0", []
  expr = " + ".join(f"COALESCE(SUM({_sql_ident(col)}), 0)" for col in used)
  return expr, used


def _ensure_preliminary_economics_tables(conn: sqlite3.Connection) -> None:
  conn.execute(
    """
    CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
  )
  conn.execute(
    """
    CREATE TABLE IF NOT EXISTS preliminary_order_economics (
      sku TEXT NOT NULL,
      date TEXT NOT NULL,
      orders_count REAL NOT NULL,
      orders_sum REAL NOT NULL,
      commission_rub REAL NOT NULL,
      acquiring_rub REAL NOT NULL,
      advertising_rub REAL NOT NULL,
      additional_expenses_rub REAL NOT NULL,
      preliminary_profit_rub REAL NOT NULL,
      additional_rate REAL NOT NULL,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (sku, date)
    )
    """
  )


def _get_or_set_preliminary_additional_rate(
  conn: sqlite3.Connection,
  columns: set[str],
  sales_column: str,
) -> tuple[float, list[str]]:
  setting_key = "preliminary_additional_rate"
  stored = conn.execute("SELECT value FROM app_settings WHERE key = ?", (setting_key,)).fetchone()
  if stored:
    try:
      rate = max(0.0, float(stored["value"]))
      return rate, []
    except ValueError:
      pass

  additional_candidates = [
    "Логистика",
    "Хранение",
    "Приемка",
    "Штрафы",
    "Удержания",
    "Доплаты",
    "НДС WB",
    "Платная приемка",
    "Платное хранение",
    "Платные услуги",
  ]
  additional_expr, used_columns = _sum_expr(columns, additional_candidates)
  sales_expr = f"COALESCE(SUM({_sql_ident(sales_column)}), 0)"
  row = conn.execute(
    f"SELECT CASE WHEN {sales_expr} > 0 THEN ({additional_expr}) / {sales_expr} ELSE 0 END AS rate "
    "FROM finance_article_day_detail"
  ).fetchone()
  rate = _to_float(row["rate"] if row else 0.0)
  if rate <= 0:
    rate = 0.05

  conn.execute(
    """
    INSERT INTO app_settings(key, value, updated_at)
    VALUES(?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP
    """,
    (setting_key, f"{rate:.8f}"),
  )
  return rate, used_columns


def _fetch_preliminary_economics(
  date_from: str,
  date_to: str,
  article_query: str = "",
  buyout_percent: float = 30.0,
  aggregate_by_period: bool = False,
) -> dict[str, object]:
  article_query = article_query.strip()
  buyout_percent = max(0.0, min(100.0, buyout_percent))
  buyout_factor = buyout_percent / 100.0
  with _db_connect() as conn:
    _ensure_preliminary_economics_tables(conn)
    columns = _table_columns(conn, "finance_article_day_detail")

    sku_col = _first_existing(columns, ["Артикул", "SKU"])
    date_col = _first_existing(columns, ["Дата", "date"])
    orders_count_col = _first_existing(columns, ["Продажи, шт", "Заказы, шт", "Заказано, шт", "Количество заказов"])
    sales_col = _first_existing(columns, ["Наша цена", "Продажи по нашей цене", "Продажи по нашей цене, р"])
    if not sku_col or not date_col or not sales_col:
      return {"rows": [], "additional_rate": "0.00%", "expense_components": []}

    commission_expr, _ = _sum_expr(columns, ["Комиссия WB", "Вознаграждение WB"])
    ads_expr, _ = _sum_expr(columns, ["Реклама", "Рекламные расходы"])
    additional_rate, used_additional_cols = _get_or_set_preliminary_additional_rate(conn, columns, sales_col)

    where = [f"{_sql_ident(date_col)} >= ?", f"{_sql_ident(date_col)} <= ?"]
    params: list[str] = [date_from, date_to]
    if article_query:
      where.append(f"{_sql_ident(sku_col)} = ?")
      params.append(article_query)

    orders_count_expr = f"COALESCE(SUM({_sql_ident(orders_count_col)}), 0)" if orders_count_col else "0"
    select_date = f"{_sql_ident(date_col)} AS date, " if not aggregate_by_period else ""
    group_by = "GROUP BY sku, date" if not aggregate_by_period else "GROUP BY sku"
    order_by = "ORDER BY date DESC, sku ASC" if not aggregate_by_period else "ORDER BY sku ASC"
    sql = (
      "SELECT "
      f"{_sql_ident(sku_col)} AS sku, "
      f"{select_date}"
      f"{orders_count_expr} AS orders_count, "
      f"COALESCE(SUM({_sql_ident(sales_col)}), 0) AS orders_sum, "
      f"{commission_expr} AS commission_rub, "
      f"{ads_expr} AS advertising_rub "
      "FROM finance_article_day_detail "
      f"WHERE {' AND '.join(where)} "
      f"{group_by} "
      f"{order_by} "
      "LIMIT 10000"
    )
    base_rows = conn.execute(sql, params).fetchall()

    funnel_where = ["date >= ?", "date <= ?"]
    funnel_params: list[str] = [date_from, date_to]
    if article_query:
      funnel_where.append("supplierArticle = ?")
      funnel_params.append(article_query)
    funnel_select_date = "date, " if not aggregate_by_period else ""
    funnel_group_by = "GROUP BY date, supplierArticle" if not aggregate_by_period else "GROUP BY supplierArticle"
    funnel_rows = conn.execute(
      (
        f"SELECT {funnel_select_date} supplierArticle AS sku, "
        "SUM(COALESCE(CAST(orderCount AS REAL), 0)) AS funnel_orders_count "
        "FROM funnel_analytics "
        f"WHERE {' AND '.join(funnel_where)} "
        f"{funnel_group_by}"
      ),
      funnel_params,
    ).fetchall()
    if aggregate_by_period:
      funnel_orders_map = {
        str(row["sku"]): _to_float(row["funnel_orders_count"])
        for row in funnel_rows
      }
    else:
      funnel_orders_map = {
        (str(row["sku"]), str(row["date"])): _to_float(row["funnel_orders_count"])
        for row in funnel_rows
      }

    payload: list[dict[str, str]] = []
    to_store: list[tuple[object, ...]] = []
    for row in base_rows:
      sku = str(row["sku"])
      row_date = str(row["date"]) if not aggregate_by_period else f"{date_from}..{date_to}"
      finance_orders_count = _to_float(row["orders_count"])
      if aggregate_by_period:
        orders_count = funnel_orders_map.get(sku, finance_orders_count)
      else:
        orders_count = funnel_orders_map.get((sku, row_date), finance_orders_count)

      raw_orders_sum = _to_float(row["orders_sum"])
      raw_commission = _to_float(row["commission_rub"])
      advertising = _to_float(row["advertising_rub"])
      orders_sum = raw_orders_sum * buyout_factor
      commission = raw_commission * buyout_factor
      acquiring = (raw_orders_sum * 0.02) * buyout_factor
      additional_expenses = (raw_orders_sum * additional_rate) * buyout_factor
      preliminary_profit = orders_sum - commission - acquiring - advertising - additional_expenses
      ad_pct = (advertising / orders_sum * 100.0) if orders_sum else 0.0
      margin_pct = (preliminary_profit / orders_sum * 100.0) if orders_sum else 0.0

      date_key = "Период" if aggregate_by_period else "Дата"
      payload.append(
        {
          "Артикул / SKU": sku,
          date_key: row_date,
          "Количество заказов": str(int(round(orders_count))),
          "Сумма заказов": _format_metric("Сумма заказов", orders_sum),
          "% выкупа": _format_percent(buyout_percent),
          "Комиссия, ₽": _format_metric("Комиссия, ₽", commission),
          "Эквайринг, ₽": _format_metric("Эквайринг, ₽", acquiring),
          "Реклама, ₽": _format_metric("Реклама, ₽", advertising),
          "% рекламы": _format_percent(ad_pct),
          "Дополнительные расходы, ₽": _format_metric("Дополнительные расходы, ₽", additional_expenses),
          "Предварительная прибыль, ₽": _format_metric("Предварительная прибыль, ₽", preliminary_profit),
          "% маржинальности": _format_percent(margin_pct),
        }
      )
      if not aggregate_by_period:
        to_store.append(
          (
            sku,
            row_date,
            orders_count,
            orders_sum,
            commission,
            acquiring,
            advertising,
            additional_expenses,
            preliminary_profit,
            additional_rate,
          )
        )

    if to_store:
      conn.executemany(
        """
        INSERT INTO preliminary_order_economics(
          sku, date, orders_count, orders_sum, commission_rub, acquiring_rub,
          advertising_rub, additional_expenses_rub, preliminary_profit_rub, additional_rate, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(sku, date) DO UPDATE SET
          orders_count=excluded.orders_count,
          orders_sum=excluded.orders_sum,
          commission_rub=excluded.commission_rub,
          acquiring_rub=excluded.acquiring_rub,
          advertising_rub=excluded.advertising_rub,
          additional_expenses_rub=excluded.additional_expenses_rub,
          preliminary_profit_rub=excluded.preliminary_profit_rub,
          additional_rate=excluded.additional_rate,
          updated_at=CURRENT_TIMESTAMP
        """,
        to_store,
      )
    conn.commit()

  return {
    "rows": payload,
    "additional_rate": f"{additional_rate * 100:.2f}%",
    "buyout_percent": _format_percent(buyout_percent),
    "expense_components": used_additional_cols,
  }


def _render_table_links_html() -> str:
  try:
    existing_tables = db_store().list_tables()
  except Exception:
    return '<div class="tables-links empty">Не удалось прочитать список таблиц.</div>'

  # Показываем фиксированный набор ключевых таблиц даже до первого sync.
  ordered_tables = list(dict.fromkeys(CORE_TABLES + existing_tables))

  links = []
  existing_set = set(existing_tables)
  for table in ordered_tables:
    table_q = quote_plus(table)
    cls = "" if table in existing_set else " pending"
    suffix = "" if table in existing_set else " (пусто)"
    links.append(f'<a class="tbl-chip{cls}" href="/db?table={table_q}&page=1">{escape(table)}{escape(suffix)}</a>')
  return '<div class="tables-links">' + "".join(links) + "</div>"

def run_sync(date_from: str, date_to: str, skip_ads: bool, skip_funnel: bool, log_q: queue.Queue) -> None:
    cmd = [PYTHON, "-u", "-m", "wb_gsheets.main", "--date-from", date_from, "--date-to", date_to]
    if skip_ads:
        cmd.append("--skip-ads")
    if skip_funnel:
        cmd.append("--skip-funnel")

    env_patch = {"PYTHONPATH": str(ROOT / "src"), "PYTHONUNBUFFERED": "1"}
    env = {**os.environ, **env_patch}

    try:
        log_q.put(("log", f"Старт: период {date_from}..{date_to}, реклама={'нет' if skip_ads else 'да'}, воронка={'нет' if skip_funnel else 'да'}"))
        log_q.put(("log", f"Команда: {' '.join(cmd)}"))
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(ROOT),
            env=env,
        )
        assert proc.stdout
        for line in proc.stdout:
            log_q.put(("log", line.rstrip()))
        proc.wait()
        if proc.returncode == 0:
            log_q.put(("done", "✅ Готово! Данные загружены."))
        else:
            log_q.put(("error", f"❌ Ошибка (код {proc.returncode})"))
    except Exception as exc:
        log_q.put(("error", f"❌ {exc}"))


HTML = """\
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WB Sync</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         background: #f0f2f5; display: flex; justify-content: center;
         align-items: flex-start; min-height: 100vh; padding: 40px 16px; }}
  .card {{ background: #fff; border-radius: 12px; padding: 32px;
           box-shadow: 0 2px 12px rgba(0,0,0,.08); width: 100%; max-width: 520px; }}
  h1 {{ font-size: 1.4rem; font-weight: 700; color: #1a1a2e; margin-bottom: 24px; }}
  label {{ display: block; font-size: .85rem; font-weight: 600;
           color: #555; margin-bottom: 6px; margin-top: 16px; }}
  input[type=date] {{ width: 100%; padding: 10px 12px; border: 1.5px solid #d1d5db;
                      border-radius: 8px; font-size: 1rem; outline: none;
                      transition: border-color .2s; }}
  input[type=date]:focus {{ border-color: #4f46e5; }}
  .row {{ display: flex; gap: 16px; }}
  .row > div {{ flex: 1; }}
  .check-row {{ display: flex; align-items: center; gap: 10px; margin-top: 20px; }}
  .check-row input {{ width: 18px; height: 18px; accent-color: #4f46e5; cursor: pointer; }}
  .check-row label {{ margin: 0; font-size: .9rem; color: #374151; cursor: pointer; }}
  .quick {{ display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap; }}
  .quick button {{ margin-top: 0; width: auto; padding: 8px 10px; font-size: .85rem; background: #e5e7eb; color: #111827; }}
  .quick button:hover:not(:disabled) {{ background: #d1d5db; }}
  .funnel-month {{ margin-top: 12px; }}
  .funnel-month button {{ margin-top: 0; width: 100%; padding: 11px 12px;
                          background: #0f766e; color: #fff; border: none;
                          border-radius: 8px; font-size: .92rem; font-weight: 700;
                          cursor: pointer; }}
  .funnel-month button:hover:not(:disabled) {{ background: #115e59; }}
  .hint {{ margin-top: 10px; color: #4b5563; font-size: .82rem; }}
  button {{ margin-top: 24px; width: 100%; padding: 13px;
            background: #4f46e5; color: #fff; border: none; border-radius: 8px;
            font-size: 1rem; font-weight: 600; cursor: pointer;
            transition: background .2s; }}
  button:hover:not(:disabled) {{ background: #4338ca; }}
  button:disabled {{ background: #a5b4fc; cursor: not-allowed; }}
  #log-wrap {{ display: none; margin-top: 24px; }}
  #log {{ background: #111827; color: #d1fae5; border-radius: 8px; padding: 16px;
          font-size: .8rem; font-family: "Menlo", "Courier New", monospace;
          max-height: 320px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }}
  .status {{ margin-top: 12px; font-size: .9rem; font-weight: 600; }}
  .status.ok {{ color: #059669; }}
  .status.err {{ color: #dc2626; }}
  .db-link {{ margin-top: 14px; text-align: center; }}
  .db-link a {{ color: #1d4ed8; text-decoration: none; font-weight: 600; }}
  .db-link a:hover {{ text-decoration: underline; }}
  .reports-links {{ margin-top: 12px; display: flex; flex-wrap: wrap; justify-content: center; gap: 8px; }}
  .reports-links a {{ padding: 7px 10px; border-radius: 999px; background: #ecfeff; color: #155e75; text-decoration: none; font-size: .82rem; font-weight: 700; border: 1px solid #bae6fd; }}
  .reports-links a:hover {{ background: #cffafe; }}
  .tables-title {{ margin-top: 16px; color: #374151; font-size: .85rem; font-weight: 700; }}
  .tables-links {{ margin-top: 8px; display: flex; flex-wrap: wrap; gap: 8px; }}
  .tables-links .tbl-chip {{ padding: 6px 9px; border-radius: 999px; background: #eef2ff; color: #3730a3; text-decoration: none; font-size: .8rem; font-weight: 600; }}
  .tables-links .tbl-chip:hover {{ background: #e0e7ff; }}
  .tables-links .tbl-chip.pending {{ background: #f3f4f6; color: #6b7280; }}
  .tables-links.empty {{ color: #6b7280; font-size: .82rem; }}
</style>
</head>
<body>
<div class="card">
  <h1>📦 Wildberries — загрузка данных</h1>
  <form id="form">
    <div class="row">
      <div>
        <label for="df">Дата с</label>
        <input type="date" id="df" name="date_from" value="{date_from}" required>
      </div>
      <div>
        <label for="dt">Дата по</label>
        <input type="date" id="dt" name="date_to" value="{date_to}" required>
      </div>
    </div>
    <div class="quick">
      <button type="button" data-range="yesterday">За вчера</button>
      <button type="button" data-range="today">Сегодня</button>
      <button type="button" data-range="last7">Последние 7 дней</button>
    </div>
    <div class="check-row">
      <input type="checkbox" id="skip_ads" name="skip_ads">
      <label for="skip_ads">Пропустить рекламу (быстрее)</label>
    </div>
    <div class="check-row">
      <input type="checkbox" id="skip_funnel" name="skip_funnel">
      <label for="skip_funnel">Пропустить воронку продаж (быстрее)</label>
    </div>
    <div class="funnel-month">
      <button type="button" id="btn-funnel-month">Обновить воронку за последний месяц</button>
    </div>
    <div class="hint">Загрузка пишет данные в SQLite: продажи, заказы, реклама, PnL, воронка.</div>
    <button type="submit" id="btn">▶ Загрузить</button>
  </form>
  <div class="db-link"><a href="/db">Открыть таблицы SQLite</a></div>
  <div class="reports-links">
    {report_links}
  </div>
  <div class="tables-title">Быстрые ссылки на таблицы:</div>
  {table_links}
  <div id="log-wrap">
    <div id="log"></div>
    <div id="status" class="status"></div>
  </div>
</div>
<script>
const form = document.getElementById('form');
const btn  = document.getElementById('btn');
const logEl = document.getElementById('log');
const logWrap = document.getElementById('log-wrap');
const statusEl = document.getElementById('status');
const dfEl = document.getElementById('df');
const dtEl = document.getElementById('dt');

function isoDate(d) {{
  return d.toISOString().slice(0, 10);
}}

for (const quickBtn of document.querySelectorAll('.quick button[data-range]')) {{
  quickBtn.addEventListener('click', () => {{
    const today = new Date();
    const mode = quickBtn.dataset.range;
    if (mode === 'today') {{
      const t = isoDate(today);
      dfEl.value = t;
      dtEl.value = t;
      return;
    }}
    if (mode === 'yesterday') {{
      const y = new Date(today);
      y.setDate(y.getDate() - 1);
      const yd = isoDate(y);
      dfEl.value = yd;
      dtEl.value = yd;
      return;
    }}
    if (mode === 'last7') {{
      const from = new Date(today);
      from.setDate(from.getDate() - 6);
      dfEl.value = isoDate(from);
      dtEl.value = isoDate(today);
    }}
  }});
}}

// Кнопка для обновления воронки за последний месяц
const funnelMonthBtn = document.getElementById('btn-funnel-month');
if (funnelMonthBtn) {{
  funnelMonthBtn.addEventListener('click', () => {{
    const today = new Date();
    const monthAgo = new Date(today);
    monthAgo.setDate(monthAgo.getDate() - 30);
    dfEl.value = isoDate(monthAgo);
    dtEl.value = isoDate(today);
    // Автоматически пропускаем рекламу для ускорения
    document.getElementById('skip_ads').checked = true;
    // Убеждаемся, что воронка НЕ пропускается
    document.getElementById('skip_funnel').checked = false;
  }});
}}

function appendLog(text) {{
  logEl.textContent += text + '\\n';
  logEl.scrollTop = logEl.scrollHeight;
}}

form.addEventListener('submit', async e => {{
  e.preventDefault();
  const df = dfEl.value;
  const dt = dtEl.value;
  const skipAds = document.getElementById('skip_ads').checked ? '1' : '0';
  const skipFunnel = document.getElementById('skip_funnel').checked ? '1' : '0';

  btn.disabled = true;
  btn.textContent = '⏳ Загружаю...';
  logEl.textContent = '';
  statusEl.textContent = '';
  statusEl.className = 'status';
  logWrap.style.display = 'block';
  appendLog(`Запрос отправлен: ${{df}}..${{dt}}` + (skipAds === '1' ? ' (без рекламы)' : '') + (skipFunnel === '1' ? ' (без воронки)' : ''));
  appendLog('Таблицы: raw_stocks + buyout_order_day + finance_article_day_detail + analytics_article_day + analytics_day + analytics_article_period');
  statusEl.textContent = 'Выполняется...';

  const url = `/stream?date_from=${{df}}&date_to=${{dt}}&skip_ads=${{skipAds}}&skip_funnel=${{skipFunnel}}`;
  const resp = await fetch(url);
  if (!resp.ok || !resp.body) {{
    statusEl.textContent = `❌ Не удалось открыть поток логов (HTTP ${{resp.status}})`;
    statusEl.className = 'status err';
    btn.disabled = false;
    btn.textContent = '▶ Загрузить';
    return;
  }}
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';

  while (true) {{
    const {{ done, value }} = await reader.read();
    if (done) break;
    buf += decoder.decode(value, {{ stream: true }});
    const lines = buf.split('\\n');
    buf = lines.pop();
    for (const line of lines) {{
      if (line.startsWith('data: ')) {{
        const payload = JSON.parse(line.slice(6));
        if (payload.type === 'log') {{
          appendLog(payload.text);
        }} else if (payload.type === 'heartbeat') {{
          statusEl.textContent = payload.text;
        }} else if (payload.type === 'done') {{
          statusEl.textContent = payload.text;
          statusEl.className = 'status ok';
        }} else if (payload.type === 'error') {{
          statusEl.textContent = payload.text;
          statusEl.className = 'status err';
        }}
      }}
    }}
  }}

  btn.disabled = false;
  btn.textContent = '▶ Загрузить';
}});
</script>
</body>
</html>
"""


ANALYTICS_HOME_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>WB Analytics</title>
  <style>
    :root {{
      --bg: #f7f6f2;
      --card: #ffffff;
      --ink: #1f2937;
      --muted: #6b7280;
      --accent: #0f766e;
      --accent-2: #0ea5e9;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "Trebuchet MS", sans-serif; color: var(--ink); background: radial-gradient(circle at 20% 20%, #d1fae5 0, transparent 42%), radial-gradient(circle at 80% 0%, #bae6fd 0, transparent 35%), var(--bg); }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 24px 16px 40px; }}
    .top {{ display: flex; justify-content: space-between; gap: 12px; align-items: center; margin-bottom: 20px; }}
    .top a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    h1 {{ margin: 0; font-size: 1.7rem; }}
    .sub {{ color: var(--muted); margin: 8px 0 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: var(--card); border: 1px solid #e5e7eb; border-radius: 14px; padding: 18px; box-shadow: 0 8px 24px rgba(15, 118, 110, .08); }}
    .card h2 {{ margin: 0 0 8px; font-size: 1.1rem; }}
    .card p {{ margin: 0 0 14px; color: var(--muted); line-height: 1.4; }}
    .btn {{ display: inline-block; padding: 10px 14px; border-radius: 10px; color: #fff; text-decoration: none; font-weight: 700; background: linear-gradient(135deg, var(--accent), var(--accent-2)); }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <h1>Аналитика Wildberries</h1>
        <p class="sub">Пользовательские страницы только для аналитических таблиц.</p>
      </div>
      <a href="/">← К загрузке</a>
    </div>
    <div class="grid">
      <div class="card">
        <h2>Сводка по дням</h2>
        <p>Смотри динамику по дням: продажи, реклама, прибыль и KPI-карточки за выбранный период.</p>
        <a class="btn" href="/analytics/day">Открыть по дням</a>
      </div>
      <div class="card">
        <h2>Период по артикулам</h2>
        <p>Выбери период на странице и получи сводную аналитику по артикулам. Значения округляются до копеек и целых процентов.</p>
        <a class="btn" href="/analytics/period">Открыть период</a>
      </div>
      <div class="card">
        <h2>Дневная аналитика артикула</h2>
        <p>Введи артикул и получи строки только по нему через AJAX без перезагрузки страницы.</p>
        <a class="btn" href="/analytics/article-day">Открыть фильтр артикула</a>
      </div>
      <div class="card">
        <h2>Выкупы по датам заказов</h2>
        <p>Pivot-таблица: метрики в строках, даты в колонках, факт выкупов и выкупы заказов выбранной даты.</p>
        <a class="btn" href="/analytics/buyout-order-day">Открыть выкупы</a>
      </div>
      <div class="card">
        <h2>Предварительная экономика по заказам</h2>
        <p>Связка заказов и рекламы по SKU/дате с расчетом комиссии, эквайринга, допрасходов и прибыли.</p>
        <a class="btn" href="/analytics/preliminary-economics">Открыть страницу</a>
      </div>
      <div class="card">
        <h2>Предварительная экономика по периоду</h2>
        <p>Та же модель экономики, но даты объединены в одну строку на SKU за выбранный период.</p>
        <a class="btn" href="/analytics/preliminary-economics-summary">Открыть страницу</a>
      </div>
    </div>
  </div>
</body>
</html>
"""


BUYOUT_ORDER_DAY_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{page_title}</title>
  <style>
    :root {{ --bg: #f8fafc; --card: #fff; --ink: #111827; --muted: #6b7280; --accent: #0f766e; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "Trebuchet MS", sans-serif; color: var(--ink); background: linear-gradient(180deg, #e0f2fe, transparent 240px), var(--bg); }}
    .wrap {{ width: calc(100% - 32px); max-width: 3000px; margin: 0 auto; padding: 18px 0 28px; }}
    .top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }}
    .top a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .report-nav {{ display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 6px; max-width: 100%; }}
    .panel {{ background: var(--card); border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
    label {{ display: block; color: var(--muted); font-size: .76rem; margin-bottom: 3px; font-weight: 700; }}
    input, select {{ padding: 7px 8px; border: 1px solid #cbd5e1; border-radius: 7px; font-size: 12px; min-width: 160px; background: #fff; }}
    button {{ padding: 8px 12px; border: 0; border-radius: 7px; background: var(--accent); color: #fff; font-weight: 700; cursor: pointer; font-size: 12px; }}
    .quick-range {{ display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }}
    .quick-range button {{ padding: 7px 10px; background: #e6f4f1; color: var(--accent); }}
    .meta {{ margin-top: 8px; color: var(--muted); font-size: .78rem; }}
    .workspace {{ --articles-width: 336px; display: grid; grid-template-columns: var(--articles-width) minmax(0, 1fr); gap: 12px; align-items: start; margin-top: 10px; }}
    .workspace.articles-collapsed {{ --articles-width: 44px; }}
    .main-pane {{ display: flex; flex-direction: column; gap: 12px; min-width: 0; }}
    .tbl {{ --graph-col-width: 58px; --metric-col-width: 240px; --total-col-width: 120px; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow-x: auto; overflow-y: auto; max-height: calc(100vh - 230px); -webkit-overflow-scrolling: touch; }}
    .articles {{ position: relative; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow: hidden; max-height: calc(100vh - 230px); display: flex; flex-direction: column; min-width: 0; transition: width .18s ease, box-shadow .18s ease, border-color .18s ease; }}
    .articles-inner {{ display: flex; flex-direction: column; min-height: 0; height: 100%; background: #fff; }}
    .articles-head {{ padding: 7px 8px; border-bottom: 1px solid #eef2f7; display: flex; justify-content: space-between; gap: 8px; align-items: center; }}
    .articles-title {{ font-weight: 800; font-size: .78rem; }}
    .articles-actions {{ display: flex; gap: 8px; }}
    .articles-actions button {{ padding: 4px 6px; border-radius: 6px; font-size: .68rem; background: #e6f4f1; color: var(--accent); }}
    .articles-mini {{ display: none; align-items: center; justify-content: center; gap: 6px; height: 100%; min-height: 260px; padding: 8px 0; writing-mode: vertical-rl; transform: rotate(180deg); color: var(--accent); font-weight: 800; font-size: .72rem; letter-spacing: .08em; background: linear-gradient(180deg, #f0fdfa, #ecfeff); }}
    .articles-mini button {{ writing-mode: horizontal-tb; transform: rotate(180deg); margin: 0; width: 24px; height: 24px; padding: 0; border-radius: 999px; background: #0f766e; color: #fff; font-size: .9rem; line-height: 1; }}
    .subject-filter {{ padding: 6px 8px; border-bottom: 1px solid #eef2f7; }}
    .subject-filter label {{ font-size: .68rem; margin-bottom: 2px; }}
    .subject-filter select {{ width: 100%; min-width: 0; padding: 5px 6px; font-size: .72rem; }}
    .article-list {{ overflow: auto; user-select: none; }}
    .article-table {{ width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 10px; }}
    .article-table th, .article-table td {{ border-bottom: 1px solid #eef2f7; padding: 3px 5px; line-height: 1.15; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .article-table th {{ position: sticky; top: 0; z-index: 1; background: #f8fafc; color: #475569; font-size: .62rem; cursor: pointer; }}
    .article-table th:first-child, .article-table td:first-child {{ text-align: left; width: 112px; }}
    .article-table th:nth-child(2), .article-table td:nth-child(2) {{ text-align: right; width: 58px; }}
    .article-table th:nth-child(3), .article-table td:nth-child(3) {{ text-align: right; }}
    .article-table tr {{ cursor: default; }}
    .article-table tbody tr:hover {{ background: #f0fdfa; }}
    .article-table tbody tr.selected {{ background: #ccfbf1; font-weight: 800; color: #115e59; }}
    .article-link {{ color: var(--accent); text-decoration: none; font-weight: 800; margin-left: 4px; }}
    .article-link:hover {{ text-decoration: underline; }}
    .tbl table {{ width: max-content; min-width: 100%; border-collapse: collapse; font-size: 11px; }}
    .tbl th, .tbl td {{ border-bottom: 1px solid #eef2f7; padding: 6px 7px; text-align: right; white-space: nowrap; }}
    .tbl th:first-child, .tbl td:first-child {{ position: sticky; left: 0; z-index: 1; text-align: center; background: #fff; width: var(--graph-col-width); min-width: var(--graph-col-width); max-width: var(--graph-col-width); padding-left: 0; padding-right: 0; }}
    .tbl td:first-child {{ vertical-align: middle; }}
    .tbl th:nth-child(2), .tbl td:nth-child(2) {{ position: sticky; left: var(--graph-col-width); z-index: 1; text-align: left; background: #fff; width: var(--metric-col-width); min-width: var(--metric-col-width); max-width: var(--metric-col-width); font-weight: 700; }}
    .tbl th:nth-child(3), .tbl td:nth-child(3) {{ position: sticky; left: calc(var(--graph-col-width) + var(--metric-col-width)); z-index: 1; background: #fff; width: var(--total-col-width); min-width: var(--total-col-width); max-width: var(--total-col-width); font-weight: 800; }}
    .tbl th {{ position: sticky; top: 0; z-index: 2; background: #f8fafc; }}
    .tbl th:first-child, .tbl th:nth-child(2), .tbl th:nth-child(3) {{ z-index: 3; background: #f8fafc; }}
    .tbl tr.group-row td {{ background: #eef6f4; color: #0f766e; font-size: .78rem; font-weight: 800; text-transform: uppercase; letter-spacing: .04em; }}
    .tbl tr.group-row td:first-child {{ background: #eef6f4; }}
    .tbl tr.group-row td:nth-child(2) {{ background: #eef6f4; }}
    .tbl tr.group-row td:nth-child(3) {{ background: #eef6f4; }}
    .group-toggle {{ width: 22px; height: 22px; padding: 0; margin: 0; border-radius: 999px; background: #0f766e; color: #fff; font-size: .92rem; line-height: 1; }}
    .group-label {{ cursor: pointer; }}
    .metric-toggle {{ display: inline-block; width: 16px; height: 16px; min-width: 16px; padding: 0; margin: 0; border: 0; border-radius: 0; background: transparent; box-shadow: none; accent-color: var(--accent); cursor: pointer; vertical-align: middle; appearance: auto; -webkit-appearance: checkbox; }}
    .chart-card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; }}
    .chart-head {{ display: flex; justify-content: space-between; align-items: center; gap: 10px; margin-bottom: 10px; }}
    .chart-title {{ font-size: .95rem; font-weight: 800; }}
    .chart-subtitle {{ color: var(--muted); font-size: .74rem; }}
    .chart-swatch {{ width: 10px; height: 10px; border-radius: 999px; flex: 0 0 auto; }}
    .chart-empty {{ color: var(--muted); font-size: .78rem; padding: 14px 0 4px; }}
    .chart-svg-wrap {{ position: relative; width: 100%; overflow-x: auto; border: 1px solid #eef2f7; border-radius: 10px; background: linear-gradient(180deg, #fcfffe, #f8fafc); }}
    .chart-svg {{ display: block; min-width: 980px; width: 100%; height: 860px; }}
    .chart-axis {{ stroke: #cbd5e1; stroke-width: 1; }}
    .chart-grid {{ stroke: #e5e7eb; stroke-width: 1; stroke-dasharray: 3 4; }}
    .chart-label {{ fill: #64748b; font-size: 11px; }}
    .chart-zone-label {{ fill: #0f766e; font-size: 12px; font-weight: 700; }}
    .chart-line {{ fill: none; stroke-width: 2.5; stroke-linecap: round; stroke-linejoin: round; }}
    .chart-dot {{ stroke: #fff; stroke-width: 1.5; }}
    .chart-hover-line {{ stroke: #0f766e; stroke-width: 1.5; stroke-dasharray: 4 4; opacity: .55; }}
    .chart-hit {{ fill: transparent; cursor: crosshair; }}
    .chart-tooltip {{ position: absolute; display: none; min-width: 180px; max-width: 280px; padding: 8px 10px; border: 1px solid #99f6e4; border-radius: 10px; background: rgba(255, 255, 255, .96); box-shadow: 0 14px 28px rgba(15, 118, 110, .16); color: #0f172a; font-size: 12px; line-height: 1.35; pointer-events: none; }}
    .chart-tooltip-date {{ font-weight: 800; margin-bottom: 6px; color: #0f766e; }}
    .chart-tooltip-row {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-top: 4px; }}
    .chart-tooltip-name {{ display: inline-flex; align-items: center; gap: 6px; min-width: 0; }}
    .chart-tooltip-name span:last-child {{ overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .chart-tooltip-dot {{ width: 8px; height: 8px; border-radius: 999px; flex: 0 0 auto; }}
    .chart-tooltip-value {{ font-weight: 700; text-align: right; white-space: nowrap; }}
    .workspace.articles-collapsed .articles {{ overflow: visible; }}
    .workspace.articles-collapsed .articles .articles-inner {{ display: none; }}
    .workspace.articles-collapsed .articles .articles-mini {{ display: flex; }}
    .workspace.articles-collapsed .articles:hover,
    .workspace.articles-collapsed .articles:focus-within {{ width: 336px; z-index: 20; box-shadow: 0 18px 40px rgba(15, 118, 110, .18); border-color: #99f6e4; }}
    .workspace.articles-collapsed .articles:hover .articles-inner,
    .workspace.articles-collapsed .articles:focus-within .articles-inner {{ display: flex; }}
    .workspace.articles-collapsed .articles:hover .articles-mini,
    .workspace.articles-collapsed .articles:focus-within .articles-mini {{ display: none; }}
    @media (max-width: 1100px) {{
      .workspace {{ grid-template-columns: 1fr; }}
      .workspace.articles-collapsed {{ --articles-width: 1fr; }}
      .articles {{ order: 2; }}
      .articles {{ max-height: 260px; }}
      .workspace.articles-collapsed .articles {{ width: auto; overflow: hidden; }}
      .workspace.articles-collapsed .articles .articles-inner {{ display: flex; }}
      .workspace.articles-collapsed .articles .articles-mini {{ display: none; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>{page_title}</h1>
      <div class="report-nav">{report_nav}</div>
    </div>
    <div class="panel">
      <div>
        <label for="article">Артикул</label>
        <input id="article" type="text" placeholder="например 12-0042-001">
      </div>
      <div>
        <label for="df">Период с</label>
        <input id="df" type="date" value="{date_from}">
      </div>
      <div>
        <label for="dt">Период по</label>
        <input id="dt" type="date" value="{date_to}">
      </div>
      <div class="quick-range">
        <button type="button" id="range-7">7 дней</button>
        <button type="button" id="range-14">14 дней</button>
        <button type="button" id="range-31">Месяц</button>
      </div>
      <div><button id="load">Показать</button></div>
    </div>
    <div class="meta" id="meta">Загрузка...</div>
    <div class="workspace" id="workspace">
      <aside class="articles">
        <div class="articles-inner">
          <div class="articles-head">
            <div class="articles-title">Артикулы</div>
            <div class="articles-actions">
              <button type="button" id="toggle-articles" title="Свернуть список">◂</button>
              <button type="button" id="select-all">Все</button>
              <button type="button" id="clear-all">Сброс</button>
            </div>
          </div>
          <div class="subject-filter">
            <label for="subject">Предмет</label>
            <select id="subject">
              <option value="">Все предметы</option>
            </select>
          </div>
          <div class="article-list" id="article-list"></div>
        </div>
        <div class="articles-mini">
          <button type="button" id="expand-articles" title="Развернуть список">▸</button>
          <span>Артикулы</span>
        </div>
      </aside>
      <div class="main-pane">
        <div class="tbl" id="tbl"></div>
        <section class="chart-card">
          <div class="chart-head">
            <div>
              <div class="chart-title">График метрик</div>
              <div class="chart-subtitle">Суммы и проценты на одном графике. Проценты вынесены ниже и масштабируются отдельно.</div>
            </div>
          </div>
          <div class="chart-svg-wrap" id="chart-wrap"></div>
        </section>
      </div>
    </div>
  </div>
<script>
const meta = document.getElementById('meta');
const tbl = document.getElementById('tbl');
const articleList = document.getElementById('article-list');
const subjectEl = document.getElementById('subject');
const chartWrap = document.getElementById('chart-wrap');
const workspaceEl = document.getElementById('workspace');
const reportGranularity = '{granularity}';
const filterKey = `wb.analytics.buyoutOrderDay.${{reportGranularity}}.filters`;
let requestSeq = 0;
let filterTimer = null;
let selectedArticles = new Set();
let articlesLoadedFor = '';
let subjectsLoadedFor = '';
let articleItems = [];
let articleSort = {{ key: 'revenue', dir: 'desc' }};
let lastArticleIndex = -1;
let activeChartMetrics = new Set();
let latestChartPayload = null;
let articlesCollapsed = false;
let collapsedGroups = new Set();
const chartPalette = ['#0f766e', '#f97316', '#0284c7', '#dc2626', '#7c3aed', '#16a34a', '#ca8a04', '#db2777', '#0891b2', '#4f46e5'];

function syncArticlesPanel() {{
  workspaceEl.classList.toggle('articles-collapsed', articlesCollapsed);
  const toggleBtn = document.getElementById('toggle-articles');
  const expandBtn = document.getElementById('expand-articles');
  if (toggleBtn) {{
    toggleBtn.textContent = articlesCollapsed ? '▸' : '◂';
    toggleBtn.title = articlesCollapsed ? 'Развернуть список' : 'Свернуть список';
  }}
  if (expandBtn) expandBtn.title = articlesCollapsed ? 'Развернуть список' : 'Свернуть список';
}}

function restoreFilters() {{
  try {{
    const saved = JSON.parse(localStorage.getItem(filterKey) || '{{}}');
    if (saved.article !== undefined) document.getElementById('article').value = saved.article;
    if (saved.df) document.getElementById('df').value = saved.df;
    if (saved.dt) document.getElementById('dt').value = saved.dt;
    if (saved.subject !== undefined) subjectEl.dataset.pendingValue = saved.subject;
    if (Array.isArray(saved.selectedArticles)) selectedArticles = new Set(saved.selectedArticles.map(String).filter(value => /^\d+$/.test(value)));
    if (Array.isArray(saved.chartMetrics)) activeChartMetrics = new Set(saved.chartMetrics);
    if (Array.isArray(saved.collapsedGroups)) collapsedGroups = new Set(saved.collapsedGroups.map(String).filter(Boolean));
    articlesCollapsed = Boolean(saved.articlesCollapsed);
  }} catch (err) {{}}
  syncArticlesPanel();
}}

function saveFilters() {{
  localStorage.setItem(filterKey, JSON.stringify({{
    article: document.getElementById('article').value,
    df: document.getElementById('df').value,
    dt: document.getElementById('dt').value,
    subject: subjectEl.value,
    selectedArticles: [...selectedArticles],
    chartMetrics: [...activeChartMetrics],
    collapsedGroups: [...collapsedGroups],
    articlesCollapsed
  }}));
}}

function isoDate(date) {{
  return date.toISOString().slice(0, 10);
}}

function applyQuickRange(days) {{
  const end = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - (days - 1));
  document.getElementById('df').value = isoDate(start);
  document.getElementById('dt').value = isoDate(end);
  loadData();
}}

function scheduleLoad() {{
  clearTimeout(filterTimer);
  filterTimer = setTimeout(loadData, 350);
}}

function escapeHtml(value) {{
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}}

function formatCompactNumber(value, fractionDigits = 0) {{
  if (value === null || value === undefined || value === '') return '';
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '';
  return numeric.toLocaleString('ru-RU', {{
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits
  }});
}}

function parseMetricValue(value) {{
  const text = String(value ?? '').trim();
  if (!text) return 0;
  const normalized = text.replaceAll(' ', '').replace('%', '').replace(',', '.');
  const numeric = Number(normalized);
  return Number.isFinite(numeric) ? numeric : 0;
}}

function chartRowsFromPayload(data) {{
  return (data.rows || []).filter(row => String(row.metric || '').trim());
}}

function ensureDefaultChartMetrics(rows) {{
  if (activeChartMetrics.size) return;
  const defaults = [
    'Сумма заказов',
    'Сумма выкупов в эту дату',
    'Реклама',
    'ДРР от заказов',
    'Маржинальность от выкупов за дату',
  ];
  const available = new Set(rows.map(row => row.metric));
  for (const metric of defaults) {{
    if (available.has(metric)) activeChartMetrics.add(metric);
  }}
  if (!activeChartMetrics.size) {{
    for (const row of rows.slice(0, 4)) activeChartMetrics.add(row.metric);
  }}
}}

function renderChart(data) {{
  latestChartPayload = data;
  const rows = chartRowsFromPayload(data);
  ensureDefaultChartMetrics(rows);
  const selectedRows = rows.filter(row => activeChartMetrics.has(row.metric));
  const dates = data.dates || [];
  if (!dates.length || !selectedRows.length) {{
    chartWrap.innerHTML = '<div class="chart-empty">Выберите хотя бы одну метрику с данными.</div>';
    return;
  }}

  const percentRows = selectedRows.filter(row => String(row.kind || '').startsWith('percent'));
  const moneyRows = selectedRows.filter(row => !String(row.kind || '').startsWith('percent'));
  const moneySeries = moneyRows.map(row => row.values.map(parseMetricValue));
  const percentSeries = percentRows.map(row => row.values.map(parseMetricValue));
  const moneyMax = Math.max(0, ...moneySeries.flat());
  const percentMax = Math.max(0, ...percentSeries.flat());

  const width = Math.max(980, dates.length * 84 + 220);
  const height = 860;
  const left = 64;
  const right = 18;
  const top = 24;
  const bottom = 110;
  const gap = 52;
  const plotWidth = width - left - right;
  const moneyHeight = 450;
  const percentHeight = 220;
  const moneyTop = top;
  const percentTop = moneyTop + moneyHeight + gap;
  const moneyBottom = moneyTop + moneyHeight;
  const percentBottom = percentTop + percentHeight;
  const xStep = dates.length > 1 ? plotWidth / (dates.length - 1) : 0;
  const moneyScale = moneyMax > 0 ? moneyHeight / moneyMax : 1;
  const percentScale = percentMax > 0 ? percentHeight / percentMax : 1;
  const formatDate = (iso) => {{
    if (String(iso).includes('..')) {{
      return String(iso).split('..').map(part => {{
        const [yy, mm, dd] = part.split('-');
        return `${{dd}}.${{mm}}`;
      }}).join('-');
    }}
    const [y, m, d] = String(iso).split('-');
    return `${{d}}.${{m}}`;
  }};
  const formatTooltipDate = (iso) => {{
    if (String(iso).includes('..')) {{
      return String(iso).split('..').map(part => {{
        const [y, m, d] = part.split('-');
        return `${{d}}.${{m}}.${{y}}`;
      }}).join(' - ');
    }}
    const [y, m, d] = String(iso).split('-');
    return `${{d}}.${{m}}.${{y}}`;
  }};

  const gridLines = [];
  for (let idx = 0; idx <= 4; idx++) {{
    const moneyY = moneyTop + (moneyHeight / 4) * idx;
    const percentY = percentTop + (percentHeight / 4) * idx;
    const moneyValue = moneyMax * (1 - idx / 4);
    const percentValue = percentMax * (1 - idx / 4);
    gridLines.push(`<line class="chart-grid" x1="${{left}}" y1="${{moneyY}}" x2="${{width - right}}" y2="${{moneyY}}"></line>`);
    gridLines.push(`<text class="chart-label" x="${{left - 8}}" y="${{moneyY + 4}}" text-anchor="end">${{formatCompactNumber(moneyValue, 0)}}</text>`);
    gridLines.push(`<line class="chart-grid" x1="${{left}}" y1="${{percentY}}" x2="${{width - right}}" y2="${{percentY}}"></line>`);
    gridLines.push(`<text class="chart-label" x="${{left - 8}}" y="${{percentY + 4}}" text-anchor="end">${{formatCompactNumber(percentValue, 0)}}%</text>`);
  }}

  const xLabels = dates.map((day, idx) => {{
    const x = left + xStep * idx;
    return `
      <line class="chart-grid" x1="${{x}}" y1="${{moneyTop}}" x2="${{x}}" y2="${{percentBottom}}"></line>
      <text class="chart-label" x="${{x}}" y="${{height - 18}}" text-anchor="end" transform="rotate(-45 ${{x}} ${{height - 18}})">${{formatDate(day)}}</text>
    `;
  }}).join('');

  const seriesSvg = selectedRows.map((row, idx) => {{
    const isPercent = String(row.kind || '').startsWith('percent');
    const values = row.values.map(parseMetricValue);
    const color = chartPalette[idx % chartPalette.length];
    const zoneTop = isPercent ? percentTop : moneyTop;
    const zoneBottom = isPercent ? percentBottom : moneyBottom;
    const scale = isPercent ? percentScale : moneyScale;
    const points = values.map((value, pointIdx) => {{
      const x = left + xStep * pointIdx;
      const y = zoneBottom - value * scale;
      return `${{x}},${{y}}`;
    }}).join(' ');
    const dots = values.map((value, pointIdx) => {{
      const x = left + xStep * pointIdx;
      const y = zoneBottom - value * scale;
      return `<circle class="chart-dot" cx="${{x}}" cy="${{y}}" r="3.5" fill="${{color}}"><title>${{escapeHtml(row.metric)}}: ${{row.values[pointIdx]}}</title></circle>`;
    }}).join('');
    const labelX = width - right - 8;
    const lastValue = values[values.length - 1] || 0;
    const labelY = zoneBottom - lastValue * scale - 6;
    return `
      <polyline class="chart-line" points="${{points}}" stroke="${{color}}"></polyline>
      ${{dots}}
      <text class="chart-label" x="${{labelX}}" y="${{Math.max(zoneTop + 12, Math.min(zoneBottom - 6, labelY))}}" text-anchor="end" fill="${{color}}">${{escapeHtml(row.metric)}}</text>
    `;
  }}).join('');

  const hoverTargets = dates.map((day, idx) => {{
    const x = left + xStep * idx;
    const rectWidth = dates.length > 1 ? Math.max(24, xStep) : plotWidth;
    const rectX = dates.length > 1 ? x - rectWidth / 2 : left;
    return `<rect class="chart-hit" data-index="${{idx}}" x="${{rectX}}" y="${{moneyTop}}" width="${{rectWidth}}" height="${{percentBottom - moneyTop}}"></rect>`;
  }}).join('');

  chartWrap.innerHTML = `
    <div class="chart-tooltip" id="chart-tooltip"></div>
    <svg class="chart-svg" viewBox="0 0 ${{width}} ${{height}}" preserveAspectRatio="none">
      <text class="chart-zone-label" x="${{left}}" y="${{moneyTop - 6}}">Суммы</text>
      <text class="chart-zone-label" x="${{left}}" y="${{percentTop - 6}}">Проценты</text>
      <line class="chart-axis" x1="${{left}}" y1="${{moneyTop}}" x2="${{left}}" y2="${{moneyBottom}}"></line>
      <line class="chart-axis" x1="${{left}}" y1="${{moneyBottom}}" x2="${{width - right}}" y2="${{moneyBottom}}"></line>
      <line class="chart-axis" x1="${{left}}" y1="${{percentTop}}" x2="${{left}}" y2="${{percentBottom}}"></line>
      <line class="chart-axis" x1="${{left}}" y1="${{percentBottom}}" x2="${{width - right}}" y2="${{percentBottom}}"></line>
      ${{gridLines.join('')}}
      ${{xLabels}}
      ${{seriesSvg}}
      <line class="chart-hover-line" id="chart-hover-line" x1="${{left}}" y1="${{moneyTop}}" x2="${{left}}" y2="${{percentBottom}}" visibility="hidden"></line>
      ${{hoverTargets}}
    </svg>
  `;

  const svgEl = chartWrap.querySelector('.chart-svg');
  const tooltipEl = document.getElementById('chart-tooltip');
  const hoverLineEl = document.getElementById('chart-hover-line');
  const allSeries = selectedRows.map((row, idx) => ({{
    row,
    color: chartPalette[idx % chartPalette.length],
  }}));
  const showTooltip = (index, clientX, clientY) => {{
    const x = left + xStep * index;
    hoverLineEl.setAttribute('x1', String(x));
    hoverLineEl.setAttribute('x2', String(x));
    hoverLineEl.setAttribute('visibility', 'visible');
    tooltipEl.innerHTML = `
      <div class="chart-tooltip-date">${{formatTooltipDate(dates[index])}}</div>
      ${{allSeries.map(series => `
        <div class="chart-tooltip-row">
          <div class="chart-tooltip-name">
            <span class="chart-tooltip-dot" style="background:${{series.color}}"></span>
            <span>${{escapeHtml(series.row.metric)}}</span>
          </div>
          <div class="chart-tooltip-value">${{escapeHtml(String((series.row.values || [])[index] || '0'))}}</div>
        </div>
      `).join('')}}
    `;
    tooltipEl.style.display = 'block';
    const wrapRect = chartWrap.getBoundingClientRect();
    const svgRect = svgEl.getBoundingClientRect();
    const tooltipWidth = tooltipEl.offsetWidth;
    const tooltipHeight = tooltipEl.offsetHeight;
    let leftPos = clientX - wrapRect.left + 14;
    let topPos = clientY - wrapRect.top - tooltipHeight - 14;
    if (leftPos + tooltipWidth > wrapRect.width - 8) leftPos = clientX - wrapRect.left - tooltipWidth - 14;
    if (leftPos < 8) leftPos = 8;
    if (topPos < 8) topPos = clientY - wrapRect.top + 14;
    if (topPos + tooltipHeight > wrapRect.height - 8) topPos = Math.max(8, wrapRect.height - tooltipHeight - 8);
    tooltipEl.style.left = `${{leftPos + chartWrap.scrollLeft}}px`;
    tooltipEl.style.top = `${{topPos + chartWrap.scrollTop}}px`;
  }};
  const hideTooltip = () => {{
    hoverLineEl.setAttribute('visibility', 'hidden');
    tooltipEl.style.display = 'none';
  }};
  for (const target of [...chartWrap.querySelectorAll('.chart-hit')]) {{
    target.addEventListener('mouseenter', (event) => showTooltip(Number(target.dataset.index || 0), event.clientX, event.clientY));
    target.addEventListener('mousemove', (event) => showTooltip(Number(target.dataset.index || 0), event.clientX, event.clientY));
    target.addEventListener('mouseleave', hideTooltip);
  }}
}}

function cellColorStyle(kind, prevRaw, currRaw, row) {{
  if (prevRaw === null || prevRaw === undefined || currRaw === null || currRaw === undefined) return '';
  const isPercent = kind === 'percent_income' || kind === 'percent_expense';
  const threshold = Number(row?.color_threshold ?? (isPercent ? 5 : 10));
  const colorMode = row?.color_mode || (isPercent ? 'absolute' : 'percent');
  let beneficial;
  if (isPercent) {{
    const diff = currRaw - prevRaw;
    if (Math.abs(diff) <= threshold) return '';
    beneficial = kind === 'percent_income' ? diff > 0 : diff < 0;
  }} else {{
    const diff = currRaw - prevRaw;
    if (colorMode === 'absolute') {{
      if (Math.abs(diff) <= threshold) return '';
      beneficial = kind === 'income' ? diff > 0 : diff < 0;
    }} else {{
      if (Math.abs(prevRaw) < 1) return '';
      const pctChange = diff / Math.abs(prevRaw) * 100;
      if (Math.abs(pctChange) <= threshold) return '';
      beneficial = kind === 'income' ? pctChange > 0 : pctChange < 0;
    }}
  }}
  return beneficial ? 'color:#1a8c40;font-weight:600' : 'color:#c0392b;font-weight:600';
}}

function isHeatmapMetric(row) {{
  const metric = String(row?.metric || '');
  const group = String(row?.group || '');
  const kind = String(row?.kind || '');
  if (group === 'Воронка' && kind === 'percent_income') return true;
  return metric === 'ДРР от выкупов в эту дату' || metric === 'Маржинальность от выкупов за дату';
}}

function heatmapCellStyle(row, raw) {{
  if (!isHeatmapMetric(row) || raw === null || raw === undefined || !Number.isFinite(raw)) return '';
  const values = (row?.raw_values || []).filter(value => Number.isFinite(value));
  if (!values.length) return '';
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min;
  let score = span > 0 ? (raw - min) / span : 1;
  if (String(row?.metric || '') === 'ДРР от выкупов в эту дату') score = 1 - score;
  const hue = Math.round(8 + score * 132);
  const lightness = Math.round(94 - score * 18);
  return `background:hsl(${{hue}} 72% ${{lightness}}%);color:#0f172a;font-weight:700`;
}}

function metricColumnWidth(rows) {{
  const labels = ['Метрика', ...(rows || []).map(row => String(row.metric || ''))];
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  if (!ctx) return 240;
  ctx.font = '700 11px "Segoe UI", "Trebuchet MS", sans-serif';
  const textWidth = Math.max(...labels.map(label => ctx.measureText(label).width), 0);
  return Math.max(240, Math.min(560, Math.ceil(textWidth + 28)));
}}

function render(data) {{
  const dates = data.dates || [];
  const rows = data.rows || [];
  const formatDate = (iso) => {{
    if (String(iso).includes('..')) {{
      return String(iso).split('..').map(part => {{
        const [yy, mm, dd] = part.split('-');
        return `${{dd}}.${{mm}}.${{String(yy).slice(2)}}`;
      }}).join('-');
    }}
    const [y, m, d] = String(iso).split('-');
    return `${{d}}.${{m}}.${{String(y).slice(2)}}`;
  }};
  const head = '<tr><th>Гр.</th><th>Метрика</th><th>Итого / среднее</th>' + dates.map(d => `<th>${{formatDate(d)}}</th>`).join('') + '</tr>';
  const body = rows.length
    ? rows.reduce((html, row, idx) => {{
        const prev = idx > 0 ? rows[idx - 1].group : null;
        const group = row.group || '';
        const kind = row.kind || 'none';
        const rawVals = row.raw_values || [];
        const groupCollapsed = group ? collapsedGroups.has(group) : false;
        const groupRow = group && group !== prev
          ? `<tr class="group-row" data-group-header="${{escapeHtml(group)}}">
              <td><button type="button" class="group-toggle" data-group-toggle="${{escapeHtml(group)}}" aria-label="Свернуть группу">${{groupCollapsed ? '+' : '−'}}</button></td>
              <td class="group-label" data-group-toggle="${{escapeHtml(group)}}">${{group}}</td>
              <td></td>
              ${{dates.map(() => '<td></td>').join('')}}
            </tr>`
          : '';
        const checked = activeChartMetrics.has(row.metric) ? ' checked' : '';
        const cells = (row.values || []).map((v, i) => {{
          const heatmapStyle = heatmapCellStyle(row, rawVals[i]);
          const style = heatmapStyle || ((kind !== 'none' && kind !== 'neutral' && i > 0)
            ? cellColorStyle(kind, rawVals[i - 1], rawVals[i], row)
            : '');
          return style ? `<td style="${{style}}">${{v}}</td>` : `<td>${{v}}</td>`;
        }}).join('');
        const hiddenAttr = groupCollapsed ? ' hidden' : '';
        return html + groupRow + `<tr data-group="${{escapeHtml(group)}}"${{hiddenAttr}}><td><input class="metric-toggle" type="checkbox" data-metric="${{escapeHtml(row.metric)}}"${{checked}}></td><td>${{row.metric}}</td><td>${{row.total || ''}}</td>${{cells}}</tr>`;
      }}, '')
    : `<tr><td colspan="${{dates.length + 3}}">Нет данных за выбранный период</td></tr>`;
  tbl.style.setProperty('--metric-col-width', `${{metricColumnWidth(rows)}}px`);
  tbl.innerHTML = `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
  for (const toggle of [...tbl.querySelectorAll('[data-group-toggle]')]) {{
    toggle.addEventListener('click', () => {{
      const group = toggle.dataset.groupToggle || '';
      if (!group) return;
      if (collapsedGroups.has(group)) collapsedGroups.delete(group);
      else collapsedGroups.add(group);
      saveFilters();
      render(data);
    }});
  }}
  for (const input of [...tbl.querySelectorAll('input[data-metric]')]) {{
    input.addEventListener('change', () => {{
      const metric = input.dataset.metric;
      if (input.checked) activeChartMetrics.add(metric);
      else activeChartMetrics.delete(metric);
      saveFilters();
      renderChart(data);
    }});
  }}
  renderChart(data);
}}

function renderArticles(articles) {{
  const normalized = articles.map(item => typeof item === 'string'
    ? {{ article: item, nmid: '', stock: null, revenue: 0 }}
    : item
  );
  articleItems = [...normalized].sort((left, right) => {{
    const key = articleSort.key;
    const dir = articleSort.dir === 'asc' ? 1 : -1;
    const leftValue = key === 'article' ? String(left.article || '') : Number(left[key] || 0);
    const rightValue = key === 'article' ? String(right.article || '') : Number(right[key] || 0);
    if (key === 'article') return leftValue.localeCompare(rightValue, 'ru') * dir;
    return (leftValue - rightValue) * dir || String(left.article || '').localeCompare(String(right.article || ''), 'ru');
  }});
  const sortMark = (key) => articleSort.key === key ? (articleSort.dir === 'asc' ? ' ▲' : ' ▼') : '';
  articleList.innerHTML = articleItems.length
    ? `<table class="article-table">
        <thead><tr>
          <th data-sort="article">Артикул${{sortMark('article')}}</th>
          <th data-sort="stock">Ост.${{sortMark('stock')}}</th>
          <th data-sort="revenue">Выручка${{sortMark('revenue')}}</th>
        </tr></thead>
	        <tbody>${{articleItems.map(item => {{
	          const article = String(item.article || '');
	          const itemKey = String(item.nmid || item.article || '').trim();
	          const selected = selectedArticles.has(itemKey) ? ' selected' : '';
	          const safeArticle = escapeHtml(article);
	          const nmid = String(item.nmid || '').trim();
          const wbLink = /^\d+$/.test(nmid)
            ? `<a class="article-link" href="https://www.wildberries.ru/catalog/${{nmid}}/detail.aspx" target="_blank" rel="noopener noreferrer" title="Открыть товар на WB" data-skip-select="1">↗</a>`
            : '';
	          return `<tr class="article-item${{selected}}" data-article="${{safeArticle}}" data-key="${{escapeHtml(itemKey)}}" title="${{safeArticle}}">
	            <td>${{safeArticle}}${{wbLink}}</td>
            <td>${{formatCompactNumber(item.stock)}}</td>
            <td>${{formatCompactNumber(item.revenue)}}</td>
          </tr>`;
        }}).join('')}}</tbody>
      </table>`
    : '<div class="meta">Нет артикулов</div>';
  for (const th of [...articleList.querySelectorAll('th[data-sort]')]) {{
    th.addEventListener('click', () => {{
      const key = th.dataset.sort;
      articleSort = {{
        key,
        dir: articleSort.key === key && articleSort.dir === 'desc' ? 'asc' : 'desc'
      }};
      renderArticles(normalized);
    }});
  }}
  for (const [idx, item] of [...articleList.querySelectorAll('.article-item')].entries()) {{
    item.addEventListener('click', (event) => {{
	      if (event.target.closest('[data-skip-select="1"]')) return;
	      const article = item.dataset.key || item.dataset.article;
	      if (event.shiftKey && lastArticleIndex >= 0) {{
	        const [from, to] = [lastArticleIndex, idx].sort((a, b) => a - b);
	        for (let pos = from; pos <= to; pos++) selectedArticles.add(String(articleItems[pos].nmid || articleItems[pos].article || '').trim());
      }} else if (selectedArticles.has(article)) {{
        selectedArticles.delete(article);
        lastArticleIndex = idx;
      }} else {{
        selectedArticles.add(article);
        lastArticleIndex = idx;
      }}
      saveFilters();
      renderArticles(articles);
      loadData();
    }});
  }}
}}

async function loadSubjects() {{
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  const key = `${{df}}..${{dt}}`;
  if (key === subjectsLoadedFor) return;
  subjectsLoadedFor = key;
  const current = subjectEl.dataset.pendingValue !== undefined ? subjectEl.dataset.pendingValue : subjectEl.value;
  delete subjectEl.dataset.pendingValue;
  const qs = new URLSearchParams({{ date_from: df, date_to: dt }});
  const res = await fetch(`/api/analytics/buyout-subjects?${{qs.toString()}}`);
  const data = await res.json();
  const subjects = data.subjects || [];
  subjectEl.innerHTML = '<option value="">Все предметы</option>' + subjects.map(subject => {{
    const selected = subject === current ? ' selected' : '';
    const safeSubject = escapeHtml(subject);
    return `<option value="${{safeSubject}}"${{selected}}>${{safeSubject}}</option>`;
  }}).join('');
}}

async function loadArticles() {{
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  await loadSubjects();
  const subject = subjectEl.value;
  const key = `${{df}}..${{dt}}..${{subject}}`;
  if (key === articlesLoadedFor) return;
  articlesLoadedFor = key;
  const qs = new URLSearchParams({{ date_from: df, date_to: dt, subject }});
  const res = await fetch(`/api/analytics/buyout-articles?${{qs.toString()}}`);
  const data = await res.json();
  renderArticles(data.articles || []);
}}

async function loadData() {{
  const seq = ++requestSeq;
  saveFilters();
  const article = document.getElementById('article').value;
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  const subject = subjectEl.value;
  meta.textContent = 'Загружаю...';
  await loadArticles();
  const qs = new URLSearchParams({{ article, date_from: df, date_to: dt, subject, granularity: reportGranularity }});
  for (const articleName of selectedArticles) qs.append('articles', articleName);
  const res = await fetch(`/api/analytics/buyout-order-day?${{qs.toString()}}`);
  const data = await res.json();
  if (seq !== requestSeq) return;
  render(data);
  const selection = selectedArticles.size ? ` | Выбрано артикулов: ${{selectedArticles.size}}` : '';
  meta.textContent = `Дат: ${{(data.dates || []).length}} | Показан период: ${{data.effective_from || df}}..${{data.effective_to || dt}}${{selection}}`;
}}

document.getElementById('load').addEventListener('click', loadData);
document.getElementById('range-7').addEventListener('click', () => applyQuickRange(7));
document.getElementById('range-14').addEventListener('click', () => applyQuickRange(14));
document.getElementById('range-31').addEventListener('click', () => applyQuickRange(31));
document.getElementById('article').addEventListener('input', scheduleLoad);
document.getElementById('article').addEventListener('keydown', (e) => {{ if (e.key === 'Enter') loadData(); }});
subjectEl.addEventListener('change', () => {{
  selectedArticles.clear();
  articlesLoadedFor = '';
  saveFilters();
  loadData();
}});
document.getElementById('df').addEventListener('change', () => {{ subjectsLoadedFor = ''; articlesLoadedFor = ''; loadData(); }});
document.getElementById('dt').addEventListener('change', () => {{ subjectsLoadedFor = ''; articlesLoadedFor = ''; loadData(); }});
document.getElementById('select-all').addEventListener('click', async () => {{
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  articlesLoadedFor = '';
  await loadArticles();
	  for (const item of articleItems) selectedArticles.add(String(item.nmid || item.article || '').trim());
  saveFilters();
  articlesLoadedFor = '';
  await loadArticles();
  loadData();
}});
document.getElementById('clear-all').addEventListener('click', () => {{
  selectedArticles.clear();
  saveFilters();
  articlesLoadedFor = '';
  loadArticles();
  loadData();
}});
document.getElementById('toggle-articles').addEventListener('click', () => {{
  articlesCollapsed = !articlesCollapsed;
  syncArticlesPanel();
  saveFilters();
}});
document.getElementById('expand-articles').addEventListener('click', () => {{
  articlesCollapsed = false;
  syncArticlesPanel();
  saveFilters();
}});
restoreFilters();
loadData();
</script>
</body>
</html>
"""


ANALYTICS_DAY_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Аналитика по дням</title>
  <style>
    :root {{ --bg: #f8fafc; --card: #fff; --ink: #111827; --muted: #6b7280; --accent: #0369a1; --accent-2: #0891b2; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "Trebuchet MS", sans-serif; color: var(--ink); background: linear-gradient(180deg, #cffafe, transparent 240px), var(--bg); }}
    .wrap {{ width: min(100% - 28px, 1800px); margin: 0 auto; padding: 20px 0 28px; }}
    .top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }}
    .top a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .report-nav {{ display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 6px; max-width: 100%; }}
    .panel {{ background: var(--card); border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
    label {{ display: block; color: var(--muted); font-size: .84rem; margin-bottom: 4px; font-weight: 700; }}
    input {{ padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; }}
    button {{ padding: 10px 14px; border: 0; border-radius: 8px; background: linear-gradient(135deg, var(--accent), var(--accent-2)); color: #fff; font-weight: 700; cursor: pointer; }}
    .kpis {{ margin-top: 12px; display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }}
    .kpi {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 12px; }}
    .kpi .name {{ color: var(--muted); font-size: .78rem; font-weight: 700; text-transform: uppercase; letter-spacing: .4px; }}
    .kpi .val {{ margin-top: 6px; font-size: 1.15rem; font-weight: 800; }}
    .meta {{ margin-top: 10px; color: var(--muted); font-size: .9rem; }}
    .tbl {{ margin-top: 12px; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow: auto; max-height: calc(100vh - 320px); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #eef2f7; padding: 8px 10px; text-align: left; white-space: nowrap; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; }}
    th {{ position: sticky; top: 0; background: #f8fafc; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Аналитика по дням</h1>
      <div class="report-nav">{report_nav}</div>
    </div>
    <div class="panel">
      <div>
        <label for="df">Дата с</label>
        <input id="df" type="date" value="{date_from}">
      </div>
      <div>
        <label for="dt">Дата по</label>
        <input id="dt" type="date" value="{date_to}">
      </div>
      <div><button id="load">Показать</button></div>
    </div>
    <div class="kpis">
      <div class="kpi"><div class="name">Дней</div><div class="val" id="kpi-days">0</div></div>
      <div class="kpi"><div class="name">Продажи</div><div class="val" id="kpi-sales">0.00</div></div>
      <div class="kpi"><div class="name">Реклама</div><div class="val" id="kpi-ads">0.00</div></div>
      <div class="kpi"><div class="name">Чистая прибыль</div><div class="val" id="kpi-profit">0.00</div></div>
      <div class="kpi"><div class="name">ДРР</div><div class="val" id="kpi-drr">0%</div></div>
      <div class="kpi"><div class="name">% маржи</div><div class="val" id="kpi-margin">0%</div></div>
    </div>
    <div class="meta" id="meta">Загрузка...</div>
    <div class="tbl" id="tbl"></div>
  </div>
<script>
const cols = ["Дата", "Продажи", "Реклама", "Чистая прибыль", "ДРР", "% маржи"];
const meta = document.getElementById('meta');
const tbl = document.getElementById('tbl');
const filterKey = 'wb.analytics.day.filters';
let requestSeq = 0;

function restoreFilters() {{
  try {{
    const saved = JSON.parse(localStorage.getItem(filterKey) || '{{}}');
    if (saved.df) document.getElementById('df').value = saved.df;
    if (saved.dt) document.getElementById('dt').value = saved.dt;
  }} catch (err) {{}}
}}

function saveFilters() {{
  localStorage.setItem(filterKey, JSON.stringify({{
    df: document.getElementById('df').value,
    dt: document.getElementById('dt').value
  }}));
}}

function render(rows) {{
  const isNumericLike = (value) => /^-?\d[\d\s.,%]*$/.test(String(value ?? '').trim());
  const head = '<tr>' + cols.map(c => `<th>${{c}}</th>`).join('') + '</tr>';
  const body = rows.length
    ? rows.map(r => '<tr>' + cols.map(c => {{
        const value = r[c] ?? '';
        const cls = isNumericLike(value) ? ' class="num"' : '';
        return `<td${{cls}}>${{value}}</td>`;
      }}).join('') + '</tr>').join('')
    : `<tr><td colspan="${{cols.length}}">Нет данных за выбранный период</td></tr>`;
  tbl.innerHTML = `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
}}

function renderSummary(summary) {{
  document.getElementById('kpi-days').textContent = summary.days || '0';
  document.getElementById('kpi-sales').textContent = summary.sales || '0.00';
  document.getElementById('kpi-ads').textContent = summary.ads || '0.00';
  document.getElementById('kpi-profit').textContent = summary.profit || '0.00';
  document.getElementById('kpi-drr').textContent = summary.drr || '0%';
  document.getElementById('kpi-margin').textContent = summary.margin || '0%';
}}

async function loadData() {{
  const seq = ++requestSeq;
  saveFilters();
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  meta.textContent = 'Загружаю...';
  const res = await fetch(`/api/analytics/day?date_from=${{encodeURIComponent(df)}}&date_to=${{encodeURIComponent(dt)}}`);
  const data = await res.json();
  if (seq !== requestSeq) return;
  renderSummary(data.summary || {{}});
  render(data.rows || []);
  meta.textContent = `Строк: ${{(data.rows || []).length}}`;
}}

document.getElementById('load').addEventListener('click', loadData);
document.getElementById('df').addEventListener('change', loadData);
document.getElementById('dt').addEventListener('change', loadData);
restoreFilters();
loadData();
</script>
</body>
</html>
"""


ANALYTICS_PERIOD_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Период по артикулам</title>
  <style>
    :root {{ --bg: #f8fafc; --card: #fff; --ink: #111827; --muted: #6b7280; --accent: #0f766e; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "Trebuchet MS", sans-serif; color: var(--ink); background: linear-gradient(180deg, #e0f2fe, transparent 240px), var(--bg); }}
    .wrap {{ width: min(100% - 28px, 1800px); margin: 0 auto; padding: 20px 0 28px; }}
    .top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }}
    .top a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .report-nav {{ display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 6px; max-width: 100%; }}
    .panel {{ background: var(--card); border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
    label {{ display: block; color: var(--muted); font-size: .84rem; margin-bottom: 4px; font-weight: 700; }}
    input {{ padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; }}
    button {{ padding: 10px 14px; border: 0; border-radius: 8px; background: var(--accent); color: #fff; font-weight: 700; cursor: pointer; }}
    .meta {{ margin-top: 10px; color: var(--muted); font-size: .9rem; }}
    .tbl {{ margin-top: 12px; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow: auto; max-height: calc(100vh - 230px); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #eef2f7; padding: 8px 10px; text-align: left; white-space: nowrap; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; }}
    th {{ position: sticky; top: 0; background: #f8fafc; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Период по артикулам</h1>
      <div class="report-nav">{report_nav}</div>
    </div>
    <div class="panel">
      <div>
        <label for="article">Артикул</label>
        <input id="article" type="text" placeholder="например 12-0040-019">
      </div>
      <div>
        <label for="df">Период с</label>
        <input id="df" type="date" value="{date_from}">
      </div>
      <div>
        <label for="dt">Период по</label>
        <input id="dt" type="date" value="{date_to}">
      </div>
      <div><button id="load">Показать</button></div>
    </div>
    <div class="meta" id="meta">Загрузка...</div>
    <div class="tbl" id="tbl"></div>
  </div>
<script>
const cols = ["Артикул", "Продажи по нашей цене", "Реклама", "Чистая прибыль", "ДРР", "% маржи"];
const meta = document.getElementById('meta');
const tbl = document.getElementById('tbl');
const filterKey = 'wb.analytics.period.filters';
let requestSeq = 0;
let filterTimer = null;

function restoreFilters() {{
  try {{
    const saved = JSON.parse(localStorage.getItem(filterKey) || '{{}}');
    if (saved.article !== undefined) document.getElementById('article').value = saved.article;
    if (saved.df) document.getElementById('df').value = saved.df;
    if (saved.dt) document.getElementById('dt').value = saved.dt;
  }} catch (err) {{}}
}}

function saveFilters() {{
  localStorage.setItem(filterKey, JSON.stringify({{
    article: document.getElementById('article').value,
    df: document.getElementById('df').value,
    dt: document.getElementById('dt').value
  }}));
}}

function scheduleLoad() {{
  clearTimeout(filterTimer);
  filterTimer = setTimeout(loadData, 350);
}}

function render(rows) {{
  const isNumericLike = (value) => /^-?\d[\d\s.,%]*$/.test(String(value ?? '').trim());
  const head = '<tr>' + cols.map(c => `<th>${{c}}</th>`).join('') + '</tr>';
  const body = rows.length
    ? rows.map(r => '<tr>' + cols.map(c => {{
        const value = r[c] ?? '';
        const cls = isNumericLike(value) ? ' class="num"' : '';
        return `<td${{cls}}>${{value}}</td>`;
      }}).join('') + '</tr>').join('')
    : `<tr><td colspan="${{cols.length}}">Нет данных за выбранный период</td></tr>`;
  tbl.innerHTML = `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
}}

async function loadData() {{
  const seq = ++requestSeq;
  saveFilters();
  const article = document.getElementById('article').value;
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  meta.textContent = 'Загружаю...';
  const qs = new URLSearchParams({{ article, date_from: df, date_to: dt }});
  const res = await fetch(`/api/analytics/period?${{qs.toString()}}`);
  const data = await res.json();
  if (seq !== requestSeq) return;
  render(data.rows || []);
  meta.textContent = `Строк: ${{(data.rows || []).length}}`;
}}

document.getElementById('load').addEventListener('click', loadData);
document.getElementById('article').addEventListener('input', scheduleLoad);
document.getElementById('article').addEventListener('keydown', (e) => {{ if (e.key === 'Enter') loadData(); }});
document.getElementById('df').addEventListener('change', loadData);
document.getElementById('dt').addEventListener('change', loadData);
restoreFilters();
loadData();
</script>
</body>
</html>
"""


ANALYTICS_ARTICLE_DAY_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Дневная аналитика артикула</title>
  <style>
    :root {{ --bg: #f8fafc; --card: #fff; --ink: #111827; --muted: #6b7280; --accent: #0f766e; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "Trebuchet MS", sans-serif; color: var(--ink); background: linear-gradient(180deg, #dcfce7, transparent 240px), var(--bg); }}
    .wrap {{ width: min(100% - 28px, 1800px); margin: 0 auto; padding: 20px 0 28px; }}
    .top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }}
    .top a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .report-nav {{ display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 6px; max-width: 100%; }}
    .panel {{ background: var(--card); border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
    label {{ display: block; color: var(--muted); font-size: .84rem; margin-bottom: 4px; font-weight: 700; }}
    input {{ padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; min-width: 180px; }}
    button {{ padding: 10px 14px; border: 0; border-radius: 8px; background: var(--accent); color: #fff; font-weight: 700; cursor: pointer; }}
    .meta {{ margin-top: 10px; color: var(--muted); font-size: .9rem; }}
    .tbl {{ margin-top: 12px; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow: auto; max-height: calc(100vh - 230px); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #eef2f7; padding: 8px 10px; text-align: left; white-space: nowrap; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; }}
    th {{ position: sticky; top: 0; background: #f8fafc; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Дни по артикулу</h1>
      <div class="report-nav">{report_nav}</div>
    </div>
    <div class="panel">
      <div>
        <label for="article">Артикул</label>
        <input id="article" type="text" placeholder="например 12-0040-019">
      </div>
      <div>
        <label for="df">Дата с</label>
        <input id="df" type="date" value="{date_from}">
      </div>
      <div>
        <label for="dt">Дата по</label>
        <input id="dt" type="date" value="{date_to}">
      </div>
      <div><button id="load">Показать</button></div>
    </div>
    <div class="meta" id="meta">Загрузка...</div>
    <div class="tbl" id="tbl"></div>
  </div>
<script>
const cols = [
  "Артикул / SKU",
  "Дата",
  "Продажи по нашей цене",
  "Реклама",
  "Чистая прибыль",
  "ДРР",
  "% маржи"
];
const meta = document.getElementById('meta');
const tbl = document.getElementById('tbl');
const filterKey = 'wb.analytics.articleDay.filters';
let requestSeq = 0;
let filterTimer = null;

function restoreFilters() {{
  try {{
    const saved = JSON.parse(localStorage.getItem(filterKey) || '{{}}');
    if (saved.article !== undefined) document.getElementById('article').value = saved.article;
    if (saved.df) document.getElementById('df').value = saved.df;
    if (saved.dt) document.getElementById('dt').value = saved.dt;
  }} catch (err) {{}}
}}

function saveFilters() {{
  localStorage.setItem(filterKey, JSON.stringify({{
    article: document.getElementById('article').value,
    df: document.getElementById('df').value,
    dt: document.getElementById('dt').value
  }}));
}}

function scheduleLoad() {{
  clearTimeout(filterTimer);
  filterTimer = setTimeout(loadData, 350);
}}

function render(rows) {{
  const isNumericLike = (value) => /^-?\d[\d\s.,%]*$/.test(String(value ?? '').trim());
  const head = '<tr>' + cols.map(c => `<th>${{c}}</th>`).join('') + '</tr>';
  const body = rows.length
    ? rows.map(r => '<tr>' + cols.map(c => {{
        const value = r[c] ?? '';
        const cls = isNumericLike(value) ? ' class="num"' : '';
        return `<td${{cls}}>${{value}}</td>`;
      }}).join('') + '</tr>').join('')
    : `<tr><td colspan="${{cols.length}}">Нет данных по фильтру</td></tr>`;
  tbl.innerHTML = `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
}}

async function loadData() {{
  const seq = ++requestSeq;
  saveFilters();
  const article = document.getElementById('article').value;
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  meta.textContent = 'Загружаю...';
  const qs = new URLSearchParams({{ article, date_from: df, date_to: dt }});
  const res = await fetch(`/api/analytics/article-day?${{qs.toString()}}`);
  const data = await res.json();
  if (seq !== requestSeq) return;
  render(data.rows || []);
  meta.textContent = `Строк: ${{(data.rows || []).length}}`;
}}

document.getElementById('load').addEventListener('click', loadData);
document.getElementById('article').addEventListener('input', scheduleLoad);
document.getElementById('article').addEventListener('keydown', (e) => {{ if (e.key === 'Enter') loadData(); }});
document.getElementById('df').addEventListener('change', loadData);
document.getElementById('dt').addEventListener('change', loadData);
restoreFilters();
loadData();
</script>
</body>
</html>
"""


PRELIMINARY_ECONOMICS_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Предварительная экономика по заказам</title>
  <style>
    :root {{ --bg: #f8fafc; --card: #fff; --ink: #111827; --muted: #6b7280; --accent: #0f766e; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "Trebuchet MS", sans-serif; color: var(--ink); background: linear-gradient(180deg, #e0f2fe, transparent 240px), var(--bg); }}
    .wrap {{ width: min(100% - 28px, 1800px); margin: 0 auto; padding: 20px 0 28px; }}
    .top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }}
    .top a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .report-nav {{ display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 6px; max-width: 100%; }}
    .panel {{ background: var(--card); border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
    label {{ display: block; color: var(--muted); font-size: .84rem; margin-bottom: 4px; font-weight: 700; }}
    input {{ padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; }}
    button {{ padding: 10px 14px; border: 0; border-radius: 8px; background: var(--accent); color: #fff; font-weight: 700; cursor: pointer; }}
    .meta {{ margin-top: 10px; color: var(--muted); font-size: .9rem; }}
    .tbl {{ margin-top: 12px; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow: auto; max-height: calc(100vh - 230px); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #eef2f7; padding: 8px 10px; text-align: left; white-space: nowrap; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; }}
    th {{ position: sticky; top: 0; background: #f8fafc; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Предварительная экономика по заказам</h1>
      <div class="report-nav">{report_nav}</div>
    </div>
    <div class="panel">
      <div>
        <label for="article">Артикул</label>
        <input id="article" type="text" placeholder="например 12-0040-019">
      </div>
      <div>
        <label for="buyout">% выкупа</label>
        <input id="buyout" type="number" min="0" max="100" step="0.1" value="30">
      </div>
      <div>
        <label for="df">Период с</label>
        <input id="df" type="date" value="{date_from}">
      </div>
      <div>
        <label for="dt">Период по</label>
        <input id="dt" type="date" value="{date_to}">
      </div>
      <div><button id="load">Показать</button></div>
    </div>
    <div class="meta" id="meta">Загрузка...</div>
    <div class="tbl" id="tbl"></div>
  </div>
<script>
const cols = [
  "Артикул / SKU",
  "Дата",
  "Количество заказов",
  "Сумма заказов",
  "% выкупа",
  "Комиссия, ₽",
  "Эквайринг, ₽",
  "Реклама, ₽",
  "% рекламы",
  "Дополнительные расходы, ₽",
  "Предварительная прибыль, ₽"
  ,"% маржинальности"
];
const meta = document.getElementById('meta');
const tbl = document.getElementById('tbl');
const filterKey = 'wb.analytics.preliminary.filters';
let requestSeq = 0;
let filterTimer = null;

function restoreFilters() {{
  try {{
    const saved = JSON.parse(localStorage.getItem(filterKey) || '{{}}');
    if (saved.article !== undefined) document.getElementById('article').value = saved.article;
    if (saved.buyout !== undefined) document.getElementById('buyout').value = saved.buyout;
    if (saved.df) document.getElementById('df').value = saved.df;
    if (saved.dt) document.getElementById('dt').value = saved.dt;
  }} catch (err) {{}}
}}

function saveFilters() {{
  localStorage.setItem(filterKey, JSON.stringify({{
    article: document.getElementById('article').value,
    buyout: document.getElementById('buyout').value,
    df: document.getElementById('df').value,
    dt: document.getElementById('dt').value
  }}));
}}

function scheduleLoad() {{
  clearTimeout(filterTimer);
  filterTimer = setTimeout(loadData, 350);
}}

function render(rows) {{
  const isNumericLike = (value) => /^-?\d[\d\s.,%]*$/.test(String(value ?? '').trim());
  const head = '<tr>' + cols.map(c => `<th>${{c}}</th>`).join('') + '</tr>';
  const body = rows.length
    ? rows.map(r => '<tr>' + cols.map(c => {{
        const value = r[c] ?? '';
        const cls = isNumericLike(value) ? ' class="num"' : '';
        return `<td${{cls}}>${{value}}</td>`;
      }}).join('') + '</tr>').join('')
    : `<tr><td colspan="${{cols.length}}">Нет данных за выбранный период</td></tr>`;
  tbl.innerHTML = `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
}}

async function loadData() {{
  const seq = ++requestSeq;
  saveFilters();
  const article = document.getElementById('article').value;
  const buyout = document.getElementById('buyout').value || '30';
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  meta.textContent = 'Загружаю...';
  const qs = new URLSearchParams({{ article, date_from: df, date_to: dt, buyout_percent: buyout }});
  const res = await fetch(`/api/analytics/preliminary-economics?${{qs.toString()}}`);
  const data = await res.json();
  if (seq !== requestSeq) return;
  render(data.rows || []);
  const extra = (data.expense_components || []).length
    ? ` | Статьи допрасходов: ${{data.expense_components.join(', ')}}`
    : '';
  meta.textContent = `Строк: ${{(data.rows || []).length}} | % выкупа: ${{data.buyout_percent || '30.00%'}} | Зафиксированный % допрасходов: ${{data.additional_rate || '0.00%'}}${{extra}}`;
}}

document.getElementById('load').addEventListener('click', loadData);
document.getElementById('article').addEventListener('input', scheduleLoad);
document.getElementById('article').addEventListener('keydown', (e) => {{ if (e.key === 'Enter') loadData(); }});
document.getElementById('buyout').addEventListener('input', scheduleLoad);
document.getElementById('df').addEventListener('change', loadData);
document.getElementById('dt').addEventListener('change', loadData);
restoreFilters();
loadData();
</script>
</body>
</html>
"""


PRELIMINARY_ECONOMICS_SUMMARY_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Предварительная экономика по заказам (период)</title>
  <style>
    :root {{ --bg: #f8fafc; --card: #fff; --ink: #111827; --muted: #6b7280; --accent: #0f766e; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "Trebuchet MS", sans-serif; color: var(--ink); background: linear-gradient(180deg, #e0f2fe, transparent 240px), var(--bg); }}
    .wrap {{ width: min(100% - 28px, 1800px); margin: 0 auto; padding: 20px 0 28px; }}
    .top {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }}
    .top a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .report-nav {{ display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 6px; max-width: 100%; }}
    .panel {{ background: var(--card); border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
    label {{ display: block; color: var(--muted); font-size: .84rem; margin-bottom: 4px; font-weight: 700; }}
    input {{ padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; }}
    button {{ padding: 10px 14px; border: 0; border-radius: 8px; background: var(--accent); color: #fff; font-weight: 700; cursor: pointer; }}
    .meta {{ margin-top: 10px; color: var(--muted); font-size: .9rem; }}
    .tbl {{ margin-top: 12px; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow: auto; max-height: calc(100vh - 230px); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #eef2f7; padding: 8px 10px; text-align: left; white-space: nowrap; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; }}
    th {{ position: sticky; top: 0; background: #f8fafc; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <h1>Предварительная экономика по периоду</h1>
      <div class="report-nav">{report_nav}</div>
    </div>
    <div class="panel">
      <div>
        <label for="article">Артикул</label>
        <input id="article" type="text" placeholder="например 12-0040-019">
      </div>
      <div>
        <label for="buyout">% выкупа</label>
        <input id="buyout" type="number" min="0" max="100" step="0.1" value="30">
      </div>
      <div>
        <label for="df">Период с</label>
        <input id="df" type="date" value="{date_from}">
      </div>
      <div>
        <label for="dt">Период по</label>
        <input id="dt" type="date" value="{date_to}">
      </div>
      <div><button id="load">Показать</button></div>
    </div>
    <div class="meta" id="meta">Загрузка...</div>
    <div class="tbl" id="tbl"></div>
  </div>
<script>
const cols = [
  "Артикул / SKU",
  "Период",
  "Количество заказов",
  "Сумма заказов",
  "% выкупа",
  "Комиссия, ₽",
  "Эквайринг, ₽",
  "Реклама, ₽",
  "% рекламы",
  "Дополнительные расходы, ₽",
  "Предварительная прибыль, ₽",
  "% маржинальности"
];
const meta = document.getElementById('meta');
const tbl = document.getElementById('tbl');
const filterKey = 'wb.analytics.preliminarySummary.filters';
let requestSeq = 0;
let filterTimer = null;

function restoreFilters() {{
  try {{
    const saved = JSON.parse(localStorage.getItem(filterKey) || '{{}}');
    if (saved.article !== undefined) document.getElementById('article').value = saved.article;
    if (saved.buyout !== undefined) document.getElementById('buyout').value = saved.buyout;
    if (saved.df) document.getElementById('df').value = saved.df;
    if (saved.dt) document.getElementById('dt').value = saved.dt;
  }} catch (err) {{}}
}}

function saveFilters() {{
  localStorage.setItem(filterKey, JSON.stringify({{
    article: document.getElementById('article').value,
    buyout: document.getElementById('buyout').value,
    df: document.getElementById('df').value,
    dt: document.getElementById('dt').value
  }}));
}}

function scheduleLoad() {{
  clearTimeout(filterTimer);
  filterTimer = setTimeout(loadData, 350);
}}

function render(rows) {{
  const isNumericLike = (value) => /^-?\d[\d\s.,%]*$/.test(String(value ?? '').trim());
  const head = '<tr>' + cols.map(c => `<th>${{c}}</th>`).join('') + '</tr>';
  const body = rows.length
    ? rows.map(r => '<tr>' + cols.map(c => {{
        const value = r[c] ?? '';
        const cls = isNumericLike(value) ? ' class="num"' : '';
        return `<td${{cls}}>${{value}}</td>`;
      }}).join('') + '</tr>').join('')
    : `<tr><td colspan="${{cols.length}}">Нет данных за выбранный период</td></tr>`;
  tbl.innerHTML = `<table><thead>${{head}}</thead><tbody>${{body}}</tbody></table>`;
}}

async function loadData() {{
  const seq = ++requestSeq;
  saveFilters();
  const article = document.getElementById('article').value;
  const buyout = document.getElementById('buyout').value || '30';
  const df = document.getElementById('df').value;
  const dt = document.getElementById('dt').value;
  meta.textContent = 'Загружаю...';
  const qs = new URLSearchParams({{ article, date_from: df, date_to: dt, buyout_percent: buyout, aggregate: '1' }});
  const res = await fetch(`/api/analytics/preliminary-economics?${{qs.toString()}}`);
  const data = await res.json();
  if (seq !== requestSeq) return;
  render(data.rows || []);
  const extra = (data.expense_components || []).length
    ? ` | Статьи допрасходов: ${{data.expense_components.join(', ')}}`
    : '';
  meta.textContent = `Строк: ${{(data.rows || []).length}} | % выкупа: ${{data.buyout_percent || '30.00%'}} | Зафиксированный % допрасходов: ${{data.additional_rate || '0.00%'}}${{extra}}`;
}}

document.getElementById('load').addEventListener('click', loadData);
document.getElementById('article').addEventListener('input', scheduleLoad);
document.getElementById('article').addEventListener('keydown', (e) => {{ if (e.key === 'Enter') loadData(); }});
document.getElementById('buyout').addEventListener('input', scheduleLoad);
document.getElementById('df').addEventListener('change', loadData);
document.getElementById('dt').addEventListener('change', loadData);
restoreFilters();
loadData();
</script>
</body>
</html>
"""


FUNNEL_UPLOAD_HTML = """\
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Загрузка воронки</title>
  <style>
    :root {{ --bg:#f8fafc; --card:#fff; --ink:#111827; --muted:#6b7280; --accent:#0f766e; }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family:"Segoe UI","Trebuchet MS",sans-serif; color:var(--ink); background:linear-gradient(180deg,#e0f2fe,transparent 240px),var(--bg); }}
    .wrap {{ max-width: 920px; margin:0 auto; padding:22px 16px 40px; }}
    .top {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; margin-bottom:14px; flex-wrap:wrap; }}
    .top a {{ color:var(--accent); text-decoration:none; font-weight:700; }}
    h1 {{ margin:0; font-size:1.35rem; }}
    .panel {{ background:var(--card); border:1px solid #e5e7eb; border-radius:12px; padding:16px; }}
    label {{ display:block; color:var(--muted); font-size:.82rem; font-weight:700; margin-bottom:6px; }}
    input {{ display:block; width:100%; border:1px solid #cbd5e1; border-radius:8px; padding:10px; background:#fff; }}
    button {{ margin-top:12px; padding:10px 14px; border:0; border-radius:8px; background:var(--accent); color:#fff; font-weight:800; cursor:pointer; }}
    button:disabled {{ opacity:.6; cursor:wait; }}
    .meta {{ margin-top:12px; color:var(--muted); font-size:.9rem; line-height:1.45; }}
    .ok {{ color:#047857; }}
    .err {{ color:#b91c1c; }}
    code {{ background:#eef2ff; padding:2px 5px; border-radius:5px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <h1>Загрузка показов и переходов</h1>
        <div class="meta">Поддерживается Excel как выгрузка WB: лист <code>Товары</code>, дата в колонке J, показы в K, переходы в M.</div>
      </div>
      <div class="report-nav">{report_nav}</div>
    </div>
    <div class="panel">
      <label for="file">Файл Excel</label>
      <input id="file" type="file" accept=".xlsx">
      <button id="upload">Загрузить</button>
      <div class="meta" id="status">Файл обновит только даты, которые есть внутри него. Остальные даты останутся без изменений.</div>
    </div>
  </div>
<script>
const fileEl = document.getElementById('file');
const btn = document.getElementById('upload');
const statusEl = document.getElementById('status');

function asBase64(file) {{
  return new Promise((resolve, reject) => {{
    const reader = new FileReader();
    reader.onload = () => {{
      const text = String(reader.result || '');
      resolve(text.includes(',') ? text.split(',', 2)[1] : text);
    }};
    reader.onerror = reject;
    reader.readAsDataURL(file);
  }});
}}

btn.addEventListener('click', async () => {{
  const file = fileEl.files && fileEl.files[0];
  if (!file) {{
    statusEl.textContent = 'Выбери файл .xlsx';
    statusEl.className = 'meta err';
    return;
  }}
  btn.disabled = true;
  statusEl.textContent = 'Загружаю...';
  statusEl.className = 'meta';
  try {{
    const content = await asBase64(file);
    const resp = await fetch('/api/analytics/funnel-upload', {{
      method: 'POST',
      headers: {{ 'Content-Type': 'application/json' }},
      body: JSON.stringify({{ filename: file.name, content }})
    }});
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `HTTP ${{resp.status}}`);
    statusEl.className = 'meta ok';
    statusEl.textContent = `Готово: строк ${{data.rows}}, товаров ${{data.nmids}}, период ${{data.date_from}}..${{data.date_to}}, показов ${{data.impressions}}. Обновлены только даты из файла.`;
  }} catch (err) {{
    statusEl.className = 'meta err';
    statusEl.textContent = `Ошибка: ${{err.message || err}}`;
  }} finally {{
    btn.disabled = false;
  }}
}});
</script>
</body>
</html>
"""


def _decimal_text(value: object) -> str:
  if value is None:
    return "0"
  try:
    return str(Decimal(str(value).replace(" ", "").replace(",", ".")))
  except InvalidOperation:
    return "0"


def _cell_text(value: object) -> str:
  if value is None:
    return ""
  if isinstance(value, float) and value.is_integer():
    return str(int(value))
  return str(value).strip()


def _import_funnel_excel(path: str, source_name: str) -> dict[str, object]:
  from openpyxl import load_workbook

  workbook = load_workbook(path, read_only=False, data_only=True)
  if "Товары" not in workbook.sheetnames:
    raise ValueError("В файле нет листа 'Товары'")
  sheet = workbook["Товары"]

  header_row = None
  headers: list[str] = []
  for row_idx, row in enumerate(sheet.iter_rows(values_only=True), start=1):
    values = [str(value).strip() if value is not None else "" for value in row]
    if "Артикул WB" in values and "Дата" in values and "Показы" in values and "Переходы в карточку" in values:
      header_row = row_idx
      headers = values
      break
  if header_row is None:
    raise ValueError("Не нашёл заголовки: нужны 'Артикул WB', 'Дата', 'Показы', 'Переходы в карточку'")

  idx = {header: pos for pos, header in enumerate(headers) if header}
  rows: list[tuple[str, str, str, str, str, str, str, str, str]] = []
  funnel_rows: list[dict[str, str]] = []
  dates: set[str] = set()
  nmids: set[str] = set()
  impressions_total = Decimal("0")
  transitions_total = Decimal("0")

  for row in sheet.iter_rows(min_row=header_row + 1, values_only=True):
    row_date = _cell_text(row[idx["Дата"]])
    nm_id = _cell_text(row[idx["Артикул WB"]])
    if not row_date or not nm_id:
      continue
    impressions = _decimal_text(row[idx["Показы"]])
    transitions = _decimal_text(row[idx["Переходы в карточку"]])
    cart_count = _decimal_text(row[idx["Положили в корзину"]]) if "Положили в корзину" in idx else "0"
    order_count = _decimal_text(row[idx["Заказали, шт"]]) if "Заказали, шт" in idx else "0"
    buyout_count = _decimal_text(row[idx["Выкупили, шт"]]) if "Выкупили, шт" in idx else "0"
    supplier_article = _cell_text(row[idx["Артикул продавца"]]) if "Артикул продавца" in idx else ""
    product_name = _cell_text(row[idx["Название"]]) if "Название" in idx else ""
    brand = _cell_text(row[idx["Бренд"]]) if "Бренд" in idx else ""
    subject = _cell_text(row[idx["Предмет"]]) if "Предмет" in idx else ""
    dates.add(row_date)
    nmids.add(nm_id)
    impressions_total += Decimal(impressions)
    transitions_total += Decimal(transitions)
    rows.append((row_date, nm_id, supplier_article, impressions, transitions, cart_count, order_count, buyout_count, source_name))
    funnel_rows.append({
      "nmId": nm_id,
      "supplierArticle": supplier_article,
      "productName": product_name,
      "brand": brand,
      "currency": "RUB",
      "date": row_date,
      "product_subjectId": "",
      "product_subjectName": subject,
      "addToCartConversion": _decimal_text(row[idx["Конверсия в корзину, %"]]) if "Конверсия в корзину, %" in idx else "0",
      "addToWishlistCount": _decimal_text(row[idx["Добавили в отложенные"]]) if "Добавили в отложенные" in idx else "0",
      "buyoutCount": buyout_count,
      "buyoutPercent": _decimal_text(row[idx["Процент выкупа"]]) if "Процент выкупа" in idx else "0",
      "buyoutSum": _decimal_text(row[idx["Выкупили на сумму, ₽"]]) if "Выкупили на сумму, ₽" in idx else "0",
      "cartCount": cart_count,
      "cartToOrderConversion": _decimal_text(row[idx["Конверсия в заказ, %"]]) if "Конверсия в заказ, %" in idx else "0",
      "openCount": transitions,
      "orderCount": order_count,
      "orderSum": _decimal_text(row[idx["Заказали на сумму, ₽"]]) if "Заказали на сумму, ₽" in idx else "0",
      "cancelCount": _decimal_text(row[idx["Отменили, шт"]]) if "Отменили, шт" in idx else "0",
      "cancelSum": _decimal_text(row[idx["Отменили на сумму, ₽"]]) if "Отменили на сумму, ₽" in idx else "0",
      "rating": _decimal_text(row[idx["Рейтинг карточки"]]) if "Рейтинг карточки" in idx else "0",
      "reviewRating": _decimal_text(row[idx["Рейтинг по отзывам"]]) if "Рейтинг по отзывам" in idx else "0",
      "isDeleted": _cell_text(row[idx["Удаленный товар"]]) if "Удаленный товар" in idx else "",
      "wbClubOrderCount": _decimal_text(row[idx["Заказали ВБ клуб, шт"]]) if "Заказали ВБ клуб, шт" in idx else "0",
      "wbClubOrderSum": _decimal_text(row[idx["Заказали на сумму ВБ клуб, ₽"]]) if "Заказали на сумму ВБ клуб, ₽" in idx else "0",
      "wbClubBuyoutCount": _decimal_text(row[idx["Выкупили ВБ клуб, шт"]]) if "Выкупили ВБ клуб, шт" in idx else "0",
      "wbClubBuyoutSum": _decimal_text(row[idx["Выкупили на сумму ВБ клуб, ₽"]]) if "Выкупили на сумму ВБ клуб, ₽" in idx else "0",
      "wbClubCancelCount": _decimal_text(row[idx["Отменили ВБ клуб, шт"]]) if "Отменили ВБ клуб, шт" in idx else "0",
      "wbClubCancelSum": _decimal_text(row[idx["Отменили на сумму ВБ клуб, ₽"]]) if "Отменили на сумму ВБ клуб, ₽" in idx else "0",
    })

  if not rows:
    raise ValueError("В файле нет строк с датой и Артикул WB")

  with sqlite3.connect(DB_PATH) as conn:
    conn.execute(
      """
      CREATE TABLE IF NOT EXISTS funnel_impressions_upload (
        date TEXT NOT NULL,
        nmId TEXT NOT NULL,
        supplierArticle TEXT,
        impressions TEXT,
        openCount TEXT,
        cartCount TEXT,
        orderCount TEXT,
        buyoutCount TEXT,
        source_file TEXT,
        PRIMARY KEY (date, nmId)
      )
      """
    )
    funnel_columns = [
      "nmId", "supplierArticle", "productName", "brand", "currency", "date",
      "product_subjectId", "product_subjectName", "addToCartConversion", "addToWishlistCount",
      "buyoutCount", "buyoutPercent", "buyoutSum", "cartCount", "cartToOrderConversion",
      "openCount", "orderCount", "orderSum", "cancelCount", "cancelSum", "rating",
      "reviewRating", "isDeleted", "wbClubOrderCount", "wbClubOrderSum",
      "wbClubBuyoutCount", "wbClubBuyoutSum", "wbClubCancelCount", "wbClubCancelSum",
    ]
    conn.execute(
      "CREATE TABLE IF NOT EXISTS funnel_analytics ("
      + ", ".join(f'"{column}" TEXT' for column in funnel_columns)
      + ")"
    )
    conn.executemany("DELETE FROM funnel_impressions_upload WHERE date = ?", [(row_date,) for row_date in dates])
    conn.executemany("DELETE FROM funnel_analytics WHERE date = ?", [(row_date,) for row_date in dates])
    conn.executemany(
      """
      INSERT INTO funnel_impressions_upload (
        date, nmId, supplierArticle, impressions, openCount, cartCount, orderCount, buyoutCount, source_file
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(date, nmId) DO UPDATE SET
        supplierArticle=excluded.supplierArticle,
        impressions=excluded.impressions,
        openCount=excluded.openCount,
        cartCount=excluded.cartCount,
        orderCount=excluded.orderCount,
        buyoutCount=excluded.buyoutCount,
        source_file=excluded.source_file
      """,
      rows,
    )
    placeholders = ", ".join("?" for _ in funnel_columns)
    quoted_columns = ", ".join(f'"{column}"' for column in funnel_columns)
    conn.executemany(
      f"INSERT INTO funnel_analytics ({quoted_columns}) VALUES ({placeholders})",
      [[row.get(column, "") for column in funnel_columns] for row in funnel_rows],
    )
    conn.commit()

  return {
    "rows": len(rows),
    "nmids": len(nmids),
    "date_from": min(dates),
    "date_to": max(dates),
    "dates": len(dates),
    "impressions": f"{int(impressions_total):,}".replace(",", " "),
    "transitions": f"{int(transitions_total):,}".replace(",", " "),
  }


def _rebuild_buyout_order_day_from_store() -> int:
  src_path = str(ROOT / "src")
  if src_path not in sys.path:
    sys.path.insert(0, src_path)

  from wb_gsheets.config import load_settings
  from wb_gsheets.sqlite_store import SQLiteStore
  from wb_gsheets.transform import (
    build_buyout_order_day_rows,
    build_nm_mapping,
    extract_filter_values,
    extract_orders_filters,
    filter_sales_rows,
    sheet_values_to_dicts,
  )

  settings = load_settings()
  store = SQLiteStore(DB_PATH)
  sku_values = store.get_values(settings.cogs_sheet)
  nm_mapping = build_nm_mapping(sku_values, article_filter_type=settings.article_filter_type)
  allowed_nm = {str(nm_id).strip() for nm_id in nm_mapping.keys() if str(nm_id).strip()}
  article_filter_values = settings.article_filter_values or extract_filter_values(
    sku_values,
    article_filter_type=settings.article_filter_type,
  )
  orders_supplier_articles, _orders_nm_ids = extract_orders_filters(sku_values)

  sales_rows = sheet_values_to_dicts(store.get_values(settings.raw_sales_sheet))
  if article_filter_values or nm_mapping:
    sales_rows = filter_sales_rows(
      sales_rows,
      article_filter_type=settings.article_filter_type,
      article_filter_values=article_filter_values,
      nm_id_filter_values=allowed_nm,
    )

  orders_rows = []
  for row in sheet_values_to_dicts(store.get_values(settings.raw_orders_sheet)):
    nm_id = str(row.get("nmId", "")).strip()
    supplier_article = str(row.get("supplierArticle", "")).strip()
    if allowed_nm and nm_id not in allowed_nm:
      continue
    if not allowed_nm and orders_supplier_articles and supplier_article not in orders_supplier_articles:
      continue
    orders_rows.append(row)

  funnel_by_nm: dict[str, dict[str, object]] = {}
  for row in sheet_values_to_dicts(store.get_values(settings.funnel_analytics_sheet)):
    nm_id = str(row.get("nmId", "")).strip()
    row_date = str(row.get("date", "")).strip()
    if not nm_id or not row_date:
      continue
    if allowed_nm and nm_id not in allowed_nm:
      continue
    entry = funnel_by_nm.setdefault(nm_id, {
      "currency": str(row.get("currency", "") or "RUB"),
      "product": {
        "nmId": nm_id,
        "vendorCode": str(row.get("supplierArticle", "") or ""),
        "title": str(row.get("productName", "") or ""),
        "brandName": str(row.get("brand", "") or ""),
      },
      "history": [],
    })
    entry["history"].append({
      "date": row_date,
      "orderSum": row.get("orderSum") or 0,
      "orderCount": row.get("orderCount") or 0,
      "buyoutCount": row.get("buyoutCount") or 0,
      "buyoutSum": row.get("buyoutSum") or 0,
    })

  rows = build_buyout_order_day_rows(
    orders_rows=orders_rows,
    sales_rows=sales_rows,
    ads_rows=sheet_values_to_dicts(store.get_values(settings.raw_ads_sheet)),
    nm_mapping=nm_mapping,
    funnel_data=list(funnel_by_nm.values()),
  )
  store.replace_table("buyout_order_day", rows)
  return max(0, len(rows) - 1)


def _render_db_page(selected_table: str | None, page: int, page_size: int) -> bytes:
    store = db_store()
    existing_tables = store.list_tables()
    tables = list(dict.fromkeys(CORE_TABLES + existing_tables))

    current_table = selected_table if selected_table in tables else (tables[0] if tables else None)
    headers: list[str] = []
    rows: list[list[str]] = []
    total = 0
    table_exists = current_table in set(existing_tables) if current_table else False

    if current_table and table_exists:
        offset = max(page - 1, 0) * page_size
        headers, rows, total = store.fetch_table_page(current_table, page_size, offset)

    total_pages = max(1, ceil(total / page_size)) if current_table else 1
    page = min(max(page, 1), total_pages)

    nav_items = []
    existing_set = set(existing_tables)
    for table in tables:
        cls = "active" if table == current_table else ""
        if table not in existing_set:
            cls = (cls + " pending").strip()
        table_q = quote_plus(table)
        suffix = "" if table in existing_set else " (пусто)"
        nav_items.append(
            f'<a class="tbl {cls}" href="/db?table={table_q}&page=1">{escape(table)}{escape(suffix)}</a>'
        )
    nav_html = "".join(nav_items)

    split_col_idx = -1
    if current_table == "finance_article_day_detail" and "Реклама" in headers:
        split_col_idx = headers.index("Реклама")

    if headers:
        head_cells = []
        for idx, col in enumerate(headers):
            cls = ' class="sep-left"' if idx == split_col_idx else ""
            head_cells.append(f"<th{cls}>{escape(col)}</th>")
        head = "".join(head_cells)

        body_rows = []
        for row in rows:
            cells = []
            for idx, cell in enumerate(row):
                cls = ' class="sep-left"' if idx == split_col_idx else ""
                cells.append(f"<td{cls}>{escape(cell)}</td>")
            body_rows.append("<tr>" + "".join(cells) + "</tr>")

        body = "".join(body_rows) if body_rows else f'<tr><td colspan="{len(headers)}">Нет данных на этой странице</td></tr>'
        table_html = (
            "<div class=\"meta\">"
            f"<strong>{escape(current_table or '')}</strong>"
            f"<span>Строк: {total}</span>"
            f"<span>Страница {page}/{total_pages}</span>"
            "</div>"
            "<div class=\"tbl-wrap\"><table><thead><tr>"
            f"{head}"
            "</tr></thead><tbody>"
            f"{body}"
            "</tbody></table></div>"
        )
    else:
        if current_table and not table_exists:
            table_html = (
                "<div class=\"empty\">"
                f"Таблица {escape(current_table)} еще не создана. Запусти синхронизацию на главной странице, и она появится автоматически."
                "</div>"
            )
        else:
            table_html = "<div class=\"empty\">Таблица пока пустая.</div>"

    prev_page = max(1, page - 1)
    next_page = min(total_pages, page + 1)
    selected_q = quote_plus(current_table or "")
    pager = ""
    if current_table and table_exists:
        pager = (
            '<div class="pager">'
            f'<a href="/db?table={selected_q}&page={prev_page}">← Назад</a>'
            f'<a href="/db?table={selected_q}&page={next_page}">Вперед →</a>'
            "</div>"
        )

    html = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>WB SQLite Browser</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f3f4f6; color: #111827; }}
    .top {{ padding: 14px 18px; border-bottom: 1px solid #d1d5db; background: #fff; display: flex; justify-content: space-between; gap: 12px; align-items: center; }}
    .top a {{ color: #1d4ed8; text-decoration: none; font-weight: 600; }}
    .layout {{ display: grid; grid-template-columns: 250px 1fr; min-height: calc(100vh - 58px); }}
    .side {{ border-right: 1px solid #d1d5db; background: #fff; padding: 12px; overflow-y: auto; }}
    .content {{ width: min(100%, 1800px); margin: 0 auto; padding: 16px; overflow-x: auto; }}
    .tbl {{ display: block; padding: 9px 10px; border-radius: 8px; color: #111827; text-decoration: none; margin-bottom: 6px; }}
    .tbl:hover {{ background: #eef2ff; }}
    .tbl.active {{ background: #4338ca; color: #fff; }}
    .tbl.pending {{ color: #6b7280; background: #f9fafb; }}
    .meta {{ margin-bottom: 10px; display: flex; gap: 14px; align-items: center; }}
    .tbl-wrap {{ background: #fff; border: 1px solid #d1d5db; border-radius: 10px; overflow: auto; max-height: calc(100vh - 180px); }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #eef2f7; border-right: 1px solid #f3f4f6; padding: 8px 10px; text-align: left; white-space: nowrap; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; }}
    th {{ position: sticky; top: 0; background: #f8fafc; }}
    .sep-left {{ border-left: 3px solid #94a3b8; }}
    .pager {{ margin-top: 12px; display: flex; gap: 12px; }}
    .pager a {{ color: #1d4ed8; text-decoration: none; font-weight: 600; }}
    .empty {{ color: #6b7280; padding: 8px; }}
    @media (max-width: 900px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .side {{ border-right: 0; border-bottom: 1px solid #d1d5db; }}
    }}
  </style>
</head>
<body>
  <div class="top">
    <strong>SQLite: {escape(DB_PATH)}</strong>
    <a href="/">← К синхронизации</a>
  </div>
  <div class="layout">
    <aside class="side">{nav_html}</aside>
    <main class="content">{table_html}{pager}</main>
  </div>
</body>
</html>"""
    return html.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # silence default access log
        pass

    def _send_html(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_redirect(self, location: str) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_unauthorized(self) -> None:
        body = "Требуется пароль".encode("utf-8")
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="WB Analytics"')
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _is_authorized(self) -> bool:
        if not SITE_PASSWORD:
            return True
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(header.split(" ", 1)[1]).decode("utf-8")
        except Exception:
            return False
        if decoded == SITE_PASSWORD:
            return True
        return decoded.split(":", 1)[-1] == SITE_PASSWORD

    def do_GET(self):
        if not self._is_authorized():
            self._send_unauthorized()
            return

        parsed = urlparse(self.path)

        if parsed.path == "/":
            today = date.today()
            week_ago = today - timedelta(days=6)
            body = HTML.format(
                date_from=week_ago.isoformat(),
                date_to=today.isoformat(),
                report_links=REPORT_LINKS_HTML,
                table_links=_render_table_links_html(),
            ).encode()
            self._send_html(body)

        elif parsed.path == "/analytics":
            self._send_redirect("/")

        elif parsed.path == "/analytics/period":
            today = date.today()
            week_ago = today - timedelta(days=6)
            body = ANALYTICS_PERIOD_HTML.format(
                date_from=week_ago.isoformat(),
                date_to=today.isoformat(),
                report_nav=REPORT_NAV_HTML,
            ).encode("utf-8")
            self._send_html(body)

        elif parsed.path == "/analytics/day":
            today = date.today()
            week_ago = today - timedelta(days=6)
            body = ANALYTICS_DAY_HTML.format(
                date_from=week_ago.isoformat(),
                date_to=today.isoformat(),
                report_nav=REPORT_NAV_HTML,
            ).encode("utf-8")
            self._send_html(body)

        elif parsed.path == "/analytics/article-day":
            today = date.today()
            week_ago = today - timedelta(days=6)
            body = ANALYTICS_ARTICLE_DAY_HTML.format(
                date_from=week_ago.isoformat(),
                date_to=today.isoformat(),
                report_nav=REPORT_NAV_HTML,
            ).encode("utf-8")
            self._send_html(body)

        elif parsed.path == "/analytics/buyout-order-day":
            today = date.today()
            month_ago = today - timedelta(days=29)
            body = BUYOUT_ORDER_DAY_HTML.format(
                date_from=month_ago.isoformat(),
                date_to=today.isoformat(),
                report_nav=REPORT_NAV_HTML,
                page_title="Выкупы по датам заказов",
                granularity="day",
            ).encode("utf-8")
            self._send_html(body)

        elif parsed.path == "/analytics/buyout-order-week":
            today = date.today()
            month_ago = today - timedelta(days=29)
            body = BUYOUT_ORDER_DAY_HTML.format(
                date_from=month_ago.isoformat(),
                date_to=today.isoformat(),
                report_nav=REPORT_NAV_HTML,
                page_title="Выкупы по неделям",
                granularity="week",
            ).encode("utf-8")
            self._send_html(body)

        elif parsed.path == "/analytics/funnel-upload":
            body = FUNNEL_UPLOAD_HTML.format(
                report_nav=REPORT_NAV_HTML,
            ).encode("utf-8")
            self._send_html(body)

        elif parsed.path == "/analytics/preliminary-economics":
          today = date.today()
          week_ago = today - timedelta(days=6)
          body = PRELIMINARY_ECONOMICS_HTML.format(
            date_from=week_ago.isoformat(),
            date_to=today.isoformat(),
            report_nav=REPORT_NAV_HTML,
          ).encode("utf-8")
          self._send_html(body)

        elif parsed.path == "/analytics/preliminary-economics-summary":
          today = date.today()
          week_ago = today - timedelta(days=6)
          body = PRELIMINARY_ECONOMICS_SUMMARY_HTML.format(
            date_from=week_ago.isoformat(),
            date_to=today.isoformat(),
            report_nav=REPORT_NAV_HTML,
          ).encode("utf-8")
          self._send_html(body)

        elif parsed.path == "/api/analytics/period":
            params = parse_qs(parsed.query)
            article = (params.get("article") or [""])[0]
            date_from = (params.get("date_from") or [""])[0]
            date_to = (params.get("date_to") or [""])[0]
            if not date_from or not date_to:
                self._send_json({"error": "date_from and date_to are required", "rows": []}, status=400)
                return
            rows = _fetch_period_analytics(date_from, date_to, article)
            self._send_json({"rows": rows})

        elif parsed.path == "/api/analytics/day":
            params = parse_qs(parsed.query)
            date_from = (params.get("date_from") or [""])[0]
            date_to = (params.get("date_to") or [""])[0]
            if not date_from or not date_to:
                self._send_json({"error": "date_from and date_to are required", "rows": [], "summary": {}}, status=400)
                return
            data = _fetch_day_analytics(date_from, date_to)
            self._send_json(data)

        elif parsed.path == "/api/analytics/article-day":
            params = parse_qs(parsed.query)
            article = (params.get("article") or [""])[0]
            date_from = (params.get("date_from") or [""])[0] or None
            date_to = (params.get("date_to") or [""])[0] or None
            rows = _fetch_article_day_analytics(article, date_from, date_to)
            self._send_json({"rows": rows})

        elif parsed.path == "/api/analytics/buyout-order-day":
            params = parse_qs(parsed.query)
            article = (params.get("article") or [""])[0]
            articles = params.get("articles") or []
            subject = (params.get("subject") or [""])[0]
            date_from = (params.get("date_from") or [""])[0]
            date_to = (params.get("date_to") or [""])[0]
            granularity = (params.get("granularity") or ["day"])[0]
            if granularity not in {"day", "week"}:
                granularity = "day"
            if not date_from or not date_to:
                self._send_json({"error": "date_from and date_to are required", "dates": [], "rows": []}, status=400)
                return
            try:
                payload = _fetch_buyout_order_day_pivot(date_from, date_to, article, articles=articles, subject=subject, granularity=granularity)
            except ValueError:
                self._send_json({"error": "invalid date format", "dates": [], "rows": []}, status=400)
                return
            self._send_json(payload)

        elif parsed.path == "/api/analytics/buyout-subjects":
            params = parse_qs(parsed.query)
            date_from = (params.get("date_from") or [""])[0]
            date_to = (params.get("date_to") or [""])[0]
            if not date_from or not date_to:
                self._send_json({"error": "date_from and date_to are required", "subjects": []}, status=400)
                return
            try:
                subjects = _fetch_buyout_subjects(date_from, date_to)
            except ValueError:
                self._send_json({"error": "invalid date format", "subjects": []}, status=400)
                return
            self._send_json({"subjects": subjects})

        elif parsed.path == "/api/analytics/buyout-articles":
            params = parse_qs(parsed.query)
            subject = (params.get("subject") or [""])[0]
            date_from = (params.get("date_from") or [""])[0]
            date_to = (params.get("date_to") or [""])[0]
            if not date_from or not date_to:
                self._send_json({"error": "date_from and date_to are required", "articles": []}, status=400)
                return
            try:
                articles = _fetch_buyout_articles(date_from, date_to, subject=subject)
            except ValueError:
                self._send_json({"error": "invalid date format", "articles": []}, status=400)
                return
            self._send_json({"articles": articles})

        elif parsed.path == "/api/analytics/preliminary-economics":
          params = parse_qs(parsed.query)
          article = (params.get("article") or [""])[0]
          date_from = (params.get("date_from") or [""])[0]
          date_to = (params.get("date_to") or [""])[0]
          buyout_raw = (params.get("buyout_percent") or ["30"])[0]
          aggregate = (params.get("aggregate") or ["0"])[0] == "1"
          if not date_from or not date_to:
            self._send_json(
              {
                "error": "date_from and date_to are required",
                "rows": [],
                "additional_rate": "0.00%",
                "expense_components": [],
              },
              status=400,
            )
            return
          try:
            buyout_percent = float(buyout_raw)
          except ValueError:
            buyout_percent = 30.0
          payload = _fetch_preliminary_economics(
            date_from,
            date_to,
            article,
            buyout_percent=buyout_percent,
            aggregate_by_period=aggregate,
          )
          self._send_json(payload)

        elif parsed.path == "/db":
            params = parse_qs(parsed.query)
            selected_table = (params.get("table") or [""])[0] or None
            try:
                page = int((params.get("page") or ["1"])[0])
            except ValueError:
                page = 1
            body = _render_db_page(selected_table=selected_table, page=max(page, 1), page_size=200)
            self._send_html(body)

        elif parsed.path == "/stream":
            params = parse_qs(parsed.query)
            date_from = (params.get("date_from") or [""])[0]
            date_to = (params.get("date_to") or [""])[0]
            skip_ads = (params.get("skip_ads") or ["0"])[0] == "1"
            skip_funnel = (params.get("skip_funnel") or ["0"])[0] == "1"

            if not date_from or not date_to:
                self.send_response(400)
                self.end_headers()
                return

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            log_q: queue.Queue = queue.Queue()
            thread = threading.Thread(
                target=run_sync,
                args=(date_from, date_to, skip_ads, skip_funnel, log_q),
                daemon=True,
            )
            thread.start()

            finished = False
            start_ts = time.time()
            last_heartbeat = 0.0
            while not finished:
                try:
                    kind, text = log_q.get(timeout=1)
                except queue.Empty:
                    if thread.is_alive():
                        now = time.time()
                        if now - last_heartbeat >= 5:
                            elapsed = int(now - start_ts)
                            minutes, seconds = divmod(elapsed, 60)
                            kind = "heartbeat"
                            text = f"Выполняется... {minutes:02d}:{seconds:02d}"
                            last_heartbeat = now
                        else:
                            continue
                    else:
                        kind = "error"
                        text = "❌ Процесс завершился без финального статуса. Проверь лог ниже."
                payload = json.dumps({"type": kind, "text": text})
                try:
                    self.wfile.write(f"data: {payload}\n\n".encode())
                    self.wfile.flush()
                except BrokenPipeError:
                    break
                if kind in ("done", "error"):
                    finished = True

            thread.join(timeout=5)

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if not self._is_authorized():
            self._send_unauthorized()
            return

        parsed = urlparse(self.path)
        if parsed.path != "/api/analytics/funnel-upload":
            self._send_json({"error": "not found"}, status=404)
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            self._send_json({"error": "empty body"}, status=400)
            return

        tmp_path = ""
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            filename = str(payload.get("filename") or "upload.xlsx")
            content = str(payload.get("content") or "")
            raw = base64.b64decode(content, validate=True)
            if not raw:
                raise ValueError("empty file")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                tmp.write(raw)
                tmp_path = tmp.name
            result = _import_funnel_excel(tmp_path, filename)
            result["buyout_order_day_rows"] = _rebuild_buyout_order_day_from_store()
        except (json.JSONDecodeError, UnicodeDecodeError, binascii.Error, ValueError) as exc:
            self._send_json({"error": str(exc)}, status=400)
            return
        except Exception as exc:
            self._send_json({"error": str(exc)}, status=400)
            return
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

        self._send_json(result)


def main():
    host = os.getenv("WEB_APP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.getenv("WEB_APP_PORT", "8765"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Открой в браузере: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановлен.")


if __name__ == "__main__":
    main()
