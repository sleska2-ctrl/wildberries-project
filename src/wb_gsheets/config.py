from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    wb_finance_token: str
    wb_adv_token: str
    sqlite_db_path: str
    google_service_account_file: str
    google_spreadsheet_id: str
    raw_sales_sheet: str
    raw_orders_sheet: str
    raw_ads_sheet: str
    daily_pnl_sheet: str
    cogs_sheet: str
    funnel_analytics_sheet: str
    article_filter_type: str
    article_filter_values: list[str]
    default_date_from: str
    default_date_to: str


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Environment variable {name} is required")
    return value


def load_settings() -> Settings:
    load_dotenv()

    shared_wb_token = os.getenv("WB_API_TOKEN", "").strip()
    wb_finance_token = os.getenv("WB_FINANCE_TOKEN", "").strip() or shared_wb_token
    wb_adv_token = os.getenv("WB_ADV_TOKEN", "").strip() or shared_wb_token
    article_filter_type = os.getenv("ARTICLE_FILTER_TYPE", "nmId").strip() or "nmId"
    if article_filter_type not in {"nmId", "vendorCode"}:
        raise ValueError("ARTICLE_FILTER_TYPE must be nmId or vendorCode")

    article_filter_values = [
        value.strip()
        for value in os.getenv("ARTICLE_FILTER_VALUES", "").split(",")
        if value.strip()
    ]

    if not wb_finance_token:
        raise ValueError("WB_FINANCE_TOKEN or WB_API_TOKEN is required")
    if not wb_adv_token:
        raise ValueError("WB_ADV_TOKEN or WB_API_TOKEN is required")

    return Settings(
        wb_finance_token=wb_finance_token,
        wb_adv_token=wb_adv_token,
        sqlite_db_path=os.getenv("SQLITE_DB_PATH", "data/wb_sync.db").strip() or "data/wb_sync.db",
        google_service_account_file=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip(),
        google_spreadsheet_id=os.getenv("GOOGLE_SPREADSHEET_ID", "").strip(),
        raw_sales_sheet=os.getenv("GOOGLE_RAW_SALES_SHEET", "raw_sales").strip() or "raw_sales",
        raw_orders_sheet=os.getenv("GOOGLE_RAW_ORDERS_SHEET", "raw_orders").strip() or "raw_orders",
        raw_ads_sheet=os.getenv("GOOGLE_RAW_ADS_SHEET", "raw_ads").strip() or "raw_ads",
        daily_pnl_sheet=os.getenv("GOOGLE_DAILY_PNL_SHEET", "daily_pnl").strip() or "daily_pnl",
        cogs_sheet=os.getenv("GOOGLE_COGS_SHEET", "SKU").strip() or "SKU",
        funnel_analytics_sheet=os.getenv("GOOGLE_FUNNEL_ANALYTICS_SHEET", "funnel_analytics").strip() or "funnel_analytics",
        article_filter_type=article_filter_type,
        article_filter_values=article_filter_values,
        default_date_from=_require("DEFAULT_DATE_FROM"),
        default_date_to=_require("DEFAULT_DATE_TO"),
    )
