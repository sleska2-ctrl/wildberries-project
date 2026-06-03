"""Ozon data sync: loads daily summary into ozon_daily_summary table."""
from __future__ import annotations

import argparse
import os
import sqlite3
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

SELLER_BASE = "https://api-seller.ozon.ru"
PERF_BASE = "https://api-performance.ozon.ru"
TIMEOUT = 30
MSK = ZoneInfo("Europe/Moscow")


def _to_float(value: Any) -> float:
    try:
        return float(str(value or "0").replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(float(str(value or "0").replace(" ", "").replace(",", ".")))
    except (TypeError, ValueError):
        return 0


def _parse_report_day(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], fmt).date().isoformat()
        except ValueError:
            pass
    return text[:10]


def _iter_month_windows(date_from: str, date_to: str):
    """Yield API-safe month-bounded windows covering date_from..date_to.

    OZON finance transaction API rejects date filters that cross calendar
    months. The first window starts at the first day of date_from's month to
    preserve the old behavior of loading the full finance month and filtering
    locally.
    """
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    if start > end:
        raise ValueError(f"date_from must be <= date_to: {date_from}..{date_to}")

    current = date(start.year, start.month, 1)
    while current <= end:
        next_month = date(current.year + (current.month // 12), (current.month % 12) + 1, 1)
        window_end = min(end, next_month - timedelta(days=1))
        yield current.isoformat(), window_end.isoformat()
        current = next_month


def _iter_bounded_month_windows(date_from: str, date_to: str):
    """Yield month-bounded windows that start exactly at date_from."""
    current = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    if current > end:
        raise ValueError(f"date_from must be <= date_to: {date_from}..{date_to}")

    while current <= end:
        next_month = date(current.year + (current.month // 12), (current.month % 12) + 1, 1)
        window_end = min(end, next_month - timedelta(days=1))
        yield current.isoformat(), window_end.isoformat()
        current = next_month

# Finance API: service names that count as forward delivery cost
_DELIVERY_SERVICES = {
    "MarketplaceServiceItemDirectFlowLogistic",
    "MarketplaceServiceItemRedistributionLastMileCourier",
    "MarketplaceServiceItemDelivToCustomer",
    "MarketplaceServiceItemDirectFlowLogisticVDC",
}
# Service names that count as return delivery cost
_RETURN_DELIVERY_SERVICES = {
    "MarketplaceServiceItemReturnFlowLogistic",
    "MarketplaceServiceItemRedistributionReturnsPVZ",
    "MarketplaceServiceItemReturnAfterDeliveryToCustomer",
}


def load_env(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


class OzonClient:
    def __init__(self, client_id: str, api_key: str, perf_client_id: str = "", perf_secret: str = "") -> None:
        self.client_id = client_id
        self.api_key = api_key
        self.perf_client_id = perf_client_id
        self.perf_secret = perf_secret
        self._perf_access_token: str | None = None
        self._perf_token_expires: float = 0.0
        self._last_ad_campaign_ids: set[str] = set()
        self._sku_ad_stats_incomplete = False
        self._seller_headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }

    def _post(self, path: str, payload: dict[str, Any]) -> Any:
        for attempt in range(5):
            try:
                r = requests.post(f"{SELLER_BASE}{path}", headers=self._seller_headers, json=payload, timeout=TIMEOUT)
            except requests.RequestException as exc:
                if attempt == 4:
                    raise
                time.sleep(15 * (attempt + 1))
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(15 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        r.raise_for_status()
        return r.json()

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        for attempt in range(5):
            try:
                r = requests.get(f"{SELLER_BASE}{path}", headers=self._seller_headers, params=params, timeout=TIMEOUT)
            except requests.RequestException as exc:
                if attempt == 4:
                    raise
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        r.raise_for_status()
        return r.json()

    def _get_perf_token(self) -> str:
        if self._perf_access_token and time.time() < self._perf_token_expires - 60:
            return self._perf_access_token
        r = requests.post(
            f"{PERF_BASE}/api/client/token",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json={"client_id": self.perf_client_id, "client_secret": self.perf_secret, "grant_type": "client_credentials"},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        self._perf_access_token = data["access_token"]
        self._perf_token_expires = time.time() + data.get("expires_in", 1800)
        return self._perf_access_token

    def _perf_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        headers = {"Authorization": f"Bearer {self._get_perf_token()}", "Accept": "application/json"}
        r = requests.get(f"{PERF_BASE}{path}", headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()

    def _perf_post(self, path: str, body: dict[str, Any]) -> Any:
        headers = {"Authorization": f"Bearer {self._get_perf_token()}", "Accept": "application/json",
                   "Content-Type": "application/json"}
        r = requests.post(f"{PERF_BASE}{path}", headers=headers, json=body, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()

    def _perf_poll_report(self, uuid: str, max_wait: int = 60) -> list[dict[str, Any]]:
        """Poll async Performance API report by UUID until state=OK, return rows."""
        headers = {"Authorization": f"Bearer {self._get_perf_token()}", "Accept": "application/json"}
        for _ in range(max_wait // 3 + 1):
            time.sleep(3)
            r = requests.get(f"{PERF_BASE}/api/client/statistics/{uuid}", headers=headers, timeout=TIMEOUT)
            data = r.json()
            state = data.get("state", "")
            if state == "OK":
                link = data.get("link", "")
                if not link.startswith("http"):
                    link = PERF_BASE + link
                dl = requests.get(link, headers=headers, timeout=TIMEOUT)
                return dl.json().get("rows", [])
            if state == "ERROR":
                print(f"  [warn] performance report {uuid} error: {data.get('error')}")
                return []
        print(f"  [warn] performance report {uuid} timed out after {max_wait}s")
        return []

    def _perf_poll_statistics_report(self, uuid: str, max_wait: int = 300) -> dict[str, Any]:
        """Poll campaign statistics report by UUID and return raw report JSON."""
        headers = {"Authorization": f"Bearer {self._get_perf_token()}", "Accept": "application/json"}
        for _ in range(max_wait // 3 + 1):
            time.sleep(3)
            r = requests.get(f"{PERF_BASE}/api/client/statistics/{uuid}", headers=headers, timeout=TIMEOUT)
            data = r.json()
            state = data.get("state", "")
            if state == "OK":
                link = data.get("link", "")
                if not link.startswith("http"):
                    link = PERF_BASE + link
                dl = requests.get(link, headers=headers, timeout=TIMEOUT)
                dl.raise_for_status()
                payload = dl.json()
                return payload if isinstance(payload, dict) else {}
            if state == "ERROR":
                print(f"  [warn] performance statistics report {uuid} error: {data.get('error')}")
                return {}
        print(f"  [warn] performance statistics report {uuid} timed out after {max_wait}s")
        return {}

    def fetch_finance_by_day(self, date_from: str, date_to: str) -> dict[str, dict[str, float]]:
        """Returns {day: {accruals_for_sale, sale_commission, delivery_charge, return_delivery, return_accruals, for_pay}}.

        delivery_charge and return_delivery are parsed from the `services` array,
        not from the top-level fields (which are always 0 in practice).
        """
        by_day: dict[str, dict[str, float]] = defaultdict(lambda: {
            "accruals_for_sale": 0.0,
            "sale_commission": 0.0,
            "delivery_charge": 0.0,
            "return_delivery": 0.0,
            "return_accruals": 0.0,
            "for_pay": 0.0,
        })

        for window_from, window_to in _iter_month_windows(date_from, date_to):
            month_from = f"{window_from}T00:00:00.000Z"
            month_to = f"{window_to}T23:59:59.999Z"
            page = 1
            while True:
                data = self._post("/v3/finance/transaction/list", {
                    "filter": {
                        "date": {"from": month_from, "to": month_to},
                        "transaction_type": "all",
                    },
                    "page": page,
                    "page_size": 1000,
                })
                operations = data.get("result", {}).get("operations", [])
                if not operations:
                    break
                for op in operations:
                    op_date = op.get("operation_date", "")[:10]
                    if not (date_from <= op_date <= date_to):
                        continue
                    d = by_day[op_date]
                    d["accruals_for_sale"] += float(op.get("accruals_for_sale", 0) or 0)
                    d["sale_commission"] += float(op.get("sale_commission", 0) or 0)
                    d["for_pay"] += float(op.get("amount", 0) or 0)
                    # Delivery costs are in the services array, not top-level fields
                    for svc in op.get("services", []):
                        svc_name = svc.get("name", "")
                        svc_price = float(svc.get("price", 0) or 0)
                        if svc_name in _DELIVERY_SERVICES:
                            d["delivery_charge"] += svc_price  # negative values
                        elif svc_name in _RETURN_DELIVERY_SERVICES:
                            d["return_delivery"] += svc_price  # negative values
                    # Return accruals: for return operations accruals_for_sale is negative
                    op_type = op.get("type", "")
                    if op_type == "returns":
                        d["return_accruals"] += float(op.get("accruals_for_sale", 0) or 0)
                if len(operations) < 1000:
                    break
                page += 1

        return dict(by_day)

    def fetch_sku_accruals_by_day(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        """Returns per-SKU per-day commission and delivery using POST /v1/finance/accrual/by-day.

        Each returned dict: {day, sku, sale_commission, delivery_charge}
        POSTING category: products[].commission and products[].delivery are per-SKU.
        ITEM category: item_fees.fees[].fees[] are per-SKU with type_id mapping.
        """
        from datetime import timedelta

        # Type IDs from /v1/finance/accrual/types
        COMMISSION_TYPES = {69}  # SaleCommission
        DELIVERY_TYPES = {28, 29, 30, 32, 59, 73, 2, 12, 63, 64}  # LastMile*, Logistic, Shipment, BackwardShipment, ReturnFlowLogistic, CrossDock, RfbsDomestic*

        by_day_sku: dict[tuple[str, str], dict[str, float]] = {}

        # Iterate day by day
        current = date.fromisoformat(date_from)
        end = date.fromisoformat(date_to)
        while current <= end:
            day_str = current.isoformat()
            last_id = ""
            while True:
                try:
                    data = self._post("/v1/finance/accrual/by-day", {
                        "date": day_str,
                        "last_id": last_id,
                    })
                except Exception as exc:
                    print(f"  [warn] accrual/by-day {day_str}: {exc}")
                    break

                accruals = data.get("accruals") or []
                for a in accruals:
                    cat = a.get("accrued_category", "")

                    if cat == "POSTING":
                        products = (a.get("posting") or {}).get("products") or []
                        for prod in products:
                            sku = str(prod.get("sku") or "")
                            if not sku:
                                continue
                            key = (day_str, sku)
                            bucket = by_day_sku.setdefault(key, {"day": day_str, "sku": sku, "sale_commission": 0.0, "delivery_charge": 0.0})
                            # commission: commission.sale_commission.amount
                            comm = prod.get("commission")
                            if comm:
                                amt = float(((comm.get("sale_commission") or {}).get("amount")) or 0)
                                if amt:
                                    bucket["sale_commission"] += amt
                            # delivery: sum all services amounts
                            deliv = prod.get("delivery")
                            if deliv:
                                svcs = deliv.get("services") or []
                                if svcs:
                                    for svc in svcs:
                                        bucket["delivery_charge"] += float((svc.get("accrued") or {}).get("amount") or 0)
                                else:
                                    bucket["delivery_charge"] += float(((deliv.get("total_accrued") or {}).get("amount")) or 0)

                    elif cat == "ITEM":
                        item_fees = a.get("item_fees") or {}
                        for fee_group in (item_fees.get("fees") or []):
                            sku = str(fee_group.get("sku") or "")
                            if not sku:
                                continue
                            key = (day_str, sku)
                            bucket = by_day_sku.setdefault(key, {"day": day_str, "sku": sku, "sale_commission": 0.0, "delivery_charge": 0.0})
                            for sub_fee in (fee_group.get("fees") or []):
                                tid = sub_fee.get("type_id")
                                amt = float(((sub_fee.get("accrued") or {}).get("amount")) or 0)
                                if tid in COMMISSION_TYPES:
                                    bucket["sale_commission"] += amt
                                elif tid in DELIVERY_TYPES:
                                    bucket["delivery_charge"] += amt

                last_id = data.get("last_id") or ""
                if not accruals or not last_id:
                    break

            current += timedelta(days=1)

        return list(by_day_sku.values())

    def fetch_spp_by_order_date(self, date_from: str, date_to: str) -> dict[str, float]:
        """Returns {order_date: avg_spp_pct} via /v2/posting/fbo/list with financial_data.

        Uses financial_data.products[].old_price as seller price and products[].price as
        customer price. Groups by in_process_at (order date).
        Formula: avg_spp = SUM((old_price - customer_price) * qty) / SUM(old_price * qty) * 100
        No individual GET requests needed — financial_data is included in list response.
        """
        price_sum: dict[str, float] = defaultdict(float)
        coinvest_sum: dict[str, float] = defaultdict(float)

        # Wide window: delivery can lag order by 3-14 days
        window_from = (datetime.fromisoformat(date_from) - timedelta(days=14)).strftime("%Y-%m-%dT00:00:00.000Z")
        window_to = datetime.fromisoformat(date_to).strftime("%Y-%m-%dT23:59:59.999Z")

        offset = 0
        limit = 1000
        total_processed = 0
        print("  Fetching FBO postings (v2 with financial_data)...")
        while True:
            data = self._post("/v2/posting/fbo/list", {
                "dir": "asc",
                "filter": {
                    "since": window_from,
                    "to": window_to,
                },
                "limit": limit,
                "offset": offset,
                "with": {"financial_data": True},
            })
            postings = data.get("result", [])
            for p in postings:
                in_process = (p.get("in_process_at") or "")[:10]
                if not in_process or not (date_from <= in_process <= date_to):
                    continue
                total_processed += 1
                products = p.get("products", [])
                fin_products = {fp["product_id"]: fp for fp in
                                (p.get("financial_data") or {}).get("products", [])}
                for item in products:
                    sku = item.get("sku") or item.get("product_id")
                    qty = int(item.get("quantity", 0) or 0)
                    # customer price = what buyer paid (products[].price)
                    customer_price = float(item.get("price", 0) or 0)
                    # old_price = original price before all discounts (financial_data)
                    fp = fin_products.get(sku, {})
                    old_price = float(fp.get("old_price", 0) or customer_price)
                    if customer_price <= 0 or old_price <= 0:
                        continue
                    price_sum[in_process] += old_price * qty
                    coinvest_sum[in_process] += (old_price - customer_price) * qty
            if len(postings) < limit:
                break
            offset += limit

        print(f"  {total_processed} postings used for SPP calculation")
        result: dict[str, float] = {}
        for day, total_price in price_sum.items():
            if total_price > 0:
                result[day] = round(coinvest_sum[day] / total_price * 100, 2)
        return result

    def fetch_stock_on_warehouses(self) -> list[dict[str, Any]]:
        """Returns list of stock rows per sku per warehouse."""
        rows = []
        offset = 0
        limit = 1000
        while True:
            data = self._post("/v2/analytics/stock_on_warehouses", {
                "limit": limit,
                "offset": offset,
                "warehouse_type": "ALL",
            })
            batch = data.get("result", {}).get("rows", [])
            rows.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return rows

    def fetch_sku_analytics(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        """Returns per-SKU aggregated analytics for the period."""
        rows = []
        offset = 0
        limit = 1000
        while True:
            data = self._post("/v1/analytics/data", {
                "date_from": date_from,
                "date_to": date_to,
                "dimension": ["sku"],
                "metrics": ["revenue", "ordered_units", "delivered_units", "returns", "cancellations"],
                "limit": limit,
                "offset": offset,
            })
            batch = data.get("result", {}).get("data", [])
            for item in batch:
                dim = item["dimensions"][0]
                m = item["metrics"]
                rows.append({
                    "sku": dim["id"],
                    "item_name": dim.get("name", ""),
                    "revenue": float(m[0]),
                    "ordered_units": int(m[1]),
                    "delivered_units": int(m[2]),
                    "returns": int(m[3]),
                    "cancellations": int(m[4]),
                })
            if len(batch) < limit:
                break
            offset += limit
        return rows

    def fetch_sku_day_analytics(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        """POST /v1/analytics/data with dimension=[sku, day] — daily per-SKU metrics.
        Main request (9 metrics) → OZON silently drops hits_tocart_pdp, returns 8 values:
          [0]revenue [1]ordered_units [2]delivered_units [3]returns [4]cancellations
          [5]avg_spp [6]hits_view [7]hits_view_pdp
        Second request fetches only funnel metrics to get hits_tocart_pdp separately.
        """
        # Request 1: combined metrics (avg_spp + hits_view/pdp come through; tocart dropped)
        rows: dict[tuple, dict] = {}
        offset = 0
        limit = 1000
        while True:
            data = self._post("/v1/analytics/data", {
                "date_from": date_from,
                "date_to": date_to,
                "dimension": ["sku", "day"],
                "metrics": [
                    "revenue", "ordered_units", "delivered_units", "returns", "cancellations",
                    "avg_spp", "hits_view", "hits_view_pdp", "hits_tocart_pdp",
                ],
                "limit": limit,
                "offset": offset,
            })
            batch = data.get("result", {}).get("data", [])
            for item in batch:
                dims = item.get("dimensions", [])
                sku = dims[0].get("id", "") if len(dims) > 0 else ""
                item_name = dims[0].get("name", "") if len(dims) > 0 else ""
                day = (dims[1].get("id", "") if len(dims) > 1 else "")[:10]
                m = item.get("metrics", [])
                if not sku or not day:
                    continue
                # OZON returns 8 values (drops hits_tocart_pdp):
                # [0]revenue [1]orders [2]delivered [3]returns [4]cancels
                # [5]avg_spp [6]hits_view [7]hits_view_pdp
                rows[(sku, day)] = {
                    "sku": sku,
                    "item_name": item_name,
                    "day": day,
                    "revenue": float(m[0] or 0) if len(m) > 0 else 0.0,
                    "ordered_units": int(m[1] or 0) if len(m) > 1 else 0,
                    "delivered_units": int(m[2] or 0) if len(m) > 2 else 0,
                    "returns": int(m[3] or 0) if len(m) > 3 else 0,
                    "cancellations": int(m[4] or 0) if len(m) > 4 else 0,
                    "avg_spp": float(m[5] or 0) if len(m) > 5 else 0.0,
                    "hits_view": int(m[6] or 0) if len(m) > 6 else 0,
                    "hits_view_pdp": int(m[7] or 0) if len(m) > 7 else 0,
                    "hits_tocart_pdp": 0,  # filled by second request below
                }
            if len(batch) < limit:
                break
            offset += limit

        # Request 2: funnel-only to get hits_tocart_pdp (incompatible with order metrics)
        try:
            offset = 0
            while True:
                data = self._post("/v1/analytics/data", {
                    "date_from": date_from,
                    "date_to": date_to,
                    "dimension": ["sku", "day"],
                    "metrics": ["hits_view", "hits_view_pdp", "hits_tocart_pdp"],
                    "limit": limit,
                    "offset": offset,
                })
                batch = data.get("result", {}).get("data", [])
                for item in batch:
                    dims = item.get("dimensions", [])
                    sku = dims[0].get("id", "") if len(dims) > 0 else ""
                    day = (dims[1].get("id", "") if len(dims) > 1 else "")[:10]
                    m = item.get("metrics", [])
                    key = (sku, day)
                    if key in rows:
                        rows[key]["hits_tocart_pdp"] = int(m[2] or 0) if len(m) > 2 else 0
                if len(batch) < limit:
                    break
                offset += limit
        except Exception as exc:
            print(f"  [warn] cart funnel request: {exc}")

        return list(rows.values())

    def fetch_ad_stats_by_day(self, date_from: str, date_to: str) -> dict[str, dict[str, float]]:
        """Returns {day: {spend, impressions, clicks}} via GET /api/client/statistics/daily/json."""
        if not self.perf_client_id or not self.perf_secret:
            return {}
        try:
            campaigns_data = self._perf_get("/api/client/campaign")
            campaign_ids = [str(c["id"]) for c in campaigns_data.get("list", [])]
        except Exception as exc:
            print(f"  [warn] performance campaigns: {exc}")
            return {}

        if not campaign_ids:
            return {}

        by_day: dict[str, dict[str, float]] = defaultdict(
            lambda: {"spend": 0.0, "impressions": 0, "clicks": 0, "orders": 0, "revenue": 0.0}
        )
        self._last_ad_campaign_ids = set()
        token = self._get_perf_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        for i in range(0, len(campaign_ids), 10):
            chunk = campaign_ids[i:i + 10]
            params = [("campaignIds", cid) for cid in chunk] + [
                ("dateFrom", date_from), ("dateTo", date_to)
            ]
            try:
                r = requests.get(f"{PERF_BASE}/api/client/statistics/daily/json",
                                 headers=headers, params=params, timeout=TIMEOUT)
                r.raise_for_status()
                for item in r.json().get("rows", []):
                    day = str(item.get("date", ""))[:10]
                    if not day:
                        continue
                    spend = float(str(item.get("moneySpent", "0") or "0").replace(",", "."))
                    by_day[day]["spend"] += spend
                    by_day[day]["impressions"] += int(item.get("views", 0) or 0)
                    by_day[day]["clicks"] += int(item.get("clicks", 0) or 0)
                    by_day[day]["orders"] += int(item.get("orders", 0) or 0)
                    by_day[day]["revenue"] += float(str(item.get("ordersMoney", "0") or "0").replace(",", "."))
                    if spend > 0:
                        campaign_id = str(item.get("id") or "")
                        if campaign_id:
                            self._last_ad_campaign_ids.add(campaign_id)
            except Exception as exc:
                print(f"  [warn] performance stats chunk {i//10+1}: {exc}")

        return dict(by_day)

    def fetch_sku_product_stats(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        """Returns day+SKU ad product stats (views, clicks) from async products report.
        Uses POST /api/client/statistic/products/generate/json.
        """
        if not self.perf_client_id or not self.perf_secret:
            return []
        by_day_sku: dict[tuple[str, str], dict[str, Any]] = {}
        for window_from, window_to in _iter_bounded_month_windows(date_from, date_to):
            try:
                resp = self._perf_post("/api/client/statistic/products/generate/json", {
                    "from": f"{window_from}T00:00:00Z",
                    "to": f"{window_to}T23:59:59Z",
                })
                uuid = resp.get("UUID")
                if not uuid:
                    print(f"  [warn] products stats report {window_from}..{window_to}: no UUID in response {resp}")
                    continue
                print(f"  Products stats report {window_from}..{window_to} UUID: {uuid}, polling...")
                rows = self._perf_poll_report(uuid, max_wait=120)
            except Exception as exc:
                print(f"  [warn] products stats {window_from}..{window_to}: {exc}")
                continue
            for row in rows:
                day = _parse_report_day(row.get("date"))
                sku = str(row.get("sku") or row.get("advSku") or "")
                if not day or not sku or not (date_from <= day <= date_to):
                    continue
                key = (day, sku)
                bucket = by_day_sku.setdefault(key, {"day": day, "sku": sku, "ad_views": 0, "ad_clicks": 0})
                bucket["ad_views"] += _to_int(row.get("views") or row.get("impressions") or 0)
                bucket["ad_clicks"] += _to_int(row.get("clicks") or 0)
        return list(by_day_sku.values())

    def fetch_sku_cpo_orders(self, date_from: str, date_to: str) -> list[dict[str, Any]]:
        """Returns day+SKU ad stats from campaign statistics report.

        This must use the same Performance statistics family as daily campaign
        totals. The orders-attribution report is narrower and can miss spend.
        """
        if not self.perf_client_id or not self.perf_secret:
            return []
        by_day_sku: dict[tuple[str, str], dict[str, Any]] = {}
        campaign_ids = sorted(self._last_ad_campaign_ids)
        if not campaign_ids:
            return []
        for window_from, window_to in _iter_bounded_month_windows(date_from, date_to):
            chunks_total = (len(campaign_ids) + 9) // 10
            for i in range(0, len(campaign_ids), 10):
                chunk = campaign_ids[i:i + 10]
                try:
                    resp = self._perf_post("/api/client/statistics/json", {
                        "campaigns": chunk,
                        "dateFrom": window_from,
                        "dateTo": window_to,
                        "groupBy": "DATE",
                    })
                    uuid = resp.get("UUID")
                    if not uuid:
                        print(f"  [warn] SKU ad statistics {window_from}..{window_to}: no UUID in response {resp}")
                        self._sku_ad_stats_incomplete = True
                        continue
                    print(
                        f"  SKU ad statistics {window_from}..{window_to} "
                        f"chunk {i // 10 + 1}/{chunks_total} UUID: {uuid}, polling..."
                    )
                    report = self._perf_poll_statistics_report(uuid, max_wait=600)
                except Exception as exc:
                    print(f"  [warn] SKU ad statistics {window_from}..{window_to} chunk {i // 10 + 1}: {exc}")
                    self._sku_ad_stats_incomplete = True
                    continue
                if not report:
                    self._sku_ad_stats_incomplete = True
                    continue
                for campaign_report in report.values():
                    rows = (campaign_report.get("report") or {}).get("rows") or []
                    for row in rows:
                        day = _parse_report_day(row.get("date"))
                        sku = str(row.get("sku") or "")
                        if not day or not sku or not (date_from <= day <= date_to):
                            continue
                        key = (day, sku)
                        bucket = by_day_sku.setdefault(
                            key,
                            {
                                "day": day,
                                "sku": sku,
                                "offer_id": "",
                                "item_name": str(row.get("title") or ""),
                                "ad_spend": 0.0,
                                "ad_orders": 0,
                                "ad_revenue": 0.0,
                                "ad_views": 0,
                                "ad_clicks": 0,
                            },
                        )
                        if not bucket["item_name"] and row.get("title"):
                            bucket["item_name"] = str(row.get("title") or "")
                        bucket["ad_spend"] += _to_float(row.get("moneySpent"))
                        bucket["ad_orders"] += _to_int(row.get("orders")) + _to_int(row.get("models"))
                        bucket["ad_revenue"] += _to_float(row.get("ordersMoney")) + _to_float(row.get("modelsMoney"))
                        bucket["ad_views"] += _to_int(row.get("views"))
                        bucket["ad_clicks"] += _to_int(row.get("clicks"))
        return list(by_day_sku.values())


def sync(date_from: str, date_to: str, db_path: str, cabinet_prefix: str, skip_spp: bool = False, skip_ads: bool = False) -> None:
    client_id = os.getenv("OZON_CLIENT_ID", "").strip()
    api_key = os.getenv("OZON_API_KEY", "").strip()
    perf_client_id = os.getenv("OZON_PERFORMANCE_CLIENT_ID", "").strip()
    perf_secret = os.getenv("OZON_PERFORMANCE_CLIENT_SECRET", "").strip()

    if not client_id or not api_key:
        raise ValueError("OZON_CLIENT_ID and OZON_API_KEY must be set")

    client = OzonClient(client_id, api_key, perf_client_id, perf_secret)

    print("Fetching finance transactions...")
    try:
        finance = client.fetch_finance_by_day(date_from, date_to)
        print(f"  {len(finance)} days with finance data")
    except Exception as exc:
        print(f"  [warn] Finance transactions fetch failed: {exc}")
        finance = {}

    if skip_ads:
        print("Skipping performance ad stats...")
        ads = {}
        sku_day_ad_rows = []
        sku_day_product_stats = []
    else:
        print("Fetching performance ad stats (daily)...")
        ads = client.fetch_ad_stats_by_day(date_from, date_to)
        print(f"  {len(ads)} days with ad data")

    print("Fetching per-SKU/day finance (commission + delivery, accrual/by-day)...")
    try:
        sku_day_finance = client.fetch_sku_accruals_by_day(date_from, date_to)
        print(f"  {len(sku_day_finance)} SKU-day rows with finance data")
    except Exception as exc:
        print(f"  [warn] SKU-day finance fetch failed: {exc}")
        sku_day_finance = []

    if not skip_ads:
        print("Fetching per-SKU/day ad statistics (campaign statistics, async)...")
        sku_day_ad_rows = client.fetch_sku_cpo_orders(date_from, date_to)
        if client._sku_ad_stats_incomplete:
            raise RuntimeError("Ozon SKU ad statistics incomplete; retry required")
        print(f"  {len(sku_day_ad_rows)} SKU-day rows with ad statistics")
        sku_day_product_stats = []

    print("Fetching stock on warehouses...")
    try:
        stocks = client.fetch_stock_on_warehouses()
        print(f"  {len(stocks)} warehouse rows")
    except Exception as exc:
        print(f"  [warn] Stock fetch failed: {exc}")
        stocks = []

    print("Fetching per-SKU analytics...")
    try:
        sku_analytics = client.fetch_sku_analytics(date_from, date_to)
        print(f"  {len(sku_analytics)} SKUs")
    except Exception as exc:
        print(f"  [warn] SKU analytics fetch failed: {exc}")
        sku_analytics = []

    print("Fetching per-SKU daily analytics...")
    try:
        sku_day_analytics = client.fetch_sku_day_analytics(date_from, date_to)
        print(f"  {len(sku_day_analytics)} sku-day rows")
    except Exception as exc:
        print(f"  [warn] SKU-day analytics fetch failed: {exc}")
        sku_day_analytics = []

    spp_by_day: dict[str, float] = {}
    if not skip_spp:
        print("Fetching SPP/Соинвест via Postings API (slow)...")
        try:
            spp_by_day = client.fetch_spp_by_order_date(date_from, date_to)
            print(f"  {len(spp_by_day)} days with SPP data")
        except Exception as exc:
            print(f"  [warn] SPP fetch failed: {exc}")

    synced_at = datetime.now(MSK).isoformat()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_daily_summary (
                cabinet_prefix TEXT NOT NULL,
                day TEXT NOT NULL,
                orders_revenue REAL NOT NULL DEFAULT 0,
                orders_qty INTEGER NOT NULL DEFAULT 0,
                delivered_qty INTEGER NOT NULL DEFAULT 0,
                returns_qty INTEGER NOT NULL DEFAULT 0,
                cancellations_qty INTEGER NOT NULL DEFAULT 0,
                accruals_for_sale REAL NOT NULL DEFAULT 0,
                sale_commission REAL NOT NULL DEFAULT 0,
                delivery_charge REAL NOT NULL DEFAULT 0,
                return_delivery REAL NOT NULL DEFAULT 0,
                return_accruals REAL NOT NULL DEFAULT 0,
                for_pay REAL NOT NULL DEFAULT 0,
                avg_spp REAL,
                ad_spend REAL NOT NULL DEFAULT 0,
                ad_impressions INTEGER NOT NULL DEFAULT 0,
                ad_clicks INTEGER NOT NULL DEFAULT 0,
                ad_orders INTEGER NOT NULL DEFAULT 0,
                ad_revenue REAL NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (cabinet_prefix, day)
            )
        """)
        # Deduplicate first, then create unique index (migration may have inserted without PK)
        conn.execute("""
            DELETE FROM ozon_daily_summary WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM ozon_daily_summary GROUP BY cabinet_prefix, day
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_daily_summary_pk ON ozon_daily_summary (cabinet_prefix, day)")
        for _col, _typ in [("ad_orders", "INTEGER NOT NULL DEFAULT 0"), ("ad_revenue", "REAL NOT NULL DEFAULT 0")]:
            try:
                conn.execute(f"ALTER TABLE ozon_daily_summary ADD COLUMN {_col} {_typ}")
            except sqlite3.OperationalError:
                pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_plugin_analytics (
                cabinet_prefix TEXT NOT NULL,
                day TEXT NOT NULL,
                hits_view INTEGER NOT NULL DEFAULT 0,
                hits_view_pdp INTEGER NOT NULL DEFAULT 0,
                hits_tocart_pdp INTEGER NOT NULL DEFAULT 0,
                ordered_units INTEGER NOT NULL DEFAULT 0,
                source_file TEXT,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (cabinet_prefix, day)
            )
        """)
        conn.execute("""
            DELETE FROM ozon_plugin_analytics WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM ozon_plugin_analytics GROUP BY cabinet_prefix, day
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_plugin_analytics_pk ON ozon_plugin_analytics (cabinet_prefix, day)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_product_day_analytics (
                sku TEXT NOT NULL,
                item_name TEXT,
                period_from TEXT NOT NULL,
                period_to TEXT NOT NULL,
                revenue REAL NOT NULL DEFAULT 0,
                ordered_units INTEGER NOT NULL DEFAULT 0,
                delivered_units INTEGER NOT NULL DEFAULT 0,
                returns INTEGER NOT NULL DEFAULT 0,
                cancellations INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (sku, period_from, period_to)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_sku_day_analytics (
                sku TEXT NOT NULL,
                item_name TEXT,
                day TEXT NOT NULL,
                orders_revenue REAL NOT NULL DEFAULT 0,
                orders_qty INTEGER NOT NULL DEFAULT 0,
                delivered_qty INTEGER NOT NULL DEFAULT 0,
                returns_qty INTEGER NOT NULL DEFAULT 0,
                cancellations_qty INTEGER NOT NULL DEFAULT 0,
                avg_spp REAL,
                hits_view INTEGER,
                hits_view_pdp INTEGER,
                hits_tocart_pdp INTEGER,
                cabinet_prefix TEXT,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (sku, day)
            )
        """)
        conn.execute("""
            DELETE FROM ozon_sku_day_analytics WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM ozon_sku_day_analytics GROUP BY sku, day
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ozon_sku_day_analytics_pk ON ozon_sku_day_analytics (sku, day)")
        for _col, _typ in [("avg_spp", "REAL"), ("hits_view", "INTEGER"),
                            ("hits_view_pdp", "INTEGER"), ("hits_tocart_pdp", "INTEGER")]:
            try:
                conn.execute(f"ALTER TABLE ozon_sku_day_analytics ADD COLUMN {_col} {_typ}")
            except Exception:
                pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_sku_period_ad_spend (
                sku TEXT NOT NULL,
                period_from TEXT NOT NULL,
                period_to TEXT NOT NULL,
                cabinet_prefix TEXT,
                ad_spend REAL NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (sku, period_from, period_to)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_sku_day_ad_spend (
                day TEXT NOT NULL,
                sku TEXT NOT NULL,
                offer_id TEXT,
                item_name TEXT,
                cabinet_prefix TEXT,
                ad_spend REAL NOT NULL DEFAULT 0,
                ad_orders INTEGER NOT NULL DEFAULT 0,
                ad_revenue REAL NOT NULL DEFAULT 0,
                ad_views INTEGER NOT NULL DEFAULT 0,
                ad_clicks INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (day, sku)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_sku_day_finance (
                day TEXT NOT NULL,
                sku TEXT NOT NULL,
                cabinet_prefix TEXT,
                sale_commission REAL NOT NULL DEFAULT 0,
                delivery_charge REAL NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (day, sku)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ozon_stock_on_warehouses (
                sku TEXT NOT NULL,
                offer_id TEXT,
                warehouse_name TEXT NOT NULL,
                item_name TEXT,
                promised_amount INTEGER NOT NULL DEFAULT 0,
                free_to_sell_amount INTEGER NOT NULL DEFAULT 0,
                reserved_amount INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL,
                PRIMARY KEY (sku, warehouse_name)
            )
        """)

        # Stock: full replace (snapshot)
        conn.execute("DELETE FROM ozon_stock_on_warehouses")
        for row in stocks:
            conn.execute(
                "INSERT OR REPLACE INTO ozon_stock_on_warehouses "
                "(sku, offer_id, warehouse_name, item_name, promised_amount, free_to_sell_amount, reserved_amount, synced_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    str(row.get("sku", "")),
                    str(row.get("item_code", "")),
                    str(row.get("warehouse_name", "")),
                    str(row.get("item_name", "")),
                    int(row.get("promised_amount", 0) or 0),
                    int(row.get("free_to_sell_amount", 0) or 0),
                    int(row.get("reserved_amount", 0) or 0),
                    synced_at,
                ),
            )

        # Per-SKU daily analytics: upsert by (sku, day)
        for row in sku_day_analytics:
            conn.execute(
                "INSERT OR REPLACE INTO ozon_sku_day_analytics "
                "(sku, item_name, day, orders_revenue, orders_qty, delivered_qty, "
                "returns_qty, cancellations_qty, avg_spp, hits_view, hits_view_pdp, "
                "hits_tocart_pdp, cabinet_prefix, synced_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    str(row["sku"]), str(row["item_name"]), str(row["day"]),
                    float(row["revenue"]), int(row["ordered_units"]), int(row["delivered_units"]),
                    int(row["returns"]), int(row["cancellations"]),
                    float(row.get("avg_spp") or 0),
                    int(row.get("hits_view") or 0),
                    int(row.get("hits_view_pdp") or 0),
                    int(row.get("hits_tocart_pdp") or 0),
                    cabinet_prefix, synced_at,
                ),
            )

        if skip_ads:
            print("Preserving existing Ozon ad tables for this period.")
        else:
            # Per-SKU/day ad attribution. This is the only source used for SKU rows.
            for _col, _typ in [("ad_views", "INTEGER"), ("ad_clicks", "INTEGER")]:
                try:
                    conn.execute(f"ALTER TABLE ozon_sku_day_ad_spend ADD COLUMN {_col} {_typ} NOT NULL DEFAULT 0")
                except Exception:
                    pass
            conn.execute(
                "DELETE FROM ozon_sku_day_ad_spend WHERE day >= ? AND day <= ?",
                (date_from, date_to),
            )
            # Merge orders attribution and product stats by (day, sku)
            views_by_day_sku = {(r["day"], r["sku"]): r for r in sku_day_product_stats}
            for row in sku_day_ad_rows:
                ps = views_by_day_sku.get((row["day"], row["sku"]), {})
                conn.execute(
                    "INSERT OR REPLACE INTO ozon_sku_day_ad_spend "
                    "(day, sku, offer_id, item_name, cabinet_prefix, ad_spend, ad_orders, ad_revenue, ad_views, ad_clicks, synced_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        row["day"], row["sku"], row.get("offer_id", ""), row.get("item_name", ""),
                        cabinet_prefix, float(row["ad_spend"]), int(row["ad_orders"]),
                        float(row["ad_revenue"]),
                        int(ps.get("ad_views", row.get("ad_views", 0))),
                        int(ps.get("ad_clicks", row.get("ad_clicks", 0))),
                        synced_at,
                    ),
                )
            # Also insert rows that only have views/clicks (no spend) for completeness.
            order_keys = {(r["day"], r["sku"]) for r in sku_day_ad_rows}
            for r in sku_day_product_stats:
                key = (r["day"], r["sku"])
                if key not in order_keys:
                    conn.execute(
                        "INSERT OR IGNORE INTO ozon_sku_day_ad_spend "
                        "(day, sku, cabinet_prefix, ad_views, ad_clicks, synced_at) "
                        "VALUES (?,?,?,?,?,?)",
                        (r["day"], r["sku"], cabinet_prefix, int(r.get("ad_views", 0)), int(r.get("ad_clicks", 0)), synced_at),
                    )

            # Legacy period table is kept only for compatibility; values come from exact day+SKU rows.
            period_spend: dict[str, float] = defaultdict(float)
            for row in sku_day_ad_rows:
                period_spend[str(row["sku"])] += float(row["ad_spend"])
            for sku, spend in period_spend.items():
                conn.execute(
                    "INSERT OR REPLACE INTO ozon_sku_period_ad_spend "
                    "(sku, period_from, period_to, cabinet_prefix, ad_spend, synced_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (str(sku), date_from, date_to, cabinet_prefix, float(spend), synced_at),
                )

        # Per-SKU/day finance: commission and delivery from accrual/by-day
        conn.execute(
            "DELETE FROM ozon_sku_day_finance WHERE day >= ? AND day <= ?",
            (date_from, date_to),
        )
        for row in sku_day_finance:
            conn.execute(
                "INSERT OR REPLACE INTO ozon_sku_day_finance "
                "(day, sku, cabinet_prefix, sale_commission, delivery_charge, synced_at) "
                "VALUES (?,?,?,?,?,?)",
                (row["day"], row["sku"], cabinet_prefix,
                 float(row["sale_commission"]), float(row["delivery_charge"]), synced_at),
            )

        # Per-SKU analytics: upsert by (sku, period)
        for row in sku_analytics:
            conn.execute(
                "INSERT OR REPLACE INTO ozon_product_day_analytics "
                "(sku, item_name, period_from, period_to, revenue, ordered_units, delivered_units, returns, cancellations, synced_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    row["sku"], row["item_name"], date_from, date_to,
                    row["revenue"], row["ordered_units"], row["delivered_units"],
                    row["returns"], row["cancellations"], synced_at,
                ),
            )

        # Rebuild ozon_daily_summary from per-SKU tables (no aggregate API calls)
        # orders/funnel from ozon_sku_day_analytics, finance from ozon_sku_day_finance,
        # ads from ozon_sku_day_ad_spend, for_pay from transaction list (no per-SKU equivalent)
        conn.execute(
            "DELETE FROM ozon_daily_summary WHERE cabinet_prefix=? AND day BETWEEN ? AND ?",
            (cabinet_prefix, date_from, date_to),
        )
        daily_rows = conn.execute("""
            SELECT
                s.day,
                SUM(s.orders_revenue)    AS orders_revenue,
                SUM(s.orders_qty)        AS orders_qty,
                SUM(s.delivered_qty)     AS delivered_qty,
                SUM(s.returns_qty)       AS returns_qty,
                SUM(s.cancellations_qty) AS cancellations_qty,
                SUM(s.hits_view)         AS hits_view,
                SUM(s.hits_view_pdp)     AS hits_view_pdp,
                SUM(s.hits_tocart_pdp)   AS hits_tocart_pdp,
                COALESCE(SUM(f.sale_commission), 0)  AS sale_commission,
                COALESCE(SUM(f.delivery_charge), 0)  AS delivery_charge,
                COALESCE(SUM(a.ad_spend), 0)         AS ad_spend,
                COALESCE(SUM(a.ad_views), 0)         AS ad_impressions,
                COALESCE(SUM(a.ad_clicks), 0)        AS ad_clicks,
                COALESCE(SUM(a.ad_orders), 0)        AS ad_orders,
                COALESCE(SUM(a.ad_revenue), 0)       AS ad_revenue
            FROM ozon_sku_day_analytics s
            LEFT JOIN ozon_sku_day_finance f ON f.day = s.day AND f.sku = s.sku
            LEFT JOIN ozon_sku_day_ad_spend a ON a.day = s.day AND a.sku = s.sku
            WHERE s.day BETWEEN ? AND ?
            GROUP BY s.day
            ORDER BY s.day
        """, (date_from, date_to)).fetchall()

        rows_written = 0
        for dr in daily_rows:
            day = dr["day"]
            fin = finance.get(day, {})
            ad_day = ads.get(day, {})
            ad_spend = ad_day.get("spend", dr["ad_spend"])
            ad_impressions = ad_day.get("impressions", dr["ad_impressions"])
            ad_clicks = ad_day.get("clicks", dr["ad_clicks"])
            ad_orders = ad_day.get("orders", dr["ad_orders"])
            ad_revenue = ad_day.get("revenue", dr["ad_revenue"])
            conn.execute("""
                INSERT OR REPLACE INTO ozon_daily_summary
                (cabinet_prefix, day, orders_revenue, orders_qty, delivered_qty,
                 returns_qty, cancellations_qty, accruals_for_sale, sale_commission,
                 delivery_charge, return_delivery, return_accruals, for_pay,
                 avg_spp, ad_spend, ad_impressions, ad_clicks, ad_orders, ad_revenue, synced_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                cabinet_prefix,
                day,
                dr["orders_revenue"],
                dr["orders_qty"],
                dr["delivered_qty"],
                dr["returns_qty"],
                dr["cancellations_qty"],
                dr["orders_revenue"],          # accruals_for_sale = orders_revenue per SKU sum
                dr["sale_commission"],
                dr["delivery_charge"],
                fin.get("return_delivery", 0.0),    # per-SKU not available from OZON
                fin.get("return_accruals", 0.0),    # per-SKU not available from OZON
                fin.get("for_pay", 0.0),            # per-SKU not available from OZON — TODO discuss
                None,                               # avg_spp: meaningless as daily average
                ad_spend,
                ad_impressions,
                ad_clicks,
                ad_orders,
                ad_revenue,
                synced_at,
            ))
            rows_written += 1

        # Rebuild ozon_plugin_analytics (per-day funnel summary) from per-SKU data
        conn.execute(
            "DELETE FROM ozon_plugin_analytics WHERE cabinet_prefix=? AND day BETWEEN ? AND ?",
            (cabinet_prefix, date_from, date_to),
        )
        conn.execute("""
            INSERT OR REPLACE INTO ozon_plugin_analytics
                (cabinet_prefix, day, hits_view, hits_view_pdp, hits_tocart_pdp, ordered_units, source_file, synced_at)
            SELECT
                ?,
                day,
                SUM(hits_view),
                SUM(hits_view_pdp),
                SUM(hits_tocart_pdp),
                SUM(orders_qty),
                'sku_day_analytics',
                ?
            FROM ozon_sku_day_analytics
            WHERE day BETWEEN ? AND ?
            GROUP BY day
        """, (cabinet_prefix, synced_at, date_from, date_to))

        conn.commit()

    print(f"\nДобавлено/обновлено строк: {rows_written}")
    print("Готово.")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT day, orders_revenue, orders_qty, delivered_qty, ad_spend, delivery_charge, avg_spp "
            "FROM ozon_daily_summary "
            "WHERE cabinet_prefix=? AND day BETWEEN ? AND ? ORDER BY day",
            (cabinet_prefix, date_from, date_to),
        ).fetchall()
        print(f"\n{'Дата':<12} {'Выручка':>12} {'Заказы':>8} {'Выкупы':>8} {'Реклама':>10} {'Логистика':>12} {'СПП%':>6}")
        print("-" * 74)
        for r in rows:
            spp_str = f"{float(r['avg_spp']):.1f}" if r['avg_spp'] is not None else "—"
            print(f"{r['day']:<12} {float(r['orders_revenue'] or 0):>12,.0f} {int(r['orders_qty'] or 0):>8} {int(r['delivered_qty'] or 0):>8} "
                  f"{float(r['ad_spend'] or 0):>10,.0f} {float(r['delivery_charge'] or 0):>12,.0f} {spp_str:>6}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Ozon data to SQLite")
    today = date.today()
    parser.add_argument("--date-from", default=(today - timedelta(days=6)).isoformat())
    parser.add_argument("--date-to", default=(today - timedelta(days=1)).isoformat())
    parser.add_argument("--db", default=os.getenv("SQLITE_DB_PATH", "data/platform.db"))
    parser.add_argument("--cabinet", default="")
    # --cabinet-id sets both the DB path (data/cabs/{id}.db) and the cabinet prefix
    parser.add_argument("--cabinet-id", default="", help="Cabinet ID for multi-cabinet platform")
    parser.add_argument("--skip-spp", action="store_true", default=True, help="Skip slow Postings API (default: true)")
    parser.add_argument("--with-spp", action="store_true", help="Enable slow Postings API for SPP")
    parser.add_argument("--skip-ads", action="store_true", help="Skip Performance API (ads) sync")
    return parser


if __name__ == "__main__":
    load_env()
    args = build_parser().parse_args()
    # Resolve cabinet ID and DB path
    cabinet_id = args.cabinet_id or args.cabinet
    db_path = args.db
    if args.cabinet_id:
        root = Path(__file__).parent.parent
        db_path = str(root / "data" / "cabs" / f"{args.cabinet_id}.db")
        (root / "data" / "cabs").mkdir(parents=True, exist_ok=True)
    elif not cabinet_id:
        cabinet_id = "default"
    skip_spp = args.skip_spp and not args.with_spp
    sync(args.date_from, args.date_to, db_path, cabinet_id, skip_spp=skip_spp, skip_ads=args.skip_ads)
