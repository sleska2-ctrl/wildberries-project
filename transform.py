from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
import json
from typing import Any

from .utils import as_decimal, to_iso_date


def _first_existing_header(header: dict[str, int], names: tuple[str, ...]) -> int | None:
    for name in names:
        if name in header:
            return header[name]
    return None


def _article_column_idx(header: dict[str, int], article_filter_type: str) -> int | None:
    if article_filter_type == "vendorCode":
        return _first_existing_header(
            header,
            ("vendorCode", "Артикул поставщика", "supplierArticle", "НАШ", "SKU", "sku"),
        )
    if article_filter_type == "nmId":
        return _first_existing_header(header, ("nmId", "nm_id", "Артикул WB"))
    return header.get(article_filter_type)


def _sale_sign(row: dict[str, Any]) -> Decimal:
    doc_type = str(row.get("docTypeName", "")).strip()
    if doc_type == "Продажа":
        return Decimal("1")
    if doc_type == "Возврат":
        return Decimal("-1")
    return Decimal("0")


def filter_sales_rows(
    rows: list[dict[str, Any]],
    article_filter_type: str,
    article_filter_values: list[str],
    nm_id_filter_values: set[str] | None = None,
) -> list[dict[str, Any]]:
    allowed = {value.strip() for value in article_filter_values}
    allowed_nm_ids = {value.strip() for value in (nm_id_filter_values or set())}
    filtered: list[dict[str, Any]] = []
    for row in rows:
        nm_id = str(row.get("nmId", "")).strip()
        if nm_id and nm_id in allowed_nm_ids:
            filtered.append(row)
            continue

        article_value = row.get(article_filter_type)
        if article_value is None:
            continue
        if str(article_value).strip() in allowed:
            filtered.append(row)
    return filtered


def filter_orders_rows(
    rows: list[dict[str, Any]],
    supplier_articles: set[str],
    nm_ids: set[str],
    date_from: str,
    date_to: str,
) -> list[dict[str, Any]]:
    date_from_norm = to_iso_date(date_from)
    date_to_norm = to_iso_date(date_to)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        supplier_article = str(row.get("supplierArticle", "")).strip()
        nm_id = str(row.get("nmId", "")).strip()
        if supplier_article not in supplier_articles and nm_id not in nm_ids:
            continue

        raw_date = str(row.get("date") or row.get("lastChangeDate") or "")
        if not raw_date:
            continue
        order_date = to_iso_date(raw_date)
        if date_from_norm <= order_date <= date_to_norm:
            filtered.append(row)
    return filtered


def extract_orders_filters(sheet_values: list[list[str]]) -> tuple[set[str], set[str]]:
    if not sheet_values:
        return set(), set()

    header = {name.strip(): index for index, name in enumerate(sheet_values[0])}
    supplier_idx = header.get("Артикул поставщика", header.get("НАШ"))
    nm_idx = header.get("Артикул WB", header.get("nmId"))

    supplier_articles: set[str] = set()
    nm_ids: set[str] = set()
    for row in sheet_values[1:]:
        if supplier_idx is not None and len(row) > supplier_idx:
            value = row[supplier_idx].strip()
            if value:
                supplier_articles.add(value)
        if nm_idx is not None and len(row) > nm_idx:
            value = row[nm_idx].strip()
            if value:
                nm_ids.add(value)
    return supplier_articles, nm_ids


def build_cogs_map(
    sheet_values: list[list[str]],
    article_filter_type: str,
) -> dict[tuple[str, str], Decimal]:
    if not sheet_values:
        return {}

    header = {name.strip(): index for index, name in enumerate(sheet_values[0])}
    article_type_idx = header.get("article_type")
    article_idx = header.get("article")
    cogs_idx = header.get("cogs")

    if article_idx is None or cogs_idx is None:
        article_idx = _article_column_idx(header, article_filter_type)
        cogs_idx = header.get("себестоимость", header.get("cost_price"))
        article_type_idx = None

    result: dict[tuple[str, str], Decimal] = {}
    for row in sheet_values[1:]:
        if article_idx is None or cogs_idx is None:
            continue
        if len(row) <= max(article_idx, cogs_idx):
            continue
        article = row[article_idx].strip()
        if not article:
            continue
        article_type = article_filter_type
        if article_type_idx is not None and len(row) > article_type_idx and row[article_type_idx].strip():
            article_type = row[article_type_idx].strip()
        result[(article_type, article)] = as_decimal(row[cogs_idx])
    return result


def build_nm_mapping(sheet_values: list[list[str]], article_filter_type: str = "vendorCode") -> dict[str, str]:
    if not sheet_values:
        return {}

    header = {name.strip(): index for index, name in enumerate(sheet_values[0])}
    article_idx = _article_column_idx(header, article_filter_type)
    nm_idx = _first_existing_header(header, ("nmId", "nm_id", "Артикул WB"))
    if article_idx is None or nm_idx is None:
        return {}

    mapping: dict[str, str] = {}
    for row in sheet_values[1:]:
        if len(row) <= max(article_idx, nm_idx):
            continue
        article = row[article_idx].strip()
        nm_id = row[nm_idx].strip()
        if article and nm_id:
            mapping[nm_id] = article
    return mapping


def extract_filter_values(
    sheet_values: list[list[str]],
    article_filter_type: str,
) -> list[str]:
    if not sheet_values:
        return []

    header = {name.strip(): index for index, name in enumerate(sheet_values[0])}
    column_idx = _article_column_idx(header, article_filter_type)
    if column_idx is None:
        return []

    values: list[str] = []
    seen: set[str] = set()
    for row in sheet_values[1:]:
        if len(row) <= column_idx:
            continue
        value = row[column_idx].strip()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def flatten_ads_rows(stats_payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for campaign in stats_payload:
        advert_id = campaign.get("advertId")
        for day in campaign.get("days", []):
            date = to_iso_date(str(day.get("date", "")))
            for app in day.get("apps", []):
                for item in app.get("nms", []):
                    nm_id = item.get("nmId") or item.get("nm")
                    if nm_id is None:
                        continue
                    if not any(
                        as_decimal(item.get(field)) > 0
                        for field in ("views", "clicks", "sum", "orders", "sum_price", "atbs", "shks")
                    ):
                        continue
                    rows.append(
                        {
                            "date": date,
                            "advertId": advert_id,
                            "appType": app.get("appType"),
                            "nmId": nm_id,
                            "views": item.get("views", 0),
                            "clicks": item.get("clicks", 0),
                            "sum": item.get("sum", 0),
                            "orders": item.get("orders", 0),
                            "sum_price": item.get("sum_price", 0),
                        }
                    )
    return rows


def aggregate_daily_pnl(
    sales_rows: list[dict[str, Any]],
    ads_rows: list[dict[str, Any]],
    cogs_map: dict[tuple[str, str], Decimal],
    article_filter_type: str,
    nm_mapping: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    nm_to_article: dict[str, str] = dict(nm_mapping or {})
    nm_to_vendor_code: dict[str, str] = {}

    for row in sales_rows:
        date = to_iso_date(str(row.get("saleDt") or row.get("dateTo") or row.get("dateFrom")))
        nm_id = str(row.get("nmId", "")).strip()
        raw_article = str(row.get(article_filter_type, "")).strip()
        article = nm_to_article.get(nm_id, raw_article)
        vendor_code = str(row.get("vendorCode", "")).strip()
        if nm_id:
            nm_to_article[nm_id] = article
            nm_to_vendor_code[nm_id] = vendor_code

        sale_sign = _sale_sign(row)
        non_sale_financial_total = sum(
            (
                as_decimal(row.get("paidStorage")),
                as_decimal(row.get("paidAcceptance")),
                as_decimal(row.get("penalty")),
                as_decimal(row.get("deduction")),
                as_decimal(row.get("additionalPayment")),
            ),
            start=Decimal("0"),
        )
        if sale_sign == 0 and non_sale_financial_total == 0:
            continue

        key = (date, article)
        if key not in grouped:
            grouped[key] = {
                "date": date,
                "article_type": article_filter_type,
                "article": article,
                "nm_id": nm_id,
                "vendor_code": vendor_code,
                "orders_amount": Decimal("0"),
                "sales_amount": Decimal("0"),
                "sales_without_spp": Decimal("0"),
                "wb_commission": Decimal("0"),
                "acquiring_fee": Decimal("0"),
                "storage_fee": Decimal("0"),
                "acceptance_fee": Decimal("0"),
                "penalties": Decimal("0"),
                "deductions": Decimal("0"),
                "additional_payments": Decimal("0"),
                "delivery_fee": Decimal("0"),
                "ad_spend": Decimal("0"),
                "cogs_amount": Decimal("0"),
                "net_profit": Decimal("0"),
                "margin_pct": Decimal("0"),
            }

        bucket = grouped[key]
        quantity = as_decimal(row.get("quantity")) * sale_sign
        retail_amount = as_decimal(row.get("retailAmount")) * sale_sign
        wb_commission = as_decimal(row.get("ppvzSalesCommission")) * sale_sign
        acquiring_fee = as_decimal(row.get("acquiringFee")) * sale_sign
        storage_fee = as_decimal(row.get("paidStorage"))
        acceptance_fee = as_decimal(row.get("paidAcceptance"))
        penalties = as_decimal(row.get("penalty"))
        deductions = as_decimal(row.get("deduction"))
        additional_payments = as_decimal(row.get("additionalPayment"))
        for_pay = as_decimal(row.get("forPay"))

        delivery_fee = as_decimal(row.get("deliveryRub", 0))
        if delivery_fee == 0:
            # Если deliveryRub не отдается API, считаем логистику как остаток до forPay.
            delivery_fee = (
                retail_amount
                - wb_commission
                - acquiring_fee
                - storage_fee
                - acceptance_fee
                - penalties
                - deductions
                + additional_payments
                - for_pay
            )
        else:
            delivery_fee *= sale_sign

        bucket["orders_amount"] += quantity
        bucket["sales_amount"] += retail_amount
        bucket["sales_without_spp"] += as_decimal(row.get("retailPriceWithDisc")) * quantity
        bucket["wb_commission"] += wb_commission
        bucket["acquiring_fee"] += acquiring_fee
        bucket["storage_fee"] += storage_fee
        bucket["acceptance_fee"] += acceptance_fee
        bucket["penalties"] += penalties
        bucket["deductions"] += deductions
        bucket["additional_payments"] += additional_payments
        bucket["delivery_fee"] += delivery_fee
        cogs = cogs_map.get((article_filter_type, article), Decimal("0"))
        bucket["cogs_amount"] += cogs * quantity

    ads_by_key: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    for row in ads_rows:
        date = str(row.get("date", "")).strip()
        nm_id = str(row.get("nmId", "")).strip()
        if not date or not nm_id:
            continue
        article = nm_to_article.get(nm_id, nm_id if article_filter_type == "nmId" else "")
        if not article:
            continue
        ads_by_key[(date, article)] += as_decimal(row.get("sum"))

    for key, ad_spend in ads_by_key.items():
        if ad_spend == 0 and key not in grouped:
            continue
        if key not in grouped:
            date, article = key
            grouped[key] = {
                "date": date,
                "article_type": article_filter_type,
                "article": article,
                "nm_id": article if article_filter_type == "nmId" else "",
                "vendor_code": nm_to_vendor_code.get(article, "") if article_filter_type == "nmId" else article,
                "orders_amount": Decimal("0"),
                "sales_amount": Decimal("0"),
                "sales_without_spp": Decimal("0"),
                "wb_commission": Decimal("0"),
                "acquiring_fee": Decimal("0"),
                "storage_fee": Decimal("0"),
                "acceptance_fee": Decimal("0"),
                "penalties": Decimal("0"),
                "deductions": Decimal("0"),
                "additional_payments": Decimal("0"),
                "delivery_fee": Decimal("0"),
                "ad_spend": Decimal("0"),
                "cogs_amount": Decimal("0"),
                "net_profit": Decimal("0"),
                "margin_pct": Decimal("0"),
            }
        grouped[key]["ad_spend"] += ad_spend

    result: list[dict[str, Any]] = []
    for bucket in grouped.values():
        bucket["net_profit"] = (
            bucket["sales_amount"]
            - bucket["wb_commission"]
            - bucket["acquiring_fee"]
            - bucket["storage_fee"]
            - bucket["acceptance_fee"]
            - bucket["penalties"]
            - bucket["deductions"]
            - bucket["delivery_fee"]
            - bucket["ad_spend"]
            - bucket["cogs_amount"]
            + bucket["additional_payments"]
        )
        sales_amount = bucket["sales_amount"]
        bucket["margin_pct"] = (bucket["net_profit"] / sales_amount * Decimal("100")) if sales_amount else Decimal("0")
        result.append(bucket)

    result.sort(key=lambda item: (item["date"], item["article"]))
    return result


def sheet_values_to_dicts(values: list[list[str]]) -> list[dict[str, Any]]:
    if not values or not values[0]:
        return []
    headers = [str(value).strip() for value in values[0]]
    rows: list[dict[str, Any]] = []
    for row in values[1:]:
        rows.append(
            {
                header: row[idx] if idx < len(row) else ""
                for idx, header in enumerate(headers)
                if header
            }
        )
    return rows


def build_buyout_order_day_rows(
    orders_rows: list[dict[str, Any]],
    sales_rows: list[dict[str, Any]],
    ads_rows: list[dict[str, Any]],
    nm_mapping: dict[str, str] | None = None,
) -> list[list[object]]:
    """Build an order-date table where late buyouts update old order dates."""
    def money(value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))

    def pct(value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))

    headers = [
        "Дата",
        "Артикул",
        "Сумма заказов",
        "Сумма выкупов в эту дату",
        "Сумма выкупов товаров, заказанных в эту дату",
        "К перечислению от выкупов в эту дату",
        "К перечислению от выкупов товаров, заказанных в эту дату",
        "Реклама",
        "Выкупы в эту дату, шт",
        "Выкупы товаров, заказанных в эту дату, шт",
        "Заказы, шт",
        "Продажи до возвратов, шт",
        "Сумма продаж до возвратов",
        "Возвраты, шт",
        "Сумма возвратов",
        "% выкупа",
        "Последняя дата события",
    ]
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "orders_sum": Decimal("0"),
            "event_sale_sum": Decimal("0"),
            "event_return_sum": Decimal("0"),
            "ordered_sale_sum": Decimal("0"),
            "ordered_return_sum": Decimal("0"),
            "event_for_pay": Decimal("0"),
            "ordered_for_pay": Decimal("0"),
            "ad_spend": Decimal("0"),
            "event_sale_count": Decimal("0"),
            "event_return_count": Decimal("0"),
            "ordered_sale_count": Decimal("0"),
            "ordered_return_count": Decimal("0"),
            "order_srids": set(),
            "last_event_date": "",
        }
    )
    nm_to_article = dict(nm_mapping or {})

    for row in orders_rows:
        order_date = to_iso_date(str(row.get("date") or row.get("lastChangeDate") or ""))
        article = str(row.get("supplierArticle", "")).strip()
        nm_id = str(row.get("nmId", "")).strip()
        if nm_id and article:
            nm_to_article[nm_id] = article
        if not order_date or not article:
            continue

        bucket = grouped[(order_date, article)]
        srid = str(row.get("srid", "")).strip()
        if srid:
            bucket["order_srids"].add(srid)
        else:
            bucket["order_srids"].add(f"__row_{len(bucket['order_srids'])}")
        bucket["orders_sum"] += as_decimal(row.get("priceWithDisc"))

    for row in sales_rows:
        doc_type = str(row.get("docTypeName", "")).strip()
        if doc_type not in {"Продажа", "Возврат"}:
            continue
        order_date = to_iso_date(str(row.get("orderDt") or ""))
        event_date = to_iso_date(str(row.get("saleDt") or ""))
        article = str(row.get("vendorCode", "")).strip()
        nm_id = str(row.get("nmId", "")).strip()
        if nm_id and article:
            nm_to_article[nm_id] = article
        if not order_date or not article:
            continue

        quantity = as_decimal(row.get("quantity"))
        if quantity <= 0:
            continue

        amount = as_decimal(row.get("retailPriceWithDisc")) * quantity
        for_pay = as_decimal(row.get("forPay"))
        event_bucket = grouped[(event_date or order_date, article)]
        ordered_bucket = grouped[(order_date, article)]
        if doc_type == "Продажа":
            event_bucket["event_sale_count"] += quantity
            event_bucket["event_sale_sum"] += amount
            ordered_bucket["ordered_sale_count"] += quantity
            ordered_bucket["ordered_sale_sum"] += amount
            event_bucket["event_for_pay"] += for_pay
            ordered_bucket["ordered_for_pay"] += for_pay
        else:
            event_bucket["event_return_count"] += quantity
            event_bucket["event_return_sum"] += amount
            ordered_bucket["ordered_return_count"] += quantity
            ordered_bucket["ordered_return_sum"] += amount
            event_bucket["event_for_pay"] -= for_pay
            ordered_bucket["ordered_for_pay"] -= for_pay
        if event_date and (not ordered_bucket["last_event_date"] or event_date > ordered_bucket["last_event_date"]):
            ordered_bucket["last_event_date"] = event_date

    for row in ads_rows:
        date = str(row.get("date", "")).strip()
        nm_id = str(row.get("nmId", "")).strip()
        if not date or not nm_id:
            continue
        article = nm_to_article.get(nm_id, "")
        if not article:
            continue
        grouped[(date, article)]["ad_spend"] += as_decimal(row.get("sum"))

    result = [headers]
    for (order_date, article), bucket in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1]), reverse=True):
        orders_count = Decimal(len(bucket["order_srids"]))
        event_buyout_count = bucket["event_sale_count"] - bucket["event_return_count"]
        event_buyout_sum = bucket["event_sale_sum"] - bucket["event_return_sum"]
        ordered_buyout_count = bucket["ordered_sale_count"] - bucket["ordered_return_count"]
        ordered_buyout_sum = bucket["ordered_sale_sum"] - bucket["ordered_return_sum"]
        buyout_percent = (ordered_buyout_count / orders_count * Decimal("100")) if orders_count else Decimal("0")
        result.append(
            [
                order_date,
                article,
                money(bucket["orders_sum"]),
                money(event_buyout_sum),
                money(ordered_buyout_sum),
                money(bucket["event_for_pay"]),
                money(bucket["ordered_for_pay"]),
                money(bucket["ad_spend"]),
                event_buyout_count,
                ordered_buyout_count,
                orders_count,
                bucket["ordered_sale_count"],
                money(bucket["ordered_sale_sum"]),
                bucket["ordered_return_count"],
                money(bucket["ordered_return_sum"]),
                pct(buyout_percent),
                bucket["last_event_date"],
            ]
        )
    return result


def sales_to_sheet_rows(rows: list[dict[str, Any]]) -> list[list[object]]:
    if not rows:
        return [[]]
    headers = list(rows[0].keys())
    numeric_fields = {
        "giId",
        "dlvPrc",
        "quantity",
        "retailAmount",
        "retailPrice",
        "retailPriceWithDisc",
        "salePercent",
        "commissionPercent",
        "deliveryService",
        "productDiscountForReport",
        "sellerPromo",
        "spp",
        "kvwBase",
        "kvw",
        "supRatingUp",
        "ppvzSalesCommission",
        "ppvzReward",
        "acquiringFee",
        "acquiringPercent",
        "vw",
        "vwNds",
        "ppvzOfficeId",
        "paidStorage",
        "paidAcceptance",
        "penalty",
        "deduction",
        "additionalPayment",
        "forPay",
        "deliveryRub",
        "deliveryAmount",
        "returnAmount",
        "srvDbs",
        "rebillLogisticCost",
        "orderId",
        "installmentCofinancingAmount",
        "wibesDiscountPercent",
        "cashbackAmount",
        "cashbackDiscount",
        "cashbackCommissionChange",
        "sellerPromoDiscount",
        "loyaltyDiscount",
        "salePricePromocodeDiscountPrc",
        "salePriceAffiliatedDiscountPrc",
        "salePriceWholesaleDiscountPrc",
    }

    values: list[list[object]] = []
    for row in rows:
        row_values: list[object] = []
        for header in headers:
            value = row.get(header, "")
            if header in numeric_fields:
                row_values.append(as_decimal(value))
            else:
                row_values.append(value)
        values.append(row_values)
    return [headers] + values


def ads_to_sheet_rows(rows: list[dict[str, Any]]) -> list[list[object]]:
    headers = ["date", "advertId", "appType", "nmId", "views", "clicks", "sum", "orders", "sum_price"]
    numeric_fields = {"views", "clicks", "sum", "orders", "sum_price"}
    values = []
    for row in rows:
        row_values: list[object] = []
        for header in headers:
            value = row.get(header, "")
            if header in numeric_fields:
                row_values.append(as_decimal(value))
            else:
                row_values.append(value)
        values.append(row_values)
    return [headers] + values


def stocks_to_sheet_rows(rows: list[dict[str, Any]]) -> list[list[object]]:
    headers = [
        "lastChangeDate",
        "supplierArticle",
        "nmId",
        "barcode",
        "quantity",
        "warehouseName",
        "warehouseType",
        "inWayToClient",
        "inWayFromClient",
        "quantityFull",
        "category",
        "subject",
        "brand",
        "techSize",
        "price",
        "discount",
        "isSupply",
        "isRealization",
        "SCCode",
    ]
    numeric_fields = {
        "nmId",
        "quantity",
        "inWayToClient",
        "inWayFromClient",
        "quantityFull",
        "price",
        "discount",
    }
    values: list[list[object]] = []
    for row in rows:
        row_values: list[object] = []
        for header in headers:
            value = row.get(header, "")
            if header in numeric_fields:
                row_values.append(as_decimal(value))
            else:
                row_values.append(value)
        values.append(row_values)
    return [headers] + values


def orders_to_sheet_rows(rows: list[dict[str, Any]]) -> list[list[object]]:
    headers = [
        "date",
        "lastChangeDate",
        "warehouseName",
        "countryName",
        "oblastOkrugName",
        "regionName",
        "supplierArticle",
        "nmId",
        "barcode",
        "category",
        "subject",
        "brand",
        "techSize",
        "incomeID",
        "isSupply",
        "isRealization",
        "totalPrice",
        "discountPercent",
        "spp",
        "finishedPrice",
        "priceWithDisc",
        "isCancel",
        "cancelDate",
        "orderType",
        "sticker",
        "gNumber",
        "srid",
        "odid",
    ]
    numeric_fields = {
        "totalPrice",
        "discountPercent",
        "spp",
        "finishedPrice",
        "priceWithDisc",
    }
    values: list[list[object]] = []
    for row in rows:
        row_values: list[object] = []
        for header in headers:
            value = row.get(header, "")
            if header in numeric_fields:
                row_values.append(as_decimal(value))
            else:
                row_values.append(value)
        values.append(row_values)
    return [headers] + values


def pnl_to_sheet_rows(rows: list[dict[str, Any]]) -> list[list[object]]:
    headers = [
        "date",
        "article_type",
        "article",
        "nm_id",
        "vendor_code",
        "orders_amount",
        "sales_amount",
        "sales_without_spp",
        "wb_commission",
        "acquiring_fee",
        "storage_fee",
        "acceptance_fee",
        "penalties",
        "deductions",
        "additional_payments",
        "delivery_fee",
        "ad_spend",
        "cogs_amount",
        "net_profit",
        "margin_pct",
    ]
    values = []
    for row in rows:
        values.append([row.get(header, "") for header in headers])
    return [headers] + values


def funnel_to_sheet_rows(funnel_data: list[dict[str, Any]]) -> list[list[object]]:
    """Convert funnel API response to sheet rows and persist every field returned by the API."""
    if not funnel_data:
        return [[]]

    def _serialize(value: Any) -> object:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return value

    # Keep existing user-facing columns first.
    base_columns = ["nmId", "supplierArticle", "productName", "brand", "currency", "date"]

    top_level_keys: set[str] = set()
    product_keys: set[str] = set()
    history_keys: set[str] = set()

    for item in funnel_data:
        top_level_keys.update(item.keys())
        product = item.get("product", {}) or {}
        if isinstance(product, dict):
            product_keys.update(product.keys())
        for history_item in item.get("history", []) or []:
            if isinstance(history_item, dict):
                history_keys.update(history_item.keys())

    top_level_keys.discard("product")
    top_level_keys.discard("history")
    top_level_keys.discard("currency")

    product_keys.discard("nmId")
    product_keys.discard("vendorCode")
    product_keys.discard("title")
    product_keys.discard("brandName")

    history_keys.discard("date")

    extra_top_level_columns = sorted(top_level_keys)
    extra_product_columns = [f"product_{key}" for key in sorted(product_keys)]
    history_columns = sorted(history_keys)

    headers = base_columns + extra_top_level_columns + extra_product_columns + history_columns
    rows = [headers]

    for item in funnel_data:
        product = item.get("product", {}) or {}
        currency = item.get("currency", "RUB")
        top_level_values = [_serialize(item.get(key, "")) for key in extra_top_level_columns]
        product_values = [_serialize(product.get(key, "")) for key in sorted(product_keys)]

        for history_item in item.get("history", []) or []:
            row = [
                product.get("nmId", ""),
                product.get("vendorCode", ""),
                product.get("title", ""),
                product.get("brandName", ""),
                currency,
                history_item.get("date", ""),
                *top_level_values,
                *product_values,
            ]
            for key in history_columns:
                row.append(_serialize(history_item.get(key, "")))
            rows.append(row)

    return rows
