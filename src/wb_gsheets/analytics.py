from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any

from .utils import as_decimal, to_iso_date


def _sale_sign(row: dict[str, Any]) -> Decimal:
    doc_type = str(row.get("docTypeName", "")).strip()
    if doc_type == "Продажа":
        return Decimal("1")
    if doc_type == "Возврат":
        return Decimal("-1")
    return Decimal("0")


def _to_pct(value: Decimal, base: Decimal) -> Decimal:
    if base == 0:
        return Decimal("0")
    return value / base * Decimal("100")


def _resolve_article(
    row: dict[str, Any],
    article_filter_type: str,
    nm_mapping: dict[str, str],
) -> str:
    nm_id = str(row.get("nmId", "")).strip()
    from_mapping = nm_mapping.get(nm_id, "").strip()
    if from_mapping:
        return from_mapping

    candidate = str(row.get(article_filter_type, "")).strip()
    if candidate:
        return candidate

    return str(row.get("vendorCode", "")).strip() or nm_id


def build_analytics_tables(
    sales_rows: list[dict[str, Any]],
    ads_rows: list[dict[str, Any]],
    cogs_map: dict[tuple[str, str], Decimal],
    article_filter_type: str,
    nm_mapping: dict[str, str],
    date_from: str,
    date_to: str,
) -> dict[str, list[list[object]]]:
    by_article_day: dict[tuple[str, str], dict[str, object]] = {}
    nm_to_article = dict(nm_mapping)

    for row in sales_rows:
        sign = _sale_sign(row)
        if sign == 0:
            continue

        row_date = to_iso_date(str(row.get("saleDt") or row.get("dateFrom") or row.get("dateTo") or ""))
        if not row_date:
            continue

        nm_id = str(row.get("nmId", "")).strip()
        article = _resolve_article(row, article_filter_type, nm_mapping)
        if nm_id and article:
            nm_to_article[nm_id] = article

        key = (row_date, article)
        if key not in by_article_day:
            by_article_day[key] = {
                "Дата": row_date,
                "Артикул": article,
                "Продажи, шт": Decimal("0"),
                "Наша цена": Decimal("0"),
                "Комиссия WB": Decimal("0"),
                "% комиссии WB": Decimal("0"),
                "СПП": Decimal("0"),
                "% СПП": Decimal("0"),
                "Вознаграждение WB": Decimal("0"),
                "% вознаграждения WB": Decimal("0"),
                "НДС WB": Decimal("0"),
                "% НДС WB": Decimal("0"),
                "Возмещение ПВЗ": Decimal("0"),
                "% возмещения ПВЗ": Decimal("0"),
                "Эквайринг": Decimal("0"),
                "% эквайринга": Decimal("0"),
                "Логистика": Decimal("0"),
                "% логистики": Decimal("0"),
                "Хранение": Decimal("0"),
                "% хранения": Decimal("0"),
                "Приемка": Decimal("0"),
                "% приемки": Decimal("0"),
                "Штрафы": Decimal("0"),
                "% штрафов": Decimal("0"),
                "Удержания": Decimal("0"),
                "% удержаний": Decimal("0"),
                "Доплаты": Decimal("0"),
                "% доплат": Decimal("0"),
                "К перечислению": Decimal("0"),
                "% к перечислению": Decimal("0"),
                "Реклама": Decimal("0"),
                "% рекламы": Decimal("0"),
                "Себестоимость": Decimal("0"),
                "% себестоимости": Decimal("0"),
                "Чистая прибыль": Decimal("0"),
                "% чистой прибыли": Decimal("0"),
                "Расчетное к перечислению": Decimal("0"),
                "Контроль forPay": Decimal("0"),
                "Сырой % комиссии": Decimal("0"),
                "Контроль % комиссии": Decimal("0"),
                "_raw_commission_numerator": Decimal("0"),
                "_nm_id": nm_id,
            }

        bucket = by_article_day[key]
        quantity = as_decimal(row.get("quantity")) * sign
        retail_price_with_disc = as_decimal(row.get("retailPriceWithDisc")) * quantity
        retail_amount = as_decimal(row.get("retailAmount")) * sign

        spp = retail_price_with_disc - retail_amount
        wb_reward = as_decimal(row.get("vw")) * sign
        wb_vat = as_decimal(row.get("vwNds")) * sign
        pvz_compensation = as_decimal(row.get("ppvzReward")) * sign
        acquiring = as_decimal(row.get("acquiringFee")) * sign

        logistics = (
            as_decimal(row.get("deliveryAmount"))
            + as_decimal(row.get("returnAmount"))
            + as_decimal(row.get("rebillLogisticCost"))
        ) * sign

        storage = as_decimal(row.get("paidStorage"))
        acceptance = as_decimal(row.get("paidAcceptance"))
        penalties = as_decimal(row.get("penalty"))
        deductions = as_decimal(row.get("deduction"))
        additional = as_decimal(row.get("additionalPayment"))
        for_pay = as_decimal(row.get("forPay")) * sign
        commission_total = spp + wb_reward + wb_vat + pvz_compensation

        raw_commission_part = as_decimal(row.get("commissionPercent")) * retail_price_with_disc / Decimal("100")

        bucket["Продажи, шт"] += quantity
        bucket["Наша цена"] += retail_price_with_disc
        bucket["СПП"] += spp
        bucket["Вознаграждение WB"] += wb_reward
        bucket["НДС WB"] += wb_vat
        bucket["Возмещение ПВЗ"] += pvz_compensation
        bucket["Эквайринг"] += acquiring
        bucket["Логистика"] += logistics
        bucket["Хранение"] += storage
        bucket["Приемка"] += acceptance
        bucket["Штрафы"] += penalties
        bucket["Удержания"] += deductions
        bucket["Доплаты"] += additional
        bucket["К перечислению"] += for_pay
        bucket["Комиссия WB"] += commission_total
        bucket["_raw_commission_numerator"] += raw_commission_part

    ads_by_key: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    for row in ads_rows:
        row_date = str(row.get("date", "")).strip()
        nm_id = str(row.get("nmId", "")).strip()
        if not row_date or not nm_id:
            continue
        article = nm_to_article.get(nm_id)
        if not article:
            continue
        ads_by_key[(row_date, article)] += as_decimal(row.get("sum"))

    for (row_date, article), bucket in by_article_day.items():
        sales_our = as_decimal(bucket["Наша цена"])
        qty = as_decimal(bucket["Продажи, шт"])

        ad_spend = ads_by_key.get((row_date, article), Decimal("0"))
        cogs = cogs_map.get((article_filter_type, article), Decimal("0")) * qty

        bucket["Реклама"] = ad_spend
        bucket["Себестоимость"] = cogs
        bucket["Чистая прибыль"] = as_decimal(bucket["К перечислению"]) - ad_spend - cogs
        bucket["Расчетное к перечислению"] = (
            sales_our
            - as_decimal(bucket["Комиссия WB"])
            - as_decimal(bucket["Эквайринг"])
            - as_decimal(bucket["Логистика"])
            - as_decimal(bucket["Хранение"])
            - as_decimal(bucket["Приемка"])
            - as_decimal(bucket["Штрафы"])
            - as_decimal(bucket["Удержания"])
            + as_decimal(bucket["Доплаты"])
        )
        bucket["Контроль forPay"] = as_decimal(bucket["К перечислению"]) - as_decimal(bucket["Расчетное к перечислению"])

        bucket["% комиссии WB"] = _to_pct(as_decimal(bucket["Комиссия WB"]), sales_our)
        bucket["% СПП"] = _to_pct(as_decimal(bucket["СПП"]), sales_our)
        bucket["% вознаграждения WB"] = _to_pct(as_decimal(bucket["Вознаграждение WB"]), sales_our)
        bucket["% НДС WB"] = _to_pct(as_decimal(bucket["НДС WB"]), sales_our)
        bucket["% возмещения ПВЗ"] = _to_pct(as_decimal(bucket["Возмещение ПВЗ"]), sales_our)
        bucket["% эквайринга"] = _to_pct(as_decimal(bucket["Эквайринг"]), sales_our)
        bucket["% логистики"] = _to_pct(as_decimal(bucket["Логистика"]), sales_our)
        bucket["% хранения"] = _to_pct(as_decimal(bucket["Хранение"]), sales_our)
        bucket["% приемки"] = _to_pct(as_decimal(bucket["Приемка"]), sales_our)
        bucket["% штрафов"] = _to_pct(as_decimal(bucket["Штрафы"]), sales_our)
        bucket["% удержаний"] = _to_pct(as_decimal(bucket["Удержания"]), sales_our)
        bucket["% доплат"] = _to_pct(as_decimal(bucket["Доплаты"]), sales_our)
        bucket["% к перечислению"] = _to_pct(as_decimal(bucket["К перечислению"]), sales_our)
        bucket["% рекламы"] = _to_pct(ad_spend, sales_our)
        bucket["% себестоимости"] = _to_pct(cogs, sales_our)
        bucket["% чистой прибыли"] = _to_pct(as_decimal(bucket["Чистая прибыль"]), sales_our)

        raw_commission_pct = _to_pct(as_decimal(bucket["_raw_commission_numerator"]), sales_our)
        bucket["Сырой % комиссии"] = raw_commission_pct
        bucket["Контроль % комиссии"] = raw_commission_pct - as_decimal(bucket["% комиссии WB"])

    detail_headers = [
        "Дата",
        "Артикул",
        "Продажи, шт",
        "Наша цена",
        "Комиссия WB",
        "% комиссии WB",
        "СПП",
        "% СПП",
        "Вознаграждение WB",
        "% вознаграждения WB",
        "НДС WB",
        "% НДС WB",
        "Возмещение ПВЗ",
        "% возмещения ПВЗ",
        "Эквайринг",
        "% эквайринга",
        "Логистика",
        "% логистики",
        "Хранение",
        "% хранения",
        "Приемка",
        "% приемки",
        "Штрафы",
        "% штрафов",
        "Удержания",
        "% удержаний",
        "Доплаты",
        "% доплат",
        "К перечислению",
        "% к перечислению",
        "Реклама",
        "% рекламы",
        "Себестоимость",
        "% себестоимости",
        "Чистая прибыль",
        "% чистой прибыли",
        "Расчетное к перечислению",
        "Контроль forPay",
        "Сырой % комиссии",
        "Контроль % комиссии",
    ]

    sorted_items = sorted(by_article_day.values(), key=lambda item: (str(item["Дата"]), str(item["Артикул"])))
    detail_rows: list[list[object]] = [detail_headers]
    for item in sorted_items:
        detail_rows.append([item.get(h, "") for h in detail_headers])

    article_day_headers = [
        "Артикул",
        "Дата",
        "Продажи по нашей цене",
        "Реклама",
        "Чистая прибыль",
        "ДРР",
        "% маржи",
    ]
    article_day_rows: list[list[object]] = [article_day_headers]
    for item in sorted_items:
        sales_our = as_decimal(item["Наша цена"])
        article_day_rows.append(
            [
                item["Артикул"],
                item["Дата"],
                sales_our,
                item["Реклама"],
                item["Чистая прибыль"],
                _to_pct(as_decimal(item["Реклама"]), sales_our),
                _to_pct(as_decimal(item["Чистая прибыль"]), sales_our),
            ]
        )

    day_grouped: dict[str, dict[str, Decimal]] = defaultdict(lambda: {
        "sales": Decimal("0"),
        "ad": Decimal("0"),
        "profit": Decimal("0"),
    })
    article_period_grouped: dict[str, dict[str, Decimal]] = defaultdict(lambda: {
        "sales": Decimal("0"),
        "ad": Decimal("0"),
        "profit": Decimal("0"),
    })

    for item in sorted_items:
        day = str(item["Дата"])
        article = str(item["Артикул"])
        sales_our = as_decimal(item["Наша цена"])
        ad = as_decimal(item["Реклама"])
        profit = as_decimal(item["Чистая прибыль"])

        day_grouped[day]["sales"] += sales_our
        day_grouped[day]["ad"] += ad
        day_grouped[day]["profit"] += profit

        article_period_grouped[article]["sales"] += sales_our
        article_period_grouped[article]["ad"] += ad
        article_period_grouped[article]["profit"] += profit

    day_headers = ["Дата", "Продажи по нашей цене, р", "Реклама, р", "Чистая прибыль", "ДРР", "% маржи"]
    day_rows: list[list[object]] = [day_headers]
    for day in sorted(day_grouped.keys()):
        sales_our = day_grouped[day]["sales"]
        ad = day_grouped[day]["ad"]
        profit = day_grouped[day]["profit"]
        day_rows.append([day, sales_our, ad, profit, _to_pct(ad, sales_our), _to_pct(profit, sales_our)])

    period_headers = [
        "Период с",
        "Период по",
        "Артикул",
        "Продажи по нашей цене",
        "Реклама",
        "Чистая прибыль",
        "ДРР",
        "% маржи",
    ]
    period_rows: list[list[object]] = [period_headers]
    for article in sorted(article_period_grouped.keys()):
        sales_our = article_period_grouped[article]["sales"]
        ad = article_period_grouped[article]["ad"]
        profit = article_period_grouped[article]["profit"]
        period_rows.append(
            [
                date_from,
                date_to,
                article,
                sales_our,
                ad,
                profit,
                _to_pct(ad, sales_our),
                _to_pct(profit, sales_our),
            ]
        )

    return {
        "finance_article_day_detail": detail_rows,
        "analytics_article_day": article_day_rows,
        "analytics_day": day_rows,
        "analytics_article_period": period_rows,
    }
