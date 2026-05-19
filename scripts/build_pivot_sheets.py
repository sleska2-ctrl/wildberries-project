from __future__ import annotations

from wb_gsheets.config import load_settings
from wb_gsheets.google_sheets import GoogleSheetsClient


ARTICLES_SHEET = "свод_артикулы"
DAYS_SHEET = "свод_дни"

# daily_pnl колонки A..T (FILTER(daily_pnl!A2:T) -> Col1..Col20):
# Col1=date  Col2=article_type  Col3=article  Col4=nm_id  Col5=vendor_code
# Col6=orders_amount  Col7=sales_amount(с СПП)  Col8=sales_without_spp
# Col9=wb_commission  Col10=acquiring_fee  Col11=storage_fee  Col12=acceptance_fee
# Col13=penalties  Col14=deductions  Col15=additional_payments
# Col16=delivery_fee  Col17=ad_spend  Col18=cogs_amount  Col19=net_profit  Col20=margin_pct

_DATE_COND = "daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$D$3"


def _articles_query() -> str:
    select = (
        "Col3, Col4, Col5, "
        "sum(Col6), sum(Col7), sum(Col8), "
        "sum(Col9), sum(Col10), sum(Col11), sum(Col12), "
        "sum(Col13), sum(Col14), sum(Col15), "
        "sum(Col16), sum(Col17), sum(Col18), sum(Col19)"
    )
    labels = (
        "Col3 'Артикул', Col4 'nmId', Col5 'Вендор', "
        "sum(Col6) 'Заказы шт', sum(Col7) 'Продажи с СПП', sum(Col8) 'Продажи без СПП', "
        "sum(Col9) 'Комиссия ВБ', sum(Col10) 'Эквайринг', "
        "sum(Col11) 'Хранение', sum(Col12) 'Приёмка', "
        "sum(Col13) 'Штрафы', sum(Col14) 'Удержания', "
        "sum(Col15) 'Доплаты ВБ', sum(Col16) 'Логистика', "
        "sum(Col17) 'Реклама', sum(Col18) 'Себестоимость', "
        "sum(Col19) 'Чистая прибыль'"
    )
    return (
        f'=QUERY(FILTER(daily_pnl!A2:T;{_DATE_COND});'
        f'"select {select} where Col8 != 0 '
        f'group by Col3, Col4, Col5 order by sum(Col19) desc '
        f'label {labels}";0)'
    )


def _days_query() -> str:
    select = (
        "Col1, "
        "sum(Col6), sum(Col7), sum(Col8), "
        "sum(Col9), sum(Col10), sum(Col11), sum(Col12), "
        "sum(Col13), sum(Col14), sum(Col15), "
        "sum(Col16), sum(Col17), sum(Col18), sum(Col19)"
    )
    labels = (
        "Col1 'Дата', "
        "sum(Col6) 'Заказы шт', sum(Col7) 'Продажи с СПП', sum(Col8) 'Продажи без СПП', "
        "sum(Col9) 'Комиссия ВБ', sum(Col10) 'Эквайринг', "
        "sum(Col11) 'Хранение', sum(Col12) 'Приёмка', "
        "sum(Col13) 'Штрафы', sum(Col14) 'Удержания', "
        "sum(Col15) 'Доплаты ВБ', sum(Col16) 'Логистика', "
        "sum(Col17) 'Реклама', sum(Col18) 'Себестоимость', "
        "sum(Col19) 'Чистая прибыль'"
    )
    return (
        f'=QUERY(FILTER(daily_pnl!A2:T;{_DATE_COND});'
        f'"select {select} '
        f'group by Col1 order by Col1 '
        f'label {labels}";0)'
    )


def _sheet_range(sheet_id: int, r1: int, r2: int, c1: int, c2: int) -> dict:
    return {
        "sheetId": sheet_id,
        "startRowIndex": r1,
        "endRowIndex": r2,
        "startColumnIndex": c1,
        "endColumnIndex": c2,
    }


def _base_format_requests(sheet_id: int) -> list[dict]:
    return [
        {"repeatCell": {
            "range": _sheet_range(sheet_id, 0, 1, 0, 21),
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.09, "green": 0.18, "blue": 0.29},
                "textFormat": {
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                    "fontSize": 14,
                    "bold": True,
                },
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }},
        {"repeatCell": {
            "range": _sheet_range(sheet_id, 1, 2, 0, 21),
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.8},
                "textFormat": {"italic": True, "fontSize": 9},
                "wrapStrategy": "WRAP",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy)",
        }},
        {"repeatCell": {
            "range": _sheet_range(sheet_id, 2, 3, 0, 6),
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.95, "green": 0.97, "blue": 0.99},
                "textFormat": {"bold": True},
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 2},
            "properties": {"pixelSize": 160},
            "fields": "pixelSize",
        }},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 19},
            "properties": {"pixelSize": 145},
            "fields": "pixelSize",
        }},
    ]


def _currency_fmt(sheet_id: int, r1: int, r2: int, c1: int, c2: int) -> dict:
    return {
        "repeatCell": {
            "range": _sheet_range(sheet_id, r1, r2, c1, c2),
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
            "fields": "userEnteredFormat.numberFormat",
        }
    }


def _number_fmt(sheet_id: int, r1: int, r2: int, c1: int, c2: int) -> dict:
    return {
        "repeatCell": {
            "range": _sheet_range(sheet_id, r1, r2, c1, c2),
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}},
            "fields": "userEnteredFormat.numberFormat",
        }
    }


def _percent_fmt(sheet_id: int, r1: int, r2: int, c1: int, c2: int) -> dict:
    return {
        "repeatCell": {
            "range": _sheet_range(sheet_id, r1, r2, c1, c2),
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
            "fields": "userEnteredFormat.numberFormat",
        }
    }


def build_pivot_sheets() -> None:
    settings = load_settings()
    client = GoogleSheetsClient(
        service_account_file=settings.google_service_account_file,
        spreadsheet_id=settings.google_spreadsheet_id,
    )

    note = (
        "Источник: raw_sales + raw_ads. "
        "Для проверки выплаты используйте Продажи с СПП: "
        "Продажи с СПП - комиссии - логистика + доплаты = К перечислению ВБ. "
        "Меняйте даты в B3 и D3."
    )

    client.recreate_sheet(ARTICLES_SHEET)
    art_id = client.get_sheet_id(ARTICLES_SHEET)

    art_rows = [
        ["Свод по артикулам — все комиссии ВБ"],
        [note],
        ["Период с", "2026-05-01", "по", "2026-05-07"],
        [],
        [
            _articles_query(),
            *[""] * 16,
            "К перечислению ВБ",
            "Маржа %",
        ],
        [
            *[""] * 17,
            '=IFERROR(MAP(A6:A;LAMBDA(vc;IF(vc="";"";SUMIFS(raw_sales!J:J;raw_sales!AC:AC;vc;raw_sales!E:E;">="&$B$3;raw_sales!E:E;"<="&$D$3))));"")',
            '=IFERROR(MAP(Q6:Q;F6:F;LAMBDA(p;s;IF((p="")+(s=0)>0;"";p/s)));"")',
        ],
    ]

    client.replace_sheet(ARTICLES_SHEET, art_rows)

    art_fmt = _base_format_requests(art_id) + [
        _number_fmt(art_id, 4, 2000, 3, 4),
        _currency_fmt(art_id, 4, 2000, 4, 18),
        _percent_fmt(art_id, 4, 2000, 18, 19),
    ]
    client.batch_update({"requests": art_fmt})

    client.recreate_sheet(DAYS_SHEET)
    days_id = client.get_sheet_id(DAYS_SHEET)

    days_rows = [
        ["Свод по дням — все комиссии ВБ"],
        [note],
        ["Период с", "2026-05-01", "по", "2026-05-07"],
        [],
        [
            _days_query(),
            *[""] * 14,
            "К перечислению ВБ",
            "Маржа %",
        ],
        [
            *[""] * 15,
            '=IFERROR(ARRAYFORMULA(IF(C6:C="";"";C6:C-E6:E-F6:F-G6:G-H6:H-I6:I-J6:J-L6:L+K6:K));"")',
            '=IFERROR(MAP(O6:O;D6:D;LAMBDA(p;s;IF((p="")+(s=0)>0;"";p/s)));"")',
        ],
    ]

    client.replace_sheet(DAYS_SHEET, days_rows)

    days_fmt = _base_format_requests(days_id) + [
        {"updateDimensionProperties": {
            "range": {"sheetId": days_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 110},
            "fields": "pixelSize",
        }},
        _number_fmt(days_id, 4, 2000, 1, 2),
        _currency_fmt(days_id, 4, 2000, 2, 16),
        _percent_fmt(days_id, 4, 2000, 16, 17),
    ]
    client.batch_update({"requests": days_fmt})

    print("Готово: листы 'свод_артикулы' и 'свод_дни' пересозданы.")


if __name__ == "__main__":
    build_pivot_sheets()
