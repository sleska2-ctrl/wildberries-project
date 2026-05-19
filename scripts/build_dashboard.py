from __future__ import annotations

from wb_gsheets.config import load_settings
from wb_gsheets.google_sheets import GoogleSheetsClient


DASHBOARD_SHEET = "dashboard"


def _sheet_range(sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int) -> dict:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


# daily_pnl columns (1-based for QUERY Col, 0-based letter index):
# A=date  B=article_type  C=article  D=nm_id  E=vendor_code
# F=orders_amount  G=sales_amount  H=sales_without_spp
# I=wb_commission  J=acquiring_fee  K=storage_fee  L=acceptance_fee
# M=penalties  N=deductions  O=additional_payments
# P=delivery_fee  Q=ad_spend  R=cogs_amount  S=net_profit  T=margin_pct

# Shorthand filter base (used many times)
_F = "daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$D$3"


def _dpnl(col: str) -> str:
    """SUM of a daily_pnl column filtered by dashboard date range."""
    return f"=IFERROR(SUMIFS(daily_pnl!{col}2:{col};daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3);0)"


def build_dashboard() -> None:
    settings = load_settings()
    client = GoogleSheetsClient(
        service_account_file=settings.google_service_account_file,
        spreadsheet_id=settings.google_spreadsheet_id,
    )

    client.recreate_sheet(DASHBOARD_SHEET)
    sheet_id = client.get_sheet_id(DASHBOARD_SHEET)

    # KPI block references — all from daily_pnl directly
    sales_spp   = _dpnl("G")   # sales_amount
    sales_nospp = _dpnl("H")   # sales_without_spp
    wb_comm     = _dpnl("I")   # wb_commission
    acquiring   = _dpnl("J")   # acquiring_fee
    storage     = _dpnl("K")   # storage_fee
    acceptance  = _dpnl("L")   # acceptance_fee
    penalties   = _dpnl("M")   # penalties
    deductions  = _dpnl("N")   # deductions
    addl_pay    = _dpnl("O")   # additional_payments
    ad_spend    = _dpnl("Q")   # ad_spend
    cogs        = _dpnl("R")   # cogs_amount
    net_profit  = _dpnl("S")   # net_profit

    drr_pct    = "=IFERROR(SUMIFS(daily_pnl!Q2:Q;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)/SUMIFS(daily_pnl!H2:H;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3);0)"
    margin_pct  = "=IFERROR(SUMIFS(daily_pnl!S2:S;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)/SUMIFS(daily_pnl!H2:H;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3);0)"
    sku_count   = "=IFERROR(COUNTA(UNIQUE(FILTER(daily_pnl!C2:C;daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$D$3)));0)"

    other_pct   = ("=IFERROR((SUMIFS(daily_pnl!K2:K;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)"
                   "+SUMIFS(daily_pnl!L2:L;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)"
                   "+SUMIFS(daily_pnl!M2:M;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)"
                   "+SUMIFS(daily_pnl!N2:N;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3))"
                   "/SUMIFS(daily_pnl!H2:H;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3);0)")

    # daily_pnl QUERY FILTER range for Топ SKU — cols C:T (Col1=article .. Col16=cogs Col17=net_profit Col18=margin_pct)
    # relative to C: F=Col4 orders, G=Col5 sales_spp, H=Col6 sales_nospp,
    #                I=Col7 wb_comm, J=Col8 acquiring, Q=Col15 ad_spend, R=Col16 cogs, S=Col17 net_profit
    top_sku_query = (
        '=QUERY(FILTER(daily_pnl!C2:T;daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$D$3);'
        '"select Col1, Col2, Col3, sum(Col4), sum(Col5), sum(Col6), sum(Col7), sum(Col8), '
        'sum(Col15), sum(Col16), sum(Col17), sum(Col17)/sum(Col6), sum(Col7)/sum(Col6) '
        'where Col5 <> 0 group by Col1, Col2, Col3 order by sum(Col17) desc limit 15 '
        "label Col1 'SKU', Col2 'nmId', Col3 'vendorCode', sum(Col4) 'Продажи, шт', "
        "sum(Col5) 'Продажи с СПП', sum(Col6) 'Продажи без СПП', sum(Col7) 'Комиссия WB', "
        "sum(Col8) 'Эквайринг', sum(Col15) 'Реклама', sum(Col16) 'Себестоимость', "
        "sum(Col17) 'Чистая прибыль', sum(Col17)/sum(Col6) 'Маржа %', "
        "sum(Col7)/sum(Col6) '% комиссии WB'"
        '";0)'
    )

    # Динамика по дням — cols A:T (Col1=date, Col6=orders, Col7=sales_spp, Col8=sales_nospp,
    # Col9=wb_comm, Col10=acquiring, Col11=storage, Col12=acceptance,
    # Col13=penalties, Col14=deductions, Col15=additional_payments,
    # Col16=delivery_fee, Col17=ad_spend, Col18=cogs, Col19=net_profit)
    dynamics_query = (
        '=QUERY(FILTER(daily_pnl!A2:T;daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$D$3);'
        '"select Col1, sum(Col6), sum(Col7), sum(Col8), sum(Col9), sum(Col9)/sum(Col8), '
        'sum(Col10), sum(Col10)/sum(Col8), sum(Col11), sum(Col12), sum(Col13), sum(Col14), '
        'sum(Col17), sum(Col17)/sum(Col8), sum(Col18), sum(Col18)/sum(Col8), sum(Col19), sum(Col19)/sum(Col8) '
        'group by Col1 order by Col1 '
        "label Col1 'Дата', sum(Col6) 'Продажи, шт', sum(Col7) 'Продажи с СПП', sum(Col8) 'Продажи без СПП', "
        "sum(Col9) 'Комиссия WB', sum(Col9)/sum(Col8) '% комиссии WB', "
        "sum(Col10) 'Эквайринг', sum(Col10)/sum(Col8) '% эквайринга', "
        "sum(Col11) 'Хранение', sum(Col12) 'Приёмка', sum(Col13) 'Штрафы', sum(Col14) 'Удержания', "
        "sum(Col17) 'Реклама', sum(Col17)/sum(Col8) 'ДРР', "
        "sum(Col18) 'Себестоимость', sum(Col18)/sum(Col8) '% себестоимости', "
        "sum(Col19) 'Чистая прибыль', sum(Col19)/sum(Col8) 'Маржа %'"
        '";0)'
    )

    rows = [
        ["WB Sales Dashboard"],
        [],
        ["Период с", "2026-05-01", "по", "2026-05-07", "Меняйте даты в B3 и D3"],
        [],
        ["KPI"],
        ["Продажи с СПП",    sales_spp,   "ДРР",       drr_pct,   "Маржа %",       margin_pct],
        ["Продажи без СПП",  sales_nospp, "Комиссия WB", wb_comm, "Эквайринг",     acquiring],
        ["Чистая прибыль",   net_profit,  "Себестоимость", cogs,  "SKU в периоде", sku_count],
        [],
        ["Структура расходов, % от продаж без СПП"],
        ["Комиссия WB %",    "=IFERROR(SUMIFS(daily_pnl!I2:I;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)/SUMIFS(daily_pnl!H2:H;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3);0)"],
        ["Эквайринг %",      "=IFERROR(SUMIFS(daily_pnl!J2:J;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)/SUMIFS(daily_pnl!H2:H;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3);0)"],
        ["ДРР %",            drr_pct],
        ["Себестоимость %",  "=IFERROR(SUMIFS(daily_pnl!R2:R;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3)/SUMIFS(daily_pnl!H2:H;daily_pnl!A2:A;\">=\"&$B$3;daily_pnl!A2:A;\"<=\"&$D$3);0)"],
        ["Хранение+приёмка+штрафы+удержания %", other_pct],
        [],
        ["Топ SKU по прибыли", "", "", "", "", "", "", "", "", "", "", "", "", "", "Динамика по дням"],
        [top_sku_query, "", "", "", "", "", "", "", "", "", "", "", "", "", dynamics_query],
    ]

    client.replace_sheet(DASHBOARD_SHEET, rows)

    client.batch_update(
        {
            "requests": [
                {
                    "updateSpreadsheetProperties": {
                        "properties": {"locale": "ru_RU", "timeZone": "Europe/Moscow"},
                        "fields": "locale,timeZone",
                    }
                },
                {"mergeCells": {"range": _sheet_range(sheet_id, 0, 1, 0, 6), "mergeType": "MERGE_ALL"}},
                {"mergeCells": {"range": _sheet_range(sheet_id, 4, 5, 0, 6), "mergeType": "MERGE_ALL"}},
                {"mergeCells": {"range": _sheet_range(sheet_id, 9, 10, 0, 3), "mergeType": "MERGE_ALL"}},
                {"mergeCells": {"range": _sheet_range(sheet_id, 16, 17, 0, 13), "mergeType": "MERGE_ALL"}},
                {"mergeCells": {"range": _sheet_range(sheet_id, 16, 17, 14, 32), "mergeType": "MERGE_ALL"}},
                # Title
                {"repeatCell": {
                    "range": _sheet_range(sheet_id, 0, 1, 0, 6),
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.09, "green": 0.18, "blue": 0.29}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 18, "bold": True}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }},
                # Period row
                {"repeatCell": {
                    "range": _sheet_range(sheet_id, 2, 3, 0, 5),
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.97, "blue": 0.99}, "textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }},
                # KPI block
                {"repeatCell": {
                    "range": _sheet_range(sheet_id, 4, 9, 0, 6),
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.99, "green": 0.99, "blue": 0.99}, "borders": {"top": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "bottom": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "left": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "right": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}}}},
                    "fields": "userEnteredFormat(backgroundColor,borders)",
                }},
                # Expenses block
                {"repeatCell": {
                    "range": _sheet_range(sheet_id, 9, 15, 0, 3),
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.99, "green": 0.99, "blue": 0.99}, "borders": {"top": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "bottom": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "left": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "right": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}}}},
                    "fields": "userEnteredFormat(backgroundColor,borders)",
                }},
                # Table headers
                {"repeatCell": {
                    "range": _sheet_range(sheet_id, 16, 18, 0, 32),
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.92, "green": 0.96, "blue": 0.94}, "textFormat": {"bold": True, "fontSize": 12}}},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }},
                # KPI number formats
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 8, 1, 2), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 6, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 6, 5, 6), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 6, 7, 3, 6), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 7, 8, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 10, 15, 1, 2), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                # Топ SKU cols (offset 0–12)
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 4, 11), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 11, 13), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                # Динамика: date col 14, then numbers/percents cols 15–31
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 14, 15), "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 15, 18), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 18, 19), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 19, 20), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 20, 21), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 21, 22), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 22, 26), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 26, 27), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 27, 28), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 28, 29), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 29, 30), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 30, 31), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 18, 500, 31, 32), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                # Column widths
                {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 180}, "fields": "pixelSize"}},
                {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 13}, "properties": {"pixelSize": 130}, "fields": "pixelSize"}},
                {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 14, "endIndex": 32}, "properties": {"pixelSize": 140}, "fields": "pixelSize"}},
            ]
        }
    )


if __name__ == "__main__":
    build_dashboard()

