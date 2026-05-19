from __future__ import annotations

from wb_gsheets.config import load_settings
from wb_gsheets.google_sheets import GoogleSheetsClient


MAX_ROWS = 2500
DATE_FROM = "2026-05-01"
DATE_TO = "2026-05-31"


def _sheet_range(sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int) -> dict:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


RAW_SALES_RANGE = "raw_sales!A:ZZ"
RAW_SALES_HEADER = "raw_sales!$1:$1"


def _col(field: str) -> str:
    return f'INDEX({RAW_SALES_RANGE};0;MATCH("{field}";{RAW_SALES_HEADER};0))'


def _article_finance_row_formulas(row_number: int) -> list[str]:
    sale_dt = f"ARRAYFORMULA(LEFT({_col('saleDt')};10))"
    nm_id = f"ARRAYFORMULA(TO_TEXT({_col('nmId')}))"
    vendor_code = (
        f"ARRAYFORMULA(IFERROR("
        f"VLOOKUP({nm_id};{{TO_TEXT(SKU!G:G)\\SKU!D:D}};2;FALSE);"
        f"TRIM({_col('vendorCode')})))"
    )
    doc_type = _col("docTypeName")
    quantity = _col("quantity")
    price_with_disc = _col("retailPriceWithDisc")
    retail_amount = _col("retailAmount")
    vw = _col("vw")
    vw_nds = _col("vwNds")
    ppvz_reward = _col("ppvzReward")
    acquiring = _col("acquiringFee")
    delivery = _col("deliveryAmount")
    returned = _col("returnAmount")
    rebill = _col("rebillLogisticCost")
    storage = _col("paidStorage")
    acceptance = _col("paidAcceptance")
    penalty = _col("penalty")
    deduction = _col("deduction")
    additional = _col("additionalPayment")
    for_pay = _col("forPay")
    commission_percent = _col("commissionPercent")
    commission_percent_num = f"ARRAYFORMULA(N({commission_percent}))"
    return [
        '=IFERROR(INDEX(keys!A$2:A;ROW()-5);"")',
        '=IFERROR(INDEX(keys!B$2:B;ROW()-5);"")',
        f'=IF($A{row_number}="";;SUMIFS({quantity};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({quantity};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;SUMIFS({price_with_disc};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({price_with_disc};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;$G{row_number}+$I{row_number}+$K{row_number}+$M{row_number})',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$E{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({price_with_disc};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({price_with_disc};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат")-(SUMIFS({retail_amount};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({retail_amount};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат")))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$G{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({vw};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({vw};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$I{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({vw_nds};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({vw_nds};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$K{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({ppvz_reward};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({ppvz_reward};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$M{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({acquiring};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({acquiring};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$O{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;(SUMIFS({delivery};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({delivery};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))+(SUMIFS({returned};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({returned};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))+(SUMIFS({rebill};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({rebill};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат")))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$Q{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({storage};{sale_dt};$A{row_number};{vendor_code};$B{row_number}))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$S{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({acceptance};{sale_dt};$A{row_number};{vendor_code};$B{row_number}))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$U{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({penalty};{sale_dt};$A{row_number};{vendor_code};$B{row_number}))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$W{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({deduction};{sale_dt};$A{row_number};{vendor_code};$B{row_number}))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$Y{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({additional};{sale_dt};$A{row_number};{vendor_code};$B{row_number}))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$AA{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;SUMIFS({for_pay};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({for_pay};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$AC{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;IFERROR(SUMIFS(raw_ads!G:G;raw_ads!A:A;TEXT($A{row_number};"yyyy-mm-dd");raw_ads!D:D;TO_TEXT(VLOOKUP($B{row_number};{{SKU!D:D\\SKU!G:G}};2;FALSE)));0))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$AE{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;IFERROR(VLOOKUP($B{row_number};{{SKU!D:D\\SKU!K:K}};2;FALSE)*$C{row_number};0))',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$AG{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;$AC{row_number}-$AE{row_number}-$AG{row_number})',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;$AI{row_number}/$D{row_number}))',
        f'=IF($A{row_number}="";;$D{row_number}-$E{row_number}-$O{row_number}-$Q{row_number}-$S{row_number}-$U{row_number}-$W{row_number}-$Y{row_number}+$AA{row_number})',
        f'=IF($A{row_number}="";;$AC{row_number}-$AK{row_number})',
        f'=IF($A{row_number}="";;IF($D{row_number}=0;;(SUM(IFERROR(FILTER({commission_percent_num}*{price_with_disc};{sale_dt}=$A{row_number};{vendor_code}=$B{row_number};{doc_type}="Продажа");0))-SUM(IFERROR(FILTER({commission_percent_num}*{price_with_disc};{sale_dt}=$A{row_number};{vendor_code}=$B{row_number};{doc_type}="Возврат");0)))/$D{row_number}/100))',
        f'=IF($A{row_number}="";;$AF{row_number}-$F{row_number})',
    ]


def _row_formulas(row_number: int) -> list[str]:
    sale_dt = f"ARRAYFORMULA(LEFT({_col('saleDt')};10))"
    nm_id = f"ARRAYFORMULA(TO_TEXT({_col('nmId')}))"
    vendor_code = (
        f"ARRAYFORMULA(IFERROR("
        f"VLOOKUP({nm_id};{{TO_TEXT(SKU!G:G)\\SKU!D:D}};2;FALSE);"
        f"TRIM({_col('vendorCode')})))"
    )
    doc_type = _col("docTypeName")
    quantity = _col("quantity")
    retail_amount = _col("retailAmount")
    retail_without_spp = _col("retailPriceWithDisc")
    commission = _col("ppvzSalesCommission")
    acquiring = _col("acquiringFee")
    return [
        '=IFERROR(INDEX(keys!A$2:A;ROW()-5);"")',
        '=IFERROR(INDEX(keys!B$2:B;ROW()-5);"")',
        f'=IF($A{row_number}="";;SUMIFS({quantity};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({quantity};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;SUMIFS({retail_amount};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({retail_amount};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;SUMIFS({retail_without_spp};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({retail_without_spp};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;SUMIFS({commission};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({commission};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;SUMIFS({acquiring};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Продажа")-SUMIFS({acquiring};{sale_dt};$A{row_number};{vendor_code};$B{row_number};{doc_type};"Возврат"))',
        f'=IF($A{row_number}="";;IFERROR(SUMIFS(raw_ads!G:G;raw_ads!A:A;TEXT($A{row_number};"yyyy-mm-dd");raw_ads!D:D;TO_TEXT(VLOOKUP($B{row_number};{{SKU!D:D\\SKU!G:G}};2;FALSE)));0))',
        f'=IF($A{row_number}="";;IFERROR(VLOOKUP($B{row_number};{{SKU!D:D\\SKU!K:K}};2;FALSE)*$C{row_number};0))',
        f'=IF($A{row_number}="";;$D{row_number}-$F{row_number}-$G{row_number}-$H{row_number}-$I{row_number})',
        f'=IF($A{row_number}="";;IF($E{row_number}=0;;$H{row_number}/$E{row_number}))',
        f'=IF($A{row_number}="";;IF($E{row_number}=0;;$J{row_number}/$E{row_number}))',
    ]


def rebuild_formula_pnl(client: GoogleSheetsClient) -> None:
    client.recreate_sheet("keys")
    client.replace_sheet(
        "keys",
        [
            ["Дата", "Артикул"],
            [
                f'=SORT(UNIQUE(QUERY({{ARRAYFORMULA(LEFT({_col("saleDt")};10))\\ARRAYFORMULA(IFERROR(VLOOKUP(ARRAYFORMULA(TO_TEXT({_col("nmId")}));{{TO_TEXT(SKU!G:G)\\SKU!D:D}};2;FALSE);TRIM({_col("vendorCode")})));FILTER(raw_ads!A2:A;raw_ads!A2:A<>"")\\ARRAYFORMULA(IFERROR(VLOOKUP(TO_TEXT(FILTER(raw_ads!D2:D;raw_ads!A2:A<>""));{{TO_TEXT(SKU!G:G)\\SKU!D:D}};2;FALSE);""))}};"select Col1, Col2 where Col1 is not null and Col2 is not null and Col1 >= \'{DATE_FROM}\' and Col1 <= \'{DATE_TO}\' label Col1 \'\', Col2 \'\'";0));1;TRUE;2;TRUE)',
                "",
            ],
        ],
    )

    client.recreate_sheet("formula_pnl")
    rows = [
        ["Формульный PnL v2"],
        [],
        ["Период с", DATE_FROM, "по", DATE_TO],
        [],
        ["Дата", "Артикул", "Продажи, шт", "Продажи с СПП", "Продажи без СПП", "Комиссия", "Эквайринг", "Реклама", "Себестоимость", "Прибыль", "ДРР", "Маржа"],
        _row_formulas(6),
    ]
    for row_number in range(7, MAX_ROWS + 1):
        rows.append(_row_formulas(row_number))

    client.replace_sheet("formula_pnl", rows)
    sheet_id = client.get_sheet_id("formula_pnl")
    client.batch_update(
        {
            "requests": [
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 0, 1),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 2, 3),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 3, 10),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 10, 12),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
            ]
        }
    )


def rebuild_dashboard_formula(client: GoogleSheetsClient) -> None:
    client.recreate_sheet("dashboard_formula")
    rows = [
        ["Дашборд WB на формулах"],
        [],
        ["Период с", '=formula_pnl!B3', "по", '=formula_pnl!D3'],
        [],
        ["KPI", "", "", "", "", "Структура расходов", "Сумма", "% от продаж без СПП"],
        ["Продажи с СПП", '=IFERROR(SUM(FILTER(formula_pnl!D6:D;formula_pnl!A6:A<>""));0)', "Продажи без СПП", '=IFERROR(SUM(FILTER(formula_pnl!E6:E;formula_pnl!A6:A<>""));0)', "", "Комиссия", '=IFERROR(SUM(FILTER(formula_pnl!F6:F;formula_pnl!A6:A<>""));0)', '=IFERROR(G6/$D$6;0)'],
        ["Продажи, шт", '=IFERROR(SUM(FILTER(formula_pnl!C6:C;formula_pnl!A6:A<>""));0)', "Прибыль", '=IFERROR(SUM(FILTER(formula_pnl!J6:J;formula_pnl!A6:A<>""));0)', "", "Эквайринг", '=IFERROR(SUM(FILTER(formula_pnl!G6:G;formula_pnl!A6:A<>""));0)', '=IFERROR(G7/$D$6;0)'],
        ["ДРР", '=IFERROR(SUM(FILTER(formula_pnl!H6:H;formula_pnl!A6:A<>""))/SUM(FILTER(formula_pnl!E6:E;formula_pnl!A6:A<>""));0)', "Маржа", '=IFERROR(SUM(FILTER(formula_pnl!J6:J;formula_pnl!A6:A<>""))/SUM(FILTER(formula_pnl!E6:E;formula_pnl!A6:A<>""));0)', "", "Реклама", '=IFERROR(SUM(FILTER(formula_pnl!H6:H;formula_pnl!A6:A<>""));0)', '=IFERROR(G8/$D$6;0)'],
        ["", "", "", "", "", "Себестоимость", '=IFERROR(SUM(FILTER(formula_pnl!I6:I;formula_pnl!A6:A<>""));0)', '=IFERROR(G9/$D$6;0)'],
        ["", "", "", "", "", "Итого расходы", '=SUM(G6:G9)', '=IFERROR(G10/$D$6;0)'],
        [],
        [],
        [],
        ["", "", "", "", "", "", "", "", "", "По дням"],
        ["", "", "", "", "", "", "", "", "", "Дата", "Продажи с СПП", "Продажи без СПП", "Комиссия", "% комиссии", "Эквайринг", "% эквайринга", "Реклама", "ДРР", "Себестоимость", "% себестоимости", "Прибыль", "Маржа"],
        ['','','','','','','','','','=QUERY({formula_pnl!A6:A\\formula_pnl!D6:D\\formula_pnl!E6:E\\formula_pnl!F6:F\\formula_pnl!G6:G\\formula_pnl!H6:H\\formula_pnl!I6:I\\formula_pnl!J6:J};"select Col1,sum(Col2),sum(Col3),sum(Col4),sum(Col4)/sum(Col3),sum(Col5),sum(Col5)/sum(Col3),sum(Col6),sum(Col6)/sum(Col3),sum(Col7),sum(Col7)/sum(Col3),sum(Col8),sum(Col8)/sum(Col3) where Col1 is not null group by Col1 order by Col1 label Col1 \'\', sum(Col2) \'\', sum(Col3) \'\', sum(Col4) \'\', sum(Col4)/sum(Col3) \'\', sum(Col5) \'\', sum(Col5)/sum(Col3) \'\', sum(Col6) \'\', sum(Col6)/sum(Col3) \'\', sum(Col7) \'\', sum(Col7)/sum(Col3) \'\', sum(Col8) \'\', sum(Col8)/sum(Col3) \'\'";0)'],
        [],
        [],
        [],
        [],
        [],
        [],
        ["Свод по артикулам"],
        [
            "Дата",
            "Артикул",
            "Продажи, шт",
            "Продажи с СПП",
            "Продажи без СПП",
            "Комиссия",
            "% комиссии",
            "Эквайринг",
            "% эквайринга",
            "Реклама",
            "ДРР",
            "Себестоимость",
            "% себестоимости",
            "Прибыль",
            "Маржа",
        ],
        ['=SORT(FILTER({formula_pnl!A6:A\\formula_pnl!B6:B\\formula_pnl!C6:C\\formula_pnl!D6:D\\formula_pnl!E6:E\\formula_pnl!F6:F\\IF(formula_pnl!E6:E=0;;formula_pnl!F6:F/formula_pnl!E6:E)\\formula_pnl!G6:G\\IF(formula_pnl!E6:E=0;;formula_pnl!G6:G/formula_pnl!E6:E)\\formula_pnl!H6:H\\formula_pnl!K6:K\\formula_pnl!I6:I\\IF(formula_pnl!E6:E=0;;formula_pnl!I6:I/formula_pnl!E6:E)\\formula_pnl!J6:J\\formula_pnl!L6:L};formula_pnl!A6:A<>"";(ABS(formula_pnl!D6:D)+ABS(formula_pnl!F6:F)+ABS(formula_pnl!G6:G)+ABS(formula_pnl!H6:H)+ABS(formula_pnl!I6:I)+ABS(formula_pnl!J6:J))>0);1;TRUE;5;FALSE)'],
    ]
    client.replace_sheet("dashboard_formula", rows)
    sheet_id = client.get_sheet_id("dashboard_formula")
    client.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"frozenRowCount": 5},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, 10, 1, 2),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, 10, 3, 4),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, 10, 6, 7),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 7, 8, 1, 4),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, 10, 7, 8),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 10, 13),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 13, 14),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 14, 15),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 15, 16),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 16, 17),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 17, 18),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 18, 19),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 19, 20),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 20, 21),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 14, MAX_ROWS, 21, 22),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 2, 3),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 3, 6),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 6, 7),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 7, 8),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 8, 9),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 9, 10),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 10, 11),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 11, 12),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 12, 13),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 13, 14),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 23, MAX_ROWS, 14, 15),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
            ]
        }
    )


def rebuild_article_finance(client: GoogleSheetsClient) -> None:
    client.recreate_sheet("wb_article_finance")
    rows = [
        ["Аналитика WB по артикулам"],
        [],
        ["Период с", DATE_FROM, "по", DATE_TO],
        [],
        [
            "Дата", "Артикул", "Продажи, шт", "Наша цена", "Комиссия WB", "% комиссии WB",
            "СПП", "% СПП", "Вознаграждение WB", "% вознаграждения WB", "НДС WB", "% НДС WB",
            "Возмещение ПВЗ", "% возмещения ПВЗ", "Эквайринг", "% эквайринга", "Логистика", "% логистики",
            "Хранение", "% хранения", "Приемка", "% приемки", "Штрафы", "% штрафов", "Удержания", "% удержаний",
            "Доплаты", "% доплат", "К перечислению", "% к перечислению", "Реклама", "% рекламы",
            "Себестоимость", "% себестоимости", "Чистая прибыль", "% чистой прибыли",
            "Расчетное к перечислению", "Контроль forPay", "Сырой % комиссии", "Контроль % комиссии",
        ],
        _article_finance_row_formulas(6),
    ]
    for row_number in range(7, MAX_ROWS + 1):
        rows.append(_article_finance_row_formulas(row_number))

    client.replace_sheet("wb_article_finance", rows)
    sheet_id = client.get_sheet_id("wb_article_finance")
    client.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 5}},
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 0, 1),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 2, 3),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 3, 5),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 5, 6),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 6, 39),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 7, 8),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 9, 10),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 11, 12),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 13, 14),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 15, 16),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 17, 18),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 19, 20),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 21, 22),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 23, 24),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 25, 26),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 27, 28),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 29, 30),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 31, 32),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 33, 34),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 35, 36),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 5, MAX_ROWS, 38, 40),
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                },
            ]
        }
    )


def main() -> None:
    settings = load_settings()
    client = GoogleSheetsClient(settings.google_service_account_file, settings.google_spreadsheet_id)
    rebuild_formula_pnl(client)
    rebuild_dashboard_formula(client)
    rebuild_article_finance(client)


if __name__ == "__main__":
    main()
