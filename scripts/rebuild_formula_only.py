from __future__ import annotations

from wb_gsheets.config import load_settings
from wb_gsheets.google_sheets import GoogleSheetsClient
from wb_gsheets.main import build_parser
from wb_gsheets.transform import (
    ads_to_sheet_rows,
    extract_filter_values,
    filter_sales_rows,
    flatten_ads_rows,
    build_nm_mapping,
    sales_to_sheet_rows,
)
from wb_gsheets.wb_client import WildberriesClient


DATE_FROM = "2026-05-01"
DATE_TO = "2026-05-31"
FORMULA_SHEET = "formula_pnl"
DASHBOARD_FORMULA_SHEET = "dashboard_formula"


def _sheet_range(sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int) -> dict:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


def _cleanup_old_sheets(client: GoogleSheetsClient, keep: set[str]) -> None:
    metadata = client._get_spreadsheet_metadata()  # noqa: SLF001
    requests: list[dict] = []
    for sheet in metadata.get("sheets", []):
        props = sheet.get("properties", {})
        title = props.get("title", "")
        if title and title not in keep:
            requests.append({"deleteSheet": {"sheetId": props["sheetId"]}})
    if requests:
        client.batch_update({"requests": requests})


def build_formula_sheet(client: GoogleSheetsClient, settings) -> None:
    client.recreate_sheet(FORMULA_SHEET)
    rows = [
        ["Формульный PnL"],
        [],
        ["Период с", DATE_FROM, "по", DATE_TO],
        [],
        ["Дата", "Артикул", "Продажи, шт", "Продажи с СПП", "Продажи без СПП", "Комиссия", "Эквайринг", "Реклама", "Себестоимость", "Прибыль", "ДРР", "Маржа"],
        [
            '=QUERY({ARRAYFORMULA(LEFT(raw_sales!Y2:Y;10))\\raw_sales!AA2:AA\\IF(raw_sales!I2:I="Возврат";-raw_sales!Q2:Q;IF(raw_sales!I2:I="Продажа";raw_sales!Q2:Q;0))\\IF(raw_sales!I2:I="Возврат";-raw_sales!T2:T;IF(raw_sales!I2:I="Продажа";raw_sales!T2:T;0))\\IF(raw_sales!I2:I="Возврат";-raw_sales!V2:V;IF(raw_sales!I2:I="Продажа";raw_sales!V2:V;0))\\IF(raw_sales!I2:I="Возврат";-raw_sales!P2:P;IF(raw_sales!I2:I="Продажа";raw_sales!P2:P;0))\\IF(raw_sales!I2:I="Возврат";-raw_sales!A2:A;IF(raw_sales!I2:I="Продажа";raw_sales!A2:A;0))};"select Col1, Col2, sum(Col3), sum(Col4), sum(Col5), sum(Col6), sum(Col7) where Col1 is not null and Col1 >= ''"&TEXT($B$3;"yyyy-mm-dd")&"'' and Col1 <= ''"&TEXT($D$3;"yyyy-mm-dd")&"'' group by Col1, Col2 label Col1 ''Дата'', Col2 ''Артикул'', sum(Col3) ''Продажи, шт'', sum(Col4) ''Продажи с СПП'', sum(Col5) ''Продажи без СПП'', sum(Col6) ''Комиссия'', sum(Col7) ''Эквайринг''";0)',
            "",
            "",
            "",
            "",
            "",
            "",
            '=ARRAYFORMULA(IF(A6:A="";;MAP(A6:A;B6:B;LAMBDA(d;a;IFERROR(SUM(FILTER(raw_ads!G:G;raw_ads!A:A=TEXT(d;"yyyy-mm-dd");raw_ads!D:D=INDEX(FILTER(SKU!G:G;SKU!D:D=a);1)));0)))))',
            '=ARRAYFORMULA(IF(A6:A="";;MAP(B6:B;C6:C;LAMBDA(a;q;IFERROR(INDEX(FILTER(SKU!K:K;SKU!D:D=a);1)*q;0)))))',
            '=ARRAYFORMULA(IF(A6:A="";;D6:D-F6:F-G6:G-H6:H-I6:I))',
            '=ARRAYFORMULA(IF(A6:A="";;IF(E6:E=0;;H6:H/E6:E)))',
            '=ARRAYFORMULA(IF(A6:A="";;IF(E6:E=0;;J6:J/E6:E)))',
        ],
    ]
    client.replace_sheet(FORMULA_SHEET, rows)
    sheet_id = client.get_sheet_id(FORMULA_SHEET)
    client.batch_update(
        {
            "requests": [
                {"mergeCells": {"range": _sheet_range(sheet_id, 0, 1, 0, 8), "mergeType": "MERGE_ALL"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 500, 0, 1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 500, 3, 10), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 500, 10, 12), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
            ]
        }
    )


def build_formula_dashboard(client: GoogleSheetsClient) -> None:
    client.recreate_sheet(DASHBOARD_FORMULA_SHEET)
    rows = [
        ["Итоговый дашборд (только формулы)"],
        [],
        ["Период с", '=formula_pnl!B3', "по", '=formula_pnl!D3'],
        [],
        ["KPI"],
        ["Продажи с СПП", '=IFERROR(SUM(formula_pnl!D6:D);0)', "Продажи без СПП", '=IFERROR(SUM(formula_pnl!E6:E);0)'],
        ["Комиссия", '=IFERROR(SUM(formula_pnl!F6:F);0)', "Эквайринг", '=IFERROR(SUM(formula_pnl!G6:G);0)'],
        ["Реклама", '=IFERROR(SUM(formula_pnl!H6:H);0)', "Себестоимость", '=IFERROR(SUM(formula_pnl!I6:I);0)'],
        ["Прибыль", '=IFERROR(SUM(formula_pnl!J6:J);0)', "Продажи, шт", '=IFERROR(SUM(formula_pnl!C6:C);0)'],
        ["ДРР", '=IFERROR(SUM(formula_pnl!H6:H)/SUM(formula_pnl!E6:E);0)', "Маржа", '=IFERROR(SUM(formula_pnl!J6:J)/SUM(formula_pnl!E6:E);0)'],
        [],
        ["Топ артикулов по прибыли"],
        ["Артикул", "Продажи, шт", "Продажи с СПП", "Продажи без СПП", "Реклама", "Прибыль", "ДРР", "Маржа"],
        ['=SORT({formula_pnl!B6:B\\formula_pnl!C6:C\\formula_pnl!D6:D\\formula_pnl!E6:E\\formula_pnl!H6:H\\formula_pnl!J6:J\\formula_pnl!K6:K\\formula_pnl!L6:L};6;FALSE)'],
    ]
    client.replace_sheet(DASHBOARD_FORMULA_SHEET, rows)
    sheet_id = client.get_sheet_id(DASHBOARD_FORMULA_SHEET)
    client.batch_update(
        {
            "requests": [
                {"mergeCells": {"range": _sheet_range(sheet_id, 0, 1, 0, 6), "mergeType": "MERGE_ALL"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 10, 1, 2), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 5, 9, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 9, 10, 1, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 13, 500, 2, 6), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 13, 500, 6, 8), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
            ]
        }
    )


def main() -> None:
    _ = build_parser().parse_args()
    settings = load_settings()
    wb_client = WildberriesClient(
        finance_token=settings.wb_finance_token,
        adv_token=settings.wb_adv_token,
    )
    sheets = GoogleSheetsClient(settings.google_service_account_file, settings.google_spreadsheet_id)

    cogs_values = sheets.get_values(settings.cogs_sheet)
    nm_mapping = build_nm_mapping(cogs_values, article_filter_type=settings.article_filter_type)
    filter_values = settings.article_filter_values or extract_filter_values(
        cogs_values,
        article_filter_type=settings.article_filter_type,
    )

    sales_rows = wb_client.fetch_sales_details(date_from=DATE_FROM, date_to=DATE_TO, period="daily")
    filtered_sales_rows = filter_sales_rows(
        sales_rows,
        article_filter_type=settings.article_filter_type,
        article_filter_values=filter_values,
        nm_id_filter_values=set(nm_mapping.keys()),
    )

    nm_ids = []
    nm_idx = 0
    header = cogs_values[0] if cogs_values else []
    if "Артикул WB" in header:
        nm_idx = header.index("Артикул WB")
    for row in cogs_values[1:]:
        if len(row) > nm_idx and row[nm_idx].strip().isdigit():
            nm_ids.append(int(row[nm_idx].strip()))
    advert_ids = wb_client.fetch_relevant_campaign_ids(nm_ids)
    ads_rows = flatten_ads_rows(
        wb_client.fetch_campaign_stats(advert_ids=advert_ids, date_from=DATE_FROM, date_to=DATE_TO)
    )

    sheets.recreate_sheet(settings.raw_sales_sheet)
    sheets.recreate_sheet(settings.raw_ads_sheet)
    sheets.replace_sheet(settings.raw_sales_sheet, sales_to_sheet_rows(filtered_sales_rows))
    sheets.replace_sheet(settings.raw_ads_sheet, ads_to_sheet_rows(ads_rows))

    keep = {settings.cogs_sheet, settings.raw_sales_sheet, settings.raw_ads_sheet, FORMULA_SHEET, DASHBOARD_FORMULA_SHEET}
    _cleanup_old_sheets(sheets, keep)
    build_formula_sheet(sheets, settings)
    build_formula_dashboard(sheets)


if __name__ == "__main__":
    main()
