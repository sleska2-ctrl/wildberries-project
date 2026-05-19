from __future__ import annotations

from datetime import date, datetime

from wb_gsheets.config import load_settings
from wb_gsheets.google_sheets import GoogleSheetsClient


SHEET_NAME = "management_viz"


def _max_fact_date(values: list[list[str]]) -> str:
    dates: list[date] = []
    for row in values[1:]:
        if not row or not row[0]:
            continue
        current = datetime.strptime(row[0], "%Y-%m-%d").date()
        if current.month == 5 and current.year == 2026:
            dates.append(current)
    if not dates:
        return "2026-05-01"
    return min(max(dates), date(2026, 5, 31)).isoformat()


def _sheet_range(sheet_id: int, start_row: int, end_row: int, start_col: int, end_col: int) -> dict:
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row,
        "endRowIndex": end_row,
        "startColumnIndex": start_col,
        "endColumnIndex": end_col,
    }


def build_management_viz() -> None:
    settings = load_settings()
    client = GoogleSheetsClient(
        service_account_file=settings.google_service_account_file,
        spreadsheet_id=settings.google_spreadsheet_id,
    )

    daily_pnl_values = client.get_values(settings.daily_pnl_sheet)
    fact_to_date = _max_fact_date(daily_pnl_values)

    client.recreate_sheet(SHEET_NAME)
    sheet_id = client.get_sheet_id(SHEET_NAME)

    rows = [
        ["Управление кабинетом с мая"],
        [],
        ["Старт управления", "2026-05-01", "", "Факт по", fact_to_date],
        ["Плановый конец", "2026-05-31", "", "Дней прошло", '=DATEDIF(B3;E3;"D")+1'],
        ["Минимальный план", 17400000, "", "Оптимальный план", 30000000],
        [],
        ["KPI"],
        ["Продажи без СПП, руб", '=IFERROR(SUM(FILTER(sale_day_summary!C2:C;sale_day_summary!A2:A>=$B$3;sale_day_summary!A2:A<=$E$3));0)', "ДРР", '=IFERROR(SUM(FILTER(daily_pnl!O2:O;daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$E$3))/SUM(FILTER(sale_day_summary!C2:C;sale_day_summary!A2:A>=$B$3;sale_day_summary!A2:A<=$E$3));0)'],
        ["Продажи с СПП, руб", '=IFERROR(SUM(FILTER(sale_day_summary!B2:B;sale_day_summary!A2:A>=$B$3;sale_day_summary!A2:A<=$E$3));0)', "Реклама, руб", '=IFERROR(SUM(FILTER(daily_pnl!O2:O;daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$E$3));0)'],
        ["Маржинальность", '=IFERROR(SUM(FILTER(daily_pnl!Q2:Q;daily_pnl!A2:A>=$B$3;daily_pnl!A2:A<=$E$3))/SUM(FILTER(sale_day_summary!C2:C;sale_day_summary!A2:A>=$B$3;sale_day_summary!A2:A<=$E$3));0)', "Выполнение мин. плана", '=IFERROR(B8/$B$5;0)'],
        ["Выполнение опт. плана", '=IFERROR(B8/$E$5;0)', "RunRate, руб", '=IFERROR(B8/$E$4*31;0)'],
        ["RunRate / мин. план", '=IFERROR(D11/$B$5;0)', "RunRate / опт. план", '=IFERROR(D11/$E$5;0)'],
        ["Средние продажи в день", '=IFERROR(B8/$E$4;0)', "Реклама / день", '=IFERROR(D9/$E$4;0)'],
        ["Нужно в день для мин. плана", '=IFERROR($B$5/31;0)', "Нужно в день для опт. плана", '=IFERROR($E$5/31;0)'],
        [],
        ["Дневная база мая"],
        ["Дата", "Продажи без СПП, руб", "Продажи с СПП, руб", "Реклама, руб", "ДРР", "Маржа", "Накопит. продажи", "Мин. план накопит.", "Опт. план накопит."],
    ]

    for day in range(1, 32):
        current = f"=DATE(2026;5;{day})"
        row_number = len(rows) + 1
        rows.append(
            [
                current,
                f'=IF(A{row_number}>$E$3;"";IFERROR(SUM(FILTER(sale_day_summary!C:C;DATEVALUE(sale_day_summary!A:A)=A{row_number}));0))',
                f'=IF(A{row_number}>$E$3;"";IFERROR(SUM(FILTER(sale_day_summary!B:B;DATEVALUE(sale_day_summary!A:A)=A{row_number}));0))',
                f'=IF(A{row_number}>$E$3;"";IFERROR(SUM(FILTER(daily_pnl!O:O;DATEVALUE(daily_pnl!A:A)=A{row_number}));0))',
                f'=IF(OR(A{row_number}>$E$3;B{row_number}=0);"";D{row_number}/B{row_number})',
                f'=IF(OR(A{row_number}>$E$3;B{row_number}=0);"";IFERROR(SUM(FILTER(daily_pnl!Q:Q;DATEVALUE(daily_pnl!A:A)=A{row_number}))/B{row_number};0))',
                f'=SUM($B$18:B{row_number})',
                f'=($B$5/31)*(A{row_number}-DATE(2026;5;1)+1)',
                f'=($E$5/31)*(A{row_number}-DATE(2026;5;1)+1)',
            ]
        )

    client.replace_sheet(SHEET_NAME, rows)

    client.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "title": SHEET_NAME, "index": 0},
                        "fields": "title,index",
                    }
                },
                {"mergeCells": {"range": _sheet_range(sheet_id, 0, 1, 0, 5), "mergeType": "MERGE_ALL"}},
                {"mergeCells": {"range": _sheet_range(sheet_id, 6, 7, 0, 4), "mergeType": "MERGE_ALL"}},
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 0, 1, 0, 5),
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.11, "green": 0.21, "blue": 0.34}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "fontSize": 18, "bold": True}}},
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 2, 5, 0, 5),
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.95, "green": 0.97, "blue": 0.99}, "textFormat": {"bold": True}, "borders": {"top": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "bottom": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "left": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "right": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}}}},
                        "fields": "userEnteredFormat(backgroundColor,textFormat,borders)",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 6, 14, 0, 4),
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.99, "green": 0.99, "blue": 0.99}, "borders": {"top": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "bottom": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "left": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}, "right": {"style": "SOLID", "color": {"red": 0.88, "green": 0.88, "blue": 0.88}}}}},
                        "fields": "userEnteredFormat(backgroundColor,borders)",
                    }
                },
                {
                    "repeatCell": {
                        "range": _sheet_range(sheet_id, 15, 17, 0, 9),
                        "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.92, "green": 0.96, "blue": 0.94}, "textFormat": {"bold": True}}},
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                {"repeatCell": {"range": _sheet_range(sheet_id, 7, 10, 1, 2), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 7, 8, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 8, 9, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 9, 13, 1, 2), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 8, 10, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 10, 12, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 9, 10, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 11, 12, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 12, 13, 1, 2), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 12, 13, 3, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 17, 48, 1, 4), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 17, 48, 4, 6), "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 17, 48, 6, 9), "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "#,##0.00"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"repeatCell": {"range": _sheet_range(sheet_id, 17, 48, 0, 1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd.mm.yyyy"}}}, "fields": "userEnteredFormat.numberFormat"}},
                {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
                {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
                {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 6}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
                {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 9}, "properties": {"pixelSize": 135}, "fields": "pixelSize"}},
                {
                    "addChart": {
                        "chart": {
                            "spec": {
                                "title": "31 день мая: накопительные продажи против плана",
                                "basicChart": {
                                    "chartType": "LINE",
                                    "legendPosition": "BOTTOM_LEGEND",
                                    "axis": [{"position": "BOTTOM_AXIS", "title": "Дата"}, {"position": "LEFT_AXIS", "title": "Рубли"}],
                                    "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 17, "endRowIndex": 48, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                                    "series": [
                                        {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 17, "endRowIndex": 48, "startColumnIndex": 6, "endColumnIndex": 7}]}}, "targetAxis": "LEFT_AXIS"},
                                        {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 17, "endRowIndex": 48, "startColumnIndex": 7, "endColumnIndex": 8}]}}, "targetAxis": "LEFT_AXIS"},
                                        {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 17, "endRowIndex": 48, "startColumnIndex": 8, "endColumnIndex": 9}]}}, "targetAxis": "LEFT_AXIS"},
                                    ],
                                    "headerCount": 1,
                                },
                            },
                            "position": {"overlayPosition": {"anchorCell": {"sheetId": sheet_id, "rowIndex": 1, "columnIndex": 7}, "offsetXPixels": 0, "offsetYPixels": 0, "widthPixels": 720, "heightPixels": 320}},
                        }
                    }
                },
                {
                    "addChart": {
                        "chart": {
                            "spec": {
                                "title": "ДРР и маржинальность по дням",
                                "basicChart": {
                                    "chartType": "LINE",
                                    "legendPosition": "BOTTOM_LEGEND",
                                    "axis": [{"position": "BOTTOM_AXIS", "title": "Дата"}, {"position": "LEFT_AXIS", "title": "%"}],
                                    "domains": [{"domain": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 17, "endRowIndex": 48, "startColumnIndex": 0, "endColumnIndex": 1}]}}}],
                                    "series": [
                                        {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 17, "endRowIndex": 48, "startColumnIndex": 4, "endColumnIndex": 5}]}}, "targetAxis": "LEFT_AXIS"},
                                        {"series": {"sourceRange": {"sources": [{"sheetId": sheet_id, "startRowIndex": 17, "endRowIndex": 48, "startColumnIndex": 5, "endColumnIndex": 6}]}}, "targetAxis": "LEFT_AXIS"},
                                    ],
                                    "headerCount": 1,
                                },
                            },
                            "position": {"overlayPosition": {"anchorCell": {"sheetId": sheet_id, "rowIndex": 19, "columnIndex": 7}, "offsetXPixels": 0, "offsetYPixels": 0, "widthPixels": 720, "heightPixels": 320}},
                        }
                    }
                },
            ]
        }
    )


if __name__ == "__main__":
    build_management_viz()
