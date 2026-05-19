from __future__ import annotations

import ssl
import time
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class GoogleSheetsClient:
    def __init__(self, service_account_file: str, spreadsheet_id: str) -> None:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_file(
            Path(service_account_file),
            scopes=scopes,
        )
        self._service = build("sheets", "v4", credentials=credentials)
        self._spreadsheet_id = spreadsheet_id

    @staticmethod
    def _execute_request(request: object, *, attempts: int = 5) -> dict:
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return request.execute(num_retries=3)
            except (ssl.SSLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt == attempts - 1:
                    raise
                time.sleep(min(2 * (attempt + 1), 10))
        if last_error is not None:
            raise last_error
        return {}

    def _get_spreadsheet_metadata(self) -> dict:
        request = self._service.spreadsheets().get(spreadsheetId=self._spreadsheet_id)
        return self._execute_request(request)

    def _get_sheet_titles(self) -> set[str]:
        metadata = self._get_spreadsheet_metadata()
        return {
            sheet["properties"]["title"]
            for sheet in metadata.get("sheets", [])
        }

    def get_sheet_id(self, sheet_name: str) -> int:
        metadata = self._get_spreadsheet_metadata()
        for sheet in metadata.get("sheets", []):
            properties = sheet.get("properties", {})
            if properties.get("title") == sheet_name:
                return properties["sheetId"]
        raise ValueError(f"Sheet {sheet_name} not found")

    def batch_update(self, body: dict) -> None:
        request = self._service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body=body,
        )
        self._execute_request(request)

    def ensure_sheet(self, sheet_name: str) -> None:
        if sheet_name in self._get_sheet_titles():
            return
        request = self._service.spreadsheets().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": sheet_name,
                            }
                        }
                    }
                ]
            },
        )
        self._execute_request(request)

    def recreate_sheet(self, sheet_name: str) -> None:
        requests = []
        if sheet_name in self._get_sheet_titles():
            requests.append({"deleteSheet": {"sheetId": self.get_sheet_id(sheet_name)}})
        requests.append({"addSheet": {"properties": {"title": sheet_name}}})
        self.batch_update({"requests": requests})

    def get_values(self, sheet_name: str) -> list[list[str]]:
        request = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._spreadsheet_id, range=sheet_name)
        )
        result = self._execute_request(request)
        return result.get("values", [])

    def replace_sheet(self, sheet_name: str, rows: Iterable[list[object]]) -> None:
        self.ensure_sheet(sheet_name)
        rows_list = [list(row) for row in rows]
        serialized_rows = [
            [self._serialize_cell(value) for value in row]
            for row in rows_list
        ]
        self._write_sheet(sheet_name, serialized_rows)

    def upsert_sheet(
        self,
        sheet_name: str,
        rows: Iterable[list[object]],
        *,
        key_columns: Sequence[str],
        update_existing: bool = False,
        allow_new_columns: bool = False,
    ) -> None:
        self.ensure_sheet(sheet_name)
        new_rows = [list(row) for row in rows]
        if not new_rows:
            return

        header = [str(value) for value in new_rows[0]]
        key_indexes = [header.index(column) for column in key_columns if column in header]
        if len(key_indexes) != len(key_columns):
            missing = [column for column in key_columns if column not in header]
            raise ValueError(f"Missing key columns in payload for {sheet_name}: {missing}")

        existing_rows = self.get_values(sheet_name)

        if existing_rows and (update_existing or allow_new_columns):
            raw_existing_header = [str(v).strip() for v in existing_rows[0]]
            existing_header: list[str] = []
            seen_existing: set[str] = set()
            for column in raw_existing_header:
                if not column or column in seen_existing:
                    continue
                seen_existing.add(column)
                existing_header.append(column)
            merged_header = list(existing_header)
            if allow_new_columns:
                for column in header:
                    if column not in merged_header:
                        merged_header.append(column)

            merged_key_indexes = [merged_header.index(column) for column in key_columns if column in merged_header]
            if len(merged_key_indexes) != len(key_columns):
                missing = [column for column in key_columns if column not in merged_header]
                raise ValueError(f"Missing key columns in merged header for {sheet_name}: {missing}")

            def remap_row(row: list[object], from_header: list[str], to_header: list[str]) -> list[object]:
                source: dict[str, object] = {}
                for i, column in enumerate(from_header):
                    name = str(column).strip()
                    if not name or name in source:
                        continue
                    source[name] = row[i] if i < len(row) else ""
                return [source.get(column, "") for column in to_header]

            merged_rows: list[list[object]] = []
            key_to_index: dict[tuple[str, ...], int] = {}

            for row in existing_rows[1:]:
                mapped = remap_row(list(row), raw_existing_header, merged_header)
                merged_rows.append(mapped)
                key = self._build_key(mapped, merged_key_indexes)
                if key is not None and key not in key_to_index:
                    key_to_index[key] = len(merged_rows) - 1

            for row in new_rows[1:]:
                mapped = remap_row(self._normalize_row(row, len(header)), header, merged_header)
                key = self._build_key(mapped, merged_key_indexes)
                if key is None:
                    merged_rows.append(mapped)
                    continue

                existing_index = key_to_index.get(key)
                if existing_index is None:
                    key_to_index[key] = len(merged_rows)
                    merged_rows.append(mapped)
                    continue

                if update_existing:
                    current = merged_rows[existing_index]
                    for idx, value in enumerate(mapped):
                        if value != "":
                            current[idx] = value

            serialized_rows = [[self._serialize_cell(v) for v in merged_header]] + [
                [self._serialize_cell(v) for v in row]
                for row in merged_rows
            ]
            self._write_sheet(sheet_name, serialized_rows)
            return

        # Build set of already-present keys
        existing_keys: set[tuple[str, ...]] = set()
        sheet_is_empty = not existing_rows
        if existing_rows:
            # Normalize existing header for comparison
            # Check if existing header has same key columns (ignoring extra columns that Google Sheets may add)
            existing_header_raw = existing_rows[0]
            existing_header = [str(v).strip() for v in existing_header_raw]
            
            # Check if key columns exist in existing header (don't require exact match)
            existing_key_indexes = []
            for column in key_columns:
                try:
                    existing_key_indexes.append(existing_header.index(column))
                except ValueError:
                    # Key column not found, skip reading existing keys
                    existing_key_indexes = []
                    break
            
            # If all key columns exist in existing sheet, read existing keys
            if existing_key_indexes and len(existing_key_indexes) == len(key_columns):
                for row in existing_rows[1:]:
                    normalized = self._normalize_row(row, len(existing_header))
                    key = self._build_key(normalized, existing_key_indexes)
                    if key is not None:
                        existing_keys.add(key)

        # Keep only rows whose key is not yet in the sheet
        rows_to_add: list[list[object]] = []
        for row in new_rows[1:]:
            normalized = self._normalize_row(row, len(header))
            key = self._build_key(normalized, key_indexes)
            if key is None or key not in existing_keys:
                rows_to_add.append(normalized)
                if key is not None:
                    existing_keys.add(key)

        if not rows_to_add and not sheet_is_empty:
            return  # nothing new to write

        serialized_rows = [
            [self._serialize_cell(value) for value in row]
            for row in rows_to_add
        ]

        if sheet_is_empty:
            # Write header + data
            serialized_header = [self._serialize_cell(v) for v in header]
            self._append_rows(sheet_name, [serialized_header] + serialized_rows)
        else:
            # Just append new rows (header already exists)
            self._append_rows(sheet_name, serialized_rows)

    @staticmethod
    def _col_letter(n: int) -> str:
        """Convert 1-based column number to spreadsheet letter (1→A, 26→Z, 27→AA)."""
        result = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def _append_rows(self, sheet_name: str, rows: list[list[object]], chunk_size: int = 500) -> None:
        """Append rows to a sheet without clearing anything."""
        if not rows:
            return
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            request = self._service.spreadsheets().values().append(
                spreadsheetId=self._spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": chunk},
            )
            self._execute_request(request)

    def _write_sheet(self, sheet_name: str, rows: list[list[object]], chunk_size: int = 500) -> None:
        # Clear a wide range so stale tail columns from previous writes do not remain in the sheet.
        clear_range = f"{sheet_name}!A:ZZZ"
        request = self._service.spreadsheets().values().clear(
            spreadsheetId=self._spreadsheet_id,
            range=clear_range,
        )
        self._execute_request(request)
        if not rows:
            return
        # Expand grid if needed (default sheet max is 1000/20000 rows)
        needed_rows = max(len(rows) + 1000, 10_000)
        sheet_id = self.get_sheet_id(sheet_name)
        self.batch_update({
            "requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"rowCount": needed_rows},
                    },
                    "fields": "gridProperties.rowCount",
                }
            }]
        })
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            row_number = start + 1
            request = self._service.spreadsheets().values().update(
                spreadsheetId=self._spreadsheet_id,
                range=f"{sheet_name}!A{row_number}",
                valueInputOption="USER_ENTERED",
                body={"values": chunk},
            )
            self._execute_request(request)

    @staticmethod
    def _normalize_row(row: list[object], width: int) -> list[object]:
        normalized = list(row)
        if len(normalized) < width:
            normalized.extend([""] * (width - len(normalized)))
        return normalized[:width]

    @staticmethod
    def _build_key(row: list[object], key_indexes: list[int]) -> tuple[str, ...] | None:
        values = [str(row[index]).strip() for index in key_indexes]
        if any(not value for value in values):
            return None
        return tuple(values)

    @staticmethod
    def _serialize_cell(value: object) -> object:
        if isinstance(value, Decimal):
            return float(value)
        return value
