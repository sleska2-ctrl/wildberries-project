from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Iterable


def as_decimal(value: object) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).replace(",", "."))
    except InvalidOperation:
        return Decimal("0")


def to_iso_date(value: str) -> str:
    return value[:10]


def chunked(items: list[int], size: int) -> Iterable[list[int]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def date_windows(date_from: str, date_to: str, window_days: int) -> Iterable[tuple[str, str]]:
    current = datetime.strptime(date_from, "%Y-%m-%d").date()
    finish = datetime.strptime(date_to, "%Y-%m-%d").date()
    while current <= finish:
        window_end = min(current + timedelta(days=window_days - 1), finish)
        yield current.isoformat(), window_end.isoformat()
        current = window_end + timedelta(days=1)

