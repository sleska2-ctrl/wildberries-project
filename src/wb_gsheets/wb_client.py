from __future__ import annotations

import fcntl
import os
import time
from pathlib import Path
from typing import Any

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError, HTTPError, SSLError, Timeout

from .utils import chunked, date_windows


class _RateLimiter:
    def __init__(self, interval_seconds: float, *, shared_name: str | None = None) -> None:
        self.interval_seconds = interval_seconds
        self.shared_name = shared_name
        self._last_call = 0.0

    def wait(self) -> None:
        if self.shared_name:
            self._wait_shared()
            return
        now = time.monotonic()
        delay = self.interval_seconds - (now - self._last_call)
        if delay > 0:
            time.sleep(delay)
        self._last_call = time.monotonic()

    def _wait_shared(self) -> None:
        lock_dir = Path(os.getenv("WB_RATE_LIMIT_DIR", "/tmp"))
        lock_dir.mkdir(parents=True, exist_ok=True)
        safe_name = "".join(ch if ch.isalnum() else "_" for ch in self.shared_name or "default")
        lock_path = lock_dir / f"wb_api_rate_limit_{safe_name}.lock"
        with lock_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw_last_call = handle.read().strip()
                try:
                    last_call = float(raw_last_call) if raw_last_call else 0.0
                except ValueError:
                    last_call = 0.0
                now = time.time()
                delay = self.interval_seconds - (now - last_call)
                if delay > 0:
                    time.sleep(delay)
                    now = time.time()
                handle.seek(0)
                handle.truncate()
                handle.write(f"{now:.6f}")
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class WildberriesClient:
    FINANCE_BASE_URL = "https://finance-api.wildberries.ru"
    ADV_BASE_URL = "https://advert-api.wildberries.ru"
    STATS_BASE_URL = "https://statistics-api.wildberries.ru"
    ANALYTICS_BASE_URL = "https://seller-analytics-api.wildberries.ru"
    MINUTE_INTERVAL_SECONDS = 61.0
    FINANCE_SALES_INTERVAL_SECONDS = float(os.getenv("WB_FINANCE_SALES_INTERVAL_SECONDS", "70"))
    FINANCE_MAX_ATTEMPTS = int(os.getenv("WB_FINANCE_MAX_ATTEMPTS", "2"))
    ADV_FAST_INTERVAL_SECONDS = 0.21
    ADV_FULLSTATS_INTERVAL_SECONDS = 21.0
    FUNNEL_INTERVAL_SECONDS = 21.0

    def __init__(self, finance_token: str, adv_token: str, timeout: int = 21) -> None:
        self._finance_session = requests.Session()
        self._finance_session.headers.update({"Authorization": finance_token})
        self._adv_session = requests.Session()
        self._adv_session.headers.update({"Authorization": adv_token})
        self._stats_session = requests.Session()
        self._stats_session.headers.update({"Authorization": finance_token})
        self._timeout = timeout
        self._finance_sales_limiter = _RateLimiter(
            self.FINANCE_SALES_INTERVAL_SECONDS,
            shared_name="finance_sales_reports_detailed",
        )
        self._stats_orders_limiter = _RateLimiter(self.MINUTE_INTERVAL_SECONDS)
        self._stats_stocks_limiter = _RateLimiter(self.MINUTE_INTERVAL_SECONDS)
        self._adv_fast_limiter = _RateLimiter(self.ADV_FAST_INTERVAL_SECONDS)
        self._adv_fullstats_limiter = _RateLimiter(self.ADV_FULLSTATS_INTERVAL_SECONDS)
        self._funnel_limiter = _RateLimiter(self.FUNNEL_INTERVAL_SECONDS)

    def _retry_sleep_seconds(
        self,
        response: requests.Response,
        *,
        fallback_seconds: float,
        attempt: int,
    ) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return max(fallback_seconds, min(15 * (attempt + 1), 180))

    def _response_excerpt(self, response: requests.Response) -> str:
        text = (response.text or "").replace("\n", " ").strip()
        return text[:240]

    def _adv_get(
        self,
        url: str,
        *,
        params: dict[str, str],
        limiter: _RateLimiter | None = None,
    ) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(5):
            if limiter is not None:
                limiter.wait()
            try:
                response = self._adv_session.get(url, params=params, timeout=self._timeout)
            except (RequestsConnectionError, Timeout, SSLError) as exc:
                last_exc = exc
                sleep_seconds = min(15 * (attempt + 1), 60)
                time.sleep(sleep_seconds)
                continue
            if response.status_code not in {429, 500, 502, 503, 504}:
                response.raise_for_status()
                return response
            sleep_seconds = self._retry_sleep_seconds(
                response,
                fallback_seconds=limiter.interval_seconds if limiter is not None else 30,
                attempt=attempt,
            )
            time.sleep(sleep_seconds)
        if last_exc is not None:
            raise last_exc
        response.raise_for_status()
        return response

    def _finance_post(
        self,
        url: str,
        *,
        payload: dict[str, Any],
        limiter: _RateLimiter | None = None,
    ) -> requests.Response:
        last_exc: Exception | None = None
        attempts = max(1, self.FINANCE_MAX_ATTEMPTS)
        for attempt in range(attempts):
            if limiter is not None:
                limiter.wait()
            try:
                response = self._finance_session.post(url, json=payload, timeout=self._timeout)
            except (RequestsConnectionError, Timeout, SSLError) as exc:
                last_exc = exc
                sleep_seconds = min(15 * (attempt + 1), 60)
                time.sleep(sleep_seconds)
                continue
            if response.status_code not in {429, 500, 502, 503, 504}:
                if response.status_code != 204:
                    response.raise_for_status()
                return response
            sleep_seconds = self._retry_sleep_seconds(
                response,
                fallback_seconds=limiter.interval_seconds if limiter is not None else 60,
                attempt=attempt,
            )
            print(
                f"[WARN] WB finance HTTP {response.status_code}; "
                f"пауза {sleep_seconds:.0f}с, попытка {attempt + 1}/{attempts}; "
                f"ответ: {self._response_excerpt(response) or '-'}",
                flush=True,
            )
            if attempt + 1 >= attempts:
                break
            time.sleep(sleep_seconds)
        if last_exc is not None:
            raise last_exc
        response.raise_for_status()
        return response

    def _stats_get(
        self,
        url: str,
        *,
        params: dict[str, str],
        limiter: _RateLimiter | None = None,
    ) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(5):
            if limiter is not None:
                limiter.wait()
            try:
                response = self._stats_session.get(url, params=params, timeout=self._timeout)
            except (RequestsConnectionError, Timeout, SSLError) as exc:
                last_exc = exc
                sleep_seconds = min(15 * (attempt + 1), 60)
                time.sleep(sleep_seconds)
                continue
            if response.status_code not in {429, 500, 502, 503, 504}:
                response.raise_for_status()
                return response
            sleep_seconds = self._retry_sleep_seconds(
                response,
                fallback_seconds=limiter.interval_seconds if limiter is not None else 60,
                attempt=attempt,
            )
            time.sleep(sleep_seconds)
        if last_exc is not None:
            raise last_exc
        response.raise_for_status()
        return response

    def _stats_post(
        self,
        url: str,
        *,
        payload: dict[str, Any],
        limiter: _RateLimiter | None = None,
    ) -> requests.Response:
        last_exc: Exception | None = None
        for attempt in range(5):
            if limiter is not None:
                limiter.wait()
            try:
                response = self._stats_session.post(url, json=payload, timeout=self._timeout)
            except (RequestsConnectionError, Timeout, SSLError) as exc:
                last_exc = exc
                sleep_seconds = min(15 * (attempt + 1), 60)
                time.sleep(sleep_seconds)
                continue
            if response.status_code not in {429, 500, 502, 503, 504}:
                response.raise_for_status()
                return response
            sleep_seconds = self._retry_sleep_seconds(
                response,
                fallback_seconds=limiter.interval_seconds if limiter is not None else 60,
                attempt=attempt,
            )
            time.sleep(sleep_seconds)
        if last_exc is not None:
            raise last_exc
        response.raise_for_status()
        return response

    def fetch_sales_details(self, date_from: str, date_to: str, period: str = "daily") -> list[dict[str, Any]]:
        url = f"{self.FINANCE_BASE_URL}/api/finance/v1/sales-reports/detailed"
        all_rows: list[dict[str, Any]] = []
        rrd_id = 0
        while True:
            payload = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "limit": 100000,
                "rrdId": rrd_id,
                "period": period,
            }
            response = self._finance_post(url, payload=payload, limiter=self._finance_sales_limiter)
            if response.status_code == 204:
                return all_rows
            rows = response.json()
            if not rows:
                return all_rows
            all_rows.extend(rows)
            rrd_id = rows[-1]["rrdId"]
            if len(rows) < payload["limit"]:
                return all_rows

    def fetch_orders(self, date_from: str) -> list[dict[str, Any]]:
        """Fetch supplier orders stream (near real-time) from statistics API."""
        url = f"{self.STATS_BASE_URL}/api/v1/supplier/orders"
        response = self._stats_get(url, params={"dateFrom": date_from}, limiter=self._stats_orders_limiter)
        payload = response.json()
        if isinstance(payload, list):
            return payload
        return []

    def fetch_stocks(self, date_from: str = "2019-06-20") -> list[dict[str, Any]]:
        """Fetch current WB warehouse stock balances from statistics API."""
        url = f"{self.STATS_BASE_URL}/api/v1/supplier/stocks"
        all_rows: list[dict[str, Any]] = []
        current_date_from = date_from
        seen_cursors: set[str] = set()

        while True:
            response = self._stats_get(url, params={"dateFrom": current_date_from}, limiter=self._stats_stocks_limiter)
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                return all_rows

            all_rows.extend(payload)
            if len(payload) < 60000:
                return all_rows

            next_cursor = str(payload[-1].get("lastChangeDate") or "").strip()
            if not next_cursor or next_cursor in seen_cursors:
                return all_rows
            seen_cursors.add(next_cursor)
            current_date_from = next_cursor

    def fetch_cards(self) -> list[dict[str, Any]]:
        """
        Fetch all seller product cards from WB Content API.

        Returns list of dicts with fields:
          nmID, vendorCode, title, brand, subject_id, subject_name,
          description, photos_count

        Uses cursor-based pagination (limit 100 per page).
        Requires WB API token with content access (same token as statistics).
        """
        url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        session = self._stats_session  # same token works for content API
        all_cards: list[dict[str, Any]] = []
        cursor: dict[str, Any] = {}

        while True:
            body: dict[str, Any] = {
                "settings": {
                    "cursor": {**cursor, "limit": 100},
                    "filter": {"withPhoto": -1},
                }
            }
            try:
                resp = session.post(url, json=body, timeout=self._timeout)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                print(f"[WARN] fetch_cards error: {exc}")
                break

            cards = data.get("cards", [])
            for c in cards:
                subj = c.get("subjectName") or ""
                subj_id = c.get("subjectID") or 0
                all_cards.append({
                    "nmID":         c.get("nmID"),
                    "vendorCode":   c.get("vendorCode", ""),
                    "title":        c.get("title", ""),
                    "brand":        c.get("brand", ""),
                    "subject_id":   subj_id,
                    "subject_name": subj,
                    "description":  (c.get("description") or "")[:500],
                    "photos_count": len(c.get("photos") or []),
                })

            cur = data.get("cursor", {})
            total = cur.get("total", 0)
            updated_at = cur.get("updatedAt", "")
            nm_id_cursor = cur.get("nmID", 0)

            if len(cards) < 100 or not nm_id_cursor:
                break
            cursor = {"updatedAt": updated_at, "nmID": nm_id_cursor}

        return all_cards

    def fetch_funnel_history(self, date_from: str, date_to: str, nm_ids: list[int] | None = None) -> list[dict[str, Any]]:
        """Fetch sales funnel metrics (shows, clicks, cart, orders, buyouts)."""
        if nm_ids is None:
            nm_ids = []

        normalized_nm_ids = sorted(
            {
                int(value)
                for value in nm_ids
                if str(value).isdigit() and int(value) > 0
            }
        )
        if not normalized_nm_ids:
            return []

        url = f"{self.ANALYTICS_BASE_URL}/api/analytics/v3/sales-funnel/products/history"
        all_rows: list[dict[str, Any]] = []
        unavailable_days: set[str] = set()
        def _request(period_start: str, period_end: str, ids: list[int]) -> list[dict[str, Any]]:
            payload = {
                "selectedPeriod": {"start": period_start, "end": period_end},
                "nmIds": ids,
                "skipDeletedNm": False,
                "aggregationLevel": "day",
            }
            response = self._stats_post(url, payload=payload, limiter=self._funnel_limiter)
            data = response.json()
            if isinstance(data, list):
                return data
            return []

        def _http_status(exc: HTTPError) -> int | None:
            return exc.response.status_code if exc.response is not None else None

        def _http_text(exc: HTTPError) -> str:
            if exc.response is None:
                return ""
            return exc.response.text.lower()

        def _is_unavailable_period(exc: HTTPError) -> bool:
            if _http_status(exc) != 400:
                return False
            text = _http_text(exc)
            return "invalid start day" in text or "excess limit on days" in text

        # Short windows reduce response size and help survive API throttling.
        for window_start, window_end in date_windows(date_from, date_to, 7):
            for nm_chunk in chunked(normalized_nm_ids, 20):
                retry_single_ids = False
                try:
                    all_rows.extend(_request(window_start, window_end, nm_chunk))
                    continue
                except (RequestsConnectionError, Timeout, SSLError):
                    continue
                except HTTPError as exc:
                    status = _http_status(exc)
                    if _is_unavailable_period(exc):
                        for day_start, day_end in date_windows(window_start, window_end, 1):
                            if day_start in unavailable_days:
                                continue
                            try:
                                all_rows.extend(_request(day_start, day_end, nm_chunk))
                            except HTTPError as day_exc:
                                day_status = _http_status(day_exc)
                                if _is_unavailable_period(day_exc):
                                    unavailable_days.add(day_start)
                                    continue
                                if day_status in {429, 500, 502, 503, 504}:
                                    break
                                if day_status == 400:
                                    retry_single_ids = True
                                    window_start = day_start
                                    window_end = day_end
                                    break
                                raise
                            except (RequestsConnectionError, Timeout, SSLError):
                                continue
                        continue
                    if status == 400:
                        retry_single_ids = True
                    elif status in {429, 500, 502, 503, 504}:
                        continue
                    else:
                        raise

                if not retry_single_ids:
                    continue

                for nm_id in nm_chunk:
                    try:
                        all_rows.extend(_request(window_start, window_end, [nm_id]))
                    except HTTPError as exc2:
                        status2 = _http_status(exc2)
                        if _is_unavailable_period(exc2):
                            unavailable_days.add(window_start)
                            break
                        # Skip only failing nmIds instead of aborting full funnel sync.
                        if status2 in {400, 404, 409}:
                            continue
                        if status2 in {429, 500, 502, 503, 504}:
                            break
                        raise
                    except (RequestsConnectionError, Timeout, SSLError):
                        continue
        return all_rows

    def fetch_campaign_ids(self) -> list[int]:
        url = f"{self.ADV_BASE_URL}/adv/v1/promotion/count"
        response = self._adv_get(url, params={}, limiter=self._adv_fast_limiter)
        data = response.json()
        adverts = data.get("adverts", [])
        result: list[int] = []
        for advert_group in adverts:
            if advert_group.get("status") not in {7, 9, 11}:
                continue
            for advert in advert_group.get("advert_list", []):
                advert_id = advert.get("advertId")
                if isinstance(advert_id, int):
                    result.append(advert_id)
        return sorted(set(result))

    def fetch_campaign_details(self, advert_ids: list[int]) -> list[dict[str, Any]]:
        if not advert_ids:
            return []

        url = f"{self.ADV_BASE_URL}/api/advert/v2/adverts"
        all_rows: list[dict[str, Any]] = []
        for chunk in chunked(advert_ids, 50):
            response = self._adv_get(
                url,
                params={"ids": ",".join(str(value) for value in chunk)},
                limiter=self._adv_fast_limiter,
            )
            payload = response.json()
            all_rows.extend(payload.get("adverts", []))
        return all_rows

    def fetch_relevant_campaign_ids(self, nm_ids: list[int]) -> list[int]:
        if not nm_ids:
            return []

        target_nm_ids = {int(value) for value in nm_ids}
        campaign_ids = self.fetch_campaign_ids()
        relevant_ids: list[int] = []
        for advert in self.fetch_campaign_details(campaign_ids):
            advert_id = advert.get("id")
            nm_settings = advert.get("nm_settings") or []
            advert_nm_ids = {
                int(item.get("nm_id"))
                for item in nm_settings
                if item.get("nm_id") is not None
            }
            if advert_id and advert_nm_ids.intersection(target_nm_ids):
                relevant_ids.append(int(advert_id))
        return sorted(set(relevant_ids))

    def fetch_campaign_stats(self, advert_ids: list[int], date_from: str, date_to: str) -> list[dict[str, Any]]:
        if not advert_ids:
            return []

        url = f"{self.ADV_BASE_URL}/adv/v3/fullstats"
        all_rows: list[dict[str, Any]] = []
        for chunk in chunked(advert_ids, 50):
            for window_start, window_end in date_windows(date_from, date_to, 31):
                response = self._adv_get(
                    url,
                    params={
                        "ids": ",".join(str(value) for value in chunk),
                        "beginDate": window_start,
                        "endDate": window_end,
                    },
                    limiter=self._adv_fullstats_limiter,
                )
                payload = response.json()
                if isinstance(payload, list):
                    all_rows.extend(payload)
        return all_rows

    def start_campaign(self, advert_id: int) -> requests.Response:
        """Launch a WB promotion campaign."""
        url = f"{self.ADV_BASE_URL}/adv/v0/start"
        return self._adv_get(url, params={"id": str(advert_id)}, limiter=self._adv_fast_limiter)

    def pause_campaign(self, advert_id: int) -> requests.Response:
        """Pause a WB promotion campaign."""
        url = f"{self.ADV_BASE_URL}/adv/v0/pause"
        return self._adv_get(url, params={"id": str(advert_id)}, limiter=self._adv_fast_limiter)
