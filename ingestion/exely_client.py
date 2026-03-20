"""
exely_client.py — Thin wrapper around Exely PMS Universal API

One client instance per property (each has its own api_key).
All methods return parsed JSON or raise on error.
"""

import time
import logging
from datetime import date, datetime, timedelta
from typing import Optional
import requests

log = logging.getLogger(__name__)

BASE_URL = "https://connect.hopenapi.com/api/exelypms/v1"
MAX_DATE_RANGE_DAYS = 31   # API hard limit for analytics endpoints
REQUEST_TIMEOUT = 30


class ExelyAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


class ExelyClient:
    def __init__(self, api_key: str, property_id: str, base_url: str = BASE_URL):
        self.property_id = property_id
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
        })
        self.base_url = base_url.rstrip("/")

    def _get(self, path: str, params: dict) -> dict:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 401:
            raise ExelyAPIError(401, f"Invalid API key for property {self.property_id}")
        if resp.status_code == 400:
            raise ExelyAPIError(400, resp.text)
        if resp.status_code == 500:
            raise ExelyAPIError(500, "Exely internal server error")
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: dict) -> dict:
        url = f"{self.base_url}{path}"
        resp = self.session.post(url, json=body, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 401:
            raise ExelyAPIError(401, f"Invalid API key for property {self.property_id}")
        if resp.status_code == 400:
            raise ExelyAPIError(400, resp.text)
        if resp.status_code == 500:
            raise ExelyAPIError(500, "Exely internal server error")
        resp.raise_for_status()
        return resp.json()

    # ── Analytics: Accruals ───────────────────────────────────────────────────

    def get_services(
        self,
        start_date: date,
        end_date: date,
        date_kind: int = 0,    # default: by departure date (matches Exely YieldAndLoad)
        language: str = "en",
    ) -> dict:
        """
        Fetch accruals. date_kind:
          0 = by departure date
          1 = by arrival date
          2 = by creation date
          3 = by modification date  ← best for incremental pulls
          4 = by departure (no split)
        Max range: 31 days.
        """
        self._validate_date_range(start_date, end_date)
        return self._get("/analytics/services", {
            "startDate": start_date.strftime("%Y%m%d"),
            "endDate": end_date.strftime("%Y%m%d"),
            "dateKind": date_kind,
            "language": language,
        })

    def get_payments(
        self,
        start_dt: datetime,
        end_dt: datetime,
        include_services: bool = False,
        language: str = "en",
    ) -> dict:
        """Fetch payments. Max range: 31 days (7 days if includeServices=True). Cannot be future dates."""
        max_days = 7 if include_services else MAX_DATE_RANGE_DAYS
        delta = (end_dt.date() - start_dt.date()).days
        if delta < 0:
            raise ValueError("start_dt must be before end_dt")
        if delta > max_days:
            raise ValueError(
                f"Date range {delta} days exceeds API limit of {max_days} days"
                + (" when includeServices=True" if include_services else "")
            )
        if end_dt > datetime.utcnow():
            raise ValueError("get_payments: endDateTime cannot be a future date (API restriction)")
        return self._get("/analytics/payments", {
            "startDateTime": start_dt.strftime("%Y%m%d%H%M"),
            "endDateTime": end_dt.strftime("%Y%m%d%H%M"),
            "includeServices": "true" if include_services else "false",
            "language": language,
        })

    # ── Bookings ──────────────────────────────────────────────────────────────

    def search_bookings(
        self,
        modified_from: Optional[datetime] = None,
        modified_to: Optional[datetime] = None,
        affects_from: Optional[datetime] = None,
        affects_to: Optional[datetime] = None,
        state: str = "Active",
    ) -> list[str]:
        """
        Returns list of booking numbers matching criteria.
        At least one date range must be provided.
        state: 'Active' | 'Cancelled'
        """
        params = {"state": state}
        if modified_from and modified_to:
            if (modified_to - modified_from).days > 365:
                raise ValueError("modifiedFrom/To range cannot exceed 365 days (API requirement)")
            params["modifiedFrom"] = modified_from.strftime("%Y-%m-%dT%H:%M")
            params["modifiedTo"] = modified_to.strftime("%Y-%m-%dT%H:%M")
        elif modified_from or modified_to:
            raise ValueError("Both modifiedFrom and modifiedTo must be provided together (API requirement)")
        if affects_from and affects_to:
            if (affects_to - affects_from).days > 365:
                raise ValueError("affectsPeriodFrom/To range cannot exceed 365 days (API requirement)")
            params["affectsPeriodFrom"] = affects_from.strftime("%Y-%m-%dT%H:%M")
            params["affectsPeriodTo"] = affects_to.strftime("%Y-%m-%dT%H:%M")
        elif affects_from or affects_to:
            raise ValueError("Both affectsPeriodFrom and affectsPeriodTo must be provided together (API requirement)")
        if "modifiedFrom" not in params and "affectsPeriodFrom" not in params:
            raise ValueError("At least one date period (modifiedFrom/To or affectsPeriodFrom/To) is required (API requirement)")
        result = self._get("/bookings", params)
        return result.get("bookingNumbers", [])

    def get_booking(self, booking_number: str, language: str = "en") -> dict:
        """Fetch full booking detail."""
        return self._get(f"/bookings/{booking_number}", {"language": language})

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _validate_date_range(self, start: date, end: date):
        delta = (end - start).days
        if delta < 0:
            raise ValueError("start_date must be before end_date")
        if delta > MAX_DATE_RANGE_DAYS:
            raise ValueError(f"Date range {delta} days exceeds API limit of {MAX_DATE_RANGE_DAYS} days")

    def fetch_services_chunked(
        self,
        start_date: date,
        end_date: date,
        date_kind: int = 0,
    ) -> list[dict]:
        """
        Auto-chunks requests if range > 31 days.
        Returns merged list of raw service records.
        """
        all_data = {"services": [], "reservations": [], "customers": [], "agents": [], "roomTypes": []}
        cursor = start_date
        while cursor <= end_date:
            chunk_end = min(cursor + timedelta(days=MAX_DATE_RANGE_DAYS - 1), end_date)
            log.info(f"[{self.property_id}] Fetching services {cursor} → {chunk_end}")
            result = self.get_services(cursor, chunk_end, date_kind)
            data = result.get("data") or {}
            for key in all_data:
                all_data[key].extend(data.get(key, []))
            cursor = chunk_end + timedelta(days=1)
            if cursor <= end_date:
                time.sleep(0.5)   # be polite to the API
        return all_data
