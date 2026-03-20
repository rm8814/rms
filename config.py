"""
config.py — App configuration

Properties are managed entirely via the Settings UI and stored in SQLite.
Only scheduler settings live here.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

VALID_CONTRACT_TYPES = {
    "revshare_revenue",
    "revshare_gop",
    "revshare_revenue_gop",
    "lease",
    "advance_payment",
}


@dataclass
class PropertyConfig:
    """Mirrors the properties table. Used by ingestion code."""
    id:            str
    name:          str
    contract_type: str
    join_date:     str        # YYYY-MM-DD
    room_count:    int
    active:        bool
    city:          Optional[str]   = None
    api_key:       Optional[str]   = None
    revshare_pct:      Optional[float] = None   # revenue revshare %
    revshare_gop_pct:  Optional[float] = None   # GOP revshare % (revshare_revenue_gop only)
    lease_monthly:     Optional[int]   = None
    advance_total:     Optional[int]   = None
    contract_months:   Optional[int]   = None   # advance_payment: amortization period

    @classmethod
    def from_db_row(cls, row: dict) -> "PropertyConfig":
        return cls(**{k: row[k] for k in cls.__dataclass_fields__ if k in row})


@dataclass
class SchedulerConfig:
    interval_minutes: int = 5
    lookback_days:    int = 2


# Scheduler settings — edit here if needed
scheduler = SchedulerConfig(interval_minutes=5, lookback_days=2)

EXELY_BASE_URL = "https://connect.hopenapi.com/api/exelypms/v1"

# Timezone helpers
WIB_OFFSET = timedelta(hours=7)   # UTC+7 — Western Indonesian Time


def to_wib_str(utc_iso: str) -> str:
    """Convert a UTC ISO datetime string to WIB display string (YYYY-MM-DD HH:MM WIB)."""
    from datetime import datetime
    try:
        dt = datetime.fromisoformat(utc_iso[:19].replace("T", " ")) + WIB_OFFSET
        return dt.strftime("%Y-%m-%d %H:%M") + " WIB"
    except Exception:
        return utc_iso
