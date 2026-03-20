"""
config.py — App configuration

Properties are managed entirely via the Settings UI and stored in SQLite.
Only scheduler settings live here.
"""

from __future__ import annotations
from dataclasses import dataclass
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


@dataclass
class SchedulerConfig:
    interval_minutes: int = 5
    lookback_days:    int = 2


# Scheduler settings — edit here if needed
scheduler = SchedulerConfig(interval_minutes=5, lookback_days=2)

EXELY_BASE_URL = "https://connect.hopenapi.com/api/exelypms/v1"
