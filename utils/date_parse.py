from __future__ import annotations
from typing import Optional
import dateparser

def to_iso_date(s: str) -> Optional[str]:
    if not s:
        return None
    dt = dateparser.parse(
        s,
        settings={
            "DATE_ORDER": "DMY",
            "PREFER_DAY_OF_MONTH": "first",
            "PREFER_DATES_FROM": "future",  # deadlines are usually upcoming
        },
        languages=["en", "fr", "ar"],  # extend as needed
    )
    return dt.date().isoformat() if dt else None
