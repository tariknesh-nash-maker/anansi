# connectors/eu_ft.py
# EU: Tenders Electronic Daily (TED) Search API v3
# - Always sends a non-empty expert query: PD>={YYYYMMDD}
# - Uses a conservative, supported payload (no brittle fields)
# - Keeps results permissive (never zeroes out due to filters)

from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import os, json, html, logging
import requests

API_URL = "https://api.ted.europa.eu/v3/notices/search"
UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Content-Type": "application/json"})

def _yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else str(v).strip().lower() in ("1","true","yes")

def _build_query(days_back: int) -> str:
    # TED "expert query" — PD is an alias for publication-date
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days_back)
    return f"PD>={_yyyymmdd(datetime(cutoff.year, cutoff.month, cutoff.day))}"

def _normalize_item(row: Dict[str, Any]) -> Dict[str, Any] | None:
    # Field names can vary; grab common variants
    pub_no = (row.get("publication-number") or row.get("publicationNumber") or row.get("publication_number") or "").strip()
    if not pub_no:
        return None
    title = (row.get("notice-title") or row.get("title") or f"TED Notice {pub_no}").strip()
    url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"  # official pattern
    # We leave deadline None (varies by form type)
    # Minimal topic inference (soft): just enough for ogp_only gating if you use it
    text = f"{title}".lower()
    topic = None
    if any(k in text for k in ("digital", "data", "ict", "software", "information system")):
        topic = "Digital Governance"
    elif any(k in text for k in ("audit", "budget", "tax", "revenue", "pfm")):
        topic = "Fiscal Openness"
    elif any(k in text for k in ("open data", "transparency", "participation", "citizen", "integrity", "anti-corruption")):
        topic = "Open Government"

    return {
        "title": html.unescape(title),
        "source": "EU TED",
        "deadline": None,
        "country": "",
        "topic": topic,
        "url": url,
        "summary": title.lower(),
    }

def _eu_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = _env_bool("EUFT_DEBUG", False)
    # Always send a non-empty expert query
    query = _build_query(days_back)

    page = 1
    limit = min(_env_int("EUFT_MAX", 40), 250)  # TED per-page cap
    items: List[Dict[str, Any]] = []

    # 1 page is usually enough for “latest”; bump if you want more
    for _ in range(1):
        payload = {
            "query": query,                # REQUIRED (previously missing → 400)
            "page": page,
            "limit": limit,
            "paginationMode": "PAGE_NUMBER",
            "checkQuerySyntax": False,
            # We om
