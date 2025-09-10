# anansi/connectors/eu_ft.py
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
import requests
import dateparser
import html

API = "https://api.ted.europa.eu/v3/notices/search"
UA  = "Mozilla/5.0 (compatible; anansi/1.0)"
HEADERS = {"User-Agent": UA, "Content-Type": "application/json"}

def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def _to_iso(s: str | None) -> str | None:
    if not s:
        return None
    # TED may return '2025-02-13Z' or '2025-02-13+01:00' → dateparser handles both
    d = dateparser.parse(s, settings={"DATE_ORDER":"DMY","PREFER_DAY_OF_MONTH":"first"})
    return d.date().isoformat() if d else None

def _euft_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    since = datetime.utcnow().date() - timedelta(days=days_back)
    since_yyyymmdd = _yyyymmdd(datetime(since.year, since.month, since.day))

    # We request a small, safe field set and rebuild the human URL from publication-number:
    fields = [
        "publication-number",      # e.g. 76817-2024
        "notice-title",            # human title
        "publication-date",        # for freshness
        "deadline-received-tenders",  # submission deadline if present
        "buyer-country",           # country code
        # (we keep it lean; more fields can be added if needed)
    ]

    page = 1
    limit = 50  # max 250; 50 is gentle

    items: List[Dict[str, Any]] = []
    while True:
        body = {
            # Expert query: all ACTIVE notices published since our window
            # Field alias PD = publication-date (official TED help docs)
            "query": f"(publication-date >= {since_yyyymmdd})",
            "fields": fields,
            "page": page,
            "limit": limit,
            "scope": "ACTIVE",
            "checkQuerySyntax": False,
            "paginationMode": "PAGE_NUMBER",
        }
        r = requests.post(API, json=body, headers=HEADERS, timeout=45)
        r.raise_for_status()
        data = r.json() or {}

        results = data.get("results") or data.get("items") or []
        if not results:
            break

        for row in results:
            pub_no = (row.get("publication-number") or "").strip()
            if not pub_no:
                continue
            url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"

            title = html.unescape((row.get("notice-title") or "").strip()) or "EU opportunity"
            pub_iso = _to_iso(row.get("publication-date"))
            deadline = _to_iso(row.get("deadline-received-tenders"))
            country = row.get("buyer-country") or ""

            items.append({
                "title": title,
                "source": "EU TED",
                "deadline": deadline,
                "country": country,
                "topic": None,
                "url": url,
                "summary": f"{title} {country} pub:{pub_iso or ''}".lower(),
            })

        total = int(data.get("total", 0) or 0)
        if total and page * limit >= total:
            break
        page += 1
        if page > 6:   # safety cap
            break

    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            items = [it for it in items
                     if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")
                     and not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
        except Exception:
            pass

    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _euft_fetch_impl(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _euft_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
