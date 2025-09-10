from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os, json, html
import requests

API = "https://api.ted.europa.eu/v3/notices/search"
UA  = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA, "Content-Type": "application/json"}

def _yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")

def _euft_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    since = datetime.utcnow().date() - timedelta(days=days_back)
    query = f"(publication-date >= {_yyyymmdd(datetime(since.year, since.month, since.day))})"

    page, limit = 1, 50
    items: List[Dict[str, Any]] = []

    while True:
        body = {
            "query": query,
            "fields": ["publication-number", "notice-title", "buyer-name", "notice-type"],
            "page": page,
            "limit": limit,
            "scope": "ACTIVE",
            "checkQuerySyntax": False,
            "paginationMode": "PAGE_NUMBER",
        }
        r = requests.post(API, headers=HEADERS, data=json.dumps(body), timeout=45)
        r.raise_for_status()
        data = r.json() or {}
        results = data.get("results") or data.get("items") or []
        if not results:
            break

        for row in results:
            pub_no = (row.get("publication-number") or "").strip()
            if not pub_no:
                continue
            url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"  # official pattern :contentReference[oaicite:1]{index=1}
            title = (row.get("notice-title") or "").strip() or "EU opportunity"
            buyer = (row.get("buyer-name") or "").strip()
            ntype = (row.get("notice-type") or "").strip()
            items.append({
                "title": html.unescape(title),
                "source": "EU TED",
                "deadline": None,              # not requested in 'fields'; can be added later if needed
                "country": "",                 # buyer country requires extra field(s); keeping lean for now
                "topic": None,
                "url": url,
                "summary": f"{title} {buyer} {ntype}".lower(),
            })

        total = int(data.get("total") or 0)
        if total and page * limit >= total:
            break
        page += 1
        if page > 6:
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
