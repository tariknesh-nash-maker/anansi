from __future__ import annotations
from typing import List, Dict, Any
import os, json, html
import requests

API = "https://api.ted.europa.eu/v3/notices/search"
UA  = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA, "Content-Type": "application/json"}

def _euft_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    """
    Temporary debug-friendly stub for EU. If the API rejects our payload, we log the body and
    return an empty list without crashing the job.
    """
    body = {
        "query": "(publication-date >= 20240101)",   # placeholder; will be set dynamically later
        "fields": ["publication-number","notice-title","publication-date","deadline-received-tenders","buyer-country"],
        "page": 1,
        "limit": 50,
        "scope": "ACTIVE",
        "paginationMode": "PAGE_NUMBER",
        "checkQuerySyntax": False,
    }
    try:
        r = requests.post(API, headers=HEADERS, data=json.dumps(body), timeout=30)
        r.raise_for_status()
        data = r.json() or {}
        results = data.get("results") or data.get("items") or []
        items: List[Dict[str, Any]] = []
        for row in results:
            pub_no = (row.get("publication-number") or "").strip()
            if not pub_no:
                continue
            url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"
            title = html.unescape((row.get("notice-title") or "").strip()) or "EU opportunity"
            items.append({
                "title": title,
                "source": "EU TED",
                "deadline": row.get("deadline-received-tenders"),
                "country": row.get("buyer-country") or "",
                "topic": None,
                "url": url,
                "summary": (title + " " + (row.get("buyer-country") or "")).lower(),
            })
        return items
    except requests.HTTPError as e:
        try:
            err_text = e.response.text
        except Exception:
            err_text = ""
        print(f"[eu_ft] WARN 400/HTTP error: {e} | body={err_text[:300]}")
        return []
    except Exception as ex:
        print(f"[eu_ft] WARN unexpected error: {ex}")
        return []

class Connector:
    def fetch(self, days_back: int = 90):
        return _euft_fetch_impl(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _euft_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
