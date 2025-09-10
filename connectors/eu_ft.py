from __future__ import annotations
from typing import List, Dict, Any
import json, re, html
import requests
import dateparser

API = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; anansi/1.0)"}

def _to_iso(s: str | None) -> str | None:
    if not s:
        return None
    dt = dateparser.parse(
        s,
        settings={"DATE_ORDER": "DMY", "PREFER_DAY_OF_MONTH": "first"},
        languages=["en","fr","es","de","it","pt"]
    )
    return dt.date().isoformat() if dt else None

def _euft_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    """
    Pull 'Open' and 'Forthcoming' Calls for Proposals from the EU Funding & Tenders portal
    via the public Search API.

    Notes:
      - status 31094502 = Open for submission
      - status 31094501 = Forthcoming
      - type 1/2 are calls/topics buckets commonly exposed in portal listings
      - We do NOT filter by frameworkProgramme to keep DG INTPA/NDICI items in
    """
    items: List[Dict[str,Any]] = []
    page = 1
    page_size = 50

    # Search body (multipart form-data per EC examples)
    query = {
        "bool": {
            "must": [
                {"terms": {"type": ["1","2"]}},
                {"terms": {"status": ["31094502","31094501"]}},
            ]
        }
    }
    languages = ["en"]  # results have translated titles; we can expand later
    sort = {"field": "sortStatus", "order": "ASC"}

    while True:
        params = {
            "apiKey": "SEDIA",
            "text": "*",
            "pageSize": str(page_size),
            "pageNumber": str(page),
        }
        resp = requests.post(
            API,
            params=params,
            files={
                "query": ("blob", json.dumps(query), "application/json"),
                "languages": ("blob", json.dumps(languages), "application/json"),
                "sort": ("blob", json.dumps(sort), "application/json"),
            },
            headers=HEADERS,
            timeout=45,
        )
        resp.raise_for_status()
        data = resp.json() or {}

        results = data.get("results") or data.get("items") or []
        if not results:
            break

        for r in results:
            title = (r.get("title") or r.get("titleTranslated") or "").strip()
            if not title:
                continue
            url = r.get("url") or r.get("destination") or r.get("destinationPage") or ""
            # common date fields seen in the API
            deadline = _to_iso(r.get("deadlineDate") or r.get("endDate") or r.get("deadline") or r.get("closeDate"))
            country = r.get("country") or r.get("geographicalZones") or ""
            summary = " ".join([
                title,
                r.get("programme", "") or r.get("programmeAcronym","") or "",
                r.get("statusLabel","") or "",
            ]).lower()

            items.append({
                "title": html.unescape(title),
                "source": "EU F&T",
                "deadline": deadline,
                "country": country if isinstance(country, str) else ", ".join(country or []),
                "topic": None,
                "url": url,
                "summary": summary,
            })

        # stop when no more pages
        total = (data.get("total") or data.get("resultCount") or 0) or 0
        if total and page * page_size >= int(total):
            break
        page += 1
        if page > 10:  # hard safety bound
            break

    # Optional OGP filtering (multilingual) + exclude auctions if your filters.py is present
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

# ---- Back-compat procedural API (for existing aggregator) ----
def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _euft_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
