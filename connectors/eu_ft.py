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
    Robust mode: query EVERYTHING, then filter locally to Funding & Tenders 'opportunities'
    (both calls for proposals and calls for tenders). This avoids brittle status/type codes.
    """
    items: List[Dict[str,Any]] = []
    page = 1
    page_size = 50

    # Minimal query; we’ll filter by URL/domain in-code.
    query = {"bool": {"must": []}}
    languages = ["en"]
    sort = {"field": "relevance", "order": "DESC"}

    while True:
        resp = requests.post(
            API,
            params={"apiKey": "SEDIA", "text": "*", "pageSize": str(page_size), "pageNumber": str(page)},
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
            # Keep only Funding & Tenders 'opportunities' pages
            url = r.get("url") or r.get("destination") or r.get("destinationPage") or ""
            if "/opportunities/" not in (url or ""):
                continue
            # Heuristic: only calls (for proposals/tenders/topics), not guidance pages
            blob = " ".join(str(r.get(k, "")) for k in ("title", "titleTranslated", "description","statusLabel","programme")).lower()
            if not any(x in blob for x in ["call", "calls", "tender", "topic"]):
                continue

            title = (r.get("title") or r.get("titleTranslated") or "").strip()
            deadline = _to_iso(r.get("deadlineDate") or r.get("endDate") or r.get("deadline") or r.get("closeDate"))

            items.append({
                "title": html.unescape(title) or "EU opportunity",
                "source": "EU F&T",
                "deadline": deadline,
                "country": r.get("country") or "",
                "topic": None,
                "url": url,
                "summary": blob,
            })

        total = (data.get("total") or data.get("resultCount") or 0) or 0
        if total and page * page_size >= int(total):
            break
        page += 1
        if page > 4:  # safety bound
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
