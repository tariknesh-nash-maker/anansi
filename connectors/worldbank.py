# connectors/worldbank.py
# World Bank Procurement Notices via Finances One dataset API (DS00979 / RS00909)
# Strategy: page newest slices, filter by publication_date within last N days (default 90).
# Robust, no reliance on the fragile search.worldbank.org procnotices endpoint.

from __future__ import annotations
import os, hashlib, requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Finances One dataset API (documented in the DS00979 API Explorer)
# https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice?datasetId=DS00979&resourceId=RS00909&type=json
API_BASE = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID = os.getenv("WB_FONE_DATASET_ID", "DS00979")
RESOURCE_ID = os.getenv("WB_FONE_RESOURCE_ID", "RS00909")

TIMEOUT = 25
ROWS  = int(os.getenv("WB_ROWS", "200"))           # rows per page (API supports up to 1000; 200 is safe)
PAGES = int(os.getenv("WB_PAGES", "10"))           # how many pages to scan
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# Publication window (default 90 days)
PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))
TODAY = datetime.utcnow().date()
CUTOFF = TODAY - timedelta(days=PUB_WINDOW_DAYS)

# Optional local keyword bias (applied client-side to title/desc/type)
# e.g. WB_QTERM='request for bids|request for expressions of interest|eoi|rfp'
WB_QTERM = os.getenv("WB_QTERM", "").strip().lower()

MAX_RESULTS = int(os.getenv("WB_MAX_RESULTS", "40"))

# WB Finances One guidance says date fields use DD-MMM-YYYY. Parse multiple formats to be safe.
DATE_FMTS = (
    "%d-%b-%Y", "%Y-%m-%d", "%d %b %Y",
    "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d",
    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
)

def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    s = str(val).strip()
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def _to_iso(val: Optional[str]) -> str:
    dt = _parse_date(val)
    return dt.date().isoformat() if dt else (str(val).strip() if val else "")

def _sig(item: Dict[str, str]) -> str:
    base = f"{item.get('title','')}|{item.get('url','')}|{item.get('_pub','')}"
    import hashlib
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def _fetch_page(skip: int) -> Dict[str, Any]:
    params = {
        "datasetId": DATASET_ID,
        "resourceId": RESOURCE_ID,
        "type": "json",
        "top": ROWS,
        "skip": skip,
    }
    r = requests.get(API_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _normalize(n: Dict[str, Any]) -> Dict[str, str]:
    # Field names per DS00979: publication_date, deadline_date, notice_type, bid_description, project_id, country_name, url, region, sector
    title = (n.get("project_id") or n.get("bid_description") or "").strip()
    desc  = (n.get("bid_description") or "").strip()
    country = (n.get("country_name") or "").strip()
    region  = (n.get("region") or "").strip()
    ntype   = (n.get("notice_type") or "").strip()

    pub_iso = _to_iso(n.get("publication_date"))
    # Show deadline if present (don’t gate on it)
    deadline_iso = _to_iso(n.get("deadline_date"))

    url = (n.get("url") or "").strip()

    # Prefer human-readable title if weak
    if not title and desc:
        title = desc[:80]

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": deadline_iso,
        "summary": (desc or f"{ntype} — {country}").strip()[:500],
        "region": region,
        "themes": "",
        "_pub": pub_iso,
        "_type": ntype,
        "_country": country,
    }

def _in_window(pub: Optional[str]) -> bool:
    dt = _parse_date(pub)
    return bool(dt and CUTOFF <= dt.date() <= TODAY)

def _matches_qterm(item: Dict[str, str]) -> bool:
    if not WB_QTERM:
        return True
    hay = " ".join([
        item.get("title",""), item.get("summary",""),
        item.get("_type","")
    ]).lower()
    # tokens separated by |
    tokens = [t.strip() for t in WB_QTERM.split("|") if t.strip()]
    return any(tok in hay for tok in tokens) if tokens else True

def fetch() -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    seen = set()

    # Heuristic: newest tend to appear early in this dataset; scan first PAGES
    for page in range(PAGES):
        skip = page * ROWS
        try:
            payload = _fetch_page(skip)
        except Exception as e:
            if DEBUG: print(f"[worldbank] FinancesOne fetch skip={skip} error: {e}")
            continue

        # Response shape: {"count": <int>, "data": [ ... ]}
        data = payload.get("data") or []
        if DEBUG:
            c = payload.get("count", "?")
            print(f"[worldbank] F1 skip={skip} rows={len(data)} count={c}")

        if not data:
            break

        for raw in data:
            item = _normalize(raw)
            if not _in_window(item.get("_pub")):
                continue
            if not _matches_qterm(item):
                continue

            sig = _sig(item)
            if sig in seen:
                continue
            seen.add(sig)

            results.append(item)
            if len(results) >= MAX_RESULTS:
                return results

    if DEBUG:
        print(f"[worldbank] FinancesOne emitted={len(results)} (window={PUB_WINDOW_DAYS}d, pages={PAGES}, rows={ROWS})")

    if results:
        return results

    # Placeholder (non-empty Slack message)
    return [{
        "title": f"No World Bank notices published in the last {PUB_WINDOW_DAYS} days (Finances One)",
        "url": "https://financesone.worldbank.org/procurement-notice/DS00979",
        "deadline": "",
        "summary": "Tip: increase WB_PAGES, WB_ROWS, or widen WB_PUB_WINDOW_DAYS; or clear WB_QTERM to see all recent notices.",
        "region": "",
        "themes": "",
        "_pub": "",
        "_type": "",
    }]

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| type:", it.get("_type",""), "|", it["url"])
