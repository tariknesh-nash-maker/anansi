# connectors/worldbank.py
# World Bank "Procurement Notices" — server-side date filtering (noticedate within last N days)

import os, hashlib, requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25

# Tunables (via GitHub Actions env)
ROWS  = int(os.getenv("WB_ROWS", "100"))          # rows per page
PAGES = int(os.getenv("WB_PAGES", "10"))          # how many pages to scan
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# Publication window (default 90 days)
PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))
TODAY = datetime.utcnow().date()
CUTOFF = TODAY - timedelta(days=PUB_WINDOW_DAYS)

# Optional keyword bias (e.g., WB_QTERM='("request for bids" OR "request for expressions of interest" OR eoi OR rfp)')
WB_QTERM = os.getenv("WB_QTERM", "").strip()

# Max items to emit
MAX_RESULTS = int(os.getenv("WB_MAX_RESULTS", "40"))

# We no longer exclude by type/status here; the aim is to get fresh items flowing first.
# You can reintroduce exclusions after you see results.
# DENY_TYPES  = {"contract award", "award"}
# DENY_STATUS = {"draft"}

DATE_FMTS = (
    "%Y-%m-%d", "%d-%b-%Y", "%d %b %Y",
    "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d",
    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M:%S.%f",
)

def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val: return None
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
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def _build_qterm() -> str:
    # Build a Solr-style date range for noticedate: [CUTOFF TO TODAY]
    start = CUTOFF.strftime("%Y-%m-%d")
    end   = TODAY.strftime("%Y-%m-%d")
    date_filter = f'noticedate:[{start} TO {end}]'
    if WB_QTERM:
        # Combine user keywords with the date filter
        return f'({WB_QTERM}) AND {date_filter}'
    return date_filter

def _fetch_page(start: int) -> Dict[str, Any]:
    params = {
        "format": "json",
        "rows": ROWS,
        "start": start,
        "qterm": _build_qterm(),  # ← server-side date filter (and optional keywords)
        # You can also pass sort; with date filter it’s less critical:
        "srt": "noticedate",
        "order": "desc",
    }
    if DEBUG:
        print(f"[worldbank] qterm={params['qterm']}")
    r = requests.get(API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    block = payload.get("procnotices") or payload.get("procurements") or {}
    if isinstance(block, dict): return [r for r in block.values() if isinstance(r, dict)]
    if isinstance(block, list): return [r for r in block if isinstance(r, dict)]
    if isinstance(payload, list): return [r for r in payload if isinstance(r, dict)]
    return []

def _normalize(n: Dict[str, Any]) -> Dict[str, str]:
    title   = (n.get("project_name") or n.get("bid_description") or "").strip()
    desc    = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country = (n.get("project_ctry_name") or "").strip()
    region0 = (n.get("regionname") or "").strip()

    pub_iso = _to_iso(n.get("noticedate") or n.get("pub_date") or n.get("publication_date") or n.get("posting_date"))

    nid = str(n.get("id") or "").strip()
    public_detail = f"https://projects.worldbank.org/en/projects-operations/procurement-detail/{nid}" if nid else ""
    api_detail    = f"https://search.worldbank.org/api/procnotices?id={nid}" if nid else ""
    url_from_api  = (n.get("url") or n.get("notice_url") or n.get("source_url") or "").strip()
    url = public_detail or url_from_api or api_detail

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": "",          # we’re filtering on publication date only
        "summary": desc[:500],
        "region": region0,
        "themes": "",
        "_pub": pub_iso,
        "_type": (n.get("notice_type") or ""),
        "_status": (n.get("notice_status") or "")
    }

def fetch() -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    seen = set()

    for page in range(PAGES):
        start = page * ROWS
        try:
            payload = _fetch_page(start)
        except Exception as e:
            if DEBUG: print(f"[worldbank] fetch start={start} error: {e}")
            continue

        rows = _extract_rows(payload)
        if DEBUG: print(f"[worldbank] start={start} rows={len(rows)}")
        if not rows:
            break

        for raw in rows:
            item = _normalize(raw)
            pub_dt = _parse_date(item.get("_pub"))
            if not pub_dt:
                # If the API returned rows without noticedate despite our qterm, skip them.
                continue

            # Extra safety gate (should already be satisfied by qterm):
            if not (CUTOFF <= pub_dt.date() <= TODAY):
                continue

            sig = _sig(item)
            if sig in seen:
                continue
            seen.add(sig)

            results.append(item)
            if len(results) >= MAX_RESULTS:
                return results

    if DEBUG: print(f"[worldbank] results={len(results)} within last {PUB_WINDOW_DAYS} days")
    if results:
        return results

    # Placeholder to avoid empty Slack posts
    return [{
        "title": f"No World Bank notices found with Published Date in last {PUB_WINDOW_DAYS} days",
        "url": "https://projects.worldbank.org/en/projects-operations/procurement",
        "deadline": "",
        "summary": "Try removing WB_QTERM, increasing WB_PAGES (e.g., 20), or widening WB_PUB_WINDOW_DAYS (e.g., 180).",
        "region": "",
        "themes": "",
    }]

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| type:", it.get("_type",""), "|", it["url"])
