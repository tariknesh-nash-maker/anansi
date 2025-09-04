# connectors/worldbank.py
# World Bank "Procurement Notices" — reverse-pagination + last-90-days by Published Date

import os, hashlib, requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25
ROWS = int(os.getenv("WB_ROWS", "100"))        # rows per page
PAGES = int(os.getenv("WB_PAGES", "10"))       # how many pages to scan (backwards)
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# Publication window (default 90 days)
PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))
PUB_CUTOFF = datetime.utcnow() - timedelta(days=PUB_WINDOW_DAYS)

# Filter out non-opportunity noise
DENY_TYPES = {"contract award", "award"}
DENY_STATUS = {"draft"}

# Optional keyword bias (leave empty to fetch all). Example:
#   WB_QTERM='("request for bids" OR "request for expressions of interest" OR "eoi" OR "rfp")'
QTERM = os.getenv("WB_QTERM", "").strip()

# Max items to emit
MAX_RESULTS = int(os.getenv("WB_MAX_RESULTS", "40"))

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

def _fetch_page(start: int) -> Dict[str, Any]:
    params = {"format": "json", "rows": ROWS, "start": start}
    if QTERM:
        params["qterm"] = QTERM
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
    title = (n.get("project_name") or n.get("bid_description") or "").strip()
    desc  = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country = (n.get("project_ctry_name") or "").strip()
    region0 = (n.get("regionname") or "").strip()

    pub_iso = _to_iso(n.get("noticedate") or n.get("pub_date") or n.get("publication_date") or n.get("posting_date"))

    nid = str(n.get("id") or "").strip()
    public_detail = f"https://projects.worldbank.org/en/projects-operations/procurement-detail/{nid}" if nid else ""
    api_detail = f"https://search.worldbank.org/api/procnotices?id={nid}" if nid else ""
    url_from_api = (n.get("url") or n.get("notice_url") or n.get("source_url") or "").strip()
    url = public_detail or url_from_api or api_detail

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": "",             # deadlines ignored with this strategy
        "summary": desc[:500],
        "region": region0,
        "themes": "",
        "_pub": pub_iso,
        "_type": (n.get("notice_type") or ""),
        "_status": (n.get("notice_status") or "")
    }

def fetch() -> List[Dict[str, str]]:
    # 1) First call to get 'total' so we can start from the newest slice
    try:
        head = _fetch_page(0)
    except Exception as e:
        if DEBUG: print(f"[worldbank] head fetch error: {e}")
        return _placeholder()

    total_str = head.get("total") or head.get("Total") or "0"
    try:
        total = int(str(total_str))
    except Exception:
        total = 0
    if DEBUG: print(f"[worldbank] total={total}")

    results: List[Dict[str, str]] = []
    seen = set()

    # 2) Build reverse start offsets (newest first)
    # If total=0 (API didn’t return it), still scan forward as a fallback.
    starts: List[int] = []
    if total > 0:
        last_index = max(0, total - ROWS)
        # example: total=12345, ROWS=100 -> starts: 12200, 12100, ...
        for k in range(PAGES):
            start = last_index - k * ROWS
            if start < 0:
                break
            starts.append(start)
    else:
        # fallback: forward paging
        starts = [i * ROWS for i in range(PAGES)]

    # 3) Page through newest slices first; stop when we pass the cutoff and have some results
    for start in starts:
        try:
            payload = _fetch_page(start)
        except Exception as e:
            if DEBUG: print(f"[worldbank] fetch start={start} error: {e}")
            continue

        rows = _extract_rows(payload)
        if DEBUG: print(f"[worldbank] start={start} rows={len(rows)}")
        if not rows:
            continue

        for raw in rows:
            nt = (str(raw.get("notice_type") or "")).lower()
            ns = (str(raw.get("notice_status") or "")).lower()
            if any(x in nt for x in DENY_TYPES):     # skip awards
                continue
            if ns in DENY_STATUS:                    # skip drafts
                continue

            item = _normalize(raw)
            pub_dt = _parse_date(item.get("_pub"))
            if not pub_dt:
                continue  # must have a publication date

            if pub_dt < PUB_CUTOFF:
                # Since we're reverse paging (newest → older), once we hit old items,
                # we can break this page early only if we already collected some.
                continue

            sig = _sig(item)
            if sig in seen:
                continue
            seen.add(sig)

            results.append(item)
            if len(results) >= MAX_RESULTS:
                return results

    if DEBUG: print(f"[worldbank] results={len(results)} within last {PUB_WINDOW_DAYS} days")

    return results if results else _placeholder()

def _placeholder() -> List[Dict[str, str]]:
    return [{
        "title": f"No World Bank opportunities published in the last {PUB_WINDOW_DAYS} days (try widening window or pages)",
        "url": "https://projects.worldbank.org/en/projects-operations/procurement",
        "deadline": "",
        "summary": "Tip: set WB_PAGES=15 and/or WB_PUB_WINDOW_DAYS=180 in your workflow env.",
        "region": "",
        "themes": "",
    }]

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| type:", it.get("_type",""), "|", it["url"])
