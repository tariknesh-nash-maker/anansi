# connectors/worldbank.py
# World Bank "Procurement Notices" — server-side sort by publication date (desc) + published-in-window filter
# Strategy to unblock: pull newest first, keep anything published in the window (no type/status filters yet).
# After data flows, re-add filters (awards, drafts) and narrow the window.

import os, hashlib, requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import Counter

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25

# Tunables (set via GitHub Actions env)
ROWS  = int(os.getenv("WB_ROWS", "100"))                # rows per page
PAGES = int(os.getenv("WB_PAGES", "10"))                # how many pages to scan
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# Publication window — start with 180 to validate the pipe; you can drop to 90 later
PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "180"))
PUB_CUTOFF = datetime.utcnow() - timedelta(days=PUB_WINDOW_DAYS)

# Optional keyword bias (leave empty initially to avoid over-filtering)
QTERM = os.getenv("WB_QTERM", "").strip()

# Server-side sort by publication date (observed field: noticedate)
SRT_FIELD = os.getenv("WB_SRT_FIELD", "noticedate")
SRT_ORDER = os.getenv("WB_SRT_ORDER", "desc")           # 'asc' or 'desc'

# How many items to emit
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
    params = {
        "format": "json",
        "rows": ROWS,
        "start": start,
        "srt": SRT_FIELD,             # ← sort by publication date
        "order": SRT_ORDER,           # ← newest first
    }
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
    title   = (n.get("project_name") or n.get("bid_description") or "").strip()
    desc    = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country = (n.get("project_ctry_name") or "").strip()
    region0 = (n.get("regionname") or "").strip()

    pub_iso = _to_iso(
        n.get("noticedate") or n.get("pub_date") or
        n.get("publication_date") or n.get("posting_date")
    )

    nid = str(n.get("id") or "").strip()
    public_detail = f"https://projects.worldbank.org/en/projects-operations/procurement-detail/{nid}" if nid else ""
    api_detail    = f"https://search.worldbank.org/api/procnotices?id={nid}" if nid else ""
    url_from_api  = (n.get("url") or n.get("notice_url") or n.get("source_url") or "").strip()
    url = public_detail or url_from_api or api_detail

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": "",  # we’re not using deadline in this mode
        "summary": desc[:500],
        "region": region0,
        "themes": "",
        "_pub": pub_iso,
        "_type": (n.get("notice_type") or ""),
        "_status": (n.get("notice_status") or ""),
    }

def fetch() -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    seen = set()
    type_counter = Counter()
    status_counter = Counter()
    first_page_pub_samples: List[str] = []

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

        # Collect debug stats for the very first page
        if page == 0:
            for raw in rows[:10]:
                pub_iso = _to_iso(raw.get("noticedate") or raw.get("pub_date") or raw.get("publication_date") or raw.get("posting_date"))
                first_page_pub_samples.append(pub_iso or "(no pub date)")

        stop_due_to_old = False

        for raw in rows:
            # no type/status filters here — we want to see *anything* in-window first
            item = _normalize(raw)

            # debug counters
            type_counter.update([str(item.get("_type","")).lower()])
            status_counter.update([str(item.get("_status","")).lower()])

            pub_dt = _parse_date(item.get("_pub"))
            if not pub_dt:
                continue

            if pub_dt < PUB_CUTOFF:
                stop_due_to_old = True
                continue

            sig = _sig(item)
            if sig in seen:
                continue
            seen.add(sig)

            results.append(item)
            if len(results) >= MAX_RESULTS:
                _maybe_log_debug(first_page_pub_samples, type_counter, status_counter, results)
                return results

        if stop_due_to_old:
            break

    _maybe_log_debug(first_page_pub_samples, type_counter, status_counter, results)

    if results:
        return results

    # Explicit placeholder with diagnostics in DEBUG
    return [{
        "title": f"No World Bank notices found with Published Date within last {PUB_WINDOW_DAYS} days",
        "url": "https://projects.worldbank.org/en/projects-operations/procurement",
        "deadline": "",
        "summary": "Try increasing WB_PAGES (e.g., 20), widening WB_PUB_WINDOW_DAYS (e.g., 365), or removing WB_QTERM.",
        "region": "",
        "themes": "",
    }]

def _maybe_log_debug(first_page_pub_samples, type_counter, status_counter, results):
    if not DEBUG:
        return
    print(f"[worldbank] first-page pub samples: {first_page_pub_samples}")
    print(f"[worldbank] notice_type histogram (top 10): {type_counter.most_common(10)}")
    print(f"[worldbank] notice_status histogram (top 10): {status_counter.most_common(10)}")
    print(f"[worldbank] emitted={len(results)}")

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| type:", it.get("_type",""), "|", it["url"])
