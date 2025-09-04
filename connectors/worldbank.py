# connectors/worldbank.py
# World Bank "Procurement Notices" — keep only notices published in the last 90 days

import os, hashlib, requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25
ROWS = int(os.getenv("WB_ROWS", "100"))
PAGES = int(os.getenv("WB_PAGES", "10"))
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

DATE_FMTS = (
    "%Y-%m-%d", "%d-%b-%Y", "%d %b %Y",
    "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d",
    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M:%S.%f",
)

DENY_TYPES = {"contract award", "award"}
DENY_STATUS = {"draft"}

PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))
PUB_CUTOFF = datetime.utcnow() - timedelta(days=PUB_WINDOW_DAYS)
MAX_RESULTS = 40

def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val: return None
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(str(val).strip(), fmt)
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

    pub_iso = _to_iso(
        n.get("noticedate") or n.get("pub_date") or
        n.get("publication_date") or n.get("posting_date")
    )

    nid = str(n.get("id") or "").strip()
    public_detail = f"https://projects.worldbank.org/en/projects-operations/procurement-detail/{nid}" if nid else ""
    api_detail = f"https://search.worldbank.org/api/procnotices?id={nid}" if nid else ""
    url_from_api = (n.get("url") or n.get("notice_url") or n.get("source_url") or "").strip()
    url = public_detail or url_from_api or api_detail

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": "",  # deadlines ignored in this strategy
        "summary": desc[:500],
        "region": region0,
        "themes": "",
        "_pub": pub_iso,
        "_type": (n.get("notice_type") or ""),
        "_status": (n.get("notice_status") or "")
    }

def fetch() -> List[Dict[str, str]]:
    seen = set()
    results: List[Dict[str, str]] = []

    for i in range(PAGES):
        try:
            rows = _extract_rows(_fetch_page(i * ROWS))
        except Exception as e:
            if DEBUG: print(f"[worldbank] fetch page {i} error: {e}")
            break
        if not rows: break

        for raw in rows:
            nt = (str(raw.get("notice_type") or "")).lower()
            ns = (str(raw.get("notice_status") or "")).lower()
            if any(x in nt for x in DENY_TYPES): continue
            if ns in DENY_STATUS: continue

            item = _normalize(raw)
            s = _sig(item)
            if s in seen: continue
            seen.add(s)

            pub_dt = _parse_date(item.get("_pub"))
            if pub_dt and pub_dt >= PUB_CUTOFF:
                results.append(item)
                if len(results) >= MAX_RESULTS:
                    return results

    if DEBUG: print(f"[worldbank] results={len(results)} within last {PUB_WINDOW_DAYS} days")

    if results:
        return results

    # If still empty: placeholder
    return [{
        "title": f"No World Bank opportunities published in the last {PUB_WINDOW_DAYS} days",
        "url": "https://projects.worldbank.org/en/projects-operations/procurement",
        "deadline": "",
        "summary": "Try widening WB_PAGES or WB_PUB_WINDOW_DAYS in workflow env variables.",
        "region": "",
        "themes": "",
    }]

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    for it in fetch()[:5]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| type:", it.get("_type",""), "|", it["url"])
