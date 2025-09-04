# connectors/worldbank.py
# World Bank Procurement Notices via Finances One dataset API (DS00979 / RS00909)
# Reverse-paginates from the newest rows and filters to publication_date within last N days (default 90).

from __future__ import annotations
import os, hashlib, requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

API_BASE   = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID = os.getenv("WB_FONE_DATASET_ID", "DS00979")
RESOURCE_ID= os.getenv("WB_FONE_RESOURCE_ID", "RS00909")

TIMEOUT = 25
ROWS    = int(os.getenv("WB_ROWS", "200"))      # up to 1000 is allowed; 200 is safe
PAGES   = int(os.getenv("WB_PAGES", "10"))      # how many newest slices to scan
DEBUG   = os.getenv("WB_DEBUG", "0") == "1"

PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))
TODAY  = datetime.utcnow().date()
CUTOFF = TODAY - timedelta(days=PUB_WINDOW_DAYS)

WB_QTERM      = os.getenv("WB_QTERM", "").strip().lower()  # e.g. 'request for bids|request for expressions of interest|eoi|rfp'
MAX_RESULTS   = int(os.getenv("WB_MAX_RESULTS", "40"))

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
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def _fone(params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(API_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _fetch_page(skip: int) -> Dict[str, Any]:
    return _fone({
        "datasetId": DATASET_ID,
        "resourceId": RESOURCE_ID,
        "type": "json",
        "top": ROWS,
        "skip": skip,
    })

def _normalize(n: Dict[str, Any]) -> Dict[str, str]:
    # DS00979 / RS00909 common fields
    title   = (n.get("project_id") or n.get("bid_description") or "").strip()
    desc    = (n.get("bid_description") or "").strip()
    country = (n.get("country_name") or "").strip()
    region  = (n.get("region") or "").strip()
    ntype   = (n.get("notice_type") or "").strip()

    pub_iso = _to_iso(n.get("publication_date"))
    dl_iso  = _to_iso(n.get("deadline_date"))
    url     = (n.get("url") or "").strip()

    if not title and desc:
        title = desc[:80]

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": dl_iso,
        "summary": (desc or f"{ntype} — {country}")[:500],
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
    tokens = [t.strip() for t in WB_QTERM.split("|") if t.strip()]
    return any(tok in hay for tok in tokens) if tokens else True

def fetch() -> List[Dict[str, str]]:
    # 1) Get total count so we can start from the newest slice
    try:
        head = _fone({"datasetId": DATASET_ID, "resourceId": RESOURCE_ID, "type": "json", "top": 1, "skip": 0})
    except Exception as e:
        if DEBUG: print(f"[worldbank] F1 head error: {e}")
        return _placeholder()

    total = int(head.get("count", 0) or 0)
    if DEBUG: print(f"[worldbank] F1 total={total}")

    if total <= 0:
        return _placeholder()

    # 2) Build reverse skips (newest first)
    last_skip = max(0, total - ROWS)
    skips: List[int] = []
    for k in range(PAGES):
        s = last_skip - k*ROWS
        if s < 0:
            break
        skips.append(s)

    results: List[Dict[str, str]] = []
    seen = set()

    for idx, skip in enumerate(skips):
        try:
            payload = _fetch_page(skip)
        except Exception as e:
            if DEBUG: print(f"[worldbank] F1 fetch skip={skip} error: {e}")
            continue

        data = payload.get("data") or []
        if DEBUG:
            print(f"[worldbank] F1 skip={skip} rows={len(data)} count={payload.get('count','?')}")
            if idx == 0:
                # Sample first 10 publication dates for sanity
                samples = [ _to_iso(r.get("publication_date")) or "(no pub)" for r in data[:10] ]
                print(f"[worldbank] F1 newest-slice pub samples: {samples}")

        if not data:
            continue

        # 3) Normalize and filter
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

    return results if results else _placeholder()

def _placeholder() -> List[Dict[str, str]]:
    return [{
        "title": f"No World Bank notices published in the last {PUB_WINDOW_DAYS} days (Finances One)",
        "url": "https://financesone.worldbank.org/procurement-notice/DS00979",
        "deadline": "",
        "summary": "No recent rows detected at the dataset tail. Try increasing WB_PAGES/ROWS or widening WB_PUB_WINDOW_DAYS, or clearing WB_QTERM.",
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
