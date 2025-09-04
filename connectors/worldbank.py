# connectors/worldbank.py
# World Bank Procurement Notices via Finances One dataset API (DS00979 / RS00909)
# Reverse-paginates from newest rows; keeps notices with publication_date within the last N days (default 90).
# Titles are human-readable, cleaned, and prefixed with country; duplicates are collapsed by URL/content.

from __future__ import annotations
import os, re, hashlib, requests
from html import unescape
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

API_BASE    = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID  = os.getenv("WB_FONE_DATASET_ID", "DS00979")
RESOURCE_ID = os.getenv("WB_FONE_RESOURCE_ID", "RS00909")

TIMEOUT = 25
ROWS    = int(os.getenv("WB_ROWS", "200"))      # up to ~1000 supported
PAGES   = int(os.getenv("WB_PAGES", "10"))      # how many newest slices to scan
DEBUG   = os.getenv("WB_DEBUG", "0") == "1"

PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))
TODAY  = datetime.utcnow().date()
CUTOFF = TODAY - timedelta(days=PUB_WINDOW_DAYS)

WB_QTERM    = os.getenv("WB_QTERM", "").strip().lower()  # e.g. 'request for bids|request for expression of interest|eoi|rfp'
MAX_RESULTS = int(os.getenv("WB_MAX_RESULTS", "40"))

# Date formats seen in Finances One
DATE_FMTS = (
    "%d-%b-%Y", "%Y-%m-%d", "%d %b %Y",
    "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d",
    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
)

TAG_RE = re.compile(r"<[^>]+>")

def _strip_html(text: str) -> str:
    if not text: return ""
    return TAG_RE.sub("", unescape(text)).strip()

def _sentence_case(s: str) -> str:
    s = s.strip()
    if not s: return s
    return s[0].upper() + s[1:]

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
    # Prefer URL for dedup; else composite (title|type|pub)
    key = item.get("url") or "|".join([
        item.get("title",""), item.get("_type",""), item.get("_pub","")
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

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
    # Common fields in DS00979 / RS00909
    proj_id   = (n.get("project_id") or "").strip()
    raw_title = (n.get("bid_description") or "").strip()
    desc_raw  = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country   = (n.get("country_name") or "").strip()
    region    = (n.get("region") or "").strip()
    ntype     = (n.get("notice_type") or "").strip()

    # Clean text
    title_clean = _sentence_case(_strip_html(raw_title))[:120]
    desc_clean  = _strip_html(desc_raw)[:500]

    # Title selection (prefix with country)
    if title_clean:
        base_title = title_clean
    elif ntype or proj_id:
        base_title = f"{ntype or 'Notice'}{(' — ' + proj_id) if proj_id else ''}"
    else:
        base_title = "World Bank procurement notice"

    if country:
        title = f"{country} — {base_title}"
    else:
        title = base_title

    # Dates
    pub_iso = _to_iso(n.get("publication_date"))
    dl_iso  = _to_iso(n.get("deadline_date"))

    # URL (already human-readable in Finances One)
    url = (n.get("url") or "").strip()

    # Summary
    if desc_clean:
        summary = desc_clean
    else:
        bits = [ntype, proj_id]
        summary = " — ".join([b for b in bits if b]) or "Procurement notice"

    return {
        "title": title,
        "url": url,
        "deadline": dl_iso,     # you can hide this in your Slack layer if not needed
        "summary": summary,
        "region": region,
        "themes": "",
        "_pub": pub_iso,
        "_type": ntype,
        "_country": country,
        "_project": proj_id,
    }

def _in_window(pub: Optional[str]) -> bool:
    dt = _parse_date(pub)
    return bool(dt and CUTOFF <= dt.date() <= TODAY)

def _matches_qterm(item: Dict[str, str]) -> bool:
    pat = WB_QTERM
    if not pat:
        return True
    # Normalize Solr-like syntax to simple tokens (supports your earlier style too)
    norm = (pat
        .replace('"', ' ')
        .replace("(", " ").replace(")", " ")
        .replace(" or ", "|").replace(" OR ", "|").replace(" Or ", "|")
        .replace(",", "|"))
    tokens = [t.strip() for t in norm.split("|") if t.strip()]
    if not tokens:
        return True
    hay = " ".join([
        item.get("title",""), item.get("summary",""), item.get("_type","")
    ]).lower()
    return any(tok in hay for tok in tokens)

def fetch() -> List[Dict[str, str]]:
    # 1) Total → compute newest skips
    try:
        head = _fone({"datasetId": DATASET_ID, "resourceId": RESOURCE_ID, "type": "json", "top": 1, "skip": 0})
    except Exception as e:
        if DEBUG: print(f"[worldbank] F1 head error: {e}")
        return _placeholder()

    total = int(head.get("count", 0) or 0)
    if DEBUG: print(f"[worldbank] F1 total={total}")
    if total <= 0:
        return _placeholder()

    last_skip = max(0, total - ROWS)
    skips: List[int] = []
    for k in range(PAGES):
        s = last_skip - k*ROWS
        if s < 0: break
        skips.append(s)

    results: List[Dict[str, str]] = []
    seen = set()
    seen_urls = set()

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
                samples = [_to_iso(r.get("publication_date")) or "(no pub)" for r in data[:10]]
                print(f"[worldbank] F1 newest-slice pub samples: {samples}")

        if not data:
            continue

        for raw in data:
            item = _normalize(raw)
            if not _in_window(item.get("_pub")):
                continue
            if not _matches_qterm(item):
                continue

            url = (item.get("url") or "").strip()
            if url:
                if url in seen_urls:  # URL-level dedup
                    continue
                seen_urls.add(url)

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
        "summary": "No recent rows detected at the dataset tail. Try increasing WB_PAGES/ROWS, widening WB_PUB_WINDOW_DAYS, or clearing WB_QTERM.",
        "region": "",
        "themes": "",
        "_pub": "",
        "_type": "",
    }]

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:8]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| deadline:", it.get("deadline",""), "| type:", it.get("_type",""), "|", it["url"])
