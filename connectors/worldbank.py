# connectors/worldbank.py
# World Bank "Procurement Notices" — future-deadline first, resilient fallbacks (never empty)

from __future__ import annotations
import os, hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25
ROWS = int(os.getenv("WB_ROWS", "100"))     # per page
PAGES = int(os.getenv("WB_PAGES", "10"))    # widen window quickly via env if needed
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# date formats incl. ISO-8601 (with/without milliseconds)
DATE_FMTS = (
    "%Y-%m-%d",
    "%d-%b-%Y", "%d %b %Y",
    "%m/%d/%Y", "%Y/%m/%d",
    "%Y.%m.%d",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
)

# filtering
DENY_TYPES = {"contract award", "award"}  # always skip awards
# status filtering relaxed: keep most statuses, only skip obvious drafts
DENY_STATUS = {"draft"}

# fallback windows
RECENT_PUB_DAYS = int(os.getenv("WB_RECENT_PUB_DAYS", "730"))  # 2 years
FINAL_MAX = 20  # cap fallback list sizes

def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    v = str(val).strip()
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            continue
    return None

def _to_iso(val: Optional[str]) -> str:
    dt = _parse_date(val)
    return dt.date().isoformat() if dt else (val.strip() if val else "")

def _themes_from_text(text: str) -> List[str]:
    t = text.lower()
    tags: List[str] = []
    if any(k in t for k in ["ai", "algorithmic", "cybersecurity", "digital", "data protection"]): tags.append("ai_digital")
    if any(k in t for k in ["budget", "public finance", "fiscal", "open budget"]): tags.append("budget")
    if any(k in t for k in ["beneficial ownership", "procurement", "anti-corruption", "integrity", "aml", "cft"]): tags.append("anti_corruption")
    if any(k in t for k in ["parliament", "legislative", "assembly", "mp disclosure"]): tags.append("open_parliament")
    if any(k in t for k in ["climate", "adaptation", "resilience", "mrv", "just transition"]): tags.append("climate")
    out = []
    for th in tags:
        if th not in out: out.append(th)
    return out[:3]

def _infer_region(text: str, region0: str = "") -> str:
    tl = text.lower()
    if any(k in tl for k in ["mena", "middle east", "north africa", "maghreb", "arab"]): return "MENA"
    if "africa" in tl or any(k in tl for k in [
        "west africa","east africa","southern africa","sahel",
        "morocco","tunisia","algeria","egypt","jordan","benin","cote d'ivoire","côte d’ivoire",
        "senegal","ghana","liberia","burkina faso","niger","mali","togo","mauritania","sierra leone"
    ]): return "Africa"
    return region0 or ""

def _sig(item: Dict[str, str]) -> str:
    base = f"{item.get('title','')}|{item.get('url','')}|{item.get('deadline','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def _fetch_page(start: int) -> Dict[str, Any]:
    # request full schema (no 'fl' param); API returns dict with 'procnotices' or 'procurements'
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
    if DEBUG: print("[worldbank] normalize keys:", list(n.keys())[:15])

    # title/desc from observed keys
    title = (n.get("project_name") or n.get("bid_description") or "").strip()
    desc  = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country = (n.get("project_ctry_name") or "").strip()
    region0 = (n.get("regionname") or "").strip()

    # deadline: check several possible fields
    deadline_raw = (
        n.get("submission_date") or n.get("closing_date") or
        n.get("bid_deadline") or n.get("deadline_date") or n.get("deadline")
    )
    deadline_iso = _to_iso(deadline_raw)

    # publication date (noticedate observed)
    pub_iso = _to_iso(n.get("noticedate") or n.get("pub_date") or n.get("publication_date"))

    # human-readable detail page (preferred), then API-provided URL, then JSON detail
    nid = str(n.get("id") or "").strip()
    public_detail = f"https://projects.worldbank.org/en/projects-operations/procurement-detail/{nid}" if nid else ""
    api_detail = f"https://search.worldbank.org/api/procnotices?id={nid}" if nid else ""
    url_from_api = (n.get("url") or n.get("notice_url") or n.get("source_url") or "").strip()
    url = public_detail or url_from_api or api_detail

    text = " ".join([title, desc, country, region0])
    region = _infer_region(text, region0)
    themes = ",".join(_themes_from_text(text))

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": deadline_iso,            # may be "", but never past if we include it
        "summary": desc[:500],
        "region": region,
        "themes": themes,
        "_pub": pub_iso,
        "_type": (n.get("notice_type") or ""),
        "_status": (n.get("notice_status") or "")
    }

def fetch() -> List[Dict[str, str]]:
    today = datetime.utcnow().date()
    pub_cutoff = datetime.utcnow() - timedelta(days=RECENT_PUB_DAYS)

    seen = set()
    future_items: List[Dict[str, str]] = []
    recent_pub_items: List[Dict[str, str]] = []
    unknown_deadline_items: List[Dict[str, str]] = []

    for i in range(PAGES):
        start = i * ROWS
        try:
            payload = _fetch_page(start)
        except Exception as e:
            if DEBUG: print(f"[worldbank] fetch page start={start} error: {e}")
            break

        rows = _extract_rows(payload)
        if DEBUG: print(f"[worldbank] page {i+1}: rows={len(rows)}")
        if not rows: break

        for raw in rows:
            nt = (str(raw.get("notice_type") or "")).lower()
            ns = (str(raw.get("notice_status") or "")).lower()
            if any(x in nt for x in DENY_TYPES):  # skip awards
                continue
            if ns in DENY_STATUS:                 # skip obvious drafts only
                continue

            item = _normalize(raw)
            s = _sig(item)
            if s in seen: 
                continue
            seen.add(s)

            # deadline logic
            dl = _parse_date(item.get("deadline"))
            if dl:
                if dl.date() >= today:
                    future_items.append(item)     # ✅ accept only future
                # else past: ignore
            else:
                # no deadline; keep for possible fallback tiers
                pub_dt = _parse_date(item.get("_pub"))
                if pub_dt and pub_dt >= pub_cutoff:
                    recent_pub_items.append(item)
                else:
                    unknown_deadline_items.append(item)

    if DEBUG:
        print(f"[worldbank] future={len(future_items)} recent_pub={len(recent_pub_items)} unknown_deadline={len(unknown_deadline_items)}")

    # Tier 1 — strict: future deadlines only
    if future_items:
        return future_items

    # Tier 2 — recent publications (noticedate in last N days)
    if recent_pub_items:
        return recent_pub_items[:FINAL_MAX]

    # Tier 3 — still empty: return some non-award items with unknown deadlines
    if unknown_deadline_items:
        return unknown_deadline_items[:FINAL_MAX]

    # Last resort: nothing found
    return []

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| type:", it.get("_type",""), "| status:", it.get("_status",""),
              "| deadline:", it["deadline"], "| pub:", it.get("_pub",""), "|", it["url"])
