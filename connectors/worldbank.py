# connectors/worldbank.py
from __future__ import annotations
import os, hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25
ROWS = 100
PAGES = 10
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

DATE_FMTS = ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d")

# opportunity filtering
ALLOW_TYPES = {
    "invitation for bids", "request for bids",
    "request for expressions of interest", "request for expression of interest",
    "specific procurement notice", "general procurement notice",
    "rfp", "rfq", "eoi"
}
DENY_TYPES = {"contract award", "award"}
DENY_STATUS = {"draft", "cancelled", "canceled", "archived", "closed"}

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
    title = (n.get("project_name") or n.get("bid_description") or "").strip()
    desc = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country = (n.get("project_ctry_name") or "").strip()
    region0 = (n.get("regionname") or "").strip()
    deadline_iso = _to_iso(n.get("submission_date"))
    pub_iso = _to_iso(n.get("noticedate"))
    nid = str(n.get("id") or "").strip()
    url = f"https://search.worldbank.org/api/v2/procnotices?id={nid}&format=html&apilang=en" if nid else ""
    text = " ".join([title, desc, country, region0])
    region = _infer_region(text, region0)
    themes = ",".join(_themes_from_text(text))
    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": deadline_iso,
        "summary": desc[:500],
        "region": region,
        "themes": themes,
        "_pub": pub_iso,
        "_type": (n.get("notice_type") or ""),
        "_status": (n.get("notice_status") or "")
    }

def fetch() -> List[Dict[str, str]]:
    today = datetime.utcnow().date()
    recent_cutoff = datetime.utcnow() - timedelta(days=365)
    seen = set()
    future_items: List[Dict[str, str]] = []
    recent_pub_items: List[Dict[str, str]] = []

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
            if ns in DENY_STATUS:
                continue
            # Optional strict allow-list:
            # if ALLOW_TYPES and not any(x in nt for x in ALLOW_TYPES): continue

            item = _normalize(raw)
            s = _sig(item)
            if s in seen: continue
            seen.add(s)

            dl = _parse_date(item.get("deadline"))
            if dl and dl.date() >= today:
                future_items.append(item)
                continue

            pub_dt = _parse_date(item.get("_pub"))
            if pub_dt and pub_dt >= recent_cutoff:
                recent_pub_items.append(item)

    if DEBUG:
        print(f"[worldbank] future={len(future_items)} recent_pub={len(recent_pub_items)}")

    if future_items:
        return future_items
    if recent_pub_items:
        return recent_pub_items[:20]

    # last-resort fallback: return 10 normalized items from first page (but still filtered for awards/drafts above)
    try:
        payload = _fetch_page(0)
        rows = _extract_rows(payload)[:20]
        fallback = []
        for raw in rows:
            nt = (str(raw.get("notice_type") or "")).lower()
            ns = (str(raw.get("notice_status") or "")).lower()
            if any(x in nt for x in DENY_TYPES) or ns in DENY_STATUS:
                continue
            fallback.append(_normalize(raw))
        return fallback[:10]
    except Exception:
        return []

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| type:", it.get("_type",""), "| status:", it.get("_status",""),
              "| deadline:", it["deadline"], "| pub:", it.get("_pub",""), "|", it["url"])
