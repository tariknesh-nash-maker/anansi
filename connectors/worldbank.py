# connectors/worldbank.py
# World Bank "Procurement Notices" — uses observed keys from logs:
#   deadline -> submission_date
#   pub date -> noticedate
#   country  -> project_ctry_name
#   title    -> project_name (fallback bid_description)
#   desc     -> bid_description (fallback notice_text)
#
# Primary: return notices with deadline >= today.
# Fallback: if none, return most recent by noticedate (last 365 days), capped to 20.

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
    if any(k in t for k in ["ai", "algorithmic", "cybersecurity", "digital", "data protection"]):
        tags.append("ai_digital")
    if any(k in t for k in ["budget", "public finance", "fiscal", "open budget"]):
        tags.append("budget")
    if any(k in t for k in ["beneficial ownership", "procurement", "anti-corruption", "integrity", "aml", "cft"]):
        tags.append("anti_corruption")
    if any(k in t for k in ["parliament", "legislative", "assembly", "mp disclosure"]):
        tags.append("open_parliament")
    if any(k in t for k in ["climate", "adaptation", "resilience", "mrv", "just transition"]):
        tags.append("climate")
    out = []
    for th in tags:
        if th not in out:
            out.append(th)
    return out[:3]

def _infer_region(text: str, region0: str = "") -> str:
    tl = text.lower()
    if any(k in tl for k in ["mena", "middle east", "north africa", "maghreb", "arab"]):
        return "MENA"
    if "africa" in tl or any(k in tl for k in [
        "west africa","east africa","southern africa","sahel",
        "morocco","tunisia","algeria","egypt","jordan",
        "benin","cote d'ivoire","côte d’ivoire","senegal","ghana","liberia",
        "burkina faso","niger","mali","togo","mauritania","sierra leone"
    ]):
        return "Africa"
    return region0 or ""

def _sig(item: Dict[str, str]) -> str:
    base = f"{item.get('title','')}|{item.get('url','')}|{item.get('deadline','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def _fetch_page(start: int) -> Dict[str, Any]:
    params = {
        "format": "json",
        "rows": ROWS,
        "start": start,
        # no 'fl' — we want all fields since WB varies schemas
    }
    r = requests.get(API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    block = payload.get("procnotices") or payload.get("procurements") or {}
    if isinstance(block, dict):
        return [r for r in block.values() if isinstance(r, dict)]
    if isinstance(block, list):
        return [r for r in block if isinstance(r, dict)]
    # some responses can be raw lists
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return []

def _normalize(n: Dict[str, Any]) -> Dict[str, str]:
    if DEBUG:
        print("[worldbank] normalize keys:", list(n.keys())[:15])

    # observed fields from your logs
    title = (n.get("project_name") or n.get("bid_description") or "").strip()
    desc = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country = (n.get("project_ctry_name") or "").strip()
    region0 = (n.get("regionname") or "").strip()

    # deadline & pub dates
    deadline_iso = _to_iso(n.get("submission_date"))
    pub_iso = _to_iso(n.get("noticedate"))

    # build a clickable URL using the notice id (points to JSON detail; OK for MVP)
    nid = str(n.get("id") or "").strip()
    url = f"https://search.worldbank.org/api/procnotices?id={nid}" if nid else ""

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
        "_pub": pub_iso
    }

def fetch() -> List[Dict[str, str]]:
    today = datetime.utcnow().date()
    recent_cutoff = datetime.utcnow() - timedelta(days=365)

    seen = set()
    future_items: List[Dict[str, str]] = []
    recent_pub_items: List[Dict[str, str]] = []

    total_rows = 0
    for i in range(PAGES):
        start = i * ROWS
        try:
            payload = _fetch_page(start)
        except Exception as e:
            if DEBUG:
                print(f"[worldbank] fetch page start={start} error: {e}")
            break

        rows = _extract_rows(payload)
        total_rows += len(rows)
        if DEBUG:
            print(f"[worldbank] page {i+1}: rows={len(rows)}")

        if not rows:
            break

        for raw in rows:
            item = _normalize(raw)
            s = _sig(item)
            if s in seen:
                continue
            seen.add(s)

            # Primary criterion: deadline >= today (uses submission_date)
            dl = _parse_date(item.get("deadline"))
            if dl and dl.date() >= today:
                future_items.append(item)
                continue

            # Fallback pool: recently published by noticedate
            pub_dt = _parse_date(item.get("_pub"))
            if pub_dt and pub_dt >= recent_cutoff:
                recent_pub_items.append(item)

    if DEBUG:
        print(f"[worldbank] scanned ~{total_rows} rows, future={len(future_items)}, recent_pub={len(recent_pub_items)}")

    if future_items:
        return future_items

    if recent_pub_items:
        return recent_pub_items[:20]

    # Last-resort: return first 10 items (normalized) from page 0 so Slack isn't empty
    try:
        payload = _fetch_page(0)
        rows = _extract_rows(payload)[:10]
        return [_normalize(r) for r in rows]
    except Exception:
        return []

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "| deadline:", it["deadline"], "| pub:", it.get("_pub",""), "|", it["url"])
