# connectors/worldbank.py
# World Bank "Procurement Notices" (official API) — future-deadline items with safe fallback.
#
# Primary: return notices with deadline >= today.
# Fallback: if none, return first 20 items so Slack isn't empty.
#
# Env (optional):
#   WB_DEBUG=1  -> print debug info

from __future__ import annotations
import os, hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
import requests

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25
ROWS = 100         # per page
PAGES = 4          # total ~400 items
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
    # No qterm → broad recent slice. We request the fields we care about.
    params = {
        "format": "json",
        "rows": ROWS,
        "start": start,
        "fl": "title,url,deadline,description,countryname,regionname,pub_date"
    }
    r = requests.get(API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _normalize(n: Dict[str, Any]) -> Dict[str, str]:
    title = (n.get("title") or "").strip()
    url = (n.get("url") or "").strip()
    desc = (n.get("description") or "").strip()
    country = (n.get("countryname") or "").strip()
    region0 = (n.get("regionname") or "").strip()
    deadline = _to_iso(n.get("deadline") or "")

    text = " ".join([title, desc, country, region0])
    region = _infer_region(text, region0)
    themes = ",".join(_themes_from_text(text))

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": deadline,
        "summary": desc[:500],
        "region": region,
        "themes": themes
    }

def fetch() -> List[Dict[str, str]]:
    today = datetime.utcnow().date()
    seen = set()
    future_items: List[Dict[str, str]] = []
    fallback_items: List[Dict[str, str]] = []

    total_rows = 0
    for i in range(PAGES):
        start = i * ROWS
        try:
            payload = _fetch_page(start)
        except Exception as e:
            if DEBUG:
                print(f"[worldbank] fetch page start={start} error: {e}")
            break

        # The API usually returns dicts under "procnotices", but we also handle "procurements"
        block = payload.get("procnotices") or payload.get("procurements") or {}
        rows = list(block.values())
        total_rows += len(rows)
        if DEBUG:
            print(f"[worldbank] page {i+1}: {len(rows)} rows")

        if not rows:
            break

        for raw in rows:
            item = _normalize(raw)
            s = _sig(item)
            if s in seen:
                continue
            seen.add(s)

            # keep if deadline >= today
            dl = _parse_date(item.get("deadline"))
            if dl and dl.date() >= today:
                future_items.append(item)
            else:
                # stash for fallback if we end up with no future deadlines
                fallback_items.append(item)

    if DEBUG:
        print(f"[worldbank] scanned ~{total_rows} rows, future_deadlines={len(future_items)}, fallback={len(fallback_items)}")

    # Primary requirement: future deadlines only
    if future_items:
        return future_items

    # Fallback: do not return empty — send first 20 items so Slack shows something
    return fallback_items[:20]

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "|", it["deadline"], "|", it["url"])
