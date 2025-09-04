# connectors/worldbank.py
# World Bank "Procurement Notices" — recent & future-deadline items (with safe fallback).
#
# What this does:
#   1) Queries the official World Bank "procnotices" API.
#   2) Collects a recent slice (first ~400 items) across a few broad queries.
#   3) Filters to notices with deadline >= today.
#   4) If none found, falls back to "recently published" (pub_date in last 365 days) so result is not empty.
#
# Output per item: {title, url, deadline, summary, region, themes}
#
# No environment variables required.

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

TIMEOUT = 25
API = "https://search.worldbank.org/api/procnotices"  # official WB endpoint
ROWS_PER_PAGE = 100
MAX_PAGES = 4               # ~400 rows total (fast enough for MVP)
RECENT_DAYS_FALLBACK = 365  # if no future-deadline items, keep notices published in last N days

# Broad queries; API accepts qterm, but empty/space also works in practice
QTERMS = [
    "",                      # everything (first page of recency ordering on WB side)
    "governance OR transparency OR anti-corruption OR procurement",
    "budget OR public finance OR fiscal",
    "digital OR data protection OR cybersecurity OR AI",
    "parliament OR legislative",
    "climate OR adaptation OR resilience"
]

# --------------------------------- helpers ---------------------------------

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

def _pick(d: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in d and d[k]:
            return str(d[k]).strip()
    return ""

# --------------------------------- API calls ---------------------------------

def _fetch_page(qterm: str, start: int = 0, rows: int = ROWS_PER_PAGE) -> Dict[str, Any]:
    # Fields known to appear: title, url, deadline, description, countryname, regionname, pub_date
    params = {
        "format": "json",
        "qterm": qterm,
        "rows": rows,
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

    # Deadline is the reliable "currentness" filter
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

# --------------------------------- main fetch ---------------------------------

def fetch() -> List[Dict[str, str]]:
    """
    Returns notices with deadline >= today.
    If none, returns notices published in last RECENT_DAYS_FALLBACK days (even if no deadline).
    """
    today = datetime.utcnow().date()
    fallback_cutoff = datetime.utcnow() - timedelta(days=RECENT_DAYS_FALLBACK)

    seen = set()
    future_deadline_items: List[Dict[str, str]] = []
    recent_pub_items: List[Dict[str, str]] = []

    # Pull a recent slice across a few qterms (first N pages each)
    for q in QTERMS:
        start = 0
        pages = 0
        while pages < MAX_PAGES:
            data = _fetch_page(q, start=start, rows=ROWS_PER_PAGE)
            procs = data.get("procnotices") or data.get("procurements") or {}
            rows = list(procs.values())
            if not rows:
                break

            for raw in rows:
                item = _normalize(raw)
                sig = _sig(item)
                if sig in seen:
                    continue
                seen.add(sig)

                # keep if deadline >= today
                dl = _parse_date(item.get("deadline"))
                if dl and dl.date() >= today:
                    future_deadline_items.append(item)
                    continue

                # build fallback bucket: recently published, even if deadline missing/past
                pub_dt = _parse_date(_pick(raw, ["pub_date", "publication_date", "published_on", "posting_date", "post_date"]))
                if pub_dt and pub_dt >= fallback_cutoff:
                    recent_pub_items.append(item)

            # pagination
            pages += 1
            start += ROWS_PER_PAGE

    # Primary requirement: only future deadlines
    if future_deadline_items:
        return future_deadline_items

    # Fallback: don't return empty — send recent publications if no future deadlines found
    # (you can cap to first 20 to avoid long Slack messages)
    return recent_pub_items[:20]

if __name__ == "__main__":
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:5]:
        print("-", it["title"], "|", it["deadline"], "|", it["url"])
