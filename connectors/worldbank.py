# connectors/worldbank.py
# World Bank "Procurement Notice" connector via Finances One API (Dataset DS00979 / Resource RS00909).
# Output for each item: {title, url, deadline, summary, region, themes}
#
# Notes:
# - Uses the public API Explorer endpoint:
#   https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice?datasetId=DS00979&resourceId=RS00909
# - Paginates with `top` and `skip`.
# - Keeps logic simple & robust for MVP.

from __future__ import annotations
import hashlib
import time
from datetime import datetime
from typing import Dict, List, Any

import requests

API_BASE = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID = "DS00979"
RESOURCE_ID = "RS00909"

PAGE_SIZE = 100  # API supports `top`, keep ≤100–200 to be polite
TIMEOUT = 30

# Simple tag rules (extend as needed)
def _themes_from_text(text: str) -> List[str]:
    t = text.lower()
    themes: List[str] = []
    if any(k in t for k in ["ai", "algorithmic", "cybersecurity", "digital", "data protection"]):
        themes.append("ai_digital")
    if any(k in t for k in ["budget", "public finance", "fiscal", "open budget"]):
        themes.append("budget")
    if any(k in t for k in ["beneficial ownership", "procurement", "anti-corruption", "integrity", "aml", "cft"]):
        themes.append("anti_corruption")
    if any(k in t for k in ["parliament", "legislative", "assembly", "mp disclosure"]):
        themes.append("open_parliament")
    if any(k in t for k in ["climate", "adaptation", "resilience", "mrv", "just transition"]):
        themes.append("climate")
    # unique + cap 3
    dedup = []
    for th in themes:
        if th not in dedup:
            dedup.append(th)
    return dedup[:3]

def _to_iso_date(val: str) -> str:
    if not val:
        return ""
    val = str(val).strip()
    # Try a few common formats seen in WB data
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(val, fmt).date().isoformat()
        except Exception:
            pass
    return val  # leave as-is if unknown

def _get_page(skip: int, top: int = PAGE_SIZE) -> Dict[str, Any]:
    params = {
        "datasetId": DATASET_ID,
        "resourceId": RESOURCE_ID,
        "type": "json",
        "top": str(top),
        "skip": str(skip),
    }
    r = requests.get(API_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _normalize_row(row: Dict[str, Any]) -> Dict[str, str]:
    """
    The Finances One dataset provides fields with generic names in 'data' rows.
    We'll extract the common ones if present; else fallback gracefully.
    Common semantic fields we try:
      - title/notice_title/subject
      - description/summary
      - deadline/closing_date/submission_deadline/bid_deadline
      - url/link/source_url
      - country/region
    """
    # Try several likely keys, with fallbacks
    def pick(keys: List[str]) -> str:
        for k in keys:
            v = row.get(k)
            if v:
                return str(v).strip()
        return ""

    title = pick(["title", "notice_title", "subject", "project_name", "tender_title"])
    desc = pick(["description", "summary", "notice_description", "tender_description"])
    url = pick(["url", "link", "source_url", "notice_url"])

    deadline_raw = pick(["deadline", "closing_date", "submission_deadline", "bid_deadline"])
    deadline = _to_iso_date(deadline_raw)

    country = pick(["country", "country_name"])
    region0 = pick(["region", "region_name"])

    # Infer region label very lightly (Africa/MENA) from text hints
    whole = " ".join([title, desc, country, region0])
    region = ""
    wl = whole.lower()
    if any(k in wl for k in ["mena", "middle east", "north africa", "maghreb", "arab"]):
        region = "MENA"
    elif "africa" in wl or any(k in wl for k in [
        "west africa","east africa","southern africa","sahel",
        "morocco","tunisia","algeria","egypt","jordan",
        "benin","cote d'ivoire","côte d’ivoire","senegal","ghana","liberia",
        "burkina faso","niger","mali","togo","mauritania","sierra leone"
    ]):
        region = "Africa"
    elif region0:
        region = region0

    themes = ",".join(_themes_from_text(whole))

    return {
        "title": title or "World Bank opportunity",
        "url": url,
        "deadline": deadline,
        "summary": (desc or "")[:500],
        "region": region,
        "themes": themes,
    }

def fetch() -> List[Dict[str, str]]:
    """
    Fetches and normalizes World Bank procurement notices from DS00979/RS00909.
    Returns a list of dicts with keys: title, url, deadline, summary, region, themes.
    """
    out: List[Dict[str, str]] = []
    seen = set()
    skip = 0

    while True:
        try:
            payload = _get_page(skip=skip, top=PAGE_SIZE)
        except Exception as e:
            print(f"[worldbank] fetch error at skip={skip}: {e}")
            break

        # The payload shape contains a "data" array with rows (dicts)
        rows = payload.get("data") or []
        if not rows:
            break

        for row in rows:
            norm = _normalize_row(row)
            sig = hashlib.sha1(f"{norm.get('title','')}|{norm.get('url','')}|{norm.get('deadline','')}".encode("utf-8")).hexdigest()
            if sig in seen:
                continue
            out.append(norm)
            seen.add(sig)

        # Pagination: stop if we received less than PAGE_SIZE rows
        if len(rows) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(0.4)  # be polite

    return out

if __name__ == "__main__":
    # Quick manual test
    items = fetch()
    print(f"fetched {len(items)} World Bank items")
    for it in items[:5]:
        print("-", it["title"], "|", it["deadline"], "|", it["url"])
