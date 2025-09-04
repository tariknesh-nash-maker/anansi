# connectors/worldbank.py
# World Bank "Procurement Notice" connector via Finances One API (Dataset DS00979 / Resource RS00909)
# Fast version: single request (top=200), no pagination, quick normalize.
#
# Output items (per aggregator expectation): {title, url, deadline, summary, region, themes}

from __future__ import annotations
import hashlib
from datetime import datetime
from typing import Dict, List, Any
import requests

API_BASE = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID = "DS00979"
RESOURCE_ID = "RS00909"
TIMEOUT = 25
TOP = 200  # pull only the latest ~200 rows to keep it fast

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
    out = []
    for th in themes:
        if th not in out:
            out.append(th)
    return out[:3]

def _to_iso_date(val: str) -> str:
    if not val:
        return ""
    v = str(val).strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(v, fmt).date().isoformat()
        except Exception:
            pass
    return v  # leave as-is

def _pick(row: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in row and row[k]:
            return str(row[k]).strip()
    return ""

def _normalize_row(row: Dict[str, Any]) -> Dict[str, str]:
    # Try several likely keys; tweak if the dataset uses different names.
    title    = _pick(row, ["title", "notice_title", "subject", "project_name", "tender_title"])
    desc     = _pick(row, ["description", "summary", "notice_description", "tender_description"])
    url      = _pick(row, ["url", "link", "source_url", "notice_url"])
    deadline = _to_iso_date(_pick(row, ["deadline", "closing_date", "submission_deadline", "bid_deadline"]))
    country  = _pick(row, ["country", "country_name"])
    region0  = _pick(row, ["region", "region_name"])

    text = " ".join([title, desc, country, region0])
    themes = ",".join(_themes_from_text(text))

    region = ""
    tl = text.lower()
    if any(k in tl for k in ["mena", "middle east", "north africa", "maghreb", "arab"]):
        region = "MENA"
    elif "africa" in tl or any(k in tl for k in [
        "west africa","east africa","southern africa","sahel",
        "morocco","tunisia","algeria","egypt","jordan",
        "benin","cote d'ivoire","côte d’ivoire","senegal","ghana","liberia",
        "burkina faso","niger","mali","togo","mauritania","sierra leone"
    ]):
        region = "Africa"
    elif region0:
        region = region0

    return {
        "title": title or "World Bank opportunity",
        "url": url,
        "deadline": deadline,
        "summary": (desc or "")[:500],
        "region": region,
        "themes": themes
    }

def _get_latest(limit: int = TOP) -> Dict[str, Any]:
    # Finances One API Explorer style endpoint
    params = {
        "datasetId": DATASET_ID,
        "resourceId": RESOURCE_ID,
        "type": "json",
        "top": str(limit),
        "skip": "0",
        # Some endpoints accept sort params (not guaranteed). If unsupported, harmlessly ignored:
        # "orderby": "pub_date desc"
    }
    r = requests.get(API_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def fetch() -> List[Dict[str, str]]:
    """
    Fast fetch: single call (~200 latest rows), normalize, return.
    """
    try:
        payload = _get_latest(TOP)
    except Exception as e:
        print(f"[worldbank] fetch error: {e}")
        return []

    rows = payload.get("data") or []
    out: List[Dict[str, str]] = []
    seen = set()

    for row in rows:
        norm = _normalize_row(row)
        sig = hashlib.sha1(f"{norm.get('title','')}|{norm.get('url','')}|{norm.get('deadline','')}".encode("utf-8")).hexdigest()
        if sig in seen:
            continue
        out.append(norm)
        seen.add(sig)

    return out

if __name__ == "__main__":
    items = fetch()
    print(f"Fetched {len(items)} World Bank items (latest batch).")
    for it in items[:5]:
        print("-", it["title"], "|", it["deadline"], "|", it["url"])
