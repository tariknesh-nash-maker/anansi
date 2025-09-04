# connectors/worldbank.py
# World Bank connector via Socrata API (generic).
# Reads env: WB_SOCRATA_DOMAIN, WB_DATASET_ID, WB_APP_TOKEN (optional)
#
# Output: list[dict] with keys: title, url, deadline, summary, region, themes

from __future__ import annotations
import os, time, hashlib
from typing import List, Dict
from datetime import datetime
import requests

SOCRATA_DOMAIN = os.getenv("WB_SOCRATA_DOMAIN", "finances.worldbank.org")  # e.g., finances.worldbank.org
DATASET_ID     = os.getenv("WB_DATASET_ID", "")  # e.g., 'abcd-1234'  <-- fill this
APP_TOKEN      = os.getenv("WB_APP_TOKEN", "")   # optional but helps with rate limits

# ---- Tuning (edit as you like) ----
# Simple governance-relevant keyword buckets
KEYWORDS = [
    "beneficial ownership", "transparency", "anti-corruption", "integrity",
    "budget", "public finance", "fiscal transparency",
    "digital governance", "data protection", "cybersecurity", "AI",
    "open parliament", "legislative transparency",
    "climate finance", "MRV", "adaptation", "resilience"
]
# Region filter: keep broad, we’ll match in text and also country/region fields if present
REGION_HINTS = [
    "africa", "west africa", "east africa", "southern africa", "north africa",
    "mena", "middle east", "maghreb", "sahel",
    # add country names to strengthen match:
    "morocco", "tunisia", "algeria", "egypt", "jordan",
    "benin", "cote d'ivoire", "côte d’ivoire", "senegal", "ghana", "liberia",
    "burkina faso", "niger", "mali", "togo", "mauritania", "sierra leone"
]

# Map dataset field names -> normalized fields (adjust after you inspect the dataset)
# TIP: after you identify the dataset, print one row and update these fields to fit.
FIELD_MAP = {
    # try common field guesses; you will refine after you see the dataset schema
    "title":         ["title", "notice_title", "tender_title", "project_name", "subject"],
    "description":   ["description", "notice_description", "summary", "tender_description"],
    "deadline":      ["bid_deadline", "deadline", "submission_deadline", "closing_date"],
    "url":           ["url", "source_url", "notice_url", "link"],
    "country":       ["country", "country_name"],
    "region":        ["region", "region_name"]
}

def _pick(record: Dict, keys: list[str]) -> str:
    for k in keys:
        if k in record and record[k]:
            return str(record[k])
    return ""

def _themes_from_text(text: str) -> List[str]:
    t = text.lower()
    themes = []
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
    return list(dict.fromkeys(themes))[:3]

def _infer_region(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["mena", "middle east", "north africa", "maghreb"]):
        return "MENA"
    if "africa" in t or any(k in t for k in [
        "west africa","east africa","southern africa","sahel",
        "morocco","tunisia","algeria","egypt","jordan",
        "benin","cote d'ivoire","côte d’ivoire","senegal","ghana","liberia","burkina faso",
        "niger","mali","togo","mauritania","sierra leone"
    ]):
        return "Africa"
    return ""

def _to_iso_date(val: str) -> str:
    if not val:
        return ""
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(val.strip(), fmt).date().isoformat()
        except Exception:
            pass
    # leave as-is if we can’t parse
    return val.strip()

def _normalize(record: Dict) -> Dict:
    title   = _pick(record, FIELD_MAP["title"]).strip()
    desc    = _pick(record, FIELD_MAP["description"]).strip()
    deadline= _to_iso_date(_pick(record, FIELD_MAP["deadline"]))
    url     = _pick(record, FIELD_MAP["url"]).strip()
    country = _pick(record, FIELD_MAP["country"]).strip()
    region0 = _pick(record, FIELD_MAP["region"]).strip()

    text = " ".join([title, desc, country, region0])
    themes = ",".join(_themes_from_text(text))
    region = _infer_region(text) or region0

    return {
        "title": title or "World Bank opportunity",
        "url": url,
        "deadline": deadline,
        "summary": desc[:500],
        "region": region,
        "themes": themes
    }

def _socrata_get(domain: str, dataset: str, where: str = "", limit: int = 2000, offset: int = 0, select: str = "") -> list[Dict]:
    url = f"https://{domain}/resource/{dataset}.json"
    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    params = {"$limit": limit, "$offset": offset}
    if where:
        params["$where"] = where
    if select:
        params["$select"] = select
    r = requests.get(url, params=params, headers=headers, timeout=30)
    if r.status_code == 429:
        # rate-limited; back off and retry once
        time.sleep(2.0)
        r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch() -> List[Dict]:
    """
    Pulls rows from the given Socrata dataset and returns normalized items.
    Start wide: no server-side filtering; do client filtering/normalization first.
    """
    if not DATASET_ID:
        # Not configured yet; return empty so the pipeline doesn't crash.
        print("[worldbank] WB_DATASET_ID not set; returning empty list.")
        return []

    out: List[Dict] = []
    offset = 0
    page   = 500  # Socrata default limits; adjust if needed
    seen   = set()

    # Optional server-side WHERE example (uncomment after you know field names):
    # where = "upper(region) like '%AFRICA%' OR upper(region) like '%MIDDLE EAST%'"
    where = ""  # start with no filter; refine later when you know schema

    while True:
        rows = _socrata_get(SOCRATA_DOMAIN, DATASET_ID, where=where, limit=page, offset=offset)
        if not rows:
            break
        for rec in rows:
            norm = _normalize(rec)
            # Basic keyword/region post-filter for Africa/MENA relevance
            text = " ".join([norm["title"], norm["summary"], norm["region"]]).lower()
            if any(k in text for k in REGION_HINTS) or any(k in text for k in KEYWORDS):
                sig = hashlib.sha1(f"{norm['title']}|{norm['url']}|{norm['deadline']}".encode("utf-8")).hexdigest()
                if sig not in seen:
                    out.append(norm)
                    seen.add(sig)
        offset += len(rows)
        if len(rows) < page:
            break

    return out
