# connectors/worldbank.py
# Real World Bank procurement connector using the public Search API.
# Docs (reference): World Bank Search API v2 (procurements endpoint).
# Output shape: list of dicts with keys: title, url, deadline, summary, region, themes

import hashlib
import json
import time
from datetime import datetime
from typing import Dict, List
import requests

# --------- Config ---------
WB_API = "https://search.worldbank.org/api/v2/procurements"
# Governance-relevant keyword buckets (feel free to edit)
QUERY_TERMS = [
    "beneficial ownership OR transparency OR anti-corruption",
    "budget OR public finance OR fiscal transparency",
    "digital governance OR data protection OR cybersecurity OR AI",
    "open parliament OR legislative transparency",
    "climate finance OR MRV OR adaptation OR resilience"
]
# Regions/countries to bias (simple post-filter)
REGION_KEYWORDS = [
    "Africa", "Middle East", "North Africa", "MENA",
    # country names (add as needed)
    "Morocco", "Benin", "Cote d'Ivoire", "Côte d’Ivoire", "Senegal",
    "Ghana", "Liberia", "Tunisia", "Jordan", "Burkina Faso"
]

# --------- Helpers ---------
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
    return list(dict.fromkeys(themes))[:3]  # unique, max 3

def _region_from_text(text: str) -> str:
    for k in REGION_KEYWORDS:
        if k.lower() in text.lower():
            # coarse label
            if k in ["Africa"] or k in ["Morocco","Benin","Cote d'Ivoire","Côte d’Ivoire","Senegal","Ghana","Liberia","Burkina Faso"]:
                return "Africa"
            if k in ["Middle East","North Africa","MENA","Tunisia","Jordan"]:
                return "MENA"
    return ""  # unknown

def _normalize_notice(n: Dict) -> Dict:
    """
    WB API returns notices under dict keyed by an ID:
    {
      "notice_no": "...",
      "title": "...",
      "url": "...",
      "procurement_method": "...",
      "pub_date": "2025-08-28",
      "deadline": "2025-09-30",
      "description": "...",
      "countryname": "Morocco",
      ...
    }
    """
    title = (n.get("title") or "").strip()
    url = (n.get("url") or "").strip()
    deadline = (n.get("deadline") or "").strip()
    desc = (n.get("description") or "").strip()
    country = (n.get("countryname") or "").strip()
    combo = " ".join([title, desc, country])
    themes = ",".join(_themes_from_text(combo))
    region = _region_from_text(" ".join([country, n.get("regionname","") or "", desc]))

    # Normalize date to YYYY-MM-DD if possible
    if deadline:
        try:
            # WB may return like '2025-09-30' or '30-Sep-2025' depending on notice
            dt = None
            for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y"):
                try:
                    dt = datetime.strptime(deadline, fmt)
                    break
                except Exception:
                    pass
            if dt:
                deadline = dt.date().isoformat()
            else:
                # leave as given if unknown format
                pass
        except Exception:
            pass

    return {
        "title": title or "World Bank procurement notice",
        "url": url,
        "deadline": deadline,
        "summary": desc[:500],
        "region": region or ( "Africa" if (country and country in REGION_KEYWORDS) else "" ),
        "themes": themes or ""
    }

def _fetch_page(qterm: str, rows: int = 50, start: int = 0) -> Dict:
    # qterm: search string
    # rows: number of results
    # start: pagination offset
    params = {
        "format": "json",
        "qterm": qterm,
        "rows": rows,
        "fl": "title,url,deadline,description,countryname,regionname,pub_date"  # fields list
    }
    if start:
        params["start"] = start
    r = requests.get(WB_API, params=params, timeout=30)
    r.raise_for_st
