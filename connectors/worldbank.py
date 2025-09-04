# connectors/worldbank.py
# World Bank "Procurement Notices" — keep only notices published in the last 90 days

from __future__ import annotations
import os, hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

API = "https://search.worldbank.org/api/procnotices"
TIMEOUT = 25
ROWS = int(os.getenv("WB_ROWS", "100"))     # per page
PAGES = int(os.getenv("WB_PAGES", "10"))    # widen via env if needed
DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# date formats incl. ISO-8601 + WB timestamp strings
DATE_FMTS = (
    "%Y-%m-%d",
    "%d-%b-%Y", "%d %b %Y",
    "%m/%d/%Y", "%Y/%m/%d",
    "%Y.%m.%d",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S.%f",
)

# filtering
DENY_TYPES = {"contract award", "award"}
DENY_STATUS = {"draft"}

# publication window (default 90 days)
PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))
PUB_CUTOFF = datetime.utcnow() - timedelta(days=PUB_WINDOW_DAYS)
MAX_RESULTS = 40

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
    base = f"{item.get('title','')}|{item.get('url','')}|{item.get('_pub','')}"
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
    title = (n.get("project_name") or n.get("bid_description") or "").strip()
    desc  = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country = (n.get("project_ctry_name") or "").strip()
    region0 = (n.get("regionname") or "").strip()

    pub_iso = _to_iso(n.get("noticedate") or n.get("pub_date") or n.get("publication_date") or n.get("posting_date"))

    nid = str(n.get("id") or "").strip()
    public_detail = f"https://projects.worldbank.org/en/projects-operations/procurement-detail/{nid}" if nid else ""
    api_detail = f"https://search.worldbank.org/api/procnotices?id={nid}" if nid else ""
    url_from_
