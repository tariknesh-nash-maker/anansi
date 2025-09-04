# connectors/worldbank.py
# Robust World Bank connector with fallback.
# 1) Tries FinancesOne DS00979/RS00909 (fast slice)
# 2) Falls back to Search API v2 (/api/v2/procurements) if no rows
#
# Output: list[{title, url, deadline, summary, region, themes}]

from __future__ import annotations
import os, hashlib, time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

DEBUG = os.getenv("WB_DEBUG", "0") == "1"

# ---- FinancesOne settings ----
FONE_API = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID = "DS00979"
RESOURCE_ID = "RS00909"
FONE_TOP = 300
TIMEOUT = 25

# ---- Search API fallback ----
SEARCH_API = "https://search.worldbank.org/api/v2/procurements"
QTERMS = [
    "transparency OR anti-corruption OR beneficial ownership",
    "budget OR public finance OR fiscal transparency",
    "digital governance OR data protection OR cybersecurity OR AI",
    "open parliament OR legislative transparency",
    "climate finance OR MRV OR adaptation OR resilience",
]

# Keep anything recent; if no dates, we still keep (MVP)
RECENCY_DAYS = 720
DATE_FMTS = ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d")

# ---------- helpers ----------
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

# ---------- FinancesOne path ----------
def _fone_get(limit: int = FONE_TOP) -> Dict[str, Any]:
    params = {
        "datasetId": DATASET_ID,
        "resourceId": RESOURCE_ID,
        "type": "json",
        "top": str(limit),
        "skip": "0",
        # "orderby": "pub_date desc"  # harmless if ignored
    }
    r = requests.get(FONE_API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _pick(row: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in row and row[k]:
            return str(row[k]).strip()
    return ""

def _fone_normalize(row: Dict[str, Any]) -> Dict[str, str]:
    title    = _pick(row, ["title", "notice_title", "subject", "project_name", "tender_title"])
    desc     = _pick(row, ["description", "summary", "notice_description", "tender_description"])
    url      = _pick(row, ["url", "link", "source_url", "notice_url"])
    deadline = _to_iso(_pick(row, ["deadline", "closing_date", "submission_deadline", "bid_deadline"]))
    country  = _pick(row, ["country", "country_name"])
    region0  = _pick(row, ["region", "region_name"])
    text = " ".join([title, desc, country, region0])
    region = _infer_region(text, region0)
    themes = ",".join(_themes_from_text(text))
    return {
        "title": title or "World Bank opportunity",
        "url": url,
        "deadline": deadline,
        "summary": (desc or "")[:500],
        "region": region,
        "themes": themes
    }

def _try_fone() -> List[Dict[str, str]]:
    try:
        payload = _fone_get(FONE_TOP)
        rows = payload.get("data") or []
        if DEBUG:
            print(f"[worldbank] FONE rows: {len(rows)} keys: {list(payload.keys())[:5]}")
        out, seen = [], set()
        cutoff = datetime.utcnow() - timedelta(days=RECENCY_DAYS)
        dropped = 0
        for row in rows:
            item = _fone_normalize(row)
            # recency using deadline if any
            d = _parse_date(item.get("deadline"))
            if d and d < cutoff:
                dropped += 1
                continue
            s = _sig(item)
            if s not in seen:
                out.append(item); seen.add(s)
        if DEBUG:
            print(f"[worldbank] FONE kept {len(out)} (dropped old: {dropped})")
        return out
    except Exception as e:
        if DEBUG:
            print(f"[worldbank] FONE error: {e}")
        return []

# ---------- Search API fallback ----------
def _search_page(q: str, rows: int = 60, start: int = 0) -> Dict[str, Any]:
    params = {
        "format": "json",
        "qterm": q,
        "rows": rows,
        "fl": "title,url,deadline,description,countryname,regionname,pub_date"
    }
    if start:
        params["start"] = start
    r = requests.get(SEARCH_API, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _search_normalize(n: Dict[str, Any]) -> Dict[str, str]:
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

def _try_search(max_items: int = 200) -> List[Dict[str, str]]:
    out, seen = [], set()
    cutoff = datetime.utcnow() - timedelta(days=RECENCY_DAYS)
    for q in QTERMS:
        try:
            # grab first page only (fast); increase if needed
            data = _search_page(q, rows=80, start=0)
            procs = data.get("procurements") or {}
            rows = list(procs.values())
            if DEBUG:
                print(f"[worldbank] SEARCH q='{q}' -> rows {len(rows)}")
            for n in rows:
                item = _search_normalize(n)
                d = _parse_date(item.get("deadline"))
                if d and d < cutoff:
                    continue
                s = _sig(item)
                if s in seen:
                    continue
                out.append(item); seen.add(s)
                if len(out) >= max_items:
                    return out
            time.sleep(0.3)
        except Exception as e:
            if DEBUG:
                print(f"[worldbank] SEARCH error for q='{q}': {e}")
            continue
    return out

# ---------- main ----------
def fetch() -> List[Dict[str, str]]:
    # 1) Try FinancesOne
    items = _try_fone()
    if items:
        return items
    # 2) Fallback to Search API
    items = _try_search()
    if DEBUG:
        print(f"[worldbank] FALLBACK returned {len(items)} items")
    return items

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    its = fetch()
    print(f"Fetched {len(its)} World Bank items.")
    for it in its[:5]:
        print("-", it["title"], "|", it["deadline"], "|", it["url"])
