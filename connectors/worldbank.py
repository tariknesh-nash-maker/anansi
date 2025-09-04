# connectors/worldbank.py
# World Bank "Procurement Notice" connector via Finances One API (Dataset DS00979 / Resource RS00909)
# Fast version with client-side recency filter.

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests

API_BASE = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID = "DS00979"
RESOURCE_ID = "RS00909"
TIMEOUT = 25
TOP = 500                 # fetch a bigger slice, then filter client-side
RECENCY_DAYS = 540        # keep only items within last ~18 months

# ---------- helpers ----------

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
    out = []
    for th in themes:
        if th not in out:
            out.append(th)
    return out[:3]

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

def _to_iso_date(val: Optional[str]) -> str:
    dt = _parse_date(val)
    return dt.date().isoformat() if dt else (val.strip() if val else "")

def _pick(row: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in row and row[k]:
            return str(row[k]).strip()
    return ""

def _pick_pub_date(row: Dict[str, Any]) -> Optional[datetime]:
    # Try common publication/update date field names
    for key in [
        "pub_date", "publication_date", "published_on", "publish_date",
        "posting_date", "post_date", "date_published", "last_update", "updated_at"
    ]:
        dt = _parse_date(row.get(key))
        if dt:
            return dt
    return None

def _infer_region(text: str, region0: str) -> str:
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

def _normalize_row(row: Dict[str, Any]) -> Dict[str, str]:
    title    = _pick(row, ["title", "notice_title", "subject", "project_name", "tender_title"])
    desc     = _pick(row, ["description", "summary", "notice_description", "tender_description"])
    url      = _pick(row, ["url", "link", "source_url", "notice_url"])
    deadline_raw = _pick(row, ["deadline", "closing_date", "submission_deadline", "bid_deadline"])
    deadline = _to_iso_date(deadline_raw)
    country  = _pick(row, ["country", "country_name"])
    region0  = _pick(row, ["region", "region_name"])

    text = " ".join([title, desc, country, region0])
    themes = ",".join(_themes_from_text(text))
    region = _infer_region(text, region0)

    return {
        "title": title or "World Bank opportunity",
        "url": url,
        "deadline": deadline,
        "summary": (desc or "")[:500],
        "region": region,
        "themes": themes
    }

def _get_latest(limit: int = TOP) -> Dict[str, Any]:
    # Some params like ordering may be ignored; harmless if so.
    params = {
        "datasetId": DATASET_ID,
        "resourceId": RESOURCE_ID,
        "type": "json",
        "top": str(limit),
        "skip": "0",
        "orderby": "pub_date desc"
    }
    r = requests.get(API_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# ---------- main fetch ----------

def fetch() -> List[Dict[str, str]]:
    """
    Single-call fetch, then client-side filter:
      - keep only items with pub_date (or deadline) within RECENCY_DAYS
    """
    try:
        payload = _get_latest(TOP)
    except Exception as e:
        print(f"[worldbank] fetch error: {e}")
        return []

    rows = payload.get("data") or []
    out: List[Dict[str, str]] = []
    seen = set()

    now = datetime.utcnow()
    cutoff = now - timedelta(days=RECENCY_DAYS)

    dropped_old = 0
    for row in rows:
        pub_dt = _pick_pub_date(row)
        # Fallback to deadline if publication date missing
        if not pub_dt:
            dl_dt = _parse_date(_pick(row, ["deadline", "closing_date", "submission_deadline", "bid_deadline"]))
            pub_dt = dl_dt

        # If we still don't have a date, keep it (to be safe), else filter by recency
        if pub_dt and pub_dt < cutoff:
            dropped_old += 1
            continue

        norm = _normalize_row(row)
        sig = hashlib.sha1(f"{norm.get('title','')}|{norm.get('url','')}|{norm.get('deadline','')}".encode("utf-8")).hexdigest()
        if sig in seen:
            continue
        out.append(norm)
        seen.add(sig)

    if dropped_old and not out:
        print(f"[worldbank] filtered out {dropped_old} old items; 0 recent remain (adjust RECENCY_DAYS or ordering).")
    return out

if __name__ == "__main__":
    items = fetch()
    print(f"Fetched {len(items)} recent World Bank items.")
    for it in items[:5]:
        print("-", it["title"], "|", it["deadline"], "|", it["url"])
