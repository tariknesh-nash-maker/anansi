# connectors/eu_ft.py
# EU: Tenders Electronic Daily (TED) Search API v3
# - Always sends a non-empty expert query (publication-date>=YYYYMMDD)
# - Minimal payload (no brittle 'fields' that 400)
# - SOFT OGP gating (prefer matches, never zero-out)
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import os, json, html, logging
import requests

API_URL = "https://api.ted.europa.eu/v3/notices/search"
UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Content-Type": "application/json"})

def _yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else str(v).strip().lower() in ("1","true","yes")

def _build_query(days_back: int) -> str:
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days_back)
    return f"publication-date>={_yyyymmdd(datetime(cutoff.year, cutoff.month, cutoff.day))}"

def _normalize_item(row: Dict[str, Any]) -> Dict[str, Any] | None:
    # Use common variants defensively—API field names can differ
    pub_no = (row.get("publication-number") or row.get("publicationNumber") or row.get("publication_number") or "").strip()
    if not pub_no:
        return None
    title = (row.get("notice-title") or row.get("title") or f"TED Notice {pub_no}").strip()
    url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"
    # Soft topical hint for ogp_only preference
    text = title.lower()
    topic = None
    if any(k in text for k in ("digital", "data", "ict", "software", "information system")):
        topic = "Digital Governance"
    elif any(k in text for k in ("audit", "budget", "tax", "revenue", "pfm")):
        topic = "Fiscal Openness"
    elif any(k in text for k in ("open data", "transparency", "participation", "citizen", "integrity", "anti-corruption")):
        topic = "Open Government"
    return {
        "title": html.unescape(title),
        "source": "EU TED",
        "deadline": None,     # deadlines vary by form type—safe to leave None
        "country": "",
        "topic": topic,
        "url": url,
        "summary": title.lower(),
    }

def _prefer_or_fallback(preferred: List[Dict[str, Any]], fallback: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return preferred if preferred else fallback

def _eu_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = _env_bool("EUFT_DEBUG", False)
    query = _build_query(days_back)
    page = 1
    limit = min(_env_int("EUFT_MAX", 40), 250)
    items: List[Dict[str, Any]] = []

    # Try a couple of query variants to avoid returning 0 if one alias changes server-side
    query_variants = [
        query,
        query.replace("publication-date", "PD"),
        f"PD=[{query.split('>=')[1]}..99991231]"
    ]

    for q in query_variants:
        payload = {
            "query": q,  # REQUIRED (avoid 400)
            "page": page,
            "limit": limit,
            "paginationMode": "PAGE_NUMBER",
            "checkQuerySyntax": False,
            # omit 'fields' and 'scope' to keep payload universally valid
        }
        try:
            resp = SESSION.post(API_URL, data=json.dumps(payload), timeout=45)
            resp.raise_for_status()
            data = resp.json() or {}
            rows = data.get("results") or data.get("items") or []
            if debug:
                total = data.get("total")
                logging.info(f"[eu_ft] query='{q}' got={len(rows)} total={total}")
        except requests.HTTPError as e:
            if debug:
                logging.warning(f"[eu_ft] HTTP {e.response.status_code}: {e} | body={e.response.text[:300] if e.response is not None else ''}")
            rows = []
        except Exception as e:
            if debug:
                logging.warning(f"[eu_ft] request failed for q='{q}': {e}")
            rows = []

        normed = []
        for r in rows:
            n = _normalize_item(r)
            if n: normed.append(n)

        if normed:  # success path on first non-empty variant
            if ogp_only:
                preferred = [it for it in normed if it.get("topic")]
                items = _prefer_or_fallback(preferred, normed)
            else:
                items = normed
            break

    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _eu_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _eu_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
