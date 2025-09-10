# connectors/eu_ft.py
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

def _build_default_query(days_back: int) -> str:
    # safest broad window by publication date; runner can override via EUFT_QUERY
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days_back)
    return f"publication-date>={_yyyymmdd(datetime(cutoff.year, cutoff.month, cutoff.day))}"

def _normalize(row: Dict[str, Any]) -> Dict[str, Any] | None:
    pub_no = (row.get("publication-number") or row.get("publicationNumber") or row.get("publication_number") or "").strip()
    if not pub_no:
        return None
    title = (row.get("notice-title") or row.get("title") or f"TED Notice {pub_no}").strip()
    url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"
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
        "deadline": None,
        "country": "",
        "topic": topic,
        "url": url,
        "summary": title.lower(),
    }

def _post(query: str, page: int, limit: int, debug: bool) -> List[Dict[str, Any]]:
    payload = {
        "query": query,  # must not be empty
        "page": page,
        "limit": limit,
        "paginationMode": "PAGE_NUMBER",
        "checkQuerySyntax": False,
    }
    r = SESSION.post(API_URL, data=json.dumps(payload), timeout=45)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        if debug:
            logging.warning(f"[eu_ft] HTTP {r.status_code} for query='{query}' body={r.text[:280]}")
        return []
    data = r.json() or {}
    rows = data.get("results") or data.get("items") or []
    if debug:
        logging.info(f"[eu_ft] query='{query}' got={len(rows)} total={data.get('total')}")
    return rows

def _eu_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = _env_bool("EUFT_DEBUG", False)
    limit = min(_env_int("EUFT_MAX", 40), 250)
    page = 1

    # 1) primary query (env-overridable)
    primary_q = os.getenv("EUFT_QUERY") or _build_default_query(days_back)
    rows = _post(primary_q, page, limit, debug)
    items = [n for r in rows if (n := _normalize(r))]

    # 2) if empty, try two known-good variants (don’t fail silently)
    if not items:
        variants = [
            primary_q.replace("publication-date", "PD"),
            "place-of-performance IN (LUX)",  # narrow but known to return something
        ]
        for q in variants:
            rows = _post(q, page, limit, debug)
            candidates = [n for r in rows if (n := _normalize(r))]
            if candidates:
                items = candidates
                break

    # 3) soft OGP preference (never zero-out)
    if ogp_only and items:
        preferred = [it for it in items if it.get("topic")]
        items = preferred or items

    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _eu_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _eu_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
