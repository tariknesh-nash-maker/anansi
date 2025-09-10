# connectors/eu_ft.py
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import os, json, html, logging, requests

from utils.debug_utils import is_on, dump_json, dump_text, kv

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

def _build_query(days_back: int) -> str:
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

def _post(query: str, page: int, limit: int, debug: bool) -> list[dict]:
    payload = {
        "query": query,  # MUST NOT be empty
        "page": page,
        "limit": limit,
        "paginationMode": "PAGE_NUMBER",
        "checkQuerySyntax": False,
    }
    if debug:
        kv("eu_ft:req", query=query, page=page, limit=limit)
        dump_json("euft-payload", payload)
    r = SESSION.post(API_URL, data=json.dumps(payload), timeout=45)
    if debug:
        dump_text("euft-response", r.text[:2000])
        kv("eu_ft:http", status=r.status_code, bytes=len(r.text or ""))
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        if debug:
            logging.warning(f"[eu_ft] HTTP {r.status_code}: {e} body={r.text[:300]}")
        return []
    data = r.json() or {}
    rows = data.get("results") or data.get("items") or []
    if debug:
        kv("eu_ft:parsed", rows=len(rows), total=data.get("total"))
        dump_json("euft-json", data)
    return rows

def _apply_soft_ogp(items: List[Dict[str, Any]], debug: bool) -> List[Dict[str, Any]]:
    raw = len(items)
    preferred = [it for it in items if it.get("topic")]
    out = preferred or items
    if debug:
        kv("eu_ft:filter", raw=raw, ogp_kept=len(preferred), returned=len(out))
    return out

def _eu_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = is_on("EUFT_DEBUG", "DEBUG")
    limit = min(_env_int("EUFT_MAX", 40), 250)
    page = 1

    primary_q = os.getenv("EUFT_QUERY") or _build_query(days_back)
    variants = [primary_q, primary_q.replace("publication-date", "PD"), "place-of-performance IN (LUX)"]

    items: List[Dict[str, Any]] = []
    for q in variants:
        rows = _post(q, page, limit, debug)
        normed = [n for r in rows if (n := _normalize(r))]
        if debug:
            kv("eu_ft:norm", q=q, normed=len(normed))
        if normed:
            items = normed
            break

    if ogp_only and items:
        items = _apply_soft_ogp(items, debug)

    if debug and not items:
        kv("eu_ft:empty", tried=len(variants), q0=variants[0], q1=variants[1], q2=variants[2])

    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _eu_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _eu_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
