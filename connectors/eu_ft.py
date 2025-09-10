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

def _post(query: str, page: int, limit: int, verbose: bool) -> list[dict]:
    payload = {
        "query": query,  # REQUIRED
        "page": page,
        "limit": limit,
        "paginationMode": "PAGE_NUMBER",
        "checkQuerySyntax": False,
    }
    r = SESSION.post(API_URL, data=json.dumps(payload), timeout=45)
    if verbose:
        kv("eu_ft:req", query=query, page=page, limit=limit, http=r.status_code, bytes=len(r.text or ""))
        dump_json("euft-payload", payload)
        dump_text("euft-response", r.text[:4000])
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print(f"[eu_ft] HTTP {r.status_code}: {e} body={r.text[:300]}")
        return []
    data = {}
    try:
        data = r.json() or {}
    except Exception:
        if verbose:
            print("[eu_ft] WARN: response not JSON-decodable")
    rows = data.get("results") or data.get("items") or []
    if verbose:
        kv("eu_ft:parsed", rows=len(rows), total=data.get("total"))
        dump_json("euft-json", data)
    return rows

def _eu_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    # Turn on verbose if env says so OR if we end up with zero results
    env_verbose = is_on("EUFT_DEBUG", "DEBUG")
    limit = min(_env_int("EUFT_MAX", 40), 250)
    page = 1

    primary_q = os.getenv("EUFT_QUERY") or _build_query(days_back)
    variants = [primary_q, primary_q.replace("publication-date", "PD"), "place-of-performance IN (LUX)"]

    items: List[Dict[str, Any]] = []
    last_rows = 0
    for idx, q in enumerate(variants):
        rows = _post(q, page, limit, verbose=env_verbose)
        last_rows = len(rows)
        normed = [n for r in rows if (n := _normalize(r))]
        if env_verbose:
            kv("eu_ft:norm", variant=idx, normed=len(normed))
        if normed:
            items = normed
            break

    # If still zero, force verbose one more time so logs show up even without env flags
    if not items and not env_verbose:
        kv("eu_ft:empty", tried=len(variants), last_rows=last_rows, forcing_verbose=True)
        # re-run the first variant just to print diagnostics
        _post(variants[0], page, limit, verbose=True)

    # Soft OGP preference (don’t zero-out)
    if ogp_only and items:
        preferred = [it for it in items if it.get("topic")]
        if env_verbose:
            kv("eu_ft:ogp", raw=len(items), preferred=len(preferred))
        items = preferred or items

    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _eu_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _eu_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
