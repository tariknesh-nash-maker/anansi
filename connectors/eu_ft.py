# connectors/eu_ft.py
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import os, json, html, requests

# --- minimal debug helpers (no external dep) ---
def _is_on(*envs: str) -> bool:
    for e in envs:
        v = os.getenv(e)
        if v and str(v).strip().lower() in ("1","true","yes","on"): return True
    return False
def _kv(prefix: str, **kw):
    print(f"[{prefix}] " + " ".join(f"{k}={repr(v)}" for k,v in kw.items()))

API_URL = "https://api.ted.europa.eu/v3/notices/search"
UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")

# Default fields per official demo (Publication no., Title, Buyer, Type, Pub date, Place)
DEFAULT_FIELDS = [
    "publication-number", "notice-title", "buyer-name",
    "notice-type", "publication-date", "place-of-performance"
]
# Allow override via env (comma-separated)
_env_fields = [x.strip() for x in os.getenv("EUFT_FIELDS","").split(",") if x.strip()]
FIELDS = _env_fields or DEFAULT_FIELDS

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Content-Type": "application/json",
    "Accept": "application/json",
})

def _yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y%m%d")

def _build_query(days_back: int) -> str:
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days_back)
    # v3 supports expert query syntax like "publication-date>=YYYYMMDD"
    return f"publication-date>={_yyyymmdd(datetime(cutoff.year, cutoff.month, cutoff.day))}"

def _normalize(row: Dict[str, Any]) -> Dict[str, Any] | None:
    pub_no = (row.get("publication-number") or "").strip()
    if not pub_no:
        return None
    title = (row.get("notice-title") or f"TED Notice {pub_no}").strip()
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
        "query": query,                      # REQUIRED
        "fields": FIELDS,                    # REQUIRED (fix for 400)
        "page": page,
        "limit": limit,
        "scope": os.getenv("EUFT_SCOPE","ACTIVE"),      # ACTIVE | LATEST | ALL
        "checkQuerySyntax": False,
        "paginationMode": "PAGE_NUMBER",
    }
    r = SESSION.post(API_URL, data=json.dumps(payload), timeout=45)
    if verbose or r.status_code >= 400:
        _kv("eu_ft:req", query=query, page=page, limit=limit, http=r.status_code, bytes=len(r.text or ""))
        if r.status_code >= 400:
            print(f"[eu_ft] HTTP {r.status_code}: {r.text[:300]}")
    try:
        r.raise_for_status()
    except requests.HTTPError:
        return []
    try:
        data = r.json() or {}
    except Exception:
        print("[eu_ft] WARN: response not JSON")
        return []
    rows = data.get("results") or data.get("items") or []
    if verbose:
        _kv("eu_ft:parsed", rows=len(rows), total=data.get("total"))
    return rows

def _eu_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    verbose = _is_on("EUFT_DEBUG","DEBUG")
    limit = min(int(os.getenv("EUFT_MAX","40")), 250)
    page = 1

    base_q = os.getenv("EUFT_QUERY") or _build_query(days_back)
    variants = [base_q, base_q.replace("publication-date","PD")]  # PD is accepted alias in demos

    items: List[Dict[str, Any]] = []
    for idx, q in enumerate(variants):
        rows = _post(q, page, limit, verbose=verbose)
        normed = [n for r in rows if (n := _normalize(r))]
        if verbose: _kv("eu_ft:norm", variant=idx, normed=len(normed))
        if normed:
            items = normed
            break

    if not items and not verbose:
        # force one diagnostic emission if still empty
        _post(base_q, page, limit, verbose=True)

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
