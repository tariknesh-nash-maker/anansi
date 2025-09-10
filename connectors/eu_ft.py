# connectors/eu_ft.py
# EU: Tenders Electronic Daily (TED) Search API v3
# - Anonymous access
# - Expert-query filters by publication date (PD) per TED help
# - Builds official notice URLs from publication number
#
# Env:
#   EUFT_SINCE_DAYS (default 60)
#   EUFT_MAX (default 40)
#   EUFT_DEBUG (0/1)

from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os, json, html
import requests

API = "https://api.ted.europa.eu/v3/notices/search"
UA  = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA, "Content-Type": "application/json"}

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

def _pick(d: Dict[str, Any], *keys):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return None

def _eu_fetch(days_back: int = 60, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = _env_bool("EUFT_DEBUG", False)
    since = datetime.utcnow().date() - timedelta(days=days_back)
    # TED "expert query" — PD is an alias of publication-date
    # docs confirm PD / publication-date are valid equivalents.  (see citations)
    query = f"PD>={_yyyymmdd(datetime(since.year, since.month, since.day))}"

    out: List[Dict[str, Any]] = []
    page = 1
    limit = min(_env_int("EUFT_MAX", 40), 250)  # TED per-page hard cap is 250

    while True:
        body = {
            "query": query,
            # omit "fields" entirely to avoid 400 with unsupported names
            "page": page,
            "limit": limit,
            "paginationMode": "PAGE_NUMBER",
            "checkQuerySyntax": False,
        }
        try:
            r = requests.post(API, headers=HEADERS, data=json.dumps(body), timeout=45)
            r.raise_for_status()
            data = r.json() or {}
        except Exception as e:
            if debug:
                print(f"[eu_ft] WARN request failed page={page}: {e}")
            break

        results = data.get("results") or data.get("items") or []
        if debug:
            total = data.get("total")
            print(f"[eu_ft] page={page} got={len(results)} total={total}")

        if not results:
            break

        for row in results:
            # The key names vary; try several common variants
            pub_no = (
                _pick(row, "publication-number", "publicationNumber", "publication_number")
                or ""
            ).strip()
            title = (
                _pick(row, "notice-title", "title", "ND-Title") or "EU opportunity"
            ).strip()

            # If no publication number, we can’t build the official URL — skip
            if not pub_no:
                continue

            # Official notice URL pattern per TED help:
            # https://ted.europa.eu/{lang}/notice/-/detail/{publication-number}
            url = f"https://ted.europa.eu/en/notice/-/detail/{pub_no}"

            out.append({
                "title": html.unescape(title),
                "source": "EU TED",
                "deadline": None,          # can be added later if we request more fields
                "country": "",
                "topic": None,
                "url": url,
                "summary": f"{title} {pub_no}".lower(),
            })

        # Stop after first page: we only need fresh items
        break

    # Light OGP filtering only if your filters.py is present (never zero-out)
    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            preferred = [it for it in out if ogp_relevant(f"{it['title']} {it.get('summary','')}")]
            out = preferred or out
            out = [it for it in out if not is_excluded(f"{it['title']} {it.get('summary','')}")]
        except Exception:
            pass

    return out

class Connector:
    def fetch(self, days_back: int = 60):
        return _eu_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 60, **kwargs):
    return _eu_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
