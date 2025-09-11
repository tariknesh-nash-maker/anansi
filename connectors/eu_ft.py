# connectors/eu_ft.py
# EU F&T (TED v3 Search API) connector
# - Uses POST /v3/notices/search with a non-empty `fields` list (required)
# - Parses `notices` and `totalNoticeCount` (current v3 response shape)
# - Emits useful diagnostics & drops JSON samples into ./debug on first page

from __future__ import annotations
from typing import List, Dict, Any
from datetime import date, timedelta
import os, json, html, logging, requests

log = logging.getLogger(__name__)

TED_URL = "https://api.ted.europa.eu/v3/notices/search"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)"),
}

# Minimal but useful fields. You can override via env: EUFT_FIELDS="publication-number,notice-title,publication-date"
DEFAULT_FIELDS = [
    "publication-number",
    "notice-title",
    "buyer-name",
    "country",
    "place-of-performance",
    "deadline-received-tenders",
    "notice-type",
]
_env_fields = [x.strip() for x in os.getenv("EUFT_FIELDS", "").split(",") if x.strip()]
FIELDS = _env_fields or DEFAULT_FIELDS

def _dump(name: str, content: str | dict) -> None:
    try:
        os.makedirs("debug", exist_ok=True)
        path = os.path.join("debug", name)
        with open(path, "w", encoding="utf-8") as f:
            if isinstance(content, (dict, list)):
                json.dump(content, f, ensure_ascii=False, indent=2)
            else:
                f.write(content)
    except Exception:
        pass

def _since_query(since_days: int | None) -> str:
    if since_days is None:
        return ""
    cutoff = date.today() - timedelta(days=since_days)
    return f"publication-date>={cutoff:%Y%m%d}"

def _normalize_date(val: Any) -> str | None:
    if not val:
        return None
    s = str(val).strip()
    # Accept forms like 20250930 or 2025-09-30Z
    if s.isdigit() and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    if "T" in s:
        return s.split("T", 1)[0]
    return s

def _guess_topic(title: str | None) -> str | None:
    t = (title or "").lower()
    if any(k in t for k in ("audit", "internal audit", "pfm", "budget")):
        return "Fiscal Openness"
    if any(k in t for k in ("digital", "data", "ict", "software", "information system")):
        return "Digital Governance"
    if any(k in t for k in ("open data", "transparency", "participation", "integrity", "anti-corruption", "citizen")):
        return "Open Government"
    return None

def _normalize_notice(n: dict) -> Dict[str, Any] | None:
    # Some responses put values directly on the notice; others under "fields"
    f = n.get("fields") or n
    pubno = (f.get("publication-number") or "").strip()
    title = (f.get("notice-title") or "").strip()
    if not (pubno or title):
        return None
    url = f"https://ted.europa.eu/en/notice/-/detail/{pubno}" if pubno else None
    deadline = _normalize_date(f.get("deadline-received-tenders"))
    country = (f.get("country") or f.get("place-of-performance") or "").strip() or None
    return {
        "source": "EU F&T (TED)",
        "title": html.unescape(title or f"TED notice {pubno}").strip(),
        "country": country,
        "deadline": deadline,          # keep key name 'deadline' for aggregator
        "url": url,
        "topic": _guess_topic(title),
        "summary": title.lower(),
    }

def fetch(ogp_only: bool = True, since_days: int | None = 90, pages: int = 1, limit: int = 40, **kwargs):
    # Build expert query; don’t send empty query (server accepts but pointless)
    q_base = _since_query(since_days)
    user_q = os.getenv("EUFT_QUERY", "").strip()
    if user_q:
        query = user_q
    else:
        # Add a broad FT() filter if ogp_only to avoid huge result sets
        ft = "(FT=(open OR governance OR transparency OR procurement OR audit OR digital))"
        query = f"{ft} AND ({q_base})" if (ogp_only and q_base) else (ft if ogp_only else (q_base or "FT=procurement"))

    scope = os.getenv("EUFT_SCOPE", "ACTIVE")  # ACTIVE | LATEST | ALL
    results: List[Dict[str, Any]] = []
    total = None

    for page in range(1, max(1, pages) + 1):
        body = {
            "query": query,
            "fields": FIELDS,
            "page": page,
            "limit": min(limit, 250),
            "scope": scope,
            "checkQuerySyntax": False,
            "paginationMode": "PAGE_NUMBER",
        }
        r = requests.post(TED_URL, headers=HEADERS, json=body, timeout=40)
        log.info("[eu_ft:req] query=%r page=%d limit=%d http=%d bytes=%d", query, page, limit, r.status_code, len(r.content))
        r.raise_for_status()

        try:
            data = r.json()
        except Exception:
            _dump("euft_raw_response.txt", r.text[:20000])
            log.warning("[eu_ft] JSON decode failed; wrote debug/euft_raw_response.txt")
            break

        # v3 shape
        notices = data.get("notices") or []
        total = data.get("totalNoticeCount")
        log.info("[eu_ft:parsed] page=%d rows=%d total=%s keys=%s", page, len(notices), total, list(data.keys())[:8])

        if page == 1:
            if len(notices) == 0:
                _dump("euft_debug_top_level.json", data)
            else:
                _dump("euft_debug_first_notice.json", notices[0])

        for n in notices:
            item = _normalize_notice(n)
            if item:
                results.append(item)

        if not notices:  # stop if page is empty
            break

    # Soft preference for OGP topics but never zero-out
    if ogp_only and results:
        preferred = [it for it in results if it.get("topic")]
        results = preferred or results

    log.info("[eu_ft:norm] returned=%d total=%s", len(results), total)
    return results

def accepted_args():
    return ["ogp_only", "since_days"]

class Connector:
    def fetch(self, days_back: int = 90):
        return fetch(ogp_only=True, since_days=days_back)
