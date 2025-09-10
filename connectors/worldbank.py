# connectors/world_bank.py
# Robust World Bank connector (classic consultants API) with soft filtering
# - Compatible with function-style imports (`fetch(...)`) and class-style (`Connector().fetch(...)`)
# - Respects your env knobs but NEVER zeros out results just because dates or topics are missing

from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os, time
import requests
import dateparser

BASE = "https://search.worldbank.org/api/consultants"
UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA}

# --------------------- helpers ---------------------

def _to_iso(s: str | None) -> str | None:
    if not s:
        return None
    dt = dateparser.parse(s, settings={"DATE_ORDER": "DMY", "PREFER_DAY_OF_MONTH": "first"})
    return dt.date().isoformat() if dt else None

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes")

def _rows_cap(default: int, env_name: str, max_allowed: int = 200) -> int:
    try:
        v = int(os.getenv(env_name, str(default)))
    except Exception:
        v = default
    return max(1, min(v, max_allowed))  # WB API dislikes very large pages

def _safe_docs(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    # API returns various shapes; normalize to a list of dicts
    if isinstance(j.get("documents"), dict):
        return list(j["documents"].values())
    for k in ("documents", "rows", "items"):
        if isinstance(j.get(k), list):
            return j[k]
    if isinstance(j.get("hits"), dict):
        return list(j["hits"].values())
    return []

def _prefer_or_fallback(preferred: List[Dict[str, Any]], fallback: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return preferred if preferred else fallback

# --------------------- core impl ---------------------

def _wb_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    """
    Pull from the classic WB consultants endpoint.
    We are permissive with fields (dates often missing). We NEVER return 0 due to strict gates.
    Env knobs (all optional):
      WB_ROWS (<=200), WB_PAGES (<=10), WB_MAX_RESULTS (cap results)
      WB_PUB_WINDOW_DAYS (default = days_back)
      WB_QTERM (free-text; leave empty to avoid starving results)
      WB_REQUIRE_TOPIC_MATCH (0/1), WB_TOPIC_LIST (pipe-separated)
      WB_DEBUG (1 to print lightweight debug)
    """
    debug = _env_bool("WB_DEBUG", False)

    since_iso_env = (datetime.utcnow().date() - timedelta(days=_env_int("WB_PUB_WINDOW_DAYS", days_back))).isoformat()
    rows = _rows_cap(default=min(100, _env_int("WB_ROWS", 100)), env_name="WB_ROWS", max_allowed=200)
    pages = max(1, min(_env_int("WB_PAGES", 8), 10))
    max_results = _env_int("WB_MAX_RESULTS", 500)
    qterm = os.getenv("WB_QTERM", "")  # keep empty unless you really want to narrow

    out: List[Dict[str, Any]] = []
    offset = 0

    for page in range(pages):
        params = {
            "format": "json",
            "qterm": qterm,
            "fl": "id,notice,submissiondeadline,publicationdate,operatingunit,countryshortname,noticeurl",
            "os": offset,
            "rows": rows,
        }
        try:
            r = requests.get(BASE, params=params, headers=HEADERS, timeout=45)
            if r.status_code >= 500:
                if debug:
                    print(f"[worldbank] page={page} rows={rows} -> {r.status_code}; retry smaller page")
                time.sleep(1.0)
                if rows > 50:
                    rows = 50
                    # retry this page with smaller rows
                    continue
            r.raise_for_status()
            j = r.json()
        except Exception as e:
            if debug:
                print(f"[worldbank] WARN request failed: {e}")
            break

        docs = _safe_docs(j)
        if not docs:
            if debug:
                print(f"[worldbank] no docs on page {page}")
            break

        added_this_page = 0
        for d in docs:
            title = (d.get("notice") or d.get("title") or "").strip()
            if not title:
                continue
            url = d.get("noticeurl") or d.get("url") or ""
            country = d.get("countryshortname") or d.get("operatingunit") or ""
            deadline = _to_iso(d.get("submissiondeadline"))
            pubdate = _to_iso(d.get("publicationdate"))

            # Keep logic (soft):
            # - If pubdate exists: keep only if within window
            # - Else if deadline exists: keep only if deadline >= since window
            # - Else: keep (don't drop for missing fields)
            keep = True
            if pubdate:
                keep = pubdate >= since_iso_env
            elif deadline:
                keep = deadline >= since_iso_env
            # if both missing, keep = True (permissive)

            if not keep:
                continue

            summary = f"{title} {country} pub:{pubdate or ''}".lower()
            item = {
                "title": title,
                "source": "World Bank",
                "deadline": deadline,  # may be None
                "country": country,
                "topic": None,         # assigned later by your normalizer if needed
                "url": url,
                "summary": summary,
            }
            out.append(item)
            added_this_page += 1
            if len(out) >= max_results:
                break

        if debug:
            print(f"[worldbank] page={page} added={added_this_page} total={len(out)}")

        if len(out) >= max_results:
            break
        offset += rows

    # ---- soft OGP filter & exclusion (never zero-out) ----
    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            relevant = [it for it in out
                        if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")]
            out = _prefer_or_fallback(relevant, out)  # prefer matches, fallback to all if none
            out = [it for it in out
                   if not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
        except Exception:
            # filters not present; keep 'out' as-is
            pass

    # ---- soft topic preference (never zero-out) ----
    require_topic_match = _env_bool("WB_REQUIRE_TOPIC_MATCH", False)
    topic_raw = os.getenv("WB_TOPIC_LIST", "")
    topic_list = [t.strip().lower() for t in topic_raw.split("|") if t.strip()]

    if require_topic_match and topic_list:
        matched = []
        for it in out:
            text = f"{it.get('title','')} {it.get('summary','')}".lower()
            if any(t in text for t in topic_list):
                matched.append(it)
        out = _prefer_or_fallback(matched, out)  # prefer matches if any; otherwise keep all

    return out

# --------------------- public APIs ---------------------

class Connector:
    def fetch(self, days_back: int = 90):
        # Class-based API
        return _wb_fetch_impl(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    # Function-based API (back-compat with your aggregator)
    return _wb_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    # For your logging
    return ["ogp_only", "since_days"]
