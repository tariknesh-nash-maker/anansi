# anansi/connectors/world_bank.py
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
import os, time
import requests
import dateparser

BASE = "https://search.worldbank.org/api/consultants"
UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA}

def _to_iso(s: str | None) -> str | None:
    if not s:
        return None
    dt = dateparser.parse(s, settings={"DATE_ORDER": "DMY", "PREFER_DAY_OF_MONTH": "first"})
    return dt.date().isoformat() if dt else None

def _rows_cap(default:int, env_name:str, max_allowed:int=200) -> int:
    try:
        v = int(os.getenv(env_name, str(default)))
    except Exception:
        v = default
    return max(1, min(v, max_allowed))  # WB API chokes on very large rows

def _pages_cap(default:int, env_name:str, hard_cap:int=10) -> int:
    try:
        v = int(os.getenv(env_name, str(default)))
    except Exception:
        v = default
    return max(1, min(v, hard_cap))

def _wb_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    """
    Robust pull from the classic WB consultants endpoint.
    We avoid strict pre-filters that can zero out results.
    Env knobs (optional):
      WB_ROWS (<=200), WB_PAGES (<=10), WB_MAX_RESULTS, WB_PUB_WINDOW_DAYS
    """
    since_iso = (datetime.utcnow().date() - timedelta(days=days_back)).isoformat()
    rows = _rows_cap(default=100, env_name="WB_ROWS", max_allowed=200)
    pages = _pages_cap(default=8, env_name="WB_PAGES", hard_cap=10)
    max_results = int(os.getenv("WB_MAX_RESULTS", "500"))
    pub_window_days = int(os.getenv("WB_PUB_WINDOW_DAYS", str(days_back)))

    out: List[Dict[str, Any]] = []
    offset = 0

    for _ in range(pages):
        params = {
            "format": "json",
            # empty qterm returns the catalogue; we filter client-side
            "qterm": os.getenv("WB_QTERM", ""),
            # ask for the specific fields we need
            "fl": "id,notice,submissiondeadline,publicationdate,operatingunit,countryshortname,noticeurl",
            "os": offset,
            "rows": rows,
        }
        try:
            r = requests.get(BASE, params=params, headers=HEADERS, timeout=45)
            if r.status_code >= 500:
                time.sleep(1.0)
                # try once more with a smaller page if needed
                if rows > 50:
                    rows = 50
                    continue
            r.raise_for_status()
            j = r.json()
        except Exception:
            # stop this connector gracefully on repeated errors
            break

        docs = []
        # API can return {"documents": {...}} or lists – normalize
        if isinstance(j.get("documents"), dict):
            docs = list(j["documents"].values())
        elif isinstance(j.get("documents"), list):
            docs = j["documents"]
        elif isinstance(j.get("rows"), list):
            docs = j["rows"]
        elif isinstance(j.get("hits"), dict):
            docs = list(j["hits"].values())

        if not docs:
            break

        for d in docs:
            title = (d.get("notice") or d.get("title") or "").strip()
            if not title:
                continue
            url = d.get("noticeurl") or d.get("url") or ""
            country = d.get("countryshortname") or d.get("operatingunit") or ""
            deadline = _to_iso(d.get("submissiondeadline"))
            pubdate = _to_iso(d.get("publicationdate"))

            # gentle time window: keep item if it's recent by pubdate OR has any deadline
            keep = True
            if pub_window_days and pubdate:
                keep = pubdate >= (datetime.utcnow().date() - timedelta(days=pub_window_days)).isoformat()
            # if no dates at all, keep (we don't want false negatives)
            if not keep:
                continue

            out.append({
                "title": title,
                "source": "World Bank",
                "deadline": deadline,  # may be None
                "country": country,
                "topic": None,         # your normalizer/filters assign topic later
                "url": url,
                "summary": f"{title} {country} pub:{pubdate or ''}".lower(),
            })

            if len(out) >= max_results:
                break
        if len(out) >= max_results:
            break

        offset += rows

    # Optional OGP filtering (shared filters.py)
    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            out = [it for it in out
                   if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")
                   and not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
        except Exception:
            pass

    return out

class Connector:
    def fetch(self, days_back: int = 90):
        return _wb_fetch_impl(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _wb_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
