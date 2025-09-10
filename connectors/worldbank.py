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

def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip() in ("1","true","True","YES","yes")

def _rows_cap(default:int, env_name:str, max_allowed:int=200) -> int:
    try:
        v = int(os.getenv(env_name, str(default)))
    except Exception:
        v = default
    return max(1, min(v, max_allowed))

def _safe_docs(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(j.get("documents"), dict):
        return list(j["documents"].values())
    for k in ("documents","rows","items"):
        if isinstance(j.get(k), list):
            return j[k]
    if isinstance(j.get("hits"), dict):
        return list(j["hits"].values())
    return []

def _wb_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    """
    Robust pull from the classic WB consultants endpoint.
    Never zero-out results because of missing fields; let downstream filters work.
    Controlled by env:
      WB_ROWS (<=200), WB_PAGES (<=10), WB_MAX_RESULTS, WB_PUB_WINDOW_DAYS
      WB_QTERM (free text), WB_REQUIRE_TOPIC_MATCH (0/1), WB_TOPIC_LIST (pipe-separated)
    """
    since_iso = (datetime.utcnow().date() - timedelta(days=days_back)).isoformat()
    rows        = _rows_cap( min(100, _int("WB_ROWS", 100)), "WB_ROWS", 200)
    pages       = max(1, min(_int("WB_PAGES", 8), 10))
    max_results = _int("WB_MAX_RESULTS", 500)
    pub_window  = _int("WB_PUB_WINDOW_DAYS", days_back)
    qterm       = os.getenv("WB_QTERM", "")
    require_topic_match = _bool("WB_REQUIRE_TOPIC_MATCH", False)
    topic_raw   = os.getenv("WB_TOPIC_LIST", "")
    topic_list  = [t.strip().lower() for t in topic_raw.split("|") if t.strip()]

    out: List[Dict[str, Any]] = []
    offset = 0

    for _ in range(pages):
        params = {
            "format": "json",
            "qterm": qterm,  # can be empty
            "fl": "id,notice,submissiondeadline,publicationdate,operatingunit,countryshortname,noticeurl",
            "os": offset,
            "rows": rows,
        }
        try:
            r = requests.get(BASE, params=params, headers=HEADERS, timeout=45)
            if r.status_code >= 500:
                time.sleep(1.0)
                if rows > 50:
                    rows = 50
                    continue
            r.raise_for_status()
            j = r.json()
        except Exception:
            break

        docs = _safe_docs(j)
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

            # Time window is gentle: if pubdate exists, keep only if recent; otherwise keep.
            if pub_window and pubdate and pubdate < (datetime.utcnow().date() - timedelta(days=pub_window)).isoformat():
                continue

            summary = f"{title} {country} pub:{pubdate or ''}".lower()
            item = {
                "title": title,
                "source": "World Bank",
                "deadline": deadline,   # may be None
                "country": country,
                "topic": None,
                "url": url,
                "summary": summary,
            }

            # Optional topic gate (but only if a non-empty topic list is configured)
            if require_topic_match and topic_list:
                text = f"{title} {summary}".lower()
                if not any(t in text for t in topic_list):
                    # skip only if an explicit topic list was provided
                    continue

            out.append(item)
            if len(out) >= max_results:
                break

        if len(out) >= max_results:
            break
        offset += rows

    # Optional OGP keywords / excludes (multilingual) — keep light to avoid zeros
    if ogp_only:
        try:
            from filters import is_excluded
            out = [it for it in out if not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
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
