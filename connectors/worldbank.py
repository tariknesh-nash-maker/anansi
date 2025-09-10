from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
import time
import requests
import dateparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://search.worldbank.org/api/consultants"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; anansi/1.0)"}

def _retry_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.8,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def _to_iso(d: str | None) -> str | None:
    if not d:
        return None
    dt = dateparser.parse(d, settings={"DATE_ORDER": "YMD", "PREFER_DAY_OF_MONTH": "first"})
    return dt.date().isoformat() if dt else None

def _parse_hits(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(j.get("documents"), dict):
        return list(j["documents"].values())
    for k in ("documents", "rows", "docs", "items"):
        if isinstance(j.get(k), list):
            return j[k]
    if isinstance(j.get("hits"), dict):
        return list(j["hits"].values())
    return []

def _total_count(j: Dict[str, Any]) -> int:
    for k in ("total", "count", "numFound"):
        if isinstance(j.get(k), int):
            return j[k]
    if isinstance(j.get("result"), dict) and isinstance(j["result"].get("total"), int):
        return j["result"]["total"]
    return 0

def _wb_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    since_date = (datetime.utcnow().date() - timedelta(days=days_back)).isoformat()
    today_iso = datetime.utcnow().date().isoformat()

    sess = _retry_session()
    items: List[Dict[str, Any]] = []
    offset = 0
    rows = 100
    tried_fallback = False

    while True:
        params = {
            "format": "json",
            "qterm": "",  # leave empty; filter after
            "fl": "id,notice,noticeid,submissiondeadline,publicationdate,operatingunit,countryshortname,noticeurl",
            "os": offset,
            "rows": rows,
        }
        try:
            r = sess.get(BASE, params=params, headers=HEADERS, timeout=45)
            if r.status_code >= 500:
                if rows > 50:
                    rows = 50
                    time.sleep(1.0)
                    continue
            r.raise_for_status()
            j = r.json()
        except Exception:
            # one-shot ultra-conservative fallback page size
            if not tried_fallback:
                tried_fallback = True
                rows = 25
                time.sleep(1.0)
                continue
            break

        hits = _parse_hits(j)
        if not hits:
            break

        for doc in hits:
            title = (doc.get("notice") or doc.get("title") or "").strip()
            if not title:
                continue
            url = doc.get("noticeurl") or doc.get("url") or ""
            country = doc.get("countryshortname") or doc.get("operatingunit") or ""
            deadline_iso = _to_iso(doc.get("submissiondeadline"))
            pub_iso = _to_iso(doc.get("publicationdate"))

            # Be permissive: keep items even if both dates are missing
            keep = True
            # But if there is a date, apply the sensible checks
            if deadline_iso:
                keep = keep and (deadline_iso >= since_date)
            if pub_iso:
                keep = keep and (pub_iso >= since_date)
            if not keep:
                continue

            items.append({
                "title": title,
                "source": "World Bank",
                "deadline": deadline_iso,
                "country": country,
                "topic": None,
                "url": url,
                "summary": f"{title} {country} pub:{pub_iso or ''}".lower(),
            })

        total = _total_count(j)
        offset += rows
        if total and offset >= total:
            break
        if offset >= 2000:  # safety
            break

    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            items = [it for it in items
                     if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")
                     and not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
        except Exception:
            pass

    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _wb_fetch_impl(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _wb_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
