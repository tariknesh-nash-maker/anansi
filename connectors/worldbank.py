from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import requests
import dateparser

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; anansi/1.0)"}
BASE = "https://search.worldbank.org/api/consultants"

def _to_iso(d: str | None) -> str | None:
    if not d:
        return None
    dt = dateparser.parse(d, settings={"DATE_ORDER": "YMD", "PREFER_DAY_OF_MONTH": "first"})
    return dt.date().isoformat() if dt else None

def _parse_hits(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    The World Bank 'consultants' API returns slightly different shapes over time.
    We normalize across possible keys.
    """
    # 1) dict of id -> doc
    if isinstance(j.get("documents"), dict):
        return list(j["documents"].values())
    # 2) list of docs
    for k in ("documents", "rows", "docs", "items"):
        if isinstance(j.get(k), list):
            return j[k]
    # 3) nested hits dict
    if isinstance(j.get("hits"), dict):
        return list(j["hits"].values())
    return []

def _total_count(j: Dict[str, Any]) -> int:
    for k in ("total", "count", "numFound"):
        if isinstance(j.get(k), int):
            return j[k]
    # sometimes provided under result/total
    if isinstance(j.get("result"), dict) and isinstance(j["result"].get("total"), int):
        return j["result"]["total"]
    return 0

# ---------------- World Bank connector: non-recursive wiring ----------------
def _wb_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    """
    Pulls World Bank consultant/procurement notices via the public search API.
    Returns normalized dicts with keys: title, source, deadline, country, topic, url, summary
    """
    since_date = (datetime.utcnow().date() - timedelta(days=days_back)).isoformat()
    today_iso = datetime.utcnow().date().isoformat()

    out: List[Dict[str, Any]] = []
    offset = 0
    rows = 200  # API max is commonly 200

    while True:
        params = {
            "format": "json",
            "qterm": "",  # no keyword filter here; we filter after parsing
            "fl": "id,notice,noticeid,submissiondeadline,publicationdate,operatingunit,countryshortname,noticeurl",
            "os": offset,
            "rows": rows,
        }
        r = requests.get(BASE, params=params, headers=HEADERS, timeout=45)
        r.raise_for_status()
        j = r.json()

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

            # Keep if (deadline is in the future) OR (published within days_back)
            keep = False
            if deadline_iso and deadline_iso >= today_iso:
                keep = True
            if pub_iso and pub_iso >= since_date:
                keep = True
            if not keep:
                continue

            item = {
                "title": title,
                "source": "World Bank",
                "deadline": deadline_iso,
                "country": country,
                "topic": None,  # inferred later by your normalizer
                "url": url,
                "summary": f"{title} {country} pub:{pub_iso or ''}".lower(),
            }
            out.append(item)

        total = _total_count(j)
        offset += rows
        if total and offset >= total:
            break
        # safety stop to avoid runaway loops
        if offset >= 4000:
            break

    # Optional: apply OGP filter/excludes here (keeps back-compat behavior)
    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            filtered = []
            for it in out:
                txt = f"{it.get('title','')} {it.get('summary','')}"
                if ogp_relevant(txt) and not is_excluded(txt):
                    filtered.append(it)
            out = filtered
        except Exception:
            pass

    return out

class Connector:
    def fetch(self, days_back: int = 90):
        return _wb_fetch_impl(days_back=days_back, ogp_only=True)

# ---- Back-compat procedural API (for existing aggregator) ----
def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _wb_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
