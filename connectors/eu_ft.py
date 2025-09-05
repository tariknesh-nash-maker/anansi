# -*- coding: utf-8 -*-
"""
EU Funding & Tenders (F&T) — Calls for proposals / topics
Data source: EC Search API (SEDIA)
Returns normalized "opportunity" dicts.

This module exposes BOTH:
  - fetch(max_items=..., since_days=..., ogp_only=True) -> List[dict]
  - class Connector(...).fetch(...)  (thin wrapper)

Notes:
- Uses the EC Search API endpoint with apiKey=SEDIA (public).
- Filters for Open + Forthcoming calls and OGP-ish keywords by default.
- Defensive parsing: several possible date fields are handled.

If you want to test locally:
    python -m connectors.eu_ft
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

import requests
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"  # EC Search API
API_KEY = os.getenv("EU_FT_API_KEY", "SEDIA")  # public key typically "SEDIA"

# Status facets used by the F&T search (observed values):
STATUS_OPEN = "31094502"
STATUS_FORTHCOMING = "31094501"

# Broad OGP-ish keywords (used as free-text)
OGP_KEYWORDS = [
    "open government", "governance", "transparency", "accountability",
    "anti-corruption", "civic", "participation", "civic tech", "digital government",
    "e-government", "open data", "public finance", "budget", "FOI", "access to information",
    "procurement", "PFM", "integrity", "rule of law", "justice reform"
]

# Minimal schema we return
@dataclass
class Opportunity:
    id: str
    title: str
    donor: str
    url: str
    deadline: Optional[str]  # ISO date (YYYY-MM-DD) or None
    published_date: Optional[str]  # ISO date or None
    status: Optional[str]  # "open"|"forthcoming"|None
    tags: List[str]
    country_scope: Optional[str]
    amount: Optional[str] = None  # leave as text if available
    currency: Optional[str] = None
    source_raw: Optional[Dict] = None  # for debugging

    def to_dict(self) -> Dict:
        d = asdict(self)
        d.pop("source_raw", None)  # keep payloads out of Slack-sized posts
        return d


def _iso(d: Optional[datetime]) -> Optional[str]:
    return d.date().isoformat() if isinstance(d, datetime) else None


def _pick_date(md: Dict) -> Optional[str]:
    """
    Try multiple metadata keys that EC search API may expose.
    """
    if not isinstance(md, dict):
        return None

    date_keys = [
        "deadlineDate", "submissionDeadlineDate", "tenderDeadlineDate",
        "endDate", "deadline", "closingDate"
    ]
    for k in date_keys:
        v = md.get(k)
        if v:
            try:
                return dateparser.parse(v).date().isoformat()
            except Exception:
                continue

    # sometimes in 'highlights' or 'teaser'
    text_fields = [md.get("teaser"), md.get("title"), md.get("summary")]
    rex = re.compile(r"(?:Deadline|Closing)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    for t in text_fields:
        if not t:
            continue
        m = rex.search(t)
        if m:
            try:
                return dateparser.parse(m.group(1)).date().isoformat()
            except Exception:
                pass
    return None


def _to_status(val: Optional[str]) -> Optional[str]:
    if val == STATUS_OPEN:
        return "open"
    if val == STATUS_FORTHCOMING:
        return "forthcoming"
    return None


def _hash_id(*parts: str) -> str:
    h = hashlib.sha1(("::".join([p for p in parts if p])).encode("utf-8")).hexdigest()
    return f"euft_{h[:16]}"


def _classify_tags(text: str) -> List[str]:
    t = text.lower()
    tags = set()
    if any(k in t for k in ["digital", "data", "ai", "e-government", "open data", "ict"]):
        tags.add("ai_digital")
    if any(k in t for k in ["budget", "public finance", "pfm"]):
        tags.add("budget")
    if any(k in t for k in ["transparen", "accountab", "anti-corruption", "integrity", "procurement"]):
        tags.add("anti_corruption")
    if any(k in t for k in ["civic", "participation", "citizen"]):
        tags.add("civic_participation")
    if any(k in t for k in ["justice", "rule of law"]):
        tags.add("justice")
    if not tags:
        tags.add("governance")
    return sorted(tags)


def _post_search(page: int, page_size: int, text_query: str, since_days: Optional[int]) -> Dict:
    params = {
        "apiKey": API_KEY,
        "pageNumber": str(page),
        "pageSize": str(page_size),
        # text parameter helps coarse filtering by keywords across fields
        "text": text_query
    }
    must_terms = [
        {"terms": {"type": ["1", "2"]}},  # "1,2" observed for Calls/Topics on F&T
        {"terms": {"status": [STATUS_OPEN, STATUS_FORTHCOMING]}},
    ]
    if since_days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat()
        # attempt a lower bound on publication date if the index supports it
        must_terms.append({"range": {"publicationDate": {"gte": cutoff}}})

    body = {
        "query": {
            "bool": {"must": must_terms}
        },
        "languages": ["en"],
        "sort": {"field": "sortStatus", "order": "ASC"},
    }

    files = {
        "query": ("blob", json.dumps(body["query"]), "application/json"),
        "languages": ("blob", json.dumps(body["languages"]), "application/json"),
        "sort": ("blob", json.dumps(body["sort"]), "application/json"),
    }
    resp = requests.post(API_URL, params=params, files=files, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch(max_items: int = 60, since_days: Optional[int] = 90, ogp_only: bool = True) -> List[Dict]:
    """
    Fetch EU F&T open/forthcoming calls relevant to OGP.

    :param max_items: cap on total results
    :param since_days: ignore older publications (None = no cap)
    :param ogp_only: if True, adds OGP_KEYWORDS to free-text filter
    """
    keywords = OGP_KEYWORDS if ogp_only else []
    text_query = " OR ".join(keywords) if keywords else "*"

    out: List[Opportunity] = []
    page, page_size = 1, min(50, max_items)

    while len(out) < max_items:
        data = _post_search(page=page, page_size=page_size, text_query=text_query, since_days=since_days)
        results = data.get("results") or []
        if not results:
            break

        for r in results:
            title = r.get("content") or r.get("title") or ""
            url = r.get("url") or ""
            md = r.get("metadata") or {}
            deadline = _pick_date(md)
            status = _to_status(md.get("status"))
            published = md.get("publicationDate") or md.get("startDate")
            if published:
                try:
                    published = dateparser.parse(published).date().isoformat()
                except Exception:
                    published = None

            text = " ".join(filter(None, [
                title, md.get("teaser"), md.get("summary"), " ".join(md.get("keywords", []) or [])
            ]))
            tags = _classify_tags(text)
            opp = Opportunity(
                id=_hash_id(title, url),
                title=title.strip(),
                donor="EU F&T",
                url=url,
                deadline=deadline,
                published_date=published,
                status=status,
                tags=tags,
                country_scope=md.get("geographicalZonesText") or md.get("geographicalZones") or None,
                amount=None,
                currency=None,
                source_raw=None,  # reduce payload size
            )
            out.append(opp)
            if len(out) >= max_items:
                break

        if len(results) < page_size:
            break
        page += 1

    return [o.to_dict() for o in out]


class Connector:
    name = "eu_ft"

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def fetch(self, **kwargs) -> List[Dict]:
        final_kwargs = {**self.kwargs, **kwargs}
        return fetch(**final_kwargs)


if __name__ == "__main__":
    import pprint
    ops = fetch(max_items=10, since_days=180, ogp_only=True)
    pprint.pp(ops)
