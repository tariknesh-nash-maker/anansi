# -*- coding: utf-8 -*-
"""
EU — TED (Tenders Electronic Daily) Search API v3
Docs: POST https://api.ted.europa.eu/v3/notices/search
Refs: https://docs.ted.europa.eu/api/latest/search.html

We query ACTIVE notices and keyword-filter for OGP-ish terms.
Returns normalized dicts (id, title, donor, url, deadline, published_date, status, tags, country_scope).
Note: TED returns publication dates reliably; deadlines are not always present => left None if unknown.
"""
from __future__ import annotations
import hashlib, logging, os, re, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)
API = "https://api.ted.europa.eu/v3/notices/search"

KEYWORDS = [
    "open government","governance","transparency","accountability","anti-corruption",
    "civic","participation","open data","digital government","rule of law","justice",
    "public finance","budget","procurement","PFM","access to information"
]

@dataclass
class Opportunity:
    id: str
    title: str
    donor: str
    url: str
    deadline: Optional[str]
    published_date: Optional[str]
    status: Optional[str]
    tags: List[str]
    country_scope: Optional[str]
    amount: Optional[str] = None
    currency: Optional[str] = None
    def to_dict(self) -> Dict: return asdict(self)

def _hash(*parts: str) -> str:
    return "eu_" + hashlib.sha1("::".join([p for p in parts if p]).encode()).hexdigest()[:16]

def _classify(text: str) -> List[str]:
    t = (text or "").lower(); tags=set()
    if any(k in t for k in ["digital","data","ai","ict","open data","e-government"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def fetch(max_items: int = 60, since_days: Optional[int] = 120, ogp_only: bool = True) -> List[Dict]:
    out: List[Opportunity] = []
    page, page_size = 1, min(50, max_items)

    # TED “expert query” (see docs): use title matches + date window; ACTIVE scope means currently valid notices
    if ogp_only:
        kw = " OR ".join([f'"{k}"' for k in KEYWORDS])
        title_query = f'(notice-title ~ ({kw}))'
    else:
        title_query = '(notice-title ~ ("*"))'
    if since_days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat()
        date_clause = f'(publication-date >= {cutoff})'
        q = f"{title_query} AND {date_clause}"
    else:
        q = title_query

    while len(out) < max_items:
        body = {
            "query": q,
            "fields": ["publication-number","notice-title","publication-date","place-of-performance","country"],
            "page": page,
            "limit": page_size,
            "scope": "ACTIVE",
            "checkQuerySyntax": False,
            "paginationMode": "PAGE_NUMBER",
        }
        r = requests.post(API, json=body, timeout=30, headers={"Accept": "application/json"})
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or data.get("items") or []
        if not results: break

        for item in results:
            # Fields are returned as flat keys as per docs/examples
            pubnum = item.get("publication-number") or item.get("publicationNumber")
            title = item.get("notice-title") or item.get("noticeTitle") or ""
            pub = item.get("publication-date") or item.get("publicationDate") or None
            if pub:
                try: pub = dateparser.parse(pub).date().isoformat()
                except Exception: pass
            url = f"https://ted.europa.eu/en/notice/-/detail/{pubnum}" if pubnum else ""

            # we rarely get a formal deadline from TED search; leave None (downstream can enrich if needed)
            out.append(Opportunity(
                id=_hash(title, url or pubnum or ""),
                title=title.strip(),
                donor="EU (TED)",
                url=url,
                deadline=None,
                published_date=pub,
                status="open",  # ACTIVE scope
                tags=_classify(title),
                country_scope=item.get("country") or item.get("place-of-performance") or None,
            ))
            if len(out) >= max_items: break

        if len(results) < page_size: break
        page += 1

    return [o.to_dict() for o in out]

class Connector:
    name = "eu"
    def __init__(self, **kw): self.kwargs = kw
    def fetch(self, **kw) -> List[Dict]: return fetch(**{**self.kwargs, **kw})
