# -*- coding: utf-8 -*-
"""
EU Funding & Tenders (F&T) — EC Search API (SEDIA)
Robust GET-with-params implementation + fallback (no facets) if the API returns 400.

Docs/refs:
- F&T “Support / APIs” page (mentions Search API & Facet API; base URL). 
- Community examples show using GET with ?apiKey=SEDIA&text=…&pageSize=…&pageNumber=….

This module exposes:
  - fetch(max_items=..., since_days=..., ogp_only=True) -> List[dict]
  - Connector().fetch(...)
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
API_KEY = os.getenv("EU_FT_API_KEY", "SEDIA")  # public key commonly 'SEDIA'

# Observed status facet codes on the portal:
STATUS_OPEN = "31094502"
STATUS_FORTHCOMING = "31094501"

OGP_KEYWORDS = [
    "open government","governance","transparency","accountability","anti-corruption",
    "civic","participation","civic tech","digital government","e-government","open data",
    "public finance","budget","procurement","PFM","integrity","rule of law","justice",
    "access to information","FOI"
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
    def to_dict(self) -> Dict: 
        return asdict(self)

def _hash_id(*parts: str) -> str:
    return "euft_" + hashlib.sha1("::".join([p for p in parts if p]).encode("utf-8")).hexdigest()[:16]

def _classify(text: str) -> List[str]:
    t = (text or "").lower()
    tags = set()
    if any(k in t for k in ["digital","data","ai","e-government","open data","ict"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _pick_deadline(meta: Dict, text_fields: List[str]) -> Optional[str]:
    # Try metadata keys first
    for k in ["deadlineDate","submissionDeadlineDate","tenderDeadlineDate","endDate","deadline","closingDate"]:
        v = (meta or {}).get(k)
        if v:
            try: return dateparser.parse(v).date().isoformat()
            except Exception: pass
    # Fallback: regex in free text
    rx = re.compile(r"(?:Deadline|Closing)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    for t in text_fields:
        if not t: continue
        m = rx.search(t)
        if m:
            try: return dateparser.parse(m.group(1)).date().isoformat()
            except Exception: pass
    return None

def _request(params: Dict) -> Dict:
    """Perform GET; if 400 and we used facets, retry without them."""
    r = requests.get(API_URL, params=params, timeout=30)
    if r.status_code == 400 and ("type" in params or "status" in params):
        LOG.warning("EU F&T: 400 with facets — retrying without 'type'/'status'")
        params = {k: v for k, v in params.items() if k not in ("type", "status")}
        r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch(max_items: int = 60, since_days: Optional[int] = 120, ogp_only: bool = True) -> List[Dict]:
    out: List[Opportunity] = []

    page = 1
    page_size = min(50, max_items)
    cutoff_date = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat() if since_days else None

    # Build base params — use GET with query string
    text_query = " OR ".join(OGP_KEYWORDS) if ogp_only else "*"
    base_params = {
        "apiKey": API_KEY,
        "text": text_query,
        "pageSize": str(page_size),
        "sort": "sortStatus",  # same sort as portal
    }
    # Attempt facet narrowing (type & status). If server rejects (400),
    # _request() will retry without them.
    facet_params = []
    for t in ("1","2"):
        facet_params.append(("type", t))
    for s in (STATUS_OPEN, STATUS_FORTHCOMING):
        facet_params.append(("status", s))

    # Pagination loop
    while len(out) < max_items:
        params = list(base_params.items()) + facet_params + [("pageNumber", str(page))]
        if cutoff_date:
            # Some deployments honor a publicationDate lower bound
            params.append(("publicationDateFrom", cutoff_date))
        data = _request(dict(params))

        results = data.get("results") or []
        if not results:
            break

        for r in results:
            title = r.get("content") or r.get("title") or ""
            url = r.get("url") or r.get("uri") or ""
            md = r.get("metadata") or {}

            # Published date
            published = md.get("publicationDate") or md.get("startDate")
            if published:
                try: published = dateparser.parse(published).date().isoformat()
                except Exception: published = None

            # Optional client-side keyword check (in case facets vanished on fallback)
            fulltext = " ".join(filter(None, [
                title, md.get("teaser"), md.get("summary"), " ".join(md.get("keywords", []) or [])
            ]))
            if ogp_only and not any(k in (fulltext or "").lower() for k in [k.lower() for k in OGP_KEYWORDS]):
                continue

            deadline = _pick_deadline(md, [fulltext])
            status = None
            try:
                if deadline and dateparser.parse(deadline).date() >= datetime.utcnow().date():
                    status = "open"
            except Exception:
                pass
            if not status and md.get("status") == STATUS_FORTHCOMING:
                status = "forthcoming"

            opp = Opportunity(
                id=_hash_id(title, url),
                title=title.strip(),
                donor="EU F&T",
                url=url,
                deadline=deadline,
                published_date=published,
                status=status,
                tags=_classify(fulltext),
                country_scope=md.get("geographicalZonesText") or md.get("geographicalZones") or None,
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
    def __init__(self, **kwargs): self.kwargs = kwargs
    def fetch(self, **kwargs) -> List[Dict]:
        return fetch(**{**self.kwargs, **kwargs})
