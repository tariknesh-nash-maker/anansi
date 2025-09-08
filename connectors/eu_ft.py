# -*- coding: utf-8 -*-
from __future__ import annotations
import hashlib, logging, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)
API = "https://api.ted.europa.eu/v3/notices/search"

KEYWORDS = [
    "open government","governance","transparency","accountability","anti-corruption",
    "civic","participation","open data","digital government","rule of law","justice",
    "public finance","budget","procurement","PFM","access to information",
    "civil society","media freedom","democracy"
]

@dataclass
class Opportunity:
    id: str; title: str; donor: str; url: str
    deadline: Optional[str]; published_date: Optional[str]; status: Optional[str]
    tags: List[str]; country_scope: Optional[str]
    amount: Optional[str]=None; currency: Optional[str]=None
    def to_dict(self)->Dict: return asdict(self)

def _hash(*parts: str) -> str:
    return "eu_" + hashlib.sha1("::".join([p for p in parts if p]).encode()).hexdigest()[:16]

def _classify(text: str) -> List[str]:
    t=(text or "").lower(); tags=set()
    if any(k in t for k in ["digital","data","ai","ict","open data","e-government"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen","civil society","media"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law","democracy"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _call(body: Dict) -> Dict:
    r = requests.post(API, json=body, timeout=30, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()

def _fetch_raw(page: int, page_size: int) -> List[Dict]:
    body = {
        "query": '(notice-title ~ ("*"))',         # minimal, always-valid
        "fields": ["publication-number", "notice-title", "publication-date"],
        "page": page,
        "limit": page_size,
        "scope": "ACTIVE",                         # currently valid notices
        "checkQuerySyntax": False,
        "paginationMode": "PAGE_NUMBER",
    }
    data = _call(body)
    return data.get("results") or data.get("items") or []

def fetch(max_items: int = 50, since_days: Optional[int] = 120, ogp_only: bool = True) -> List[Dict]:
    out: List[Opportunity] = []
    page, page_size = 1, min(50, max_items)
    today = datetime.now(timezone.utc).date()
    fetched_raw = 0

    while len(out) < max_items:
        try:
            batch = _fetch_raw(page, page_size)
        except requests.HTTPError as e:
            LOG.warning("EU (TED) failed: %s", e)
            break
        if not batch: break
        fetched_raw += len(batch)

        for it in batch:
            pubnum = it.get("publication-number") or it.get("publicationNumber")
            title = (it.get("notice-title") or it.get("noticeTitle") or "").strip()
            pub = it.get("publication-date") or it.get("publicationDate")
            if pub:
                try:
                    pub_dt = dateparser.parse(pub).date()
                    pub_iso = pub_dt.isoformat()
                except Exception:
                    pub_dt = None
                    pub_iso = None
            else:
                pub_dt = None
                pub_iso = None

            # client-side since_days
            if since_days and pub_dt:
                if pub_dt < (today - timedelta(days=since_days)):
                    continue

            # client-side OGP title filter
            if ogp_only and title:
                low = title.lower()
                if not any(k in low for k in [k.lower() for k in KEYWORDS]):
                    continue

            url = f"https://ted.europa.eu/en/notice/-/detail/{pubnum}" if pubnum else ""
            out.append(Opportunity(
                id=_hash(title, url or (pubnum or "")),
                title=title,
                donor="EU (TED)",
                url=url,
                deadline=None,                   # TED search rarely exposes deadline
                published_date=pub_iso,
                status="open",                   # ACTIVE scope
                tags=_classify(title),
                country_scope=None,
            ))
            if len(out) >= max_items: break

        if len(batch) < page_size: break
        page += 1

    LOG.info("EU (TED): raw=%s, after_filters=%s (since_days=%s, ogp_only=%s)", fetched_raw, len(out), since_days, ogp_only)

    # If nothing survived the title keyword filter, try once without it.
    if not out and ogp_only:
        LOG.info("EU (TED): 0 after OGP filter — retrying without ogp_only")
        return fetch(max_items=max_items, since_days=since_days, ogp_only=False)

    return [o.to_dict() for o in out]
class Connector:
    name = "eu"
    def __init__(self, **kw): self.kwargs = kw
    def fetch(self, **kw)->List[Dict]: return fetch(**{**self.kwargs, **kw})
