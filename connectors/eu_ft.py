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
    "public finance","budget","procurement","PFM","access to information"
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
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _call(body: Dict) -> Dict:
    r = requests.post(API, json=body, timeout=30, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()

def fetch(max_items: int = 50, since_days: Optional[int] = 120, ogp_only: bool = True) -> List[Dict]:
    out: List[Opportunity] = []
    page, page_size = 1, min(50, max_items)
    today = datetime.now(timezone.utc).date()

    # Wide query: only date, no title keywords (we filter client-side)
    date_q = f"(publication-date >= today(-{since_days}))" if since_days else ""
    q = date_q if date_q else '(notice-title ~ ("*"))'

    while len(out) < max_items:
        body = {
            "query": q,
            "fields": ["publication-number", "notice-title", "publication-date"],
            "page": page,
            "limit": page_size,
            "scope": "ACTIVE",
            "checkQuerySyntax": False,
            "paginationMode": "PAGE_NUMBER",
        }
        try:
            data = _call(body)
        except requests.HTTPError as e:
            # Fallback: drop date clause, then filter client-side by pubdate
            LOG.warning("EU (TED) wide query failed (%s). Retrying without date clause.", e)
            body["query"] = '(notice-title ~ ("*"))'
            data = _call(body)

        results = data.get("results") or data.get("items") or []
        if not results: break

        for it in results:
            pubnum = it.get("publication-number") or it.get("publicationNumber")
            title = (it.get("notice-title") or it.get("noticeTitle") or "").strip()
            pub = it.get("publication-date") or it.get("publicationDate")
            if pub:
                try: pub = dateparser.parse(pub).date().isoformat()
                except Exception: pass

            # Client-side filters
            if since_days and pub:
                try:
                    if dateparser.parse(pub).date() < (today - timedelta(days=since_days)):
                        continue
                except Exception:
                    pass
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
                deadline=None,
                published_date=pub,
                status="open",  # ACTIVE scope
                tags=_classify(title),
                country_scope=None,
            ))
            if len(out) >= max_items: break

        if len(results) < page_size: break
        page += 1

    return [o.to_dict() for o in out]
class Connector:
    name = "eu"
    def __init__(self, **kw): self.kwargs=kw
    def fetch(self, **kw)->List[Dict]: return fetch(**{**self.kwargs, **kw})
