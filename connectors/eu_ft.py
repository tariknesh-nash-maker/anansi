# connectors/eu_ft.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib, json, logging, os, re, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
API_KEY = os.getenv("EU_FT_API_KEY", "SEDIA")  # public default works

STATUS_OPEN = "31094502"
STATUS_FORTHCOMING = "31094501"

OGP_KEYWORDS = [
    "open government","governance","transparency","accountability","anti-corruption",
    "civic","participation","civic tech","digital government","e-government","open data",
    "public finance","budget","procurement","PFM","integrity","rule of law","justice",
    "access to information","FOI",
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

def _hash_id(*parts: str) -> str:
    return "euft_" + hashlib.sha1("::".join([p for p in parts if p]).encode("utf-8")).hexdigest()[:16]

def _classify(text: str) -> List[str]:
    t = (text or "").lower(); tags = set()
    if any(k in t for k in ["digital","data","ai","e-government","open data","ict"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _pick_deadline(meta: Dict, text_fields: List[str]) -> Optional[str]:
    for k in ["deadlineDate","submissionDeadlineDate","tenderDeadlineDate","endDate","deadline","closingDate"]:
        v = (meta or {}).get(k)
        if v:
            try: return dateparser.parse(v).date().isoformat()
            except Exception: pass
    rx = re.compile(r"(?:Deadline|Closing)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    for t in text_fields:
        if not t: continue
        m = rx.search(t)
        if m:
            try: return dateparser.parse(m.group(1)).date().isoformat()
            except Exception: pass
    return None

def _post(page: int, page_size: int, text_query: str, since_days: Optional[int], strict_facets: bool) -> Dict:
    params = {
        "apiKey": API_KEY,
        "text": text_query,          # free-text hits across content/keywords/teaser
        "pageNumber": str(page),
        "pageSize": str(page_size),
    }
    # Build the Elasticsearch-style query object
    must_terms = []
    if strict_facets:
        must_terms.append({"terms": {"type": ["1","2"]}})  # Calls/Topics
        must_terms.append({"terms": {"status": [STATUS_OPEN, STATUS_FORTHCOMING]}})

    if since_days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat()
        # Not all indices honor this, but it’s harmless if ignored:
        must_terms.append({"range": {"publicationDate": {"gte": cutoff}}})

    query = {"bool": {"must": must_terms}} if must_terms else {"match_all": {}}
    languages = ["en"]
    sort = {"field": "sortStatus", "order": "ASC"}

    # Important: send as multipart 'files' parts (content-type application/json)
    resp = requests.post(
        API_URL, params=params, timeout=30,
        files={
            "query": ("blob", json.dumps(query), "application/json"),
            "languages": ("blob", json.dumps(languages), "application/json"),
            "sort": ("blob", json.dumps(sort), "application/json"),
        },
        headers={"Accept": "application/json"}
    )
    # The API returns 400 when facets are too strict/unknown; we handle upstream
    resp.raise_for_status()
    return resp.json()

def fetch(max_items: int = 60, since_days: Optional[int] = 120, ogp_only: bool = True) -> List[Dict]:
    out: List[Opportunity] = []
    page, page_size = 1, min(50, max_items)
    text_query = " OR ".join(OGP_KEYWORDS) if ogp_only else "*"

    while len(out) < max_items:
        # First try with facets; if 400, retry *once* without facets (broader results + client-side filter)
        try:
            data = _post(page, page_size, text_query, since_days, strict_facets=True)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                LOG.warning("EU F&T: 400 with facets — retrying without facets")
                data = _post(page, page_size, text_query, since_days, strict_facets=False)
            else:
                raise

        results = data.get("results") or []
        if not results: break

        for r in results:
            title = r.get("content") or r.get("title") or ""
            url = r.get("url") or r.get("uri") or ""
            md = r.get("metadata") or {}

            published = md.get("publicationDate") or md.get("startDate")
            if published:
                try: published = dateparser.parse(published).date().isoformat()
                except Exception: published = None

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

            out.append(Opportunity(
                id=_hash_id(title, url),
                title=title.strip(),
                donor="EU F&T",
                url=url,
                deadline=deadline,
                published_date=published,
                status=status,
                tags=_classify(fulltext),
                country_scope=md.get("geographicalZonesText") or md.get("geographicalZones") or None,
            ))
            if len(out) >= max_items: break

        if len(results) < page_size: break
        page += 1

    return [o.to_dict() for o in out]

class Connector:
    name = "eu_ft"
    def __init__(self, **kwargs): self.kwargs = kwargs
    def fetch(self, **kwargs) -> List[Dict]:
        return fetch(**{**self.kwargs, **kwargs})
