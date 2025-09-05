# connectors/eu_ft.py
from __future__ import annotations
import hashlib, json, logging, os, re, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"
API_KEY = os.getenv("EU_FT_API_KEY", "SEDIA")  # public key typically 'SEDIA'
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
    def to_dict(self) -> Dict: return asdict(self)

def _hash_id(*parts: str) -> str:
    return "euft_" + hashlib.sha1("::".join([p for p in parts if p]).encode()).hexdigest()[:16]

def _classify(text: str) -> List[str]:
    t = text.lower()
    tags = set()
    if any(k in t for k in ["digital","data","ai","e-government","open data","ict"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _pick_deadline(meta: Dict, text_fields: List[str]) -> Optional[str]:
    for k in ["deadlineDate","submissionDeadlineDate","tenderDeadlineDate","endDate","deadline","closingDate"]:
        v = meta.get(k)
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

def fetch(max_items: int = 60, since_days: Optional[int] = 120, ogp_only: bool = True) -> List[Dict]:
    out: List[Opportunity] = []
    page, page_size = 1, min(50, max_items)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date().isoformat() if since_days else None

    while len(out) < max_items:
        payload = {
            "apiKey": API_KEY,
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"type": ["1","2"]}},           # Calls/Topics
                        {"terms": {"status": [STATUS_OPEN, STATUS_FORTHCOMING]}},
                    ] + ([{"range": {"publicationDate": {"gte": cutoff}}}] if cutoff else [])
                }
            },
            "languages": ["en"],
            "pageNumber": page,
            "pageSize": page_size,
            "sort": {"field": "sortStatus", "order": "ASC"}
        }
        r = requests.post(API_URL, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
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
            text = " ".join(filter(None, [title, md.get("teaser"), md.get("summary"), " ".join(md.get("keywords", []) or [])]))
            if ogp_only and not any(k in text.lower() for k in [k.lower() for k in OGP_KEYWORDS]):
                continue
            deadline = _pick_deadline(md, [text])
            status = "open" if deadline and dateparser.parse(deadline).date() >= datetime.utcnow().date() else "forthcoming" if md.get("status")==STATUS_FORTHCOMING else None
            out.append(Opportunity(
                id=_hash_id(title, url),
                title=title.strip(),
                donor="EU F&T",
                url=url,
                deadline=deadline,
                published_date=published,
                status=status,
                tags=_classify(text),
                country_scope=md.get("geographicalZonesText") or md.get("geographicalZones") or None
            ))
            if len(out) >= max_items: break

        if len(results) < page_size: break
        page += 1

    return [o.to_dict() for o in out]
class Connector:
    name = "eu_ft"
    def __init__(self, **kwargs): self.kwargs = kwargs
    def fetch(self, **kwargs) -> List[Dict]: return fetch(**{**self.kwargs, **kwargs})
