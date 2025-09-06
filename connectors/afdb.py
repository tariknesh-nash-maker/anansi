# -*- coding: utf-8 -*-
from __future__ import annotations
import hashlib, logging, re, requests, io
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dateutil import parser as dateparser
import feedparser

LOG = logging.getLogger(__name__)

# Public AfDB RSS (project-related procurement); RSS endpoints are documented/linked from AfDB procurement pages.
FEEDS = [
    # General Procurement Notices
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml",
    # Specific Procurement Notices
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml",
    # Requests for Expression of Interest
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/request-for-expression-of-interest-reoi/rss.xml",
]

HEADERS = {
    "User-Agent": "anansi/afdb (GitHub: tariknesh-nash-maker/anansi)",
    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
    "Accept-Language": "en",
    "Connection": "close",
}

OGP_HINTS = [
    "governance","transparen","accountab","open data","digital","ict","pfm",
    "public finance","budget","audit","integrity","anti-corruption","justice","rule of law",
    "citizen","participation","civic"
]
DL_RX = re.compile(r"(?:Deadline|Closing)\s*(?:Date|Time)?\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)

@dataclass
class Opportunity:
    id: str; title: str; donor: str; url: str
    deadline: Optional[str]; published_date: Optional[str]; status: Optional[str]
    tags: List[str]; country_scope: Optional[str]
    amount: Optional[str]=None; currency: Optional[str]=None
    def to_dict(self)->Dict: return asdict(self)

def _hash(*p: str)->str: return "afdb_" + hashlib.sha1("::".join([x for x in p if x]).encode()).hexdigest()[:16]

def _classify(text: str)->List[str]:
    t=(text or "").lower(); tags=set()
    if any(k in t for k in ["digital","data","ict","open data"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm","audit"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _fetch_feed(url: str):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    # parse from bytes to control headers/UA
    return feedparser.parse(io.BytesIO(r.content))

def fetch(max_items: int=60, since_days: Optional[int]=180, ogp_only: bool=True)->List[Dict]:
    cutoff = (datetime.now(timezone.utc)-timedelta(days=since_days)).date() if since_days else None
    out: List[Opportunity] = []

    for feed_url in FEEDS:
        try:
            feed = _fetch_feed(feed_url)
        except Exception as e:
            LOG.warning("AfDB RSS failed %s: %s", feed_url, e)
            continue

        for e in feed.entries:
            title = (e.get("title") or "").strip()
            link  = e.get("link") or ""
            summary = (e.get("summary") or e.get("description") or "").strip()
            # published
            pub = None
            if e.get("published"):
                try: pub = dateparser.parse(e["published"]).date().isoformat()
                except Exception: pass
            elif e.get("updated"):
                try: pub = dateparser.parse(e["updated"]).date().isoformat()
                except Exception: pass

            if cutoff and pub:
                try:
                    if dateparser.parse(pub).date() < cutoff:
                        continue
                except Exception:
                    pass

            text = f"{title} {summary}"
            if ogp_only and not any(h in text.lower() for h in OGP_HINTS):
                continue

            # deadline (best-effort)
            deadline = None
            m = DL_RX.search(summary) or DL_RX.search(title)
            if m:
                try: deadline = dateparser.parse(m.group(1)).date().isoformat()
                except Exception: pass

            status = None
            try:
                if deadline and dateparser.parse(deadline).date() >= datetime.utcnow().date():
                    status = "open"
            except Exception:
                pass

            out.append(Opportunity(
                id=_hash(title, link), title=title, donor="AfDB", url=link,
                deadline=deadline, published_date=pub, status=status,
                tags=_classify(text), country_scope=None
            ))

            if len(out) >= max_items:
                break
        if len(out) >= max_items:
            break

    # de-dup on URL
    uniq, seen = [], set()
    for o in out:
        if o.url in seen: continue
        seen.add(o.url); uniq.append(o)

    # sort (deadline soonest, else newest)
    def sk(o: Opportunity):
        try: dl = dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date()
        except Exception: dl = datetime.max.date()
        try: pu = dateparser.parse(o.published_date).date() if o.published_date else datetime.min.date()
        except Exception: pu = datetime.min.date()
        return (dl, -int(pu.strftime("%s")))
    uniq.sort(key=sk)
    return [o.to_dict() for o in uniq[:max_items]]

class Connector:
    name="afdb"
    def __init__(self, **kw): self.kwargs=kw
    def fetch(self, **kw)->List[Dict]: return fetch(**{**self.kwargs, **kw})
