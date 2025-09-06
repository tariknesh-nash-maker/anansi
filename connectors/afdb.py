# -*- coding: utf-8 -*-
"""
AfDB — Project-related procurement (SPN/REOI/GPN)
We avoid the WAF-protected 'current-solicitations' page and use documents paths:
 - /documents/project-related-procurement/procurement-notices/specific-procurement-notices
 - /documents/project-related-procurement/procurement-notices/request-for-expression-of-interest
 - /documents/project-related-procurement/procurement-notices/general-procurement-notices
Refs: AfDB documents portal categories. 
"""
from __future__ import annotations
import hashlib, logging, re, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)
BASE = "https://www.afdb.org"
CATS = [
    "/en/documents/project-related-procurement/procurement-notices/specific-procurement-notices",
    "/en/documents/project-related-procurement/procurement-notices/request-for-expression-of-interest",
    "/en/documents/project-related-procurement/procurement-notices/general-procurement-notices",
]

HEADERS = {
    "User-Agent": "anansi/afdb (+https://github.com/tariknesh-nash-maker/anansi)",
    "Accept-Language": "en",
    "Accept": "text/html,application/xhtml+xml",
    "Referer": "https://www.afdb.org/en/documents",
}

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
    if any(k in t for k in ["digital","data","ict"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","integrity","procurement"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

DL_RE = re.compile(r"(?:Deadline|Closing)\s*(?:Date|Time)?\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)

def _get(url: str)->BeautifulSoup:
    r = requests.get(url, timeout=30, headers=HEADERS)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _extract_list(cat_url: str, max_pages: int, since_days: Optional[int]) -> List[Opportunity]:
    out: List[Opportunity]=[]
    cutoff=(datetime.now(timezone.utc)-timedelta(days=since_days)).date() if since_days else None

    for page in range(max_pages):
        url = f"{BASE}{cat_url}?page={page}" if page>0 else f"{BASE}{cat_url}"
        try:
            soup = _get(url)
        except requests.HTTPError as e:
            LOG.warning("AfDB list failed %s: %s", url, e); break
        items = soup.select("article a[href*='/en/documents/']") or soup.select("h3 a[href]")
        seen=set()
        for a in items:
            href=a.get("href",""); title=a.get_text(strip=True)
            if not href or not title: continue
            detail=urljoin(BASE, href)
            if detail in seen: continue
            seen.add(detail)
            # detail page parse
            try:
                s2=_get(detail)
            except requests.HTTPError:
                continue
            txt = s2.get_text(" ", strip=True)
            # published
            pub=None
            t=s2.select_one("time")
            if t:
                try: pub=dateparser.parse(t.get_text(strip=True)).date().isoformat()
                except Exception: pass
            # cutoff filter
            if cutoff and pub:
                try:
                    if dateparser.parse(pub).date()<cutoff: continue
                except Exception: pass
            # deadline (regex)
            m=DL_RE.search(txt)
            deadline=None
            if m:
                try: deadline=dateparser.parse(m.group(1)).date().isoformat()
                except Exception: pass
            status=None
            try:
                if deadline and dateparser.parse(deadline).date()>=datetime.utcnow().date():
                    status="open"
            except Exception: pass
            out.append(Opportunity(
                id=_hash(title, detail), title=title, donor="AfDB", url=detail,
                deadline=deadline, published_date=pub, status=status,
                tags=_classify(title+" "+txt), country_scope=None
            ))
    return out

def fetch(max_items: int=60, since_days: Optional[int]=180, ogp_only: bool=True)->List[Dict]:
    ops: List[Opportunity]=[]
    for cat in CATS:
        ops.extend(_extract_list(cat, max_pages=4, since_days=since_days))

    # de-dup by URL
    uniq=[]; seen=set()
    for o in ops:
        if o.url in seen: continue
        seen.add(o.url); uniq.append(o)

    # coarse governance filter if requested
    if ogp_only:
        keep=[]
        for o in uniq:
            t=(o.title or "").lower()
            if any(k in t for k in ["governance","procurement","transparen","accountab","public finance","budget","audit","justice","rule of law","civic","participation","open data"]):
                keep.append(o)
        uniq=keep

    # sort by nearest deadline first, else recent published
    def sk(o: Opportunity):
        try: return (dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date(), 
                     -int(dateparser.parse(o.published_date).strftime("%s")) if o.published_date else 0)
        except Exception: return (datetime.max.date(), 0)
    uniq.sort(key=sk)
    return [o.to_dict() for o in uniq[:max_items]]

class Connector:
    name="afdb"
    def __init__(self, **kw): self.kwargs=kw
    def fetch(self, **kw)->List[Dict]: return fetch(**{**self.kwargs, **kw})
