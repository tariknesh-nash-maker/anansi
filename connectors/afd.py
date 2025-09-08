# -*- coding: utf-8 -*-
from __future__ import annotations
import hashlib, logging, re, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)
BASE = "https://www.afd.fr"
LIST_EN = f"{BASE}/en/calls-for-projects/list?status[ongoing]=ongoing&status[soon]=soon"
LIST_FR = f"{BASE}/fr/appels-a-projets/liste?status[ongoing]=ongoing&status[soon]=soon"
LIST_EN_ALL = f"{BASE}/en/calls-for-projects/list"
LIST_FR_ALL = f"{BASE}/fr/appels-a-projets/liste"

HEADERS = {"User-Agent": "anansi/afd", "Accept-Language": "en,fr;q=0.9"}

OGP = ["gouvernance","governance","transparency","transparence","accountability","open data","données ouvertes","participation","société civile","integrity","intégrité","justice","rule of law","état de droit","finances publiques","public finance","budget","PFM","numérique","digital","data"]

@dataclass
class Opportunity:
    id: str; title: str; donor: str; url: str
    deadline: Optional[str]; published_date: Optional[str]; status: Optional[str]
    tags: List[str]; country_scope: Optional[str]
    amount: Optional[str]=None; currency: Optional[str]=None
    def to_dict(self)->Dict: return asdict(self)

def _hash(*p: str)->str: return "afd_" + hashlib.sha1("::".join([x for x in p if x]).encode()).hexdigest()[:16]

def _classify(text: str)->List[str]:
    t = (text or "").lower(); tags=set()
    if any(k in t for k in ["digital","data","ai","numérique","données"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","finances publiques","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","intégrit","integrity"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen","société civile","media"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law","état de droit","democracy"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _get(url: str) -> BeautifulSoup:
    r = requests.get(url, timeout=30, headers=HEADERS)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _parse_dates(text: str)->(Optional[str],Optional[str]):
    open_re  = re.compile(r"(Opening|Ouverture)[^:]*:\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    close_re = re.compile(r"(Closing|Cl[ôo]ture)[^:]*:\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    o=c=None
    m=open_re.search(text);  o=dateparser.parse(m.group(2)).date().isoformat() if m else None
    m=close_re.search(text); c=dateparser.parse(m.group(2)).date().isoformat() if m else None
    return o,c

def _extract(list_url: str, max_pages: int)->List[Opportunity]:
    out: List[Opportunity]=[]
    seen=set()
    for page in range(max_pages):
        url = f"{list_url}&page={page}" if ("?" in list_url and page>0) else (f"{list_url}?page={page}" if page>0 else list_url)
        try:
            soup=_get(url)
        except requests.HTTPError as e:
            LOG.warning("AFD listing failed %s: %s", url, e); break

        for a in soup.select("a[href*='/calls-for-projects/'], a[href*='/appels-a-projets/']"):
            href=a.get("href",""); title=a.get_text(strip=True)
            if not href or not title: continue
            detail=urljoin(BASE, href)
            if detail in seen: continue
            seen.add(detail)
            try:
                s2=_get(detail)
            except requests.HTTPError:
                continue
            text=s2.get_text(" ", strip=True)
            opening, closing=_parse_dates(text)
            scope=None
            chip = s2.select_one(".field--name-field-country, .field--name-field-geographical-area, .chips, .tags")
            if chip: scope=chip.get_text(" ", strip=True)
            out.append(Opportunity(
                id=_hash(title, detail), title=title, donor="AFD", url=detail,
                deadline=closing, published_date=None,
                status="open" if (closing and dateparser.parse(closing).date()>=datetime.now(timezone.utc).date()) else ("forthcoming" if opening and not closing else None),
                tags=_classify(title+" "+text), country_scope=scope
            ))
    return out

def fetch(max_items: int = 60, since_days: Optional[int] = 365, ogp_only: bool = True) -> List[Dict]:
    raw = []
    for lst in (LIST_EN, LIST_FR, LIST_EN_ALL, LIST_FR_ALL):
        raw.extend(_extract(lst, max_pages=5))
    LOG.info("AFD: raw found=%s before filters", len(raw))

    # keep deadline strictly after today when present
    today = datetime.now(timezone.utc).date()
    filtered = []
    for o in raw:
        if o.deadline:
            try:
                if dateparser.parse(o.deadline).date() <= today:
                    continue
            except Exception:
                pass
        if ogp_only:
            t=(o.title+" "+" ".join(o.tags)).lower()
            if not any(k in t for k in [k.lower() for k in OGP]):
                continue
        filtered.append(o)

    LOG.info("AFD: after filters=%s (ogp_only=%s)", len(filtered), ogp_only)

    # sort by earliest deadline first
    def sk(o: Opportunity):
        try: return dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date()
        except Exception: return datetime.max.date()
    filtered.sort(key=sk)

    return [o.to_dict() for o in filtered[:max_items]]

class Connector:
    name="afd"
    def __init__(self, **kw): self.kwargs=kw
    def fetch(self, **kw)->List[Dict]: return fetch(**{**self.kwargs, **kw})
