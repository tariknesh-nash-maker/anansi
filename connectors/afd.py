# connectors/afd.py
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

def _get(url: str) -> requests.Response:
    r = requests.get(url, timeout=30, headers={"User-Agent": "anansi/afd"})
    r.raise_for_status()
    return r

OGP_WORDS = ["gouvernance","governance","transparency","transparence","accountability","open data","donn\u00e9es ouvertes","participation","soci\u00e9t\u00e9 civile","int\u00e9grit\u00e9","integrity","justice","rule of law","\u00e9tat de droit","finances publiques","public finance","budget","PFM","num\u00e9rique","digital","data"]

@dataclass
class Opportunity:
    id: str; title: str; donor: str; url: str
    deadline: Optional[str]; published_date: Optional[str]; status: Optional[str]
    tags: List[str]; country_scope: Optional[str]
    amount: Optional[str]=None; currency: Optional[str]=None
    def to_dict(self)->Dict: return asdict(self)

def _hash(*p: str)->str: return "afd_" + hashlib.sha1("::".join([x for x in p if x]).encode()).hexdigest()[:16]

def _classify(text: str)->List[str]:
    t = text.lower(); tags=set()
    if any(k in t for k in ["digital","data","num\u00e9rique","donn\u00e9es"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","finances publiques","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","int\u00e9grit","integrity"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen","soci\u00e9t\u00e9 civile"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law","\u00e9tat de droit"]): tags.add("justice")
    if not tags: tags.add("governance"); return sorted(tags)

def _parse_dates(text: str)->(Optional[str],Optional[str]):
    open_re  = re.compile(r"(Opening|Ouverture)[^:]*:\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    close_re = re.compile(r"(Closing|Cl\u00f4ture)[^:]*:\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    o=c=None
    m=open_re.search(text);  o=dateparser.parse(m.group(2)).date().isoformat() if m else None
    m=close_re.search(text); c=dateparser.parse(m.group(2)).date().isoformat() if m else None
    return o,c

def _extract_from_listing(list_url: str, max_pages: int, since_days: Optional[int])->List[Opportunity]:
    out: List[Opportunity]=[]
    cutoff=(datetime.now(timezone.utc)-timedelta(days=since_days)).date() if since_days else None
    for page in range(max_pages):
        url = f"{list_url}&page={page}" if page>0 else list_url
        try: soup=BeautifulSoup(_get(url).text,"lxml")
        except Exception as e: 
            LOG.warning("AFD listing fetch failed %s: %s", url, e); break

        cards = soup.select("article.node--type-call-for-project, article.teaser, .view-content article, .card a[href*='/calls-for-projects/'], .card a[href*='/appels-a-projets/']")
        links = []
        for a in soup.select("a[href*='/calls-for-projects/'], a[href*='/appels-a-projets/']"):
            title=a.get_text(strip=True); href=a.get("href","")
            if not title or not href: continue
            links.append((title, urljoin(BASE, href)))
        # de-dup
        seen=set()
        for title,detail_url in links:
            if detail_url in seen: continue
            seen.add(detail_url)
            try:
                s2=BeautifulSoup(_get(detail_url).text,"lxml")
            except Exception: 
                continue
            page_text=s2.get_text(" ", strip=True)
            opening,closing=_parse_dates(page_text)
            # rough governance check
            text_check=(title+" "+page_text).lower()
            if any(w in text_check for w in [w.lower() for w in OGP_WORDS]):
                pub=None; scope=None
                chip = s2.select_one(".field--name-field-country, .field--name-field-geographical-area, .chips, .tags")
                if chip: scope=chip.get_text(" ", strip=True)
                status = "open" if (closing and dateparser.parse(closing).date() >= datetime.utcnow().date()) else ("forthcoming" if opening and not closing else None)
                if cutoff and opening:
                    try:
                        if dateparser.parse(opening).date()<cutoff: continue
                    except Exception: pass
                out.append(Opportunity(
                    id=_hash(title, detail_url), title=title, donor="AFD", url=detail_url,
                    deadline=closing, published_date=pub, status=status, tags=_classify(text_check), country_scope=scope
                ))
    return out

def fetch(max_items: int=60, since_days: Optional[int]=365, ogp_only: bool=True)->List[Dict]:
    ops=_extract_from_listing(LIST_EN, max_pages=3, since_days=since_days)
    if not ops:
        ops=_extract_from_listing(LIST_FR, max_pages=3, since_days=since_days)
    # sort by earliest deadline first
    def sk(o: Opportunity): 
        return dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date()
    ops.sort(key=sk)
    return [o.to_dict() for o in ops[:max_items]]

class Connector:
    name="afd"
    def __init__(self, **kw): self.kwargs=kw
    def fetch(self, **kw)->List[Dict]: return fetch(**{**self.kwargs, **kw})
