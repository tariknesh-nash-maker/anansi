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

BASE = "https://econsultant2.afdb.org"
LISTING = f"{BASE}/advertisements"   # public listing of AfDB procurement adverts

HEADERS = {
    # mimic a real browser; AfDB WAF tends to allow this origin
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
    "Referer": "https://econsultant2.afdb.org/",
}

OGP_HINTS = [
    "governance","transparen","accountab","open data","digital","ict","pfm",
    "public finance","budget","audit","integrity","anti-corruption","justice","rule of law",
    "citizen","participation","civic","procurement","monitoring","evaluation","data"
]
DL_RX = re.compile(r"(?:Deadline|Closing)\s*(?:Date|Time)?\s*[:\-]?\s*"
                   r"(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)

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

def _get(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _pages(max_pages: int = 5) -> List[str]:
    # eConsultant2 paginates with ?page=N (0-based). We'll probe a few pages.
    return [LISTING] + [f"{LISTING}?page={i}" for i in range(1, max_pages)]

def fetch(max_items: int = 60, since_days: Optional[int] = 180, ogp_only: bool = True) -> List[Dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date() if since_days else None
    out: List[Opportunity] = []
    seen = set()

    for url in _pages(max_pages=6):
        try:
            soup = _get(url)
        except requests.HTTPError as e:
            LOG.warning("AfDB eConsultant2 list failed %s: %s", url, e)
            continue

        # Each card has a link to /advertisement/<id>
        links = soup.select("a[href*='/advertisement/']")
        for a in links:
            href = a.get("href","")
            title = a.get_text(" ", strip=True)
            if not href or not title: continue
            detail = urljoin(BASE, href)
            if detail in seen: continue
            seen.add(detail)

            # detail page
            try:
                s2 = _get(detail)
            except requests.HTTPError:
                continue

            text = s2.get_text(" ", strip=True)
            # published (first <time> or "Posted on")
            pub = None
            t = s2.select_one("time")
            if t:
                try: pub = dateparser.parse(t.get_text(" ", strip=True)).date().isoformat()
                except Exception: pass
            else:
                m = re.search(r"(Posted on|Publication Date)\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+\d{4})", text, re.I)
                if m:
                    try: pub = dateparser.parse(m.group(2)).date().isoformat()
                    except Exception: pass
            if cutoff and pub:
                try:
                    if dateparser.parse(pub).date() < cutoff:
                        continue
                except Exception:
                    pass

            # deadline
            deadline = None
            m = DL_RX.search(text)
            if m:
                try: deadline = dateparser.parse(m.group(1)).date().isoformat()
                except Exception: pass

            # quick governance filter
            if ogp_only and not any(h in (title + " " + text).lower() for h in OGP_HINTS):
                continue

            status = None
            try:
                if deadline and dateparser.parse(deadline).date() >= datetime.utcnow().date():
                    status = "open"
            except Exception:
                pass

            out.append(Opportunity(
                id=_hash(title, detail),
                title=title.strip(),
                donor="AfDB",
                url=detail,
                deadline=deadline,
                published_date=pub,
                status=status,
                tags=_classify(title + " " + text),
                country_scope=None
            ))
            if len(out) >= max_items:
                break
        if len(out) >= max_items:
            break

    # sort: nearest deadline, then newest publication
    def sk(o: Opportunity):
        try: dl = dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date()
        except Exception: dl = datetime.max.date()
        try: pu = dateparser.parse(o.published_date).date() if o.published_date else datetime.min.date()
        except Exception: pu = datetime.min.date()
        return (dl, -int(pu.strftime("%s")))
    out.sort(key=sk)
    return [o.to_dict() for o in out[:max_items]]

class Connector:
    name = "afdb"
    def __init__(self, **kw): self.kwargs = kw
    def fetch(self, **kw)->List[Dict]: return fetch(**{**self.kwargs, **kw})
