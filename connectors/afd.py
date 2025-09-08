# connectors/afd.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import hashlib, logging, re, requests
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

BASE = "https://www.afd.fr"
LIST_EN = f"{BASE}/en/calls-for-projects/list?status[ongoing]=ongoing&status[soon]=soon"
LIST_FR = f"{BASE}/fr/appels-a-projets/liste?status[ongoing]=ongoing&status[soon]=soon"
LIST_EN_ALL = f"{BASE}/en/calls-for-projects/list"
LIST_FR_ALL = f"{BASE}/fr/appels-a-projets/liste"

HEADERS = {"User-Agent": "anansi/afd", "Accept-Language": "en,fr;q=0.9"}

DETAIL_PATTERNS = [
    re.compile(r"^/en/calls-for-projects/[^/?#]+/?$", re.I),
    re.compile(r"^/fr/appels-a-projets/[^/?#]+/?$", re.I),
]
BLACKLIST_SEGMENTS = {"list", "liste", "previous", "precedents", "voir", "see", "en", "fr"}

CLOSE_RE = re.compile(
    r"(?:Closing|Cl[ôo]ture|Date\s+limite)\s*(?:date|:)?\s*"
    r"(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})",
    re.I,
)

OGP = [
    "gouvernance","governance","transparency","transparence","accountability",
    "open data","données ouvertes","participation","société civile","integrity",
    "intégrité","justice","rule of law","état de droit","finances publiques",
    "public finance","budget","pfm","numérique","digital","data","media","démocratie","democracy",
]

@dataclass
class Opportunity:
    id: str; title: str; donor: str; url: str
    deadline: Optional[str]; published_date: Optional[str]; status: Optional[str]
    tags: List[str]; country_scope: Optional[str]
    amount: Optional[str]=None; currency: Optional[str]=None
    def to_dict(self)->Dict: return asdict(self)

def _hash(*p: str)->str:
    return "afd_" + hashlib.sha1("::".join([x for x in p if x]).encode()).hexdigest()[:16]

def _classify(text: str)->List[str]:
    t = (text or "").lower()
    tags=set()
    if any(k in t for k in ["digital","data","ai","numérique","données"]): tags.add("ai_digital")
    if any(k in t for k in ["budget","finances publiques","public finance","pfm"]): tags.add("budget")
    if any(k in t for k in ["transparen","accountab","anti-corruption","intégrit","integrity"]): tags.add("anti_corruption")
    if any(k in t for k in ["civic","participation","citizen","société civile","media"]): tags.add("civic_participation")
    if any(k in t for k in ["justice","rule of law","état de droit","democracy","démocratie"]): tags.add("justice")
    if not tags: tags.add("governance")
    return sorted(tags)

def _get(url: str) -> BeautifulSoup:
    r = requests.get(url, timeout=30, headers=HEADERS)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _is_detail_url(abs_url: str, title_text: str) -> bool:
    try:
        p = urlparse(abs_url)
        path = p.path.rstrip("/")
        junk_titles = {
            "fr - français", "en - english",
            "list of calls for projects", "liste des appels à projets",
            "see previous calls for projects", "voir les précédents appels à projets",
        }
        if title_text.strip().lower() in junk_titles:
            return False
        if any(seg in path.lower().split("/") for seg in BLACKLIST_SEGMENTS):
            return False
        return any(rx.match(path) for rx in DETAIL_PATTERNS)
    except Exception:
        return False

def _parse_detail(detail_url: str) -> Optional[Opportunity]:
    try:
        s2 = _get(detail_url)
    except requests.HTTPError:
        return None

    title_el = s2.select_one("h1, .page-title, .node__title")
    title = (title_el.get_text(" ", strip=True) if title_el else "").strip()
    if not title:
        return None

    text = s2.get_text(" ", strip=True)

    scope = None
    chip = s2.select_one(".field--name-field-country, .field--name-field-geographical-area, .chips, .tags")
    if chip:
        scope = chip.get_text(" ", strip=True)

    deadline = None
    m = CLOSE_RE.search(text)
    if m:
        try:
            deadline = dateparser.parse(m.group(1)).date().isoformat()
        except Exception:
            deadline = None

    if not deadline:
        return None

    today = datetime.now(timezone.utc).date()
    try:
        if dateparser.parse(deadline).date() <= today:
            return None
    except Exception:
        return None

    tags = _classify(title + " " + text)
    return Opportunity(
        id=_hash(title, detail_url),
        title=title,
        donor="AFD",
        url=detail_url,
        deadline=deadline,
        published_date=None,
        status="open",
        tags=tags,
        country_scope=scope,
    )

def _extract(list_url: str, max_pages: int) -> List[Opportunity]:
    out: List[Opportunity] = []
    seen = set()
    for page in range(max_pages):
        url = f"{list_url}&page={page}" if ("?" in list_url and page > 0) else (f"{list_url}?page={page}" if page > 0 else list_url)
        try:
            soup = _get(url)
        except requests.HTTPError as e:
            LOG.warning("AFD listing failed %s: %s", url, e); break

        for a in soup.select("a[href*='/calls-for-projects/'], a[href*='/appels-a-projets/']"):
            href = a.get("href", ""); title = a.get_text(strip=True)
            if not href or not title:
                continue
            detail = urljoin(BASE, href)
            if detail in seen:
                continue
            if not _is_detail_url(detail, title):
                continue
            seen.add(detail)
            opp = _parse_detail(detail)
            if opp:
                out.append(opp)
    return out

def fetch(max_items: int = 60, since_days: Optional[int] = 365, ogp_only: bool = True) -> List[Dict]:
    raw = []
    for lst in (LIST_EN, LIST_FR, LIST_EN_ALL, LIST_FR_ALL):
        raw.extend(_extract(lst, max_pages=5))
    LOG.info("AFD: detail pages with future deadlines found=%s", len(raw))

    if ogp_only:
        filtered = []
        for o in raw:
            t = (o.title + " " + " ".join(o.tags or [])).lower()
            if any(k in t for k in [k.lower() for k in OGP]):
                filtered.append(o)
    else:
        filtered = raw

    def sk(o: Opportunity):
        try:
            return dateparser.parse(o.deadline).date()
        except Exception:
            return datetime.max.date()

    filtered.sort(key=sk)
    return [o.to_dict() for o in filtered[:max_items]]

class Connector:
    name = "afd"
    def __init__(self, **kw): self.kwargs = kw
    def fetch(self, **kw) -> List[Dict]: return fetch(**{**self.kwargs, **kw})
