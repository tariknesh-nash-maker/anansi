# -*- coding: utf-8 -*-
"""
AFD (Agence Française de Développement) — Calls for projects.

Primary listing:
  https://www.afd.fr/en/calls-for-projects/list?status[ongoing]=ongoing&status[soon]=soon

We parse the card grid to capture Title, URL, opening/closing dates, geography
and apply a lightweight OGP keyword classifier.

Exposed:
  - fetch(max_items=..., since_days=..., ogp_only=True)
  - Connector().fetch(...)

Test:
    python -m connectors.afd
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

BASE = "https://www.afd.fr"
LISTING = f"{BASE}/en/calls-for-projects/list?status[ongoing]=ongoing&status[soon]=soon"

OGP_WORDS = [
    "governance", "transparency", "accountability", "open data", "civic",
    "participation", "anti-corruption", "integrity", "justice", "rule of law",
    "public finance", "budget", "procurement", "PFM", "citizen", "digital", "data"
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

    def to_dict(self) -> Dict:
        return asdict(self)


def _hash_id(*parts: str) -> str:
    return "afd_" + hashlib.sha1(("::".join([p for p in parts if p])).encode("utf-8")).hexdigest()[:16]


def _classify_tags(text: str) -> List[str]:
    t = text.lower()
    tags = set()
    if any(k in t for k in ["digital", "data", "ai", "numérique", "données"]):
        tags.add("ai_digital")
    if any(k in t for k in ["budget", "finances publiques", "public finance", "pfm"]):
        tags.add("budget")
    if any(k in t for k in ["transparen", "accountab", "anti-corruption", "intégrité", "integrity"]):
        tags.add("anti_corruption")
    if any(k in t for k in ["civic", "participation", "citizen", "société civile"]):
        tags.add("civic_participation")
    if any(k in t for k in ["justice", "rule of law", "état de droit"]):
        tags.add("justice")
    if not tags:
        tags.add("governance")
    return sorted(tags)


def _get(url: str) -> requests.Response:
    r = requests.get(url, timeout=30, headers={"User-Agent": "anansi/afd"})
    r.raise_for_status()
    return r


def _parse_dates(text: str) -> (Optional[str], Optional[str]):
    """
    Returns (opening, closing) as ISO strings when found.
    """
    open_re = re.compile(r"(Opening|Ouverture)[^:]*:\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)
    close_re = re.compile(r"(Closing|Clôture)[^:]*:\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I)

    opening = None
    closing = None
    m = open_re.search(text)
    if m:
        try:
            opening = dateparser.parse(m.group(2)).date().isoformat()
        except Exception:
            pass
    m = close_re.search(text)
    if m:
        try:
            closing = dateparser.parse(m.group(2)).date().isoformat()
        except Exception:
            pass
    return opening, closing


def fetch(max_items: int = 60, since_days: Optional[int] = 365, ogp_only: bool = True) -> List[Dict]:
    resp = _get(LISTING)
    soup = BeautifulSoup(resp.text, "lxml")
    cards = soup.select("article, div.card, div.views-row") or []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date() if since_days else None
    out: List[Opportunity] = []

    for c in cards:
        a = c.select_one("a")
        if not a:
            continue
        title = a.get_text(strip=True)
        url = urljoin(BASE, a.get("href", ""))

        # Quick governance filter on card text (we’ll refine with the detail page)
        card_text = c.get_text(" ", strip=True)
        if ogp_only and not any(w in card_text.lower() for w in [w.lower() for w in OGP_WORDS]):
            # still fetch details; some cards have generic thumbnails but governance inside
            pass

        # fetch detail to read opening/closing & scope
        try:
            rd = _get(url)
        except Exception:
            continue
        s2 = BeautifulSoup(rd.text, "lxml")
        page_text = s2.get_text(" ", strip=True)

        opening, closing = _parse_dates(page_text)
        pub = None  # AFD often omits explicit publish date on calls

        if cutoff and opening:
            try:
                if dateparser.parse(opening).date() < cutoff:
                    continue
            except Exception:
                pass

        scope = None
        # try to find country/region chip
        chip = s2.select_one(".field--name-field-country, .field--name-field-geographical-area, .chips, .tags")
        if chip:
            scope = chip.get_text(" ", strip=True)

        tags = _classify_tags(title + " " + page_text)

        opp = Opportunity(
            id=_hash_id(title, url),
            title=title,
            donor="AFD",
            url=url,
            deadline=closing,
            published_date=pub,
            status="open" if (closing and dateparser.parse(closing).date() >= datetime.utcnow().date()) else "forthcoming" if opening and not closing else None,
            tags=tags,
            country_scope=scope,
        )
        out.append(opp)

        if len(out) >= max_items:
            break

    # Sort by closing date ascending (soonest first)
    def sort_key(o: Opportunity):
        return dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date()
    out.sort(key=sort_key)

    return [o.to_dict() for o in out]


class Connector:
    name = "afd"
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def fetch(self, **kwargs) -> List[Dict]:
        return fetch(**{**self.kwargs, **kwargs})


if __name__ == "__main__":
    import pprint
    pprint.pp(fetch(max_items=12))
