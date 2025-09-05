# -*- coding: utf-8 -*-
"""
African Development Bank (AfDB) — calls & project-related notices relevant to OGP.

Strategy:
- Parse AfDB "Current solicitations" (corporate procurement) and project-related procurement pages,
  then filter to governance-ish terms to avoid IT/office tenders.
- Additionally, discover active "Call for Proposals" landing pages (e.g., TSF Pillar III on SMApply).

Caveats:
- AfDB’s site uses multiple sections; we do conservative parsing and keyword filtering.
- Deadlines are pulled from card text or detail pages (regex), then normalized.

Exposed API:
  - fetch(max_items=..., since_days=..., ogp_only=True) -> List[dict]
  - Connector().fetch(...)

Run quick test:
    python -m connectors.afdb
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

LOG = logging.getLogger(__name__)

BASE = "https://www.afdb.org"
CURRENT_SOLICITATIONS = f"{BASE}/en/about-us/corporate-procurement/procurement-notices/current-solicitations"
REQ_EOI_ROOT = f"{BASE}/en/documents/project-related-procurement/procurement-notices/request-for-expression-of-interest"
SPN_ROOT = f"{BASE}/en/documents/project-related-procurement/procurement-notices/invitation-for-bids"
SEARCH_CFP = f"{BASE}/en/search?query=call%20for%20proposals&size=50&sort=publication_date,desc"

SMAPPLY_HINTS = [
    # Known AfDB calls hosted on SMApply (kept extensible)
    "https://afdb.smapply.org/",
]

OGP_WORDS = [
    "governance", "transparency", "accountability", "open data", "civic",
    "participation", "anti-corruption", "integrity", "justice", "rule of law",
    "public finance", "budget", "procurement", "PFM", "citizen"
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
    import hashlib
    h = hashlib.sha1(("::".join([p for p in parts if p])).encode("utf-8")).hexdigest()
    return f"afdb_{h[:16]}"


def _classify_tags(text: str) -> List[str]:
    t = text.lower()
    tags = set()
    if any(k in t for k in ["digital", "data", "ict"]):
        tags.add("ai_digital")
    if any(k in t for k in ["budget", "public finance", "pfm"]):
        tags.add("budget")
    if any(k in t for k in ["transparen", "accountab", "anti-corruption", "integrity", "procurement"]):
        tags.add("anti_corruption")
    if any(k in t for k in ["civic", "participation", "citizen"]):
        tags.add("civic_participation")
    if any(k in t for k in ["justice", "rule of law"]):
        tags.add("justice")
    if not tags:
        tags.add("governance")
    return sorted(tags)


def _parse_deadline(text: str) -> Optional[str]:
    if not text:
        return None
    rx = re.compile(
        r"(?:Deadline|Closing)\s*(?:Date|date|time|:)?\s*(?:\w+)?\s*(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})",
        re.I
    )
    m = rx.search(text)
    if m:
        try:
            return dateparser.parse(m.group(1)).date().isoformat()
        except Exception:
            return None
    # Also match “Deadline: 11-Aug-2025”
    m = re.search(r"(\d{1,2}[-\s][A-Za-z]{3,9}[-\s]\d{4})", text)
    if m:
        try:
            return dateparser.parse(m.group(1)).date().isoformat()
        except Exception:
            return None
    return None


def _get(url: str) -> requests.Response:
    r = requests.get(url, timeout=30, headers={"User-Agent": "anansi/afdb"})
    r.raise_for_status()
    return r


def _scrape_current_solicitations(since_days: Optional[int]) -> List[Opportunity]:
    r = _get(CURRENT_SOLICITATIONS)
    soup = BeautifulSoup(r.text, "lxml")
    cards = soup.select("div.views-row, article") or soup.select("article")
    out: List[Opportunity] = []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date() if since_days else None

    for c in cards:
        title_el = c.select_one("a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        url = urljoin(BASE, href)

        text = c.get_text(" ", strip=True)
        if not any(w.lower() in text.lower() for w in OGP_WORDS):
            continue  # skip non-governance items (e.g., printers, HVAC)

        deadline = _parse_deadline(text)

        # published date often present on detail page; fetch if needed
        pub = None
        if cutoff or not deadline:
            try:
                rd = _get(url)
                s2 = BeautifulSoup(rd.text, "lxml")
                # look for date fields
                meta_date = s2.select_one("time, span.date, div.field--name-field-publish-date")
                if meta_date:
                    pub = dateparser.parse(meta_date.get_text(strip=True)).date().isoformat()
                if not deadline:
                    deadline = _parse_deadline(s2.get_text(" ", strip=True))
            except Exception:
                pass

        if cutoff and pub:
            try:
                if dateparser.parse(pub).date() < cutoff:
                    continue
            except Exception:
                pass

        opp = Opportunity(
            id=_hash_id(title, url),
            title=title,
            donor="AfDB",
            url=url,
            deadline=deadline,
            published_date=pub,
            status="open" if (deadline and dateparser.parse(deadline).date() >= datetime.utcnow().date()) else None,
            tags=_classify_tags(text),
            country_scope=None,
        )
        out.append(opp)

    return out


def _scrape_search_cfps() -> List[Opportunity]:
    # Crawl AfDB site search results for "call for proposals"
    out: List[Opportunity] = []
    try:
        r = _get(SEARCH_CFP)
        soup = BeautifulSoup(r.text, "lxml")
        items = soup.select("article") or []
        for it in items[:30]:
            title_el = it.select_one("a")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            url = urljoin(BASE, title_el.get("href", ""))
            snippet = it.get_text(" ", strip=True)
            if not any(w in snippet.lower() for w in OGP_WORDS):
                continue

            # fetch detail for deadline
            deadline = None
            pub = None
            try:
                rd = _get(url)
                s2 = BeautifulSoup(rd.text, "lxml")
                pub_el = s2.select_one("time, span.date")
                if pub_el:
                    pub = dateparser.parse(pub_el.get_text(strip=True)).date().isoformat()
                deadline = _parse_deadline(s2.get_text(" ", strip=True))
            except Exception:
                pass

            out.append(
                Opportunity(
                    id=_hash_id(title, url),
                    title=title,
                    donor="AfDB",
                    url=url,
                    deadline=deadline,
                    published_date=pub,
                    status="open" if (deadline and dateparser.parse(deadline).date() >= datetime.utcnow().date()) else None,
                    tags=_classify_tags(snippet + " " + (title or "")),
                    country_scope=None,
                )
            )
    except Exception as e:
        LOG.warning("AfDB search crawl failed: %s", e)
    return out


def _include_smapply_calls() -> List[Opportunity]:
    out: List[Opportunity] = []
    for base in SMAPPLY_HINTS:
        try:
            r = _get(base)
            # just list landing; specific programs change slug — detect visible “Call for Proposals”
            if "smapply" in r.url:
                # A minimal placeholder listing (deadline often present on the program page)
                opp = Opportunity(
                    id=_hash_id("SMApply landing", base),
                    title="AfDB — Calls for Proposals (SMApply entrypoint)",
                    donor="AfDB",
                    url=base,
                    deadline=None,
                    published_date=None,
                    status=None,
                    tags=["governance"],
                    country_scope="Africa (varies)",
                )
                out.append(opp)
        except Exception:
            continue
    return out


def fetch(max_items: int = 60, since_days: Optional[int] = 180, ogp_only: bool = True) -> List[Dict]:
    ops: List[Opportunity] = []
    ops.extend(_scrape_current_solicitations(since_days=since_days))
    ops.extend(_scrape_search_cfps())
    ops.extend(_include_smapply_calls())

    # de-dup by URL
    seen = set()
    uniq: List[Opportunity] = []
    for o in ops:
        if o.url in seen:
            continue
        seen.add(o.url)
        uniq.append(o)

    # optional governance filter (already mostly filtered)
    if ogp_only:
        uniq = [o for o in uniq if any(w in (o.title + " ").lower() for w in [w.lower() for w in OGP_WORDS]) or "governance" in " ".join(o.tags)]

    # sort by deadline (then published_date) descending freshness
    def sort_key(o: Opportunity):
        dl = dateparser.parse(o.deadline).date() if o.deadline else datetime.max.date()
        pub = dateparser.parse(o.published_date).date() if o.published_date else datetime.min.date()
        return (dl, pub)

    uniq.sort(key=sort_key)
    return [o.to_dict() for o in uniq[:max_items]]


class Connector:
    name = "afdb"
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def fetch(self, **kwargs) -> List[Dict]:
        return fetch(**{**self.kwargs, **kwargs})


if __name__ == "__main__":
    import pprint
    pprint.pp(fetch(max_items=12))
