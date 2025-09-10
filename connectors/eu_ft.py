from __future__ import annotations
from typing import List, Dict, Any
import re, html
import requests
from bs4 import BeautifulSoup
import dateparser

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; anansi/1.0)"}
URL = "https://international-partnerships.ec.europa.eu/funding/funding-opportunities_en"

def _to_iso(d: str | None) -> str | None:
    if not d:
        return None
    dt = dateparser.parse(
        d,
        settings={"DATE_ORDER": "DMY", "PREFER_DAY_OF_MONTH": "first"},
        languages=["en","fr","es","de","it"]
    )
    return dt.date().isoformat() if dt else None

def _euft_fetch_impl(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    r = requests.get(URL, headers=HEADERS, timeout=45)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    out: List[Dict[str, Any]] = []
    cards = soup.select("article, .card, .listing__item, li")

    for card in cards:
        a = card.select_one("a[href]")
        if not a:
            continue
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        href = a.get("href", "")
        url = href if href.startswith("http") else f"https://international-partnerships.ec.europa.eu{href}"

        blob = card.get_text(" ", strip=True)
        # Extract a deadline if present
        deadline = None
        m = re.search(r"(deadline|closing|closes on)\s*[:\-]?\s*(.+?)($|\s{2,}|\. )", blob, flags=re.I)
        if m:
            deadline = _to_iso(m.group(2))

        country = ""
        tag = card.select_one(".ecl-tag, .tag, .badge")
        if tag:
            country = tag.get_text(" ", strip=True)

        out.append({
            "title": html.unescape(title),
            "source": "EU F&T (INTPA)",
            "deadline": deadline,
            "country": country,
            "topic": None,
            "url": url,
            "summary": blob.lower(),
        })

    # Optional: OGP filter/exclude to mirror others
    if ogp_only:
        try:
            from filters import ogp_relevant, is_excluded
            out = [it for it in out
                   if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")
                   and not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
        except Exception:
            pass

    return out

class Connector:
    def fetch(self, days_back: int = 90):
        return _euft_fetch_impl(days_back=days_back, ogp_only=True)

# ---- Back-compat procedural API (for existing aggregator) ----
def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _euft_fetch_impl(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
