from __future__ import annotations
import re, html
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
from utils.date_parse import to_iso_date

HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; anansi/1.0)"}

URLS = [
    # FR resource hub filtered to "appel_a_projets"
    "https://www.afd.fr/fr/ressources?query=&filters=type:%5B%22appel_a_projets%22%5D",
    # EN resource hub filtered to "call_for_projects"
    "https://www.afd.fr/en/resources?query=&filters=type:%5B%22call_for_projects%22%5D",
]

def _parse_page(u: str) -> List[Dict[str,Any]]:
    r = requests.get(u, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    out: List[Dict[str,Any]] = []

    # Cards are fairly generic; look for anchors pointing to /fr/ or /en/ resource pages
    for a in soup.select("a[href*='/fr/'], a[href*='/en/']"):
        href = a.get("href","")
        if not any(seg in href for seg in ["/ressources/", "/resources/"]):
            continue
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        url = href if href.startswith("http") else f"https://www.afd.fr{href}"

        # Crawl target page for deadline (AFD often puts dates on detail)
        deadline, country, blob = None, "", ""
        try:
            rr = requests.get(url, headers=HEADERS, timeout=30)
            rr.raise_for_status()
            ss = BeautifulSoup(rr.text, "lxml")
            txt = ss.get_text(" ", strip=True)
            blob = txt.lower()
            # generic deadline patterns
            m = re.search(r"(date\s+limite|deadline|closing)\s*[:\-]?\s*(.+?)($|\s{2,}|\. )", txt, flags=re.I)
            if m:
                deadline = to_iso_date(m.group(2))
        except Exception:
            pass

        out.append({
            "title": html.unescape(title),
            "source": "AFD",
            "deadline": deadline,
            "country": country,
            "topic": None,
            "url": url,
            "summary": blob,
        })
    return out

class Connector:
    def fetch(self, days_back: int = 60) -> List[Dict[str,Any]]:
        out: List[Dict[str,Any]] = []
        for u in URLS:
            try:
                out.extend(_parse_page(u))
            except Exception:
                continue
        return out

# ---- Back-compat procedural API (for existing aggregator) ----
def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    """
    Backwards-compatible wrapper so old aggregator imports work:
      from connectors.X import fetch as fetch_X
    """
    items = Connector().fetch(days_back=since_days)
    if ogp_only:
        try:
            from filters import ogp_relevant
            filt = []
            for it in items:
                text = f"{it.get('title','')} {it.get('summary','')}"
                if ogp_relevant(text):
                    filt.append(it)
            items = filt
        except Exception:
            # If filters module not present, just return items
            pass
    return items

def accepted_args():
    # Preserve your logging of accepted args
    return ["ogp_only", "since_days"]

