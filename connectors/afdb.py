# connectors/afdb.py
# AfDB procurement: try RSS feeds first; if empty, fall back to scraping listings.
from __future__ import annotations
from typing import List, Dict, Any, Set
from datetime import datetime, timedelta, timezone
import os, time, re, logging
import requests, feedparser
from bs4 import BeautifulSoup

UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (compatible; anansi/1.0)")
HEADERS = {"User-Agent": UA}

RSS_FEEDS = [
    # Project-related procurement feeds
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml",
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml",
    # Corporate procurement (Bank’s own tenders) – keep both forms as different sites resolve
    "https://www.afdb.org/en/about-us/corporate-procurement?format=rss",
    "https://www.afdb.org/en/corporate-procurement/news-and-events/rss",
]

LISTING_PAGES = [
    # Project procurement listings
    "https://www.afdb.org/en/projects-and-operations/procurement/notices",
    "https://www.afdb.org/en/projects-and-operations/procurement/consultancy",
]

DEADLINE_RE = re.compile(r"(deadline|closing(?: date)?)\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})", re.I)

def _to_iso_date_str(dt: datetime | None) -> str | None:
    return dt.date().isoformat() if dt else None

def _rss_fetch(days_back: int, max_items: int = 50, debug: bool = False) -> List[Dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    out: List[Dict[str, Any]] = []
    for url in RSS_FEEDS:
        try:
            fp = feedparser.parse(url)
            if debug:
                logging.info(f"[afdb] RSS feed={url} status={getattr(fp,'status','?')} entries={len(fp.entries)}")
            for e in fp.entries:
                title = (getattr(e, "title", "") or "").strip()
                link = (getattr(e, "link", "") or "").strip()
                if not title or not link:
                    continue
                pub = None
                if getattr(e, "published_parsed", None):
                    pub = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=timezone.utc)
                elif getattr(e, "updated_parsed", None):
                    pub = datetime.fromtimestamp(time.mktime(e.updated_parsed), tz=timezone.utc)
                if pub and pub.date() < cutoff:
                    continue
                summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "").lower()
                # try to parse an inline deadline if present
                dl = None
                m = DEADLINE_RE.search(summary)
                if m:
                    try:
                        dl = datetime.strptime(m.group(2), "%d %B %Y").date().isoformat()
                    except Exception:
                        dl = None
                out.append({
                    "title": title,
                    "source": "AfDB",
                    "deadline": dl,
                    "country": "",
                    "topic": None,
                    "url": link,
                    "summary": summary,
                })
                if len(out) >= max_items:
                    return out
        except Exception as e:
            if debug:
                logging.warning(f"[afdb] RSS failed {url}: {e}")
            continue
    return out

def _html_listing_urls(url: str, debug: bool = False) -> Set[str]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    urls: Set[str] = set()
    # collect anchors that look like notice detail pages
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = href if href.startswith("http") else f"https://www.afdb.org{href}"
        if "/procurement/" in full and "/en/" in full:
            urls.add(full)
    if debug:
        logging.info(f"[afdb] listing {url} -> {len(urls)} candidate URLs")
    return urls

def _html_parse_detail(url: str, debug: bool = False) -> Dict[str, Any] | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        h = soup.select_one("h1") or soup.select_one("h2")
        title = (h.get_text(" ", strip=True) if h else "").strip()
        if not title:
            # fallback to <title>
            ttag = soup.select_one("title")
            title = (ttag.get_text(" ", strip=True) if ttag else "") or "AfDB Notice"
        text = soup.get_text(" ", strip=True)
        # Try structured terms lists first
        deadline = None
        for dt in soup.select("dt, strong, b"):
            lbl = dt.get_text(" ", strip=True).lower()
            if "dead" in lbl or "clos" in lbl:
                sib = dt.find_next("dd") or dt.parent.find_next("dd")
                if sib:
                    raw = sib.get_text(" ", strip=True)
                    m = DEADLINE_RE.search(f"deadline {raw}")
                    if m:
                        try:
                            deadline = datetime.strptime(m.group(2), "%d %B %Y").date().isoformat()
                            break
                        except Exception:
                            pass
        # Fallback: regex across page text
        if not deadline:
            m2 = DEADLINE_RE.search(text)
            if m2:
                try:
                    deadline = datetime.strptime(m2.group(2), "%d %B %Y").date().isoformat()
                except Exception:
                    deadline = None
        return {
            "title": title,
            "source": "AfDB",
            "deadline": deadline,
            "country": "",
            "topic": None,
            "url": url,
            "summary": text.lower()[:800],
        }
    except Exception as e:
        if debug:
            logging.warning(f"[afdb] detail failed {url}: {e}")
        return None

def _afdb_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    debug = _env_bool("AFDB_DEBUG", False)
    max_items = _env_int("AFDB_MAX", 40)

    # 1) Try RSS first
    out = _rss_fetch(days_back=days_back, max_items=max_items, debug=debug)
    if out:
        return _apply_filters(out, ogp_only)

    # 2) Fallback: HTML listings → detail pages
    urls: Set[str] = set()
    for lp in LISTING_PAGES:
        try:
            urls |= _html_listing_urls(lp, debug=debug)
        except Exception as e:
            if debug:
                logging.warning(f"[afdb] listing failed {lp}: {e}")
            continue

    items: List[Dict[str, Any]] = []
    for u in list(urls)[: max_items * 2]:
        it = _html_parse_detail(u, debug=debug)
        if it:
            items.append(it)
        if len(items) >= max_items:
            break

    return _apply_filters(items, ogp_only)

# ------- helpers & public API -------

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v is None else str(v).strip().lower() in ("1","true","yes")

def _apply_filters(items: List[Dict[str, Any]], ogp_only: bool) -> List[Dict[str, Any]]:
    # Soft OGP preference + exclude auctions if filters.py exists
    try:
        from filters import ogp_relevant, is_excluded
        items = [it for it in items if not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
        if ogp_only:
            preferred = [it for it in items if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")]
            items = preferred or items
    except Exception:
        pass
    return items

class Connector:
    def fetch(self, days_back: int = 90):
        return _afdb_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _afdb_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
