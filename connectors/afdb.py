# connectors/afdb.py
# African Development Bank (AfDB) connector
# Strategy:
#   1) Try RSS (often blocked by WAF). If blocked or bozo, skip quickly.
#   2) Crawl server-rendered "documents" listings (multiple entry points).
#   3) As needed, enable AFDB_USE_READER=1 to route through a fetch-only reader (no JS).
#   4) Parse detail pages for title/country/deadline; be tolerant, never crash.
#
# Env knobs:
#   AFDB_DEBUG=1           -> verbose logs
#   AFDB_MAX=40            -> max items to return
#   AFDB_USE_READER=1      -> enable reader fallback (https://r.jina.ai)
#   AFDB_ACCEPT_LANGUAGE   -> override Accept-Language header

from __future__ import annotations
from typing import List, Dict, Any, Set
from datetime import date, timedelta
import os, re, time, logging, requests, feedparser
from urllib.parse import urljoin
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

UA = os.getenv("ANANSI_UA",
               "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": os.getenv("AFDB_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
    "Referer": "https://www.afdb.org/en/projects-and-operations/procurement",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# RSS (often 403 or HTML "human check")
RSS_FEEDS = [
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml",
    "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml",
]

# Multiple server-rendered entry points for documents
LIST_PAGES = [
    "https://www.afdb.org/en/documents/project-related-procurement/procurement-notices/specific-procurement-notices",
    "https://www.afdb.org/en/documents/category/general-procurement-notices",
    "https://www.afdb.org/en/documents/category/invitation-for-bids",
    "https://www.afdb.org/en/documents/category/request-for-expression-of-interest",
    # Site search pages (document-only) as a further fallback:
    "https://www.afdb.org/en/search?keys=procurement%20notice&type=document&sort_by=created&sort_order=DESC",
    "https://www.afdb.org/en/search?keys=general%20procurement%20notice&type=document&sort_by=created&sort_order=DESC",
    "https://www.afdb.org/en/search?keys=specific%20procurement%20notice&type=document&sort_by=created&sort_order=DESC",
    "https://www.afdb.org/en/search?keys=invitation%20for%20bids&type=document&sort_by=created&sort_order=DESC",
    "https://www.afdb.org/en/search?keys=expression%20of%20interest&type=document&sort_by=created&sort_order=DESC",
]

DEADLINE_RE = re.compile(r"(?:deadline|closing(?: date)?)\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})", re.I)

def _is_on(*names: str) -> bool:
    for n in names:
        v = os.getenv(n)
        if v and str(v).strip().lower() in ("1", "true", "yes", "on"):
            return True
    return False

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _reader_url(url: str) -> str:
    base = os.getenv("READER_BASE", "https://r.jina.ai/http://")
    if url.startswith("https://"):
        return f"{base}{url[len('https://'):]}"
    if url.startswith("http://"):
        return f"{base}{url[len('http://'):]}"
    return f"{base}{url}"

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    # Warm-up (may set harmless cookies)
    try:
        s.get("https://www.afdb.org/en", timeout=15)
    except Exception:
        pass
    return s

def _rss_fetch(days_back: int, max_items: int, verbose: bool) -> List[Dict[str, Any]]:
    cutoff = date.today() - timedelta(days=days_back or 90)
    s = _session()
    out: List[Dict[str, Any]] = []
    for url in RSS_FEEDS:
        body_text = None
        try:
            r = s.get(url, timeout=20)
            if verbose:
                log.info("[afdb:rss_http] url=%r status=%s bytes=%d", url, r.status_code, len(r.text or ""))
            if r.status_code == 403 and _is_on("AFDB_USE_READER"):
                rr = s.get(_reader_url(url), timeout=25, headers={"Accept": "application/xml"})
                if verbose:
                    log.info("[afdb:rss_reader] url=%r status=%s bytes=%d", url, rr.status_code, len(rr.text or ""))
                if rr.ok:
                    body_text = rr.text
            elif r.ok:
                body_text = r.text
        except Exception as ex:
            if verbose:
                log.warning("[afdb:rss_err] url=%r err=%s", url, ex)

        if not body_text:
            continue

        feed = feedparser.parse(body_text)
        if verbose:
            log.info("[afdb:rss_entries] url=%r entries=%d bozo=%s", url, len(feed.entries), getattr(feed, "bozo", "?"))

        for e in feed.entries:
            title = (getattr(e, "title", "") or "").strip()
            link  = (getattr(e, "link", "") or "").strip()
            if not title or not link:
                continue
            # Simple time window, if present
            pub_dt = None
            if getattr(e, "published_parsed", None):
                from datetime import datetime as _dt
                tm = e.published_parsed
                pub_dt = _dt(*tm[:6]).date()
            if pub_dt and pub_dt < cutoff:
                continue
            summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "")
            deadline = _parse_deadline(summary)
            out.append({
                "source": "AfDB",
                "title": title,
                "country": None,
                "deadline": deadline,
                "url": link,
                "topic": "Open Government",
                "summary": (summary or title).lower(),
            })
            if len(out) >= max_items:
                return out
    if verbose:
        log.info("[afdb:rss_result] kept=%d", len(out))
    return out

def _parse_deadline(text: str) -> str | None:
    m = DEADLINE_RE.search(text or "")
    if not m:
        return None
    raw = m.group(1)
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            from datetime import datetime as _dt
            return _dt.strptime(raw, fmt).date().isoformat()
        except Exception:
            pass
    return None

def _get_html(s: requests.Session, url: str, verbose: bool) -> str | None:
    try:
        r = s.get(url, timeout=25)
        if r.status_code == 403 and _is_on("AFDB_USE_READER"):
            rr = s.get(_reader_url(url), timeout=25)
            if verbose:
                log.info("[afdb:list_reader] url=%r status=%s bytes=%d", url, rr.status_code, len(rr.text or ""))
            if rr.ok:
                return rr.text
        if r.ok:
            if verbose:
                log.info("[afdb:list_http] url=%r status=%s bytes=%d", url, r.status_code, len(r.text or ""))
            return r.text
        if verbose:
            log.info("[afdb:list_http] url=%r status=%s bytes=%d", url, r.status_code, len(r.text or ""))
        return None
    except Exception as ex:
        if verbose:
            log.warning("[afdb:get_err] url=%r err=%s", url, ex)
        return None

def _collect_links_from_listing(html: str, base: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    # Collect ANY /en/documents/ anchor (server-rendered)
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(base, href)
        if "/en/documents/" in full:
            links.append(full.split("#")[0])
    # De-dupe, preserve order
    seen: Set[str] = set()
    out: List[str] = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _parse_detail(s: requests.Session, url: str, verbose: bool) -> Dict[str, Any] | None:
    html = _get_html(s, url, verbose)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    title_tag = soup.select_one("h1, h2") or soup.select_one("title")
    title = (title_tag.get_text(" ", strip=True) if title_tag else "AfDB notice").strip()

    # Extract labeled fields if present
    labels = {}
    for dl in soup.select("dl, .field--name-field-document, .field__items"):
        for dt in dl.select("dt"):
            key = dt.get_text(" ", strip=True).lower()
            dd = dt.find_next("dd")
            val = dd.get_text(" ", strip=True) if dd else ""
            if key: labels[key] = val

    deadline = None
    for k, v in labels.items():
        if "deadline" in k or "closing" in k:
            deadline = _parse_deadline(f"deadline {v}") or v

    # Sometimes “country” appears as a field, sometimes in body text
    country = None
    for k, v in labels.items():
        if "country" in k:
            country = v
            break

    text = soup.get_text(" ", strip=True)
    if not deadline:
        deadline = _parse_deadline(text)

    return {
        "source": "AfDB",
        "title": title,
        "country": country,
        "deadline": deadline,
        "url": url,
        "topic": "Open Government",
        "summary": text.lower()[:800],
    }

def fetch(ogp_only: bool = True, since_days: int | None = 90, **kwargs) -> List[Dict[str, Any]]:
    verbose = _is_on("AFDB_DEBUG", "DEBUG")
    max_items = _env_int("AFDB_MAX", 40)

    # 1) RSS first (quick win if not blocked)
    items = _rss_fetch(days_back=since_days or 90, max_items=max_items, verbose=verbose)
    if items:
        return items if not ogp_only else items  # (topic is fixed to Open Government here)

    # 2) Listings & search pages
    s = _session()
    out: List[Dict[str, Any]] = []
    for base in LIST_PAGES:
        html = _get_html(s, base, verbose)
        if not html:
            continue
        links = _collect_links_from_listing(html, base)
        log.info("[afdb:list_links] url=%r links=%d", base, len(links))
        if not links:
            continue

        # Parse a reasonable number of detail pages
        for u in links[: max_items * 2]:
            it = _parse_detail(s, u, verbose)
            if it:
                out.append(it)
            if len(out) >= max_items:
                break
        if len(out) >= max_items:
            break

    log.info("[afdb:links_total] count=%d", len(out))
    return out

def accepted_args():
    return ["ogp_only", "since_days"]

class Connector:
    def fetch(self, days_back: int = 90):
        return fetch(ogp_only=True, since_days=days_back)
