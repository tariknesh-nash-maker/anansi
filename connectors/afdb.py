# connectors/afdb.py
from __future__ import annotations
from typing import List, Dict, Any, Set, Tuple
from datetime import datetime, timedelta, timezone
import os, re, requests, feedparser
from bs4 import BeautifulSoup

def _is_on(*envs: str) -> bool:
    for e in envs:
        v = os.getenv(e)
        if v and str(v).strip().lower() in ("1","true","yes","on"): return True
    return False
def _kv(prefix: str, **kw):
    print(f"[{prefix}] " + " ".join(f"{k}={repr(v)}" for k,v in kw.items()))

UA = os.getenv("ANANSI_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                             "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": os.getenv("AFDB_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

SPN_RSS = "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/specific-procurement-notices-spns/rss.xml"
GPN_RSS = "https://www.afdb.org/en/projects-and-operations/procurement/resources-for-businesses/general-procurement-notices-gpns/rss.xml"
RSS_FEEDS = [SPN_RSS, GPN_RSS]

LISTING_PAGES = [
    "https://www.afdb.org/en/documents/project-related-procurement/procurement-notices/specific-procurement-notices",
    "https://www.afdb.org/en/documents/category/general-procurement-notices",
]

DEADLINE_RE = re.compile(r"(?:deadline|closing(?: date)?)\s*[:\-]?\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})", re.I)

def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except Exception: return default

def _to_date_from_struct(tm) -> datetime | None:
    try: return datetime(*tm[:6], tzinfo=timezone.utc)
    except Exception: return None

def _parse_deadline(text: str) -> str | None:
    m = DEADLINE_RE.search(text or "")
    if not m: return None
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try: return datetime.strptime(m.group(1), fmt).date().isoformat()
        except Exception: pass
    return None

def _reader_url(url: str) -> str:
    # Simple, fast, no-JS reader (mirrors raw HTML/XML)
    base = os.getenv("READER_BASE", "https://r.jina.ai/http://")
    # Ensure http:// or https:// prefix for reader
    if url.startswith("http://") or url.startswith("https://"):
        return f"{base}{url.replace('https://','').replace('http://','')}"
    return f"{base}{url}"

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    # Warm-up home to get any basic cookies
    try:
        s.get("https://www.afdb.org/en", timeout=20)
    except Exception:
        pass
    return s

def _rss_fetch(days_back: int, max_items: int, verbose: bool) -> List[Dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    s = _session()
    out: List[Dict[str, Any]] = []
    for url in RSS_FEEDS:
        use_reader = False
        # 1) Try direct
        rtext = None
        try:
            r = s.get(url, timeout=20)
            if verbose: _kv("afdb:rss_http", url=url, status=r.status_code, bytes=len(r.text or ""))
            if r.status_code == 403 and _is_on("AFDB_USE_READER"):
                use_reader = True
            elif r.ok:
                rtext = r.text
        except Exception as ex:
            if verbose: _kv("afdb:rss_err", url=url, err=str(ex)[:200])

        # 2) Reader fallback (opt-in)
        if (rtext is None) and (_is_on("AFDB_USE_READER") or use_reader):
            try:
                rr = s.get(_reader_url(url), timeout=25, headers={"Accept":"application/xml"})
                if verbose: _kv("afdb:rss_reader", url=url, status=getattr(rr,"status_code","?"), bytes=len(getattr(rr,"text","") or ""))
                if rr.ok:
                    rtext = rr.text
            except Exception as ex:
                if verbose: _kv("afdb:rss_reader_err", url=url, err=str(ex)[:200])

        if not rtext:
            continue

        feed = feedparser.parse(rtext)
        if verbose:
            _kv("afdb:rss_entries", url=url, entries=len(feed.entries), bozo=getattr(feed, "bozo", "?"))
        for e in feed.entries:
            title = (getattr(e, "title", "") or "").strip()
            link  = (getattr(e, "link", "") or "").strip()
            if not title or not link: continue
            pub_dt = None
            if getattr(e, "published_parsed", None): pub_dt = _to_date_from_struct(e.published_parsed)
            elif getattr(e, "updated_parsed", None): pub_dt = _to_date_from_struct(e.updated_parsed)
            if pub_dt and pub_dt.date() < cutoff: continue
            summary = (getattr(e, "summary", "") or getattr(e, "description", "") or "")
            deadline = _parse_deadline(summary)
            out.append({
                "title": title, "source": "AfDB", "deadline": deadline,
                "country": "", "topic": None, "url": link,
                "summary": (summary or title).lower(),
            })
            if len(out) >= max_items: break
        if len(out) >= max_items: break
    if verbose: _kv("afdb:rss_result", kept=len(out))
    return out

def _collect_listing_links(url: str, verbose: bool) -> Set[str]:
    s = _session()
    texts: List[Tuple[str,str]] = []
    # Direct
    try:
        r = s.get(url, timeout=25)
        if verbose: _kv("afdb:list_http", url=url, status=r.status_code, bytes=len(r.text or ""))
        if r.ok: texts.append(("direct", r.text))
    except Exception as ex:
        if verbose: _kv("afdb:list_err", url=url, err=str(ex)[:200])
    # Reader fallback
    if not texts and _is_on("AFDB_USE_READER"):
        try:
            rr = s.get(_reader_url(url), timeout=25)
            if verbose: _kv("afdb:list_reader", url=url, status=rr.status_code, bytes=len(rr.text or ""))
            if rr.ok: texts.append(("reader", rr.text))
        except Exception as ex:
            if verbose: _kv("afdb:list_reader_err", url=url, err=str(ex)[:200])

    links: Set[str] = set()
    for mode, html_text in texts:
        soup = BeautifulSoup(html_text, "lxml")
        for a in soup.select("a[href]"):
            href = a.get("href","")
            if not href: continue
            full = href if href.startswith("http") else f"https://www.afdb.org{href}"
            if "/documents/" in full and ("/specific-procurement-notices" in full or "/general-procurement-notices" in full):
                links.add(full.split("#")[0])
    if verbose: _kv("afdb:list_links", url=url, links=len(links))
    return links

def _parse_detail(url: str, verbose: bool) -> Dict[str, Any] | None:
    s = _session()
    # direct first
    html_text = None
    try:
        r = s.get(url, timeout=25)
        if verbose: _kv("afdb:detail_http", url=url, status=r.status_code, bytes=len(r.text or ""))
        if r.ok: html_text = r.text
    except Exception as ex:
        if verbose: _kv("afdb:detail_err", url=url, err=str(ex)[:200])
    # reader fallback
    if html_text is None and _is_on("AFDB_USE_READER"):
        try:
            rr = s.get(_reader_url(url), timeout=25)
            if verbose: _kv("afdb:detail_reader", url=url, status=rr.status_code, bytes=len(rr.text or ""))
            if rr.ok: html_text = rr.text
        except Exception as ex:
            if verbose: _kv("afdb:detail_reader_err", url=url, err=str(ex)[:200])
    if html_text is None:
        return None

    soup = BeautifulSoup(html_text, "lxml")
    title_tag = soup.select_one("h1, h2") or soup.select_one("title")
    title = (title_tag.get_text(" ", strip=True) if title_tag else "AfDB Notice").strip()
    text = soup.get_text(" ", strip=True)
    # try common label/value pairs
    deadline = None
    for dt in soup.select("dt, strong, b"):
        lbl = dt.get_text(" ", strip=True).lower()
        if "dead" in lbl or "clos" in lbl:
            val = dt.find_next("dd")
            raw = val.get_text(" ", strip=True) if val else ""
            maybe = _parse_deadline(f"deadline {raw}")
            if maybe: deadline = maybe; break
    if not deadline:
        deadline = _parse_deadline(text)
    return {
        "title": title, "source": "AfDB", "deadline": deadline,
        "country": "", "topic": None, "url": url,
        "summary": text.lower()[:800],
    }

def _apply_filters(items: List[Dict[str, Any]], ogp_only: bool, verbose: bool) -> List[Dict[str, Any]]:
    raw = len(items)
    try:
        from filters import is_excluded
        items = [it for it in items if not is_excluded(f"{it.get('title','')} {it.get('summary','')}")]
    except Exception:
        pass
    after_ex = len(items)
    if ogp_only:
        try:
            from filters import ogp_relevant
            preferred = [it for it in items if ogp_relevant(f"{it.get('title','')} {it.get('summary','')}")]
            items = preferred or items
        except Exception:
            pass
    if verbose: _kv("afdb:filter_counts", raw=raw, after_exclude=after_ex, returned=len(items))
    return items

def _afdb_fetch(days_back: int = 90, ogp_only: bool = True) -> List[Dict[str, Any]]:
    verbose = _is_on("AFDB_DEBUG","DEBUG")
    max_items = _env_int("AFDB_MAX", 40)

    # 1) RSS first (with reader fallback if enabled)
    items = _rss_fetch(days_back, max_items, verbose)
    if items:
        return _apply_filters(items, ogp_only, verbose)

    # 2) HTML listings (with reader fallback if enabled)
    all_links: Set[str] = set()
    for lp in LISTING_PAGES:
        try:
            all_links |= _collect_listing_links(lp, verbose)
        except Exception as ex:
            print(f"[afdb] listing failed {lp}: {ex}")
    if verbose: _kv("afdb:links_total", count=len(all_links))

    out: List[Dict[str, Any]] = []
    for u in list(all_links)[: max_items * 2]:
        it = _parse_detail(u, verbose)
        if it: out.append(it)
        if len(out) >= max_items: break

    if not out and not verbose:
        _kv("afdb:empty", links=len(all_links))
    return _apply_filters(out, ogp_only, verbose)

class Connector:
    def fetch(self, days_back: int = 90):
        return _afdb_fetch(days_back=days_back, ogp_only=True)

def fetch(ogp_only: bool = True, since_days: int = 90, **kwargs):
    return _afdb_fetch(days_back=since_days, ogp_only=ogp_only)

def accepted_args():
    return ["ogp_only", "since_days"]
