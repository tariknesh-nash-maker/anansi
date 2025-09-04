# connectors/undp.py
# UNDP Procurement Notices (legacy site) - robust HTML scrape with multiple list variants + deep debug
from __future__ import annotations
import os, re, hashlib, requests
from html import unescape
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional

BASE = "https://procurement-notices.undp.org"
LIST_URLS = [
    BASE + "/search.cfm?cur={page}",   # classic
    BASE + "/search.cfm?page={page}",  # alt param
    BASE + "/search.cfm",              # sometimes page param unused; still returns latest
]
VIEW_URL = BASE + "/view_notice.cfm?notice_id={nid}"

TIMEOUT = 25
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; anansi-undp/1.4; +https://example.org)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "Cache-Control": "no-cache",
}

# Tunables
PAGES   = int(os.getenv("UNDP_PAGES", "10"))
DEBUG   = os.getenv("UNDP_DEBUG", "0") == "1"
PUB_WINDOW_DAYS = int(os.getenv("UNDP_PUB_WINDOW_DAYS", "90"))
MAX_RESULTS     = int(os.getenv("UNDP_MAX_RESULTS", "60"))

# Topic filtering
UNDP_TOPIC_LIST = os.getenv("UNDP_TOPIC_LIST",
    "Access to Information|Anti-Corruption|Civic Space|Climate and Environment|Digital Governance|Fiscal Openness|Gender and Inclusion|Justice|Media Freedom|Public Participation"
).strip()
UNDP_REQUIRE_TOPIC_MATCH = os.getenv("UNDP_REQUIRE_TOPIC_MATCH", "1") == "1"

# Optional free-text keyword filter (pipe-separated); leave empty to disable
UNDP_QTERM = os.getenv("UNDP_QTERM", "").strip().lower()

TODAY: date  = datetime.utcnow().date()
CUTOFF: date = TODAY - timedelta(days=PUB_WINDOW_DAYS)

# Date formats commonly seen on UNDP pages
DATE_FMTS = (
    "%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d",
    "%d-%b-%Y", "%d %b %Y", "%d-%b-%y", "%d %b %y",
    "%d %B %Y", "%d-%B-%Y",
    "%B %d %Y", "%b %d %Y",
    "%B %d, %Y", "%b %d, %Y",
)

TAG_RE      = re.compile(r"<[^>]+>")
NID_ANY_RE  = re.compile(r'view_notice\.cfm\?notice_id=(\d+)', re.I)

# Liberal capture of labels in plain text
TITLE_RE    = re.compile(r'(?is)<h\d[^>]*>\s*(.+?)\s*</h\d>')
COUNTRY_RE  = re.compile(r'(?i)^\s*(country|country of assignment)\s*:\s*(.+?)\s*$', re.M)
POSTED_RE   = re.compile(r'(?i)^\s*(posted on|publication date|posted)\s*:\s*(.+?)\s*$', re.M)
DEADLINE_RE = re.compile(r'(?i)^\s*(deadline|closing date|closing)\s*:\s*(.+?)\s*$', re.M)
TYPE_RE     = re.compile(r'(?i)^\s*(procurement method|notice type|process|category)\s*:\s*(.+?)\s*$', re.M)

# HTML-nearby regex (label ± 120 chars, then any non-< until next tag)
HTML_LABEL_NEAR = {
    "posted": re.compile(r'(?is)(posted on|publication date|posted)\s*[:\-]?\s*(.{0,120}?)(?:<|$)'),
    "deadline": re.compile(r'(?is)(deadline|closing date|closing)\s*[:\-]?\s*(.{0,120}?)(?:<|$)'),
}

# Generic date token finder (broad)
DATE_TOKEN_RE = re.compile(
    r'(\d{4}-\d{2}-\d{2})|(\d{1,2}[ \-/][A-Za-z]{3,9}[ \-/]\d{2,4})|([A-Za-z]{3,9}\s+\d{1,2}(?:,)?\s+\d{2,4})',
    re.I
)

def _strip_html(text: str) -> str:
    if not text: return ""
    return TAG_RE.sub("", unescape(text)).strip()

def _sentence_case(s: str) -> str:
    s = s.strip()
    if not s: return s
    return s[0].upper() + s[1:]

def _clean_date_string(s: str) -> str:
    """Trim times/timezones like '@ 05:00 PM (GMT+0)' keeping an obvious date token."""
    s = unescape(s or "").replace("&nbsp;", " ")
    s = s.replace(",", " ").strip()
    # Pick first clear date token
    m = DATE_TOKEN_RE.search(s)
    if m:
        for g in m.groups():
            if g:
                s = g
                break
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s = re.sub(r"(?i)\b(posted on|publication date|posted|deadline|closing date|closing)\s*:\s*", "", s.strip())
    s = _clean_date_string(s)
    for fmt in DATE_FMTS:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year < 100:
                dt = dt.replace(year=2000 + dt.year)
            return dt
        except Exception:
            continue
    return None

def _to_iso(s: Optional[str]) -> str:
    dt = _parse_date(s)
    return dt.date().isoformat() if dt else (s.strip() if s else "")

def _sig(item: Dict[str, str]) -> str:
    key = item.get("url") or "|".join([
        item.get("title",""), item.get("_type",""), item.get("_pub","")
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def _get(url: str, params: Dict[str, Any] | None = None) -> str:
    r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

# ----- Topic taxonomy -----
TOPIC_KEYWORDS: Dict[str, List[str]] = {
    "Access to Information": [
        "access to information","right to information","freedom of information","foi",
        "information disclosure","open data portal","data transparency","records management"
    ],
    "Anti-Corruption": [
        "anti-corruption","anticorruption","integrity","whistleblow","illicit","money laundering","aml","cft",
        "beneficial ownership","conflict of interest","procurement integrity","audit","oversight","asset recovery"
    ],
    "Civic Space": [
        "civil society","cso","ngo","human rights defender","freedom of association","freedom of assembly",
        "shrinking civic","civic space"
    ],
    "Climate and Environment": [
        "climate","adaptation","mitigation","resilience","biodiversity","emission","mrv","environment","sustainab"
    ],
    "Digital Governance": [
        "digital government","egovernment","e-government","govtech","open source","interoperability","api",
        "digital identity","digital id","cybersecurity","privacy","data protection","ai ","artificial intelligence",
        "machine learning","cloud","platform","registry","blockchain"
    ],
    "Fiscal Openness": [
        "budget transparency","open budget","public finance","pfm","treasury","fiscal","tax administration",
        "open contracting","contract transparency","procurement reform","e-procurement"
    ],
    "Gender and Inclusion": [
        "gender","women","girls","inclusion","inclusive","disability","pwd","youth","vulnerable","minorities"
    ],
    "Justice": [
        "justice","judiciary","court","case management","legal aid","access to justice","prosecution","rule of law"
    ],
    "Media Freedom": [
        "media","journalism","press freedom","fact-check","newsroom","independent media","media literacy"
    ],
    "Public Participation": [
        "participation","co-creation","co creation","consultation","stakeholder engagement","citizen feedback",
        "participatory","deliberative","social accountability","grm","grievance redress"
    ],
}

def _detect_topics(text: str) -> List[str]:
    t = text.lower()
    found: List[str] = []
    for topic, kws in TOPIC_KEYWORDS.items():
        if any(kw in t for kw in kws):
            found.append(topic)
    out: List[str] = []
    for x in found:
        if x not in out: out.append(x)
    return out

def _parse_list(html: str) -> List[str]:
    nids = NID_ANY_RE.findall(html)
    out, seen = [], set()
    for nid in nids:
        if nid not in seen:
            out.append(nid)
            seen.add(nid)
    return out

def _extract_labelled_dates(html: str, text: str) -> Dict[str, str]:
    """Try to pull posted/deadline using both text-line and HTML-near patterns."""
    def find_line(rex: re.Pattern) -> str:
        m = rex.search(text)
        return m.group(2).strip() if m else ""
    posted = find_line(POSTED_RE)
    deadline = find_line(DEADLINE_RE)

    if not posted:
        m = HTML_LABEL_NEAR["posted"].search(html)
        if m:
            posted = _clean_date_string(m.group(2))
    if not deadline:
        m = HTML_LABEL_NEAR["deadline"].search(html)
        if m:
            deadline = _clean_date_string(m.group(2))

    return {"posted": posted, "deadline": deadline}

def _parse_detail(html: str) -> Dict[str, str]:
    text = _strip_html(html)
    m = TITLE_RE.search(html)
    title = _sentence_case(_strip_html(m.group(1))) if m else ""
    # country/type via line labels
    def _line(rex: re.Pattern) -> str:
        m = rex.search(text);  return m.group(2).strip() if m else ""
    country  = _line(COUNTRY_RE)
    ntype    = _line(TYPE_RE)

    dates = _extract_labelled_dates(html, text)
    posted   = dates["posted"]
    deadline = dates["deadline"]

    # Fallback: if still empty, grab first 2 date-like tokens from page
    if not posted or not deadline:
        tokens = DATE_TOKEN_RE.findall(unescape(html))
        tokens_flat = [g for tup in tokens for g in tup if g]
        if not posted and tokens_flat:
            posted = _clean_date_string(tokens_flat[0])
        if not deadline and tokens_flat:
            # pick the latest-looking token as a guess for deadline
            cand = tokens_flat[-1]
            deadline = _clean_date_string(cand)

    return {
        "title": title,
        "country": country,
        "posted": posted,
        "deadline": deadline,
        "type": ntype,
        "summary": text[:1200],
    }

def _in_window_or_future_deadline(pub_iso: str | None, deadline_iso: str | None) -> bool:
    pub_dt = _parse_date(pub_iso) if pub_iso else None
    if pub_dt:
        return CUTOFF <= pub_dt.date() <= TODAY
    dl_dt = _parse_date(deadline_iso) if deadline_iso else None
    return bool(dl_dt and dl_dt.date() >= TODAY)

def _matches_qterm(item: Dict[str, str]) -> bool:
    pat = UNDP_QTERM
    if not pat: return True
    norm = (pat
        .replace('"', ' ').replace("(", " ").replace(")", " ")
        .replace(" or ", "|").replace(" OR ", "|").replace(" Or ", "|")
        .replace(",", "|"))
    tokens = [t.strip() for t in norm.split("|") if t.strip()]
    if not tokens: return True
    hay = " ".join([item.get("title",""), item.get("summary",""), item.get("_type","")]).lower()
    return any(tok in hay for tok in tokens)

def _matches_topics(item: Dict[str, str]) -> bool:
    if not UNDP_REQUIRE_TOPIC_MATCH: return True
    allowed = [t.strip() for t in UNDP_TOPIC_LIST.split("|") if t.strip()]
    if not allowed: return True
    topics = [t.strip() for t in (item.get("themes","") or "").split(",") if t.strip()]
    return any(t in allowed for t in topics)

def fetch() -> List[Dict[str, str]]:
    seen_urls, seen_sigs = set(), set()
    results: List[Dict[str, str]] = []

    # Debug counters
    total_ids = 0
    detail_ok = 0
    dropped_old = dropped_no_pub = dropped_topics = dropped_qterm = 0

    debug_samples_remaining = 8  # print detailed per-item debug for first few

    for page in range(1, PAGES + 1):
        nids: List[str] = []
        used_variant = ""
        for variant in LIST_URLS:
            url = variant.format(page=page)
            try:
                html = _get(url)
            except Exception as e:
                if DEBUG: print(f"[undp] list {url} error: {e}")
                continue
            ids = _parse_list(html)
            if ids:
                nids = ids
                used_variant = url
                break
        if DEBUG:
            print(f"[undp] page {page}: variant={used_variant or '(none)'} found {len(nids)} notice ids")
        total_ids += len(nids)
        if not nids:
            continue

        for nid in nids:
            url = VIEW_URL.format(nid=nid)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            try:
                detail_html = _get(url)
            except Exception as e:
                if DEBUG: print(f"[undp] detail {nid} error: {e}")
                continue

            info = _parse_detail(detail_html)
            country = info.get("country","").strip()
            base_title = info.get("title","").strip()
            ntype = info.get("type","").strip()

            posted_raw = info.get("posted","") or ""
            deadline_raw = info.get("deadline","") or ""
            posted_iso = _to_iso(posted_raw)
            deadline_iso = _to_iso(deadline_raw)

            # Per-item deep debug (first N samples)
            if DEBUG and debug_samples_remaining > 0:
                snippet = _strip_html(detail_html)[:220].replace("\n"," ")
                print(f"[undp][sample] url={url}")
                print(f"[undp][sample] country='{country}' type='{ntype}' title='{base_title[:140]}'")
                print(f"[undp][sample] posted_raw='{posted_raw}' -> posted_iso='{posted_iso}'")
                print(f"[undp][sample] deadline_raw='{deadline_raw}' -> deadline_iso='{deadline_iso}'")
                print(f"[undp][sample] text_snippet='{snippet}'")
                debug_samples_remaining -= 1

            # Build final item
            if base_title:
                final_title = _sentence_case(base_title)
            elif ntype:
                final_title = ntype
            else:
                final_title = f"UNDP Notice {nid}"

            title = f"{country} — {final_title}" if country else final_title
            raw_summary = info.get("summary","").strip()
            summary = raw_summary[:600] if raw_summary else (ntype or "Procurement notice")

            topics = _detect_topics(" ".join([title, summary, ntype]))
            themes = ",".join(topics)

            item = {
                "title": title,
                "url": url,
                "deadline": deadline_iso,
                "summary": summary,
                "region": "",
                "themes": themes,
                "_pub": posted_iso,      # may be empty
                "_type": ntype,
                "_country": country,
                "_nid": nid,
            }

            # Filters + counters
            if not _in_window_or_future_deadline(item.get("_pub"), item.get("deadline")):
                if not _parse_date(item.get("_pub")) and not _parse_date(item.get("deadline")):
                    dropped_no_pub += 1
                else:
                    dropped_old += 1
                continue
            if UNDP_QTERM and not _matches_qterm(item):
                dropped_qterm += 1
                continue
            if UNDP_REQUIRE_TOPIC_MATCH and not _matches_topics(item):
                dropped_topics += 1
                continue

            sig = _sig(item)
            if sig in seen_sigs:
                continue
            seen_sigs.add(sig)

            results.append(item)
            detail_ok += 1
            if len(results) >= MAX_RESULTS:
                if DEBUG:
                    print(f"[undp] reached MAX_RESULTS={MAX_RESULTS} (ids={total_ids}, kept={detail_ok}, old={dropped_old}, no_pub={dropped_no_pub}, no_qterm={dropped_qterm}, no_topic={dropped_topics})")
                return results

    if DEBUG:
        print(f"[undp] emitted={len(results)} (window={PUB_WINDOW_DAYS}d, pages={PAGES}) "
              f"[ids={total_ids}, kept={detail_ok}, old={dropped_old}, no_pub={dropped_no_pub}, no_qterm={dropped_qterm}, no_topic={dropped_topics}]")

    if not results:
        return [{
            "title": f"No UNDP notices in the last {PUB_WINDOW_DAYS} days (or with future deadlines) matching selected OGP topics",
            "url": BASE,
            "deadline": "",
            "summary": "Try increasing UNDP_PAGES, widening UNDP_PUB_WINDOW_DAYS, or relaxing UNDP_TOPIC_LIST / UNDP_REQUIRE_TOPIC_MATCH.",
            "region": "",
            "themes": "",
            "_pub": "",
            "_type": "",
        }]

    return results

if __name__ == "__main__":
    os.environ["UNDP_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} UNDP items")
    for it in items[:10]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| deadline:", it.get("deadline",""), "| topics:", it.get("themes",""), "|", it["url"])
