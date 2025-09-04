# connectors/undp.py
# UNDP Procurement Notices (legacy site) - HTML scrape
# - Scans listing pages on procurement-notices.undp.org
# - Opens each notice detail page (view_notice.cfm?notice_id=XXXX)
# - Extracts title, country, publication ("Posted on"), deadline, notice type
# - Filters by publication_date within last N days (default 90)
# - Tags OGP topics; optionally keeps only OGP-relevant notices
# - Output is similar to other connectors: list[dict] with title/url/deadline/summary/region/themes

from __future__ import annotations
import os, re, hashlib, requests
from html import unescape
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

BASE = "https://procurement-notices.undp.org"
LIST_URL = BASE + "/search.cfm"         # ?cur=<page> (1-based)
VIEW_URL = BASE + "/view_notice.cfm?notice_id={nid}"

TIMEOUT = 25
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; anansi-undp/1.0; +https://example.org)"
}

# Tunables
PAGES   = int(os.getenv("UNDP_PAGES", "6"))                 # number of newest list pages to scan
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

TODAY  = datetime.utcnow().date()
CUTOFF = TODAY - timedelta(days=PUB_WINDOW_DAYS)

# Date formats commonly seen on UNDP pages
DATE_FMTS = (
    "%d-%b-%Y", "%d %b %Y", "%Y-%m-%d",
    "%m/%d/%Y", "%Y/%m/%d",
)

TAG_RE = re.compile(r"<[^>]+>")

def _strip_html(text: str) -> str:
    if not text: return ""
    return TAG_RE.sub("", unescape(text)).strip()

def _sentence_case(s: str) -> str:
    s = s.strip()
    if not s: return s
    return s[0].upper() + s[1:]

def _parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    s = s.strip()
    # normalize like "Posted on: 02-Sep-2025"
    s = re.sub(r"(?i)\b(posted on|publication date|posted)\s*:\s*", "", s)
    s = re.sub(r"(?i)\b(deadline|closing date|closing)\s*:\s*", "", s)
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
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

# ----- Topic taxonomy (same approach you approved) -----
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
    # de-dupe preserve order
    out: List[str] = []
    for x in found:
        if x not in out: out.append(x)
    return out

# ----- Parsers -----

# List page: look for links like view_notice.cfm?notice_id=XXXXX
LINK_RE = re.compile(r'href\s*=\s*"(?P<href>/view_notice\.cfm\?notice_id=(?P<nid>\d+))"', re.I)

def _parse_list(html: str) -> List[str]:
    nids = [m.group("nid") for m in LINK_RE.finditer(html)]
    # unique while preserving order
    out, seen = [], set()
    for nid in nids:
        if nid not in seen:
            out.append(nid)
            seen.add(nid)
    return out

# Detail page fields: title, country, posted, deadline, type
# These pages are fairly consistent; we’ll match by label prefixes.
TITLE_RE    = re.compile(r'(?is)<h\d[^>]*>\s*(.+?)\s*</h\d>')
COUNTRY_RE  = re.compile(r'(?i)^\s*(country|country of assignment)\s*:\s*(.+?)\s*$', re.M)
POSTED_RE   = re.compile(r'(?i)^\s*(posted on|publication date|posted)\s*:\s*(.+?)\s*$', re.M)
DEADLINE_RE = re.compile(r'(?i)^\s*(deadline|closing date|closing)\s*:\s*(.+?)\s*$', re.M)
TYPE_RE     = re.compile(r'(?i)^\s*(procurement method|notice type|process|category)\s*:\s*(.+?)\s*$', re.M)

def _parse_detail(html: str) -> Dict[str, str]:
    text = _strip_html(html)
    title = ""
    m = TITLE_RE.search(html)
    if m:
        title = _sentence_case(_strip_html(m.group(1)))
    # find lines by labels
    country = ""
    posted = ""
    deadline = ""
    ntype = ""
    m = COUNTRY_RE.search(text)
    if m: country = m.group(2).strip()
    m = POSTED_RE.search(text)
    if m: posted = m.group(2).strip()
    m = DEADLINE_RE.search(text)
    if m: deadline = m.group(2).strip()
    m = TYPE_RE.search(text)
    if m: ntype = m.group(2).strip()
    # Sometimes these labels are inside table rows; try secondary patterns
    if not posted:
        m = re.search(r'(?i)posted\s*on\s*[:\-]\s*([A-Za-z0-9, \-\/]+)', text)
        if m: posted = m.group(1).strip()
    if not deadline:
        m = re.search(r'(?i)(deadline|closing date)\s*[:\-]\s*([A-Za-z0-9, \-\/]+)', text)
        if m: deadline = m.group(2).strip()
    return {
        "title": title,
        "country": country,
        "posted": posted,
        "deadline": deadline,
        "type": ntype,
        "summary": text[:1200],  # raw summary fallback (will be refined)
    }

# ----- Filters -----

def _in_window(pub_iso: str | None) -> bool:
    dt = _parse_date(pub_iso)
    return bool(dt and CUTOFF <= dt.date() <= TODAY)

def _matches_qterm(item: Dict[str, str]) -> bool:
    pat = UNDP_QTERM
    if not pat:
        return True
    # normalize OR-like syntax to pipes
    norm = (pat
        .replace('"', ' ')
        .replace("(", " ").replace(")", " ")
        .replace(" or ", "|").replace(" OR ", "|").replace(" Or ", "|")
        .replace(",", "|"))
    tokens = [t.strip() for t in norm.split("|") if t.strip()]
    if not tokens:
        return True
    hay = " ".join([item.get("title",""), item.get("summary",""), item.get("_type","")]).lower()
    return any(tok in hay for tok in tokens)

def _matches_topics(item: Dict[str, str]) -> bool:
    if not UNDP_REQUIRE_TOPIC_MATCH:
        return True
    allowed = [t.strip() for t in UNDP_TOPIC_LIST.split("|") if t.strip()]
    if not allowed:
        return True
    topics = [t.strip() for t in (item.get("themes","") or "").split(",") if t.strip()]
    return any(t in allowed for t in topics)

# ----- Main -----

def fetch() -> List[Dict[str, str]]:
    seen_urls = set()
    seen_sigs = set()
    results: List[Dict[str, str]] = []

    # Iterate newest list pages (usually ?cur=1 is newest)
    for page in range(1, PAGES + 1):
        try:
            html = _get(LIST_URL, params={"cur": page})
        except Exception as e:
            if DEBUG: print(f"[undp] list page {page} error: {e}")
            continue

        nids = _parse_list(html)
        if DEBUG: print(f"[undp] page {page}: found {len(nids)} notice ids")

        for nid in nids:
            url = VIEW_URL.format(nid=nid)
            if url in seen_urls:
                continue
            seen_urls.add(url)

            try:
                detail = _get(url)
            except Exception as e:
                if DEBUG: print(f"[undp] detail {nid} error: {e}")
                continue

            info = _parse_detail(detail)

            # Build normalized item
            country = info.get("country","").strip()
            base_title = info.get("title","").strip()
            ntype = info.get("type","").strip()
            posted_iso = _to_iso(info.get("posted",""))
            deadline_iso = _to_iso(info.get("deadline",""))

            # Clean title; prefix with country
            if base_title:
                final_title = _sentence_case(base_title)
            elif ntype:
                final_title = ntype
            else:
                final_title = f"UNDP Notice {nid}"

            if country:
                title = f"{country} — {final_title}"
            else:
                title = final_title

            # Compose summary
            raw_summary = info.get("summary","").strip()
            # If summary is huge, trim
            summary = raw_summary[:600] if raw_summary else (ntype or "Procurement notice")

            # Topic tags (title + summary + type)
            topics = _detect_topics(" ".join([title, summary, ntype]))
            themes = ",".join(topics)

            item = {
                "title": title,
                "url": url,
                "deadline": deadline_iso,
                "summary": summary,
                "region": "",  # UNDP pages don’t always carry explicit region; can infer from country if needed
                "themes": themes,
                "_pub": posted_iso,
                "_type": ntype,
                "_country": country,
                "_nid": nid,
            }

            # Filters
            if not _in_window(item.get("_pub")):
                continue
            if not _matches_qterm(item):
                continue
            if not _matches_topics(item):
                continue

            # Dedup (URL first, then signature)
            sig = _sig(item)
            if sig in seen_sigs:
                continue
            seen_sigs.add(sig)

            results.append(item)
            if len(results) >= MAX_RESULTS:
                if DEBUG: print(f"[undp] reached MAX_RESULTS={MAX_RESULTS}")
                return results

    if DEBUG:
        print(f"[undp] emitted={len(results)} (window={PUB_WINDOW_DAYS}d, pages={PAGES})")

    # Placeholder when none found (so Slack isn’t empty)
    if not results:
        return [{
            "title": f"No UNDP notices in the last {PUB_WINDOW_DAYS} days matching selected OGP topics",
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
