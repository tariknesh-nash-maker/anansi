# connectors/worldbank.py
# World Bank Procurement Notices via Finances One dataset API (DS00979 / RS00909)
# Reverse-paginates from newest rows; keeps items with publication_date in last N days (default 90).
# Adds OGP-theme tagging + optional filtering to only relevant themes.
# Titles are human-readable, prefixed with country; duplicates collapsed by URL/content.

from __future__ import annotations
import os, re, hashlib, requests
from html import unescape
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

API_BASE    = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"
DATASET_ID  = os.getenv("WB_FONE_DATASET_ID", "DS00979")
RESOURCE_ID = os.getenv("WB_FONE_RESOURCE_ID", "RS00909")

TIMEOUT = 25
ROWS    = int(os.getenv("WB_ROWS", "500"))      # up to ~1000 supported; larger = fewer roundtrips
PAGES   = int(os.getenv("WB_PAGES", "12"))      # how many newest slices to scan
DEBUG   = os.getenv("WB_DEBUG", "0") == "1"

PUB_WINDOW_DAYS = int(os.getenv("WB_PUB_WINDOW_DAYS", "90"))  # widen to 180 if you want more
TODAY  = datetime.utcnow().date()
CUTOFF = TODAY - timedelta(days=PUB_WINDOW_DAYS)

# Client-side keyword filter (pipe-separated OR list); empty = no filter
WB_QTERM    = os.getenv("WB_QTERM", "").strip().lower()
MAX_RESULTS = int(os.getenv("WB_MAX_RESULTS", "60"))

# --- OGP topic filtering ---
# Pipe-separated list of topics to KEEP (tagged); empty = keep all topics
WB_TOPIC_LIST = os.getenv("WB_TOPIC_LIST",
    "Access to Information|Anti-Corruption|Civic Space|Climate and Environment|Digital Governance|Fiscal Openness|Gender and Inclusion|Justice|Media Freedom|Public Participation"
).strip()

# If '1' (default), ONLY return items that match one or more of WB_TOPIC_LIST
WB_REQUIRE_TOPIC_MATCH = os.getenv("WB_REQUIRE_TOPIC_MATCH", "1") == "1"

# Date formats seen in Finances One
DATE_FMTS = (
    "%d-%b-%Y", "%Y-%m-%d", "%d %b %Y",
    "%m/%d/%Y", "%Y/%m/%d", "%Y.%m.%d",
    "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ",
)

TAG_RE = re.compile(r"<[^>]+>")

def _strip_html(text: str) -> str:
    if not text: return ""
    return TAG_RE.sub("", unescape(text)).strip()

def _sentence_case(s: str) -> str:
    s = s.strip()
    if not s: return s
    return s[0].upper() + s[1:]

def _parse_date(val: Optional[str]) -> Optional[datetime]:
    if not val: return None
    s = str(val).strip()
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def _to_iso(val: Optional[str]) -> str:
    dt = _parse_date(val)
    return dt.date().isoformat() if dt else (str(val).strip() if val else "")

def _sig(item: Dict[str, str]) -> str:
    # Prefer URL; else composite (title|type|pub)
    key = item.get("url") or "|".join([
        item.get("title",""), item.get("_type",""), item.get("_pub","")
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def _fone(params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(API_BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _fetch_page(skip: int) -> Dict[str, Any]:
    return _fone({
        "datasetId": DATASET_ID,
        "resourceId": RESOURCE_ID,
        "type": "json",
        "top": ROWS,
        "skip": skip,
    })

# --- OGP Topic taxonomy (simple, pragmatic keyword sets) ---
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
    matched: List[str] = []
    for topic, kws in TOPIC_KEYWORDS.items():
        for kw in kws:
            if kw in t:
                matched.append(topic)
                break
    # de-dupe while preserving order
    out: List[str] = []
    for x in matched:
        if x not in out: out.append(x)
    return out

def _normalize(n: Dict[str, Any]) -> Dict[str, str]:
    # Common fields
    proj_id   = (n.get("project_id") or "").strip()
    raw_title = (n.get("bid_description") or "").strip()
    desc_raw  = (n.get("bid_description") or n.get("notice_text") or "").strip()
    country   = (n.get("country_name") or "").strip()
    region    = (n.get("region") or "").strip()
    ntype     = (n.get("notice_type") or "").strip()

    # Clean text
    title_clean = _sentence_case(_strip_html(raw_title))[:140]
    desc_clean  = _strip_html(desc_raw)[:550]

    # Title (prefix with country)
    if title_clean:
        base_title = title_clean
    elif ntype or proj_id:
        base_title = f"{ntype or 'Notice'}{(' — ' + proj_id) if proj_id else ''}"
    else:
        base_title = "World Bank procurement notice"

    title = f"{country} — {base_title}" if country else base_title

    # Dates
    pub_iso = _to_iso(n.get("publication_date"))
    dl_iso  = _to_iso(n.get("deadline_date"))

    # URL
    url = (n.get("url") or "").strip()

    # Summary
    if desc_clean:
        summary = desc_clean
    else:
        bits = [ntype, proj_id, country]
        summary = " — ".join([b for b in bits if b]) or "Procurement notice"

    # Topic tags (use title + summary + type)
    hay = " ".join([title, summary, ntype])
    topics = _detect_topics(hay)

    return {
        "title": title,
        "url": url,
        "deadline": dl_iso,
        "summary": summary,
        "region": region,
        "themes": ",".join(topics),
        "_pub": pub_iso,
        "_type": ntype,
        "_country": country,
        "_project": proj_id,
    }

def _in_window(pub: Optional[str]) -> bool:
    dt = _parse_date(pub)
    return bool(dt and CUTOFF <= dt.date() <= TODAY)

def _matches_qterm(item: Dict[str, str]) -> bool:
    pat = WB_QTERM
    if not pat:
        return True
    # Normalize Solr-like syntax to simple tokens
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
    if not WB_REQUIRE_TOPIC_MATCH:
        return True
    allowed = [t.strip() for t in WB_TOPIC_LIST.split("|") if t.strip()]
    if not allowed:
        return True
    item_topics = [t.strip() for t in (item.get("themes","") or "").split(",") if t.strip()]
    return any(t in allowed for t in item_topics)

def fetch() -> List[Dict[str, str]]:
    # 1) Total → compute newest skips
    try:
        head = _fone({"datasetId": DATASET_ID, "resourceId": RESOURCE_ID, "type": "json", "top": 1, "skip": 0})
    except Exception as e:
        if DEBUG: print(f"[worldbank] F1 head error: {e}")
        return _placeholder()

    total = int(head.get("count", 0) or 0)
    if DEBUG: print(f"[worldbank] F1 total={total}")
    if total <= 0:
        return _placeholder()

    last_skip = max(0, total - ROWS)
    skips: List[int] = []
    for k in range(PAGES):
        s = last_skip - k*ROWS
        if s < 0: break
        skips.append(s)

    results: List[Dict[str, str]] = []
    seen = set()
    seen_urls = set()

    for idx, skip in enumerate(skips):
        try:
            payload = _fetch_page(skip)
        except Exception as e:
            if DEBUG: print(f"[worldbank] F1 fetch skip={skip} error: {e}")
            continue

        data = payload.get("data") or []
        if DEBUG:
            print(f"[worldbank] F1 skip={skip} rows={len(data)} count={payload.get('count','?')}")
            if idx == 0:
                samples = [_to_iso(r.get("publication_date")) or "(no pub)" for r in data[:10]]
                print(f"[worldbank] F1 newest-slice pub samples: {samples}")

        if not data:
            continue

        for raw in data:
            item = _normalize(raw)
            if not _in_window(item.get("_pub")):
                continue
            if not _matches_qterm(item):
                continue
            if not _matches_topics(item):
                continue

            url = (item.get("url") or "").strip()
            if url:
                if url in seen_urls:
                    continue
                seen_urls.add(url)

            sig = _sig(item)
            if sig in seen:
                continue
            seen.add(sig)

            results.append(item)
            if len(results) >= MAX_RESULTS:
                return results

    if DEBUG:
        print(f"[worldbank] FinancesOne emitted={len(results)} (window={PUB_WINDOW_DAYS}d, pages={PAGES}, rows={ROWS})")

    return results if results else _placeholder()

def _placeholder() -> List[Dict[str, str]]:
    return [{
        "title": f"No World Bank notices in the last {PUB_WINDOW_DAYS} days matching selected OGP topics",
        "url": "https://financesone.worldbank.org/procurement-notice/DS00979",
        "deadline": "",
        "summary": "Try increasing WB_PAGES/ROWS, widening WB_PUB_WINDOW_DAYS, or relaxing WB_TOPIC_LIST / WB_REQUIRE_TOPIC_MATCH.",
        "region": "",
        "themes": "",
        "_pub": "",
        "_type": "",
    }]

if __name__ == "__main__":
    os.environ["WB_DEBUG"] = "1"
    items = fetch()
    print(f"Fetched {len(items)} items")
    for it in items[:10]:
        print("-", it["title"], "| pub:", it.get("_pub",""), "| deadline:", it.get("deadline",""), "| topics:", it.get("themes",""), "|", it["url"])
