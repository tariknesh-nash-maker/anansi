import os, json, hashlib, textwrap
from datetime import datetime
from pathlib import Path

from connectors.eu_ft import fetch as fetch_eu
from connectors.undp import fetch as fetch_undp
from connectors.afdb import fetch as fetch_afdb
from connectors.worldbank import fetch as fetch_wb
from post_slack import post_to_slack

STATE_FILE = Path("state.json")

def _sig(item):
    base = f"{item.get('title','')}|{item.get('url','')}|{item.get('deadline','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"seen": []}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))

def normalize(item, source_name):
    title = (item.get("title") or "").strip()
    url = (item.get("url") or "").strip()
    deadline = item.get("deadline") or ""
    summary = (item.get("summary") or "").strip()
    region  = item.get("region") or ""
    themes  = item.get("themes") or ""

    return {
        "title": title, "url": url, "deadline": deadline,
        "summary": summary, "region": region, "themes": themes, "source": source_name
    }

def main():
    state = load_state()
    seen = set(state.get("seen", []))

    sources = [
        ("EU F&T", fetch_eu),
        ("UNDP",   fetch_undp),
        ("AfDB",   fetch_afdb),
        ("World Bank", fetch_wb),
    ]

    new_items = []
    for name, fn in sources:
        try:
            for raw in fn():
                it = normalize(raw, name)
                sig = _sig(it)
                if sig not in seen:
                    it["sig"] = sig
                    new_items.append(it)
        except Exception as e:
            # keep going even if one source fails
            print(f"[warn] {name} failed: {e}")

    if not new_items:
        print("No new items.")
        return

    # Build Slack message
    lines = [f"*New funding opportunities ({len(new_items)})* — {datetime.utcnow().strftime('%Y-%m-%d')}"]
    for it in new_items[:12]:  # keep message short; slack has limits
        dl = f" — deadline: {it['deadline']}" if it['deadline'] else ""
        th = f" — _{it['themes']}_" if it['themes'] else ""
        lines.append(f"• <{it['url']}|{it['title']}> ({it['source']}){dl}{th}")

    post_to_slack("\n".join(lines))

    # Update state after successful post
    seen.update([it["sig"] for it in new_items])
    state["seen"] = list(seen)
    save_state(state)

if __name__ == "__main__":
    main()
