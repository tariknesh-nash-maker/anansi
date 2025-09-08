# aggregator.py
# -*- coding: utf-8 -*-
import os
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

# ---- Connectors (import robustly) -----------------------------------------
EU_NAME = "EU"
try:
    # Prefer the newer TED-based connector if present
    from connectors.eu import fetch as fetch_eu
    EU_NAME = "EU (TED)"
except Exception:
    # Fallback to the earlier F&T connector
    from connectors.eu_ft import fetch as fetch_eu
    EU_NAME = "EU F&T"

from connectors.undp import fetch as fetch_undp
from connectors.afdb import fetch as fetch_afdb
from connectors.worldbank import fetch as fetch_wb

# AFD is optional — include if available
try:
    from connectors.afd import fetch as fetch_afd
    HAVE_AFD = True
except Exception:
    HAVE_AFD = False

# ---- Normalizer ------------------------------------------------------------
from normalizer import normalize as normalize_ops

# ---- Slack -----------------------------------------------------------------
from post_slack import post_to_slack

# ---- State -----------------------------------------------------------------
STATE_FILE = Path("state.json")


def _sig(item: dict) -> str:
    """
    Stable signature to dedupe across runs.
    Use normalized fields so it stays consistent across sources.
    """
    base = f"{item.get('title','')}|{item.get('url','')}|{item.get('deadline','')}|{item.get('donor','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"seen": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


def _render_line(op: dict) -> str:
    """Slack-friendly single-line formatter."""
    url = op.get("url") or ""
    title = op.get("title") or "Untitled"
    donor = op.get("donor") or "Unknown"
    deadline = op.get("deadline") or "N/A"
    themes = op.get("themes") or ["Open Government"]
    theme = themes[0] if isinstance(themes, list) and themes else str(themes)

    # Optional location prefix if available and not already in the title
    loc_list = op.get("country_scope") or []
    loc = " / ".join(loc_list) if isinstance(loc_list, list) else str(loc_list)
    show_loc = bool(loc) and not title.lower().startswith(loc.lower())
    prefix = f"{loc} — " if show_loc else ""

    return f"• <{url}|{prefix}{title}> ({donor}) — deadline: {deadline} — {theme}"


def main():
    # ---------------------- Configuration -----------------------------------
    # Global switches
    FUTURE_ONLY = _env_bool("ANANSI_FUTURE_ONLY", True)          # only keep items with future deadlines when provided
    REQUIRE_DEADLINE = _env_bool("ANANSI_REQUIRE_DEADLINE", False)  # if True, drop items with missing deadlines
    MAX_LINES = _env_int("ANANSI_MAX_LINES", 14)                 # Slack lines (header + items)
    UTC_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Per-source knobs (env overrides allowed)
    EU_SINCE = _env_int("EU_SINCE_DAYS", 120)
    UNDP_SINCE = _env_int("UNDP_SINCE_DAYS", 120)
    AFDB_SINCE = _env_int("AFDB_SINCE_DAYS", 365)
    WB_SINCE = _env_int("WB_SINCE_DAYS", 120)
    AFD_SINCE = _env_int("AFD_SINCE_DAYS", 365)

    EU_OGP = _env_bool("EU_OGP_ONLY", True)
    UNDP_OGP = _env_bool("UNDP_OGP_ONLY", True)
    AFDB_OGP = _env_bool("AFDB_OGP_ONLY", True)
    WB_OGP = _env_bool("WB_OGP_ONLY", True)
    AFD_OGP = _env_bool("AFD_OGP_ONLY", True)

    INCLUDE_AFD = _env_bool("INCLUDE_AFD", True) and HAVE_AFD

    # ---------------------- Fetch -------------------------------------------
    sources = [
        (EU_NAME, fetch_eu, {"since_days": EU_SINCE, "ogp_only": EU_OGP}),
        ("UNDP", fetch_undp, {"since_days": UNDP_SINCE, "ogp_only": UNDP_OGP}),
        ("AfDB", fetch_afdb, {"since_days": AFDB_SINCE, "ogp_only": AFDB_OGP}),
        ("World Bank", fetch_wb, {"since_days": WB_SINCE, "ogp_only": WB_OGP}),
    ]
    if INCLUDE_AFD:
        sources.append(("AFD", fetch_afd, {"since_days": AFD_SINCE, "ogp_only": AFD_OGP}))

    all_raw = []
    for name, fn, kwargs in sources:
        try:
            items = fn(**kwargs) if kwargs else fn()
            # Ensure donor name is set (some connectors already set it)
            for it in items:
                if not it.get("donor"):
                    it["donor"] = name
            print(f"[info] {name}: fetched {len(items)}")
            all_raw.extend(items)
        except Exception as e:
            print(f"[warn] {name} failed: {e}")

    if not all_raw:
        print("No items fetched from any source.")
        return

    # ---------------------- Normalize & filter -------------------------------
    # Global normalization (dedupe, theme mapping, etc.)
    normalized = normalize_ops(
        all_raw,
        future_only=FUTURE_ONLY,
        require_deadline=REQUIRE_DEADLINE,
    )
    print(f"[info] normalized total: {len(normalized)}")

    # Extra rule (per earlier requirement): AfDB must have a future deadline
    # (drop AfDB items with missing deadlines)
    normalized = [
        op for op in normalized
        if not (op.get("donor") == "AfDB" and not op.get("deadline"))
    ]

    if not normalized:
        print("No items after normalization/filters.")
        return

    # ---------------------- State & new items --------------------------------
    state = load_state()
    seen = set(state.get("seen", []))

    new_items = []
    for op in normalized:
        sig = _sig(op)
        if sig not in seen:
            op["sig"] = sig
            new_items.append(op)

    if not new_items:
        print("No new items.")
        return

    # ---------------------- Slack digest -------------------------------------
    # Cap the number of lines to avoid Slack limits (header + items)
    header = f"*New funding opportunities ({len(new_items)})* — {UTC_DATE}"
    capacity = max(1, MAX_LINES - 1)
    lines = [header]
    for op in new_items[:capacity]:
        lines.append(_render_line(op))

    # If there are more items than we can display, add a footer hint
    if len(new_items) > capacity:
        lines.append(f"…and {len(new_items) - capacity} more new items.")

    post_to_slack("\n".join(lines))

    # ---------------------- Persist state ------------------------------------
    seen.update(op["sig"] for op in new_items)
    state["seen"] = list(seen)
    save_state(state)
    print(f"[info] posted {min(len(new_items), capacity)} lines to Slack; seen set now {len(seen)} entries.")


if __name__ == "__main__":
    main()
