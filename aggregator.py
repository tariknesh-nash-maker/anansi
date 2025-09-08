# aggregator.py
# -*- coding: utf-8 -*-
import os
import json
import hashlib
import inspect
from datetime import datetime, timezone
from pathlib import Path

# --------- Connectors (import with graceful fallbacks) ----------------------
EU_NAME = "EU"
try:
    # Use TED connector if present
    from connectors.eu import fetch as fetch_eu
    EU_NAME = "EU (TED)"
except Exception:
    try:
        from connectors.eu_ft import fetch as fetch_eu
        EU_NAME = "EU F&T"
    except Exception:
        fetch_eu = None

from connectors.undp import fetch as fetch_undp
from connectors.afdb import fetch as fetch_afdb
from connectors.worldbank import fetch as fetch_wb

# AFD optional
try:
    from connectors.afd import fetch as fetch_afd
    HAVE_AFD = True
except Exception:
    HAVE_AFD = False
    fetch_afd = None

# --------- Normalizer & Slack ----------------------------------------------
from normalizer import normalize as normalize_ops
from post_slack import post_to_slack

# --------- State ------------------------------------------------------------
STATE_FILE = Path("state.json")


def _sig(item: dict) -> str:
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
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _render_line(op: dict) -> str:
    url = op.get("url") or ""
    title = op.get("title") or "Untitled"
    donor = op.get("donor") or "Unknown"
    deadline = op.get("deadline") or "N/A"
    themes = op.get("themes") or ["Open Government"]
    theme = themes[0] if isinstance(themes, list) and themes else str(themes)
    loc_list = op.get("country_scope") or []
    loc = " / ".join(loc_list) if isinstance(loc_list, list) else str(loc_list)
    prefix = f"{loc} — " if (loc and not title.lower().startswith(loc.lower())) else ""
    return f"• <{url}|{prefix}{title}> ({donor}) — deadline: {deadline} — {theme}"


def _safe_fetch(name: str, fn, desired_kwargs: dict) -> list:
    """
    Call a connector fetch() with only the kwargs it supports.
    If result is empty, widen once (ogp_only=False) and once without since_days.
    """
    if fn is None:
        print(f"[warn] {name}: connector unavailable")
        return []

    # Filter kwargs by function signature
    try:
        sig = inspect.signature(fn)
        accepted = {k: v for k, v in (desired_kwargs or {}).items() if k in sig.parameters}
        param_names = set(sig.parameters.keys())
    except Exception:
        accepted, param_names = {}, set()

    # First attempt
    try:
        items = fn(**accepted) if accepted else fn()
    except TypeError:
        # If we somehow still got a TypeError, try no-arg call
        items = fn()
    except Exception as e:
        print(f"[warn] {name} failed: {e}")
        return []

    items = list(items or [])
    print(f"[info] {name}: fetched {len(items)} (accepted args: {sorted(accepted.keys())})")

    # Wideners if empty
    if not items:
        # Try ogp_only=False if supported
        if "ogp_only" in param_names:
            try:
                widened = dict(accepted)
                widened["ogp_only"] = False
                items = fn(**widened)
                items = list(items or [])
                print(f"[info] {name}: retry ogp_only=False -> {len(items)}")
            except Exception as e:
                print(f"[warn] {name} retry ogp_only=False failed: {e}")
        # Try without since_days if still empty and supported
        if not items and "since_days" in param_names:
            try:
                widened = dict(accepted)
                widened.pop("since_days", None)
                items = fn(**widened)
                items = list(items or [])
                print(f"[info] {name}: retry without since_days -> {len(items)}")
            except Exception as e:
                print(f"[warn] {name} retry without since_days failed: {e}")

    # Ensure donor is set
    for it in items:
        it.setdefault("donor", name)

    return items


def main():
    # ---------------------- Config ------------------------------------------
    FUTURE_ONLY = _env_bool("ANANSI_FUTURE_ONLY", True)
    REQUIRE_DEADLINE = _env_bool("ANANSI_REQUIRE_DEADLINE", False)
    MAX_LINES = _env_int("ANANSI_MAX_LINES", 14)
    UTC_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Per-source windows (only used if connector supports them)
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
    sources = []
    if fetch_eu:
        sources.append((EU_NAME, fetch_eu, {"since_days": EU_SINCE, "ogp_only": EU_OGP}))
    sources.extend([
        ("UNDP",        fetch_undp, {"since_days": UNDP_SINCE, "ogp_only": UNDP_OGP}),
        ("AfDB",        fetch_afdb, {"since_days": AFDB_SINCE, "ogp_only": AFDB_OGP}),
        ("World Bank",  fetch_wb,   {"since_days": WB_SINCE,   "ogp_only": WB_OGP}),
    ])
    if INCLUDE_AFD:
        sources.append(("AFD", fetch_afd, {"since_days": AFD_SINCE, "ogp_only": AFD_OGP}))

    all_raw = []
    for name, fn, desired_kwargs in sources:
        items = _safe_fetch(name, fn, desired_kwargs)
        all_raw.extend(items)

    if not all_raw:
        print("No items fetched from any source.")
        return

    # ---------------------- Normalize & filter -------------------------------
    normalized = normalize_ops(
        all_raw,
        future_only=FUTURE_ONLY,
        require_deadline=REQUIRE_DEADLINE,
    )
    print(f"[info] normalized total: {len(normalized)}")

    # Hard rule: drop AFD/AfDB items without deadline (extra safety)
    normalized = [
        op for op in normalized
        if not (op.get("donor") in ("AFD", "AfDB") and not op.get("deadline"))
    ]

    if not normalized:
        print("No items after normalization/filters.")
        return

    # ---------------------- New items vs state -------------------------------
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
    header = f"*New funding opportunities ({len(new_items)})* — {UTC_DATE}"
    capacity = max(1, MAX_LINES - 1)
    lines = [header]
    for op in new_items[:capacity]:
        lines.append(_render_line(op))
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
