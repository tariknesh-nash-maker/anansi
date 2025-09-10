from __future__ import annotations
import os, json, time, pathlib, typing as T

def is_on(*envs: str) -> bool:
    for e in envs:
        v = os.getenv(e)
        if v and str(v).strip().lower() in ("1","true","yes","on"):
            return True
    return False

def dump_dir() -> pathlib.Path | None:
    d = os.getenv("DEBUG_DUMP_DIR")
    if not d: return None
    p = pathlib.Path(d)
    p.mkdir(parents=True, exist_ok=True)
    return p

def dump_json(name: str, obj: T.Any) -> None:
    p = dump_dir()
    if not p: return
    ts = time.strftime("%Y%m%d-%H%M%S")
    f = p / f"{ts}-{name}.json"
    try:
        with f.open("w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2)
    except Exception:
        pass

def dump_text(name: str, text: str) -> None:
    p = dump_dir()
    if not p: return
    ts = time.strftime("%Y%m%d-%H%M%S")
    f = p / f"{ts}-{name}.txt"
    try:
        with f.open("w", encoding="utf-8") as fh:
            fh.write(text or "")
    except Exception:
        pass

def kv(prefix: str, **kwargs):
    kvs = " ".join(f"{k}={repr(v)}" for k, v in kwargs.items())
    print(f"[{prefix}] {kvs}")
