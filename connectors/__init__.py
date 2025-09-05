
# optional: expose common fetchers here
try:
    from .eu_ft import fetch as fetch_eu
except Exception:
    fetch_eu = None

try:
    from .afdb import fetch as fetch_afdb
except Exception:
    fetch_afdb = None

try:
    from .afd import fetch as fetch_afd
except Exception:
    fetch_afd = None
