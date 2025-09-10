# filters.py
from __future__ import annotations

OGP_KEYWORDS = [
    # EN
    "open government","transparency","accountability","participation",
    "access to information","right to information","budget","audit","pfm",
    "citizen","civic","data","digital","govtech","ict","open data",
    "procurement","e-procurement","anti-corruption","integrity","tax","revenue",
    # FR
    "gouvernance","gouvernement ouvert","transparence","redevabilité","participation",
    "accès à l'information","droit d'accès","budget","audit","pfm","citoyen",
    "données","numérique","données ouvertes","commande publique","anticorruption",
    "appel à projets","appel a projets","subvention","financement",
    # ES
    "gobierno abierto","transparencia","rendición de cuentas","participación",
    "acceso a la información","presupuesto","datos","digital","datos abiertos",
    # AR (basic forms)
    "حكومة منفتحة","حكومة مفتوحة","شفافية","مساءلة","مشاركة","حق الحصول على المعلومات",
    "ميزانية","بيانات","رقمي","مفتوحة",
]

EXCLUDE_KEYWORDS = [
    # Filter out auctions/sales and non-programmatic tenders
    "auction","auctions","sealed-bid","sale of vehicles","vehicle sale",
    "vente aux enchères","enchères","vente de véhicules","subasta","venta de vehículos",
    "sale of it equipment","selling equipment","disposal of assets",
]

def ogp_relevant(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in OGP_KEYWORDS)

def is_excluded(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in EXCLUDE_KEYWORDS)
