"""
Configuration — Finance Lin-Bonnet
Lit les valeurs depuis variables d'environnement (GitHub Secrets)
ou .env local (developpement).

Setup local  : copier .env.example -> .env et remplir
Setup GitHub : Settings -> Secrets and variables -> Actions
"""

import os
from dotenv import load_dotenv

load_dotenv()  # no-op si absent (GitHub Actions)


def _req(key):
    val = os.environ.get(key, "")
    if not val:
        raise EnvironmentError(f"Variable manquante : {key}")
    return val


def _opt(key, default=""):
    return os.environ.get(key, default)


# ─── GOOGLE SHEETS ───────────────────────────────────────────────
SHEETS_ID          = _req("SHEETS_ID")
SHEET_TRANSACTIONS = "TRANSACTIONS"
SHEET_PATRIMOINE   = "PATRIMOINE"
SHEET_FICHIERS     = "FICHIERS_TRAITES"

# ─── GOOGLE DRIVE — IDs dossiers ─────────────────────────────────
GOOGLE_DRIVE_FOLDERS = {
    "cic":           _opt("DRIVE_FOLDER_CIC"),
    "fortuneo_cc":   _opt("DRIVE_FOLDER_FORTUNEO_CC"),
    "fortuneo_pea":  _opt("DRIVE_FOLDER_FORTUNEO_PEA"),
    "metrobank":     _opt("DRIVE_FOLDER_METROBANK"),
    "ibkr":          _opt("DRIVE_FOLDER_IBKR"),
    "immo":          _opt("DRIVE_FOLDER_IMMO"),
    "amortissement": _opt("DRIVE_FOLDER_AMORTISSEMENT"),
}

# ─── IBKR FLEX API ───────────────────────────────────────────────
IBKR_FLEX_TOKEN    = _opt("IBKR_FLEX_TOKEN")
IBKR_FLEX_QUERY_ID = _opt("IBKR_FLEX_QUERY_ID")

# ─── FORTUNEO ────────────────────────────────────────────────────
FORTUNEO_LOGIN    = _opt("FORTUNEO_LOGIN")
FORTUNEO_PASSWORD = _opt("FORTUNEO_PASSWORD")

# ─── GOOGLE SERVICE ACCOUNT ──────────────────────────────────────
GOOGLE_SERVICE_ACCOUNT_JSON = _opt(
    "GOOGLE_SERVICE_ACCOUNT_PATH",
    "credentials/service_account.json"
)

# ─── FX FALLBACK ─────────────────────────────────────────────────
FX_RATES_FALLBACK = {
    "PHP": float(_opt("FX_PHP", "0.016")),
    "USD": float(_opt("FX_USD", "0.92")),
    "TWD": float(_opt("FX_TWD", "0.028")),
}

# ─── MAPPING COMPTE -> ENTITE ────────────────────────────────────
COMPTE_ENTITE = {
    "CIC_CC_antoine":       "perso",
    "CIC_CB_antoine":       "perso",
    "CIC_SCI":              "sci",
    "CIC_LMNP_freland":     "lmnp",
    "CIC_livret":           "perso",
    "Fortuneo_CC_joint":    "perso",
    "Fortuneo_PEA_antoine": "perso",
    "IBKR_antoine":         "perso",
    "Metrobank_antoine":    "perso",
}

# ─── CONFIGURATION PRETS IMMOBILIERS ─────────────────────────────
# Cle = nom du fichier CSV sans extension (ex: "PRET_CIC_SCI")
# Adapter selon vos prets reels
LOAN_CONFIG = {
    "PRET_CIC_SCI":     {"entite": "sci",  "compte_id": "CIC_SCI"},
    "PRET_CIC_LMNP":    {"entite": "lmnp", "compte_id": "CIC_LMNP_freland"},
    "PRET_CIC_PERSO":   {"entite": "perso","compte_id": "CIC_CC_antoine"},
}
