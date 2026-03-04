"""Utils partages entre tous les parsers."""

import hashlib
import re
from datetime import datetime
from config import FX_RATES_FALLBACK


def make_id(*parts) -> str:
    """Hash MD5 deterministe pour deduplication."""
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


def to_eur(amount: float, devise: str) -> float:
    """Convertit un montant en EUR via taux fallback."""
    if devise == "EUR":
        return round(amount, 2)
    rate = FX_RATES_FALLBACK.get(devise.upper(), 1.0)
    return round(amount * rate, 2)


def parse_date_fr(s) -> str:
    """Normalise une date en YYYY-MM-DD."""
    s = str(s).strip()
    s = s.split(";")[0]  # strip IBKR dateTime suffix (e.g. "20240315;143000")
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def clean_amount(s) -> float:
    """Nettoie un montant : '1 234,56' ou '1,234.56' -> float."""
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip().replace("\xa0", "").replace(" ", "")
    if re.search(r",\d{1,2}$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def normalize_libelle(s: str) -> str:
    """Nettoie un libelle bancaire pour la categorisation."""
    s = str(s).upper().strip()
    s = re.sub(r"\d{2}/\d{2}(/\d{2,4})?", "", s)
    s = re.sub(r"\b\d{4,}\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


_TRANSFER_PATTERNS = [
    # (list of keywords,  counterpart account_id)
    (["FORTUNEO", "FRT BANQUE"],        "Fortuneo_CC_joint"),
    (["INTERACTIVE BROKERS", "IBKR",
      "CAPTRADER", "LYNX BROKER"],      "IBKR_antoine"),
    (["METROBANK", "METRO BANK"],        "Metrobank_antoine"),
    # CIC internal transfers — identified by account number fragment
    (["20624106"],                       "CIC_LMNP_freland"),
    (["20624108"],                       "CIC_livret"),
    (["20607401"],                       "CIC_SCI"),
    (["20624101"],                       "CIC_CC_antoine"),
]


def detect_contre_partie(libelle: str, compte_id: str) -> str:
    """Detecte le compte contrepartie d'un virement interne."""
    lib = libelle.upper()
    for patterns, cid in _TRANSFER_PATTERNS:
        if cid == compte_id:          # skip self-match
            continue
        if any(p in lib for p in patterns):
            return cid
    return ""


def detect_nature(libelle: str, montant: float, compte_id: str) -> str:
    """Heuristique de base pour la nature d'une transaction."""
    lib = libelle.upper()

    if any(x in lib for x in ["VIR IBKR", "INTERACTIVE BROKERS", "FORTUNEO",
                               "CIC VIRT", "CAPTRADER", "LYNX"]):
        return "epargne"
    if any(x in lib for x in ["LOYER", "VIREMENT LOCATAIRE"]):
        return "revenu"
    if any(x in lib for x in ["SALAIRE", "PAIE", "EMMA", "VIREMENT EMPLOYEUR"]):
        return "revenu"
    if any(x in lib for x in ["REMBT PRET", "ECHEANCE PRET", "CREDIT IMMO",
                               "ECHEANCE CRD", "PRET IMMO"]):
        return "depense"

    return "revenu" if montant > 0 else "depense"
