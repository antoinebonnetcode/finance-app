"""
Parser amortissement — tableau d'amortissement des prets immobiliers.

Produit :
  TRANSACTIONS : 3 lignes par echeance passee (capital, interets, assurance)
                 Interets + assurance : deductible_ir=oui pour sci/lmnp
  PATRIMOINE   : capital restant du le plus recent (valeur negative = dette)

Format CSV — export direct depuis le site de la banque (CIC, etc.) :
  Separateur : virgule  |  Decimales : point
  En-tetes attendus (correspondance insensible a la casse / accents) :
    Date_echeance
    Capital_du_avant_echeance_EUR
    Capital_EUR
    Interets_EUR
    Assurance_groupe_et_frais_EUR
    Echeance_assurance_groupe_comprise_EUR   (colonne de controle, ignoree)

Convention de nommage du fichier :
  Le nom du fichier (sans extension) est utilise comme identifiant du pret.
  Ex : PRET_CIC_SCI.csv  ->  nom_pret = "PRET_CIC_SCI"

  L'entite et le compte_id sont derives via LOAN_CONFIG dans config.py :
    LOAN_CONFIG = {
        "PRET_CIC_SCI":  {"entite": "sci",  "compte_id": "CIC_SCI"},
        "PRET_CIC_LMNP": {"entite": "lmnp", "compte_id": "CIC_LMNP_freland"},
    }
  Si le fichier n'est pas dans LOAN_CONFIG, entite = "perso", compte_id = "".
"""

import io
import csv
import os
import unicodedata
from datetime import datetime, date

from utils import make_id, parse_date_fr, clean_amount
from config import LOAN_CONFIG


def _nk(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower().strip().replace(" ", "_").replace("-", "_")


# Fuzzy column lookup — maps our internal key to candidate normalized header names
_COL_CANDIDATES = {
    "date":       ["date_echeance", "date"],
    "capital_av": ["capital_du_avant_echeance_eur", "capital_restant_avant",
                   "capital_restant_du", "capital_restant", "solde_avant"],
    "principal":  ["capital_eur", "capital", "principal", "remboursement_capital",
                   "amortissement"],
    "interets":   ["interets_eur", "interets", "interet", "taux_interet_montant"],
    "assurance":  ["assurance_groupe_et_frais_eur", "assurance_eur", "assurance",
                   "cotisation_assurance"],
}


def _map_columns(headers_norm: list[str]) -> dict[str, int]:
    """Returns {internal_key: column_index} for each recognized column."""
    mapping = {}
    for key, candidates in _COL_CANDIDATES.items():
        for c in candidates:
            if c in headers_norm:
                mapping[key] = headers_norm.index(c)
                break
    return mapping


def parse_amortissement(file_bytes, file_id, file_name, **kwargs):
    # Derive loan name from filename (strip path and extension)
    stem     = os.path.splitext(os.path.basename(file_name or "PRET"))[0]
    nom_pret = stem
    loan_cfg = LOAN_CONFIG.get(nom_pret, {})
    entite   = loan_cfg.get("entite", "perso")
    compte_id = loan_cfg.get("compte_id", "")

    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = file_bytes.decode(enc)
            break
        except Exception:
            continue
    else:
        return {"transactions": [], "patrimoine": [],
                "file_id": file_id, "file_name": file_name, "source": "amortissement"}

    # The bank export uses commas as separator and dots as decimal
    # (even if the system locale is French), so we detect carefully
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return {"transactions": [], "patrimoine": [],
                "file_id": file_id, "file_name": file_name, "source": "amortissement"}

    # Detect separator from header line
    header_line = lines[0]
    sep = ";" if header_line.count(";") > header_line.count(",") else ","

    reader = csv.reader(lines, delimiter=sep)
    raw_headers = next(reader, [])
    headers_norm = [_nk(h) for h in raw_headers]
    col = _map_columns(headers_norm)

    if "date" not in col or "principal" not in col:
        print(f"    [Amortissement] {nom_pret}: colonnes non reconnues dans {file_name}")
        print(f"      Headers trouves: {raw_headers}")
        return {"transactions": [], "patrimoine": [],
                "file_id": file_id, "file_name": file_name, "source": "amortissement"}

    def cell(row, key, default=0):
        idx = col.get(key, -1)
        if idx < 0 or idx >= len(row):
            return default
        return row[idx]

    transactions = []
    patrimoine   = []
    today_dt     = date.today()
    today_str    = datetime.today().strftime("%Y-%m-%d")
    last_past    = None
    deductible   = "oui" if entite in ("sci", "lmnp") else "non"

    for row in reader:
        if not row or not any(row):
            continue

        date_raw     = str(cell(row, "date", "")).strip()
        capital_av   = clean_amount(cell(row, "capital_av", 0))
        principal    = clean_amount(cell(row, "principal",  0))
        interets     = clean_amount(cell(row, "interets",   0))
        assurance    = clean_amount(cell(row, "assurance",  0))

        date_str = parse_date_fr(date_raw)
        if not date_str:
            continue

        try:
            ech_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        # Only produce transactions for past or current echeances
        if ech_dt <= today_dt:
            if principal > 0:
                transactions.append(_build_tx(
                    nom_pret, date_str, entite, compte_id,
                    -principal, "remboursement_capital", "capital_pret", "non",
                    f"{nom_pret} capital {date_str}",
                ))
            if interets > 0:
                transactions.append(_build_tx(
                    nom_pret, date_str, entite, compte_id,
                    -interets, "depense", "interets_emprunt", deductible,
                    f"{nom_pret} interets {date_str}",
                ))
            if assurance > 0:
                transactions.append(_build_tx(
                    nom_pret, date_str, entite, compte_id,
                    -assurance, "depense", "assurance_pret", deductible,
                    f"{nom_pret} assurance {date_str}",
                ))

            # Track most recent past row for patrimoine snapshot
            if last_past is None or date_str > last_past["date"]:
                last_past = {
                    "date":           date_str,
                    "capital_restant": capital_av - principal,
                }

    # Patrimoine: remaining debt as negative value
    if last_past and last_past["capital_restant"] > 0:
        cap = round(last_past["capital_restant"], 2)
        patrimoine.append({
            "date_snapshot":       today_str,
            "entite":              entite,
            "poste":               nom_pret,
            "classe_actif":        "dette_emprunt",
            "valeur_eur":          -cap,
            "devise_origine":      "EUR",
            "quantite":            1,
            "prix_unitaire":       -cap,
            "source_valorisation": "amortissement_csv",
            "isin":                "",
            "description":         f"Pret immobilier {nom_pret}",
            "pv_latente_eur":      0,
            "cout_base_eur":       0,
            "commentaire":         f"derniere_echeance={last_past['date']} capital_restant={cap}",
        })

    print(f"    [Amortissement] {nom_pret} : {len(transactions)} lignes tx, "
          f"{len(patrimoine)} pret(s) patrimoine")
    return {"transactions": transactions, "patrimoine": patrimoine,
            "file_id": file_id, "file_name": file_name, "source": "amortissement"}


def _build_tx(nom_pret, date, entite, compte_id, montant, nature, categorie,
              deductible_ir, commentaire):
    return {
        "id":             make_id(nom_pret, date, categorie, montant),
        "date":           date,
        "date_valeur":    date,
        "source":         "amortissement",
        "entite":         entite,
        "compte_id":      compte_id,
        "libelle_brut":   commentaire,
        "libelle_clean":  commentaire,
        "montant":        round(montant, 2),
        "devise":         "EUR",
        "montant_eur":    round(montant, 2),
        "nature":         nature,
        "categorie":      categorie,
        "sous_categorie": "",
        "deductible_ir":  deductible_ir,
        "contre_partie":  "",
        "statut":         "amortissement",
        "flag_doublon":   "",
        "commentaire":    commentaire,
    }
