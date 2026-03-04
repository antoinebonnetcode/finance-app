"""
Parser amortissement — tableau d'amortissement des prets immobiliers.

Produit :
  TRANSACTIONS : 3 lignes par echeance (capital, interets, assurance)
                 -> permet l'analyse fiscale (interets deductibles sci/lmnp)
  PATRIMOINE   : capital restant du le plus recent (valeur negative = dette)

Format CSV (exportable depuis le site de la banque, ou calcule via Excel) :
  Colonnes (en-tete obligatoire) :
    nom_pret              - identifiant du pret, ex : PRET_CIC_SCI
    entite                - sci | lmnp | perso
    compte_id             - compte debite,  ex : CIC_SCI
    date                  - date de l'echeance  YYYY-MM-DD
    capital_restant_avant - capital restant DU avant paiement
    principal             - part capital de l'echeance
    interets              - part interets de l'echeance
    assurance             - part assurance emprunteur
    total_echeance        - total = principal + interets + assurance (colonne de controle)

Exemple :
  nom_pret,entite,compte_id,date,capital_restant_avant,principal,interets,assurance,total_echeance
  PRET_CIC_SCI,sci,CIC_SCI,2024-01-01,185000,320.50,450.00,85.00,855.50
  PRET_CIC_SCI,sci,CIC_SCI,2024-02-01,184679.50,322.50,448.00,85.00,855.50

Notes :
- Seules les echeances passees ou presentes sont inserees en TRANSACTIONS.
- La ligne de PATRIMOINE utilise la date la plus recente du CSV comme snapshot.
- Les interets et assurance sont marques deductible_ir=oui pour sci/lmnp.
"""

import io
import csv
from datetime import datetime, date

from utils import make_id, parse_date_fr, clean_amount
from config import COMPTE_ENTITE


def parse_amortissement(file_bytes, file_id, file_name, **kwargs):
    transactions = []
    patrimoine   = []

    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = file_bytes.decode(enc)
            break
        except Exception:
            continue
    else:
        return {"transactions": [], "patrimoine": [],
                "file_id": file_id, "file_name": file_name, "source": "amortissement"}

    sep = ";" if text.count(";") > text.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=sep)
    rows   = [{k.strip().lower(): str(v or "").strip() for k, v in r.items()} for r in reader]

    today_str = datetime.today().strftime("%Y-%m-%d")
    today_dt  = date.today()

    # Group by nom_pret to build one patrimoine entry per pret
    prets = {}
    for r in rows:
        nom = r.get("nom_pret", "")
        if nom not in prets:
            prets[nom] = []
        prets[nom].append(r)

    for nom, echeances in prets.items():
        last_past = None

        for r in echeances:
            date_str  = parse_date_fr(r.get("date", ""))
            entite    = r.get("entite", "perso")
            compte_id = r.get("compte_id", "")
            capital_avant = clean_amount(r.get("capital_restant_avant", 0))
            principal = clean_amount(r.get("principal", 0))
            interets  = clean_amount(r.get("interets", 0))
            assurance = clean_amount(r.get("assurance", 0))

            if not date_str or not nom:
                continue

            # Only create transactions for past/present echeances
            try:
                ech_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if ech_dt > today_dt:
                continue

            deductible = "oui" if entite in ("sci", "lmnp") else "non"

            if principal > 0:
                transactions.append(_build_tx(
                    nom, date_str, entite, compte_id,
                    -principal, "remboursement_capital", "capital_pret",
                    "non", f"{nom} echeance {date_str}",
                ))
            if interets > 0:
                transactions.append(_build_tx(
                    nom, date_str, entite, compte_id,
                    -interets, "depense", "interets_emprunt",
                    deductible, f"{nom} interets {date_str}",
                ))
            if assurance > 0:
                transactions.append(_build_tx(
                    nom, date_str, entite, compte_id,
                    -assurance, "depense", "assurance_pret",
                    deductible, f"{nom} assurance {date_str}",
                ))

            # Track most recent past echeance for patrimoine snapshot
            if last_past is None or date_str > last_past["date"]:
                last_past = {
                    "date":           date_str,
                    "capital_restant": capital_avant - principal,
                    "entite":         entite,
                    "compte_id":      compte_id,
                }

        # One patrimoine entry per pret (remaining debt as negative value)
        if last_past and last_past["capital_restant"] > 0:
            patrimoine.append({
                "date_snapshot":       today_str,
                "entite":              last_past["entite"],
                "poste":               nom,
                "classe_actif":        "dette_emprunt",
                "valeur_eur":          -round(last_past["capital_restant"], 2),
                "devise_origine":      "EUR",
                "quantite":            1,
                "prix_unitaire":       -round(last_past["capital_restant"], 2),
                "source_valorisation": "amortissement_csv",
                "isin":                "",
                "description":         f"Pret immobilier {nom}",
                "pv_latente_eur":      0,
                "cout_base_eur":       0,
                "commentaire":         f"derniere_echeance={last_past['date']}",
            })

    print(f"    [Amortissement] {len(transactions)} lignes tx, {len(patrimoine)} pret(s)")
    return {"transactions": transactions, "patrimoine": patrimoine,
            "file_id": file_id, "file_name": file_name, "source": "amortissement"}


def _build_tx(nom_pret, date, entite, compte_id, montant, nature, categorie,
              deductible_ir, commentaire):
    suffix = categorie  # makes the ID unique per sub-type
    return {
        "id":             make_id(nom_pret, date, suffix, montant),
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
