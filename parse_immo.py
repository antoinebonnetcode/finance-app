"""
Parser immobilier — valeurs declarees des biens immobiliers.

Produit des snapshots PATRIMOINE uniquement (pas de transactions).

Format CSV a maintenir manuellement dans Google Drive :
  Colonnes (en-tete obligatoire) :
    date               - date de l'estimation  YYYY-MM-DD
    poste              - identifiant unique du bien, ex : IMMO_Campagne_Seconde
    classe_actif       - immeuble_sci | immeuble_lmnp | immeuble_rp | immeuble_locatif
    entite             - sci | lmnp | perso
    valeur_eur         - valeur estimee en EUR
    cout_achat_eur     - prix d'achat initial en EUR
    devise_origine     - EUR (toujours EUR pour l'immo en France)
    description        - libelle court du bien, ex : "Appartement T4 Bordeaux SCI"
    source_valorisation - estimation_agence | estimation_notaire | prix_achat | dvf
    commentaire        - note libre

Exemple de fichier :
  date,poste,classe_actif,entite,valeur_eur,cout_achat_eur,devise_origine,description,source_valorisation,commentaire
  2026-03-01,IMMO_Campagne_Seconde,immeuble_sci,sci,250000,195000,EUR,Appartement T4 SCI Bordeaux,estimation_agence,Estimation agence mars 2026
  2026-03-01,IMMO_Freland,immeuble_lmnp,lmnp,145000,130000,EUR,Studio LMNP Freland,estimation_notaire,Acte 2021
"""

import io
import csv

from utils import parse_date_fr, clean_amount


def parse_immo(file_bytes, file_id, file_name, **kwargs):
    snapshots = []

    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = file_bytes.decode(enc)
            break
        except Exception:
            continue
    else:
        return {"transactions": [], "patrimoine": [],
                "file_id": file_id, "file_name": file_name, "source": "immo"}

    sep = ";" if text.count(";") > text.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=sep)

    for row in reader:
        r = {k.strip().lower(): str(v or "").strip() for k, v in row.items()}
        snap = _row_to_snap(r)
        if snap:
            snapshots.append(snap)

    print(f"    [Immo] {len(snapshots)} bien(s) charge(s)")
    return {"transactions": [], "patrimoine": snapshots,
            "file_id": file_id, "file_name": file_name, "source": "immo"}


def _row_to_snap(r):
    try:
        date     = parse_date_fr(r.get("date", ""))
        poste    = r.get("poste", "").strip()
        classe   = r.get("classe_actif", "immeuble_locatif").strip()
        entite   = r.get("entite", "perso").strip()
        valeur   = clean_amount(r.get("valeur_eur", 0))
        cout     = clean_amount(r.get("cout_achat_eur", 0))
        devise   = r.get("devise_origine", "EUR").strip() or "EUR"
        desc     = r.get("description", poste).strip()
        source   = r.get("source_valorisation", "estimation_manuelle").strip()
        comment  = r.get("commentaire", "").strip()

        if not poste or valeur == 0:
            return None

        return {
            "date_snapshot":       date,
            "entite":              entite,
            "poste":               poste,
            "classe_actif":        classe,
            "valeur_eur":          valeur,
            "devise_origine":      devise,
            "quantite":            1,
            "prix_unitaire":       valeur,
            "source_valorisation": source,
            "isin":                "",
            "description":         desc,
            "pv_latente_eur":      round(valeur - cout, 2) if cout else 0,
            "cout_base_eur":       cout,
            "commentaire":         comment,
        }
    except Exception:
        return None
