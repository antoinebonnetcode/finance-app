"""
Parser immobilier — valeurs declarees des biens immobiliers.

Produit des snapshots PATRIMOINE uniquement (pas de transactions).

Format CSV (maintenu manuellement dans Google Drive, séparateur , ou ;) :
  date               DD/MM/YYYY ou YYYY-MM-DD
  poste              identifiant unique, ex : IMMO_Campagne_Seconde
  classe_actif       ex : actif_immobilier
  entite             libelle libre, ex : "SCI Campagne Seconde" / "Perso - LMNP"
  part détention     ex : 100% ou 20%  — la valeur retenue = valeur × part
  Nom bien           adresse ou nom court du bien
  valeur_eur         valeur estimee totale (avant application de la part)
  cout_achat_eur     prix d'achat total
  description        texte libre (type de bien, surface…)
  source_valorisation ex : estimation / notaire / dvf / prix_achat

Règles :
- Si un bien apparait plusieurs fois (mises a jour successives de la valeur),
  seule la ligne a la date la plus recente est retenue pour le snapshot.
- La valeur et le cout sont multiplies par la part de detention.
"""

import io
import csv
import unicodedata

from utils import parse_date_fr, clean_amount


def _nk(s: str) -> str:
    """Normalise un nom de colonne : minuscules, sans accents, espaces -> _."""
    s = unicodedata.normalize("NFD", str(s))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower().strip().replace(" ", "_").replace("-", "_")


def _get(row_norm: dict, *candidates: str, default="") -> str:
    for c in candidates:
        if c in row_norm:
            return row_norm[c]
    return default


def _parse_pct(s: str) -> float:
    """'100%' -> 1.0,  '20%' -> 0.2,  '0.2' -> 0.2"""
    s = str(s).strip().replace(",", ".").replace(" ", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100
        except ValueError:
            return 1.0
    try:
        v = float(s)
        return v if v <= 1 else v / 100
    except ValueError:
        return 1.0


def parse_immo(file_bytes, file_id, file_name, **kwargs):
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = file_bytes.decode(enc)
            break
        except Exception:
            continue
    else:
        return {"transactions": [], "patrimoine": [],
                "file_id": file_id, "file_name": file_name, "source": "immo"}

    # Auto-detect separator
    sep = ";" if text.count(";") > text.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=sep)

    # Normalize column names once; store all rows
    all_rows = []
    for row in reader:
        norm = {_nk(k): str(v or "").strip() for k, v in row.items()}
        all_rows.append(norm)

    # Keep only the most-recent row per poste
    latest: dict[str, dict] = {}
    for r in all_rows:
        poste = _get(r, "poste").strip()
        date  = parse_date_fr(_get(r, "date"))
        if not poste:
            continue
        if poste not in latest or date > latest[poste]["_date"]:
            r["_date"] = date
            latest[poste] = r

    snapshots = [snap for snap in (_row_to_snap(r) for r in latest.values()) if snap]

    print(f"    [Immo] {len(snapshots)} bien(s) charge(s) (sur {len(all_rows)} ligne(s))")
    return {"transactions": [], "patrimoine": snapshots,
            "file_id": file_id, "file_name": file_name, "source": "immo"}


def _row_to_snap(r: dict):
    try:
        date    = r.get("_date") or parse_date_fr(_get(r, "date"))
        poste   = _get(r, "poste").strip()
        classe  = _get(r, "classe_actif", default="actif_immobilier").strip() or "actif_immobilier"
        entite  = _get(r, "entite").strip()
        part    = _parse_pct(_get(r, "part_detention", "part_detente", "detention", default="100%"))
        nom     = _get(r, "nom_bien", "nom", "adresse").strip()
        valeur_total = clean_amount(_get(r, "valeur_eur", "valeur", default="0"))
        cout_total   = clean_amount(_get(r, "cout_achat_eur", "cout_achat", "prix_achat", default="0"))
        desc    = _get(r, "description", "desc", default=nom or poste).strip() or nom or poste
        source  = _get(r, "source_valorisation", "source", default="estimation_manuelle").strip()

        if not poste or valeur_total == 0:
            return None

        # Apply ownership share
        valeur = round(valeur_total * part, 2)
        cout   = round(cout_total * part, 2)
        pv     = round(valeur - cout, 2) if cout else 0

        comment = f"part={part:.0%} valeur_totale={valeur_total}"
        if nom:
            comment = f"{nom} | " + comment

        return {
            "date_snapshot":       date,
            "entite":              entite,
            "poste":               poste,
            "classe_actif":        classe,
            "valeur_eur":          valeur,
            "devise_origine":      "EUR",
            "quantite":            1,
            "prix_unitaire":       valeur,
            "source_valorisation": source,
            "isin":                "",
            "description":         desc,
            "pv_latente_eur":      pv,
            "cout_base_eur":       cout,
            "commentaire":         comment,
        }
    except Exception:
        return None
