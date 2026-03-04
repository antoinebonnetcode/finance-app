# finance-perso

Pipeline automatise d'ingestion financiere multi-sources vers Google Sheets.

**Sources :** CIC (CC + CB + SCI + LMNP + Livret) · Fortuneo CC · Fortuneo PEA · IBKR (API Flex) · Metrobank PHP

**Destination :** Google Sheets — tables `TRANSACTIONS`, `PATRIMOINE`, `FICHIERS_TRAITES`

**Execution :** GitHub Actions — automatique le 1er du mois · manuel via workflow_dispatch

---

## Setup

### 1. Google Cloud

```
console.cloud.google.com
→ New Project (Organization: No organization)
→ APIs & Services → Library → activer :
    Google Drive API
    Google Sheets API
→ IAM & Admin → Service Accounts → Create
    Nom : finance-pipeline
    Role : Editor
→ Service account → Keys → Add Key → JSON → telecharger
```

Partager chaque **dossier Drive** et le **fichier Sheets** avec l'email du service account (`finance-pipeline@....iam.gserviceaccount.com`) en mode **Editeur**.

### 2. IBKR Flex API

```
Account Management → Reports → Flex Queries → Create New Query
Sections : Open Positions + Trades + Cash Transactions
Format : XML
Flex Web Services → generer Token
Noter : Token + Query ID
```

### 3. Variables locales

```bash
cp .env.example .env
# Remplir .env
pip install -r requirements.txt
python main.py --dry   # test
python main.py         # live
```

### 4. GitHub Secrets

`Settings → Secrets and variables → Actions → New repository secret`

| Secret | Valeur |
|--------|--------|
| `GOOGLE_SA_JSON` | Contenu complet du fichier JSON service account |
| `SHEETS_ID` | ID Sheets dans l'URL : `/d/XXXX/edit` |
| `DRIVE_FOLDER_CIC` | ID dossier Drive CIC |
| `DRIVE_FOLDER_FORTUNEO_CC` | ID dossier Drive Fortuneo CC |
| `DRIVE_FOLDER_FORTUNEO_PEA` | ID dossier Drive Fortuneo PEA |
| `DRIVE_FOLDER_METROBANK` | ID dossier Drive Metrobank (optionnel) |
| `IBKR_FLEX_TOKEN` | Token IBKR Flex |
| `IBKR_FLEX_QUERY_ID` | Query ID IBKR Flex |

**Lancement manuel :** GitHub → Actions → Finance Pipeline → Run workflow

---

## Structure Google Sheets

| Onglet | Role |
|--------|------|
| `TRANSACTIONS` | Toutes les operations — source du P&L |
| `PATRIMOINE` | Snapshots mensuels actifs/passifs — source du Bilan |
| `FICHIERS_TRAITES` | Tracking deduplication — ne pas supprimer |
| `CLASSIFICATION` | Regles de categorisation (a remplir manuellement) |

## Fichiers

```
main.py                       Orchestrateur
config.py                     Parametres (env vars)
utils.py                      Fonctions partagees
drive_client.py               Interface Google Drive
sheets_client.py              Interface Google Sheets
parse_cic.py                  Parser CIC XLSX multi-onglets
parse_fortuneo_metrobank.py   Parsers Fortuneo CC/PEA + Metrobank
parse_ibkr.py                 Parser IBKR API Flex + CSV
.github/workflows/pipeline.yml
```
