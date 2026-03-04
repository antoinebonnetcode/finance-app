"""
SheetsClient — interface Google Sheets
Gere TRANSACTIONS, PATRIMOINE, et FICHIERS_TRAITES.
Cree automatiquement les onglets au premier run.
"""

from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

from config import (
    GOOGLE_SERVICE_ACCOUNT_JSON,
    SHEET_TRANSACTIONS,
    SHEET_PATRIMOINE,
    SHEET_FICHIERS,
)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

HEADERS_TRANSACTIONS = [
    "id", "date", "date_valeur", "source", "entite", "compte_id",
    "libelle_brut", "libelle_clean", "montant", "devise", "montant_eur",
    "nature", "categorie", "sous_categorie",
    "deductible_ir", "contre_partie", "statut", "flag_doublon", "commentaire",
]

HEADERS_PATRIMOINE = [
    "date_snapshot", "entite", "poste", "classe_actif",
    "valeur_eur", "devise_origine", "quantite", "prix_unitaire",
    "source_valorisation",
    "isin", "description", "pv_latente_eur", "cout_base_eur", "commentaire",
]

HEADERS_FICHIERS = [
    "fichier_id", "nom", "source", "date_traitement",
    "nb_lignes", "statut", "erreur",
]


class SheetsClient:
    def __init__(self, spreadsheet_id: str):
        creds = Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_JSON, scopes=SCOPES
        )
        self.gc = gspread.authorize(creds)
        self.spreadsheet = self.gc.open_by_key(spreadsheet_id)
        self._ensure_sheets()

    def _ensure_sheets(self):
        """Cree les onglets s'ils n'existent pas."""
        existing = [ws.title for ws in self.spreadsheet.worksheets()]
        for title, headers in [
            (SHEET_TRANSACTIONS, HEADERS_TRANSACTIONS),
            (SHEET_PATRIMOINE,   HEADERS_PATRIMOINE),
            (SHEET_FICHIERS,     HEADERS_FICHIERS),
        ]:
            if title not in existing:
                ws = self.spreadsheet.add_worksheet(
                    title=title, rows=10000, cols=len(headers)
                )
                ws.append_row(headers, value_input_option="RAW")
                print(f"  [SHEETS] Onglet '{title}' cree")

    # ─── FICHIERS TRAITES ────────────────────────────────────────

    def get_processed_files(self) -> set:
        ws = self.spreadsheet.worksheet(SHEET_FICHIERS)
        records = ws.get_all_records()
        return {r["fichier_id"] for r in records if r.get("statut") == "ok"}

    def mark_file_processed(self, file_id, file_name, source, nb_lignes):
        ws = self.spreadsheet.worksheet(SHEET_FICHIERS)
        ws.append_row([
            file_id, file_name, source,
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            nb_lignes, "ok", "",
        ], value_input_option="RAW")

    def mark_file_error(self, file_id, file_name, error_msg):
        ws = self.spreadsheet.worksheet(SHEET_FICHIERS)
        ws.append_row([
            file_id, file_name, "unknown",
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            0, "erreur", str(error_msg)[:500],
        ], value_input_option="RAW")

    # ─── TRANSACTIONS ────────────────────────────────────────────

    def append_transactions(self, transactions: list):
        """Ajoute des lignes dans TRANSACTIONS. Deduplique par 'id'."""
        ws = self.spreadsheet.worksheet(SHEET_TRANSACTIONS)
        try:
            existing_ids = set(ws.col_values(1)[1:])
        except Exception:
            existing_ids = set()

        rows_to_add = []
        dupes = 0
        for tx in transactions:
            if tx.get("id") in existing_ids:
                dupes += 1
                continue
            rows_to_add.append([tx.get(h, "") for h in HEADERS_TRANSACTIONS])

        if rows_to_add:
            ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        if dupes:
            print(f"    [DEDUP] {dupes} doublon(s) ignore(s)")
        print(f"    [SHEETS] {len(rows_to_add)} transaction(s) ajoutee(s)")

    # ─── PATRIMOINE ──────────────────────────────────────────────

    def append_patrimoine(self, snapshots: list):
        ws = self.spreadsheet.worksheet(SHEET_PATRIMOINE)
        rows = [[s.get(h, "") for h in HEADERS_PATRIMOINE] for s in snapshots]
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"    [SHEETS] {len(rows)} snapshot(s) patrimoine ajoute(s)")

    # ─── CLASSIFICATION ──────────────────────────────────────────

    def get_classification_rules(self) -> list:
        try:
            ws = self.spreadsheet.worksheet("CLASSIFICATION")
            return ws.get_all_records()
        except Exception:
            return []
