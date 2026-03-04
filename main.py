"""
Pipeline principal — Finance Lin-Bonnet
Usage : python main.py [--dry]
"""

import sys
from datetime import datetime

from config import GOOGLE_DRIVE_FOLDERS, SHEETS_ID
from drive_client import DriveClient
from sheets_client import SheetsClient
from parse_cic import parse_cic
from parse_fortuneo_metrobank import parse_fortuneo_cc, parse_fortuneo_pea, parse_metrobank
from parse_ibkr import parse_ibkr


def run_pipeline(dry_run=False):
    print(f"\n{'='*60}")
    print(f"  PIPELINE FINANCE LIN-BONNET")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"{'='*60}\n")

    drive  = DriveClient()
    sheets = SheetsClient(SHEETS_ID)

    processed = sheets.get_processed_files()
    print(f"[INFO] {len(processed)} fichiers deja traites\n")

    total_tx  = 0
    total_pat = 0

    # ── Sources avec fichiers Drive ──────────────────────────────
    file_sources = [
        {
            "name":       "CIC (CC + CB + SCI + LMNP + Livret)",
            "folder_key": "cic",
            "extensions": [".xlsx", ".xls", ".csv"],
            "parser":     parse_cic,
        },
        {
            "name":       "Fortuneo CC (joint)",
            "folder_key": "fortuneo_cc",
            "extensions": [".csv", ".pdf"],
            "parser":     parse_fortuneo_cc,
        },
        {
            "name":       "Fortuneo PEA",
            "folder_key": "fortuneo_pea",
            "extensions": [".xls", ".xlsx", ".csv"],
            "parser":     parse_fortuneo_pea,
        },
        {
            "name":       "Metrobank (PHP)",
            "folder_key": "metrobank",
            "extensions": [".csv"],
            "parser":     parse_metrobank,
        },
    ]

    for src in file_sources:
        print(f"[SOURCE] {src['name']}")
        print("-" * 50)

        folder_id = GOOGLE_DRIVE_FOLDERS.get(src["folder_key"])
        if not folder_id:
            print(f"  [SKIP] Dossier non configure pour '{src['folder_key']}'\n")
            continue

        files = drive.list_files(folder_id, src["extensions"])
        new   = [f for f in files if f["id"] not in processed]
        print(f"  {len(files)} fichier(s) total, {len(new)} nouveau(x)\n")

        for f in new:
            print(f"  [FILE] {f['name']}")
            try:
                file_bytes = drive.download_file(f["id"])
                result = src["parser"](
                    file_bytes=file_bytes,
                    file_id=f["id"],
                    file_name=f["name"],
                )
            except Exception as e:
                print(f"  [ERREUR] {e}")
                if not dry_run:
                    sheets.mark_file_error(f["id"], f["name"], str(e))
                continue

            if result:
                n_tx, n_pat = _upload(result, sheets, dry_run, f)
                total_tx  += n_tx
                total_pat += n_pat
        print()

    # ── IBKR (API directe) ───────────────────────────────────────
    print("[SOURCE] IBKR (API Flex automatique)")
    print("-" * 50)
    if "IBKR_API" not in processed:
        result = parse_ibkr()
        if result:
            n_tx, n_pat = _upload(result, sheets, dry_run,
                                  {"id": "IBKR_API", "name": "ibkr_flex_api"})
            total_tx  += n_tx
            total_pat += n_pat
    else:
        print("  [SKIP] IBKR API deja traite ce cycle")
    print()

    print(f"{'='*60}")
    print(f"  RESUME")
    print(f"  Transactions : {total_tx}")
    print(f"  Patrimoine   : {total_pat}")
    print(f"  Mode         : {'DRY RUN' if dry_run else 'LIVE OK'}")
    print(f"{'='*60}\n")


def _upload(result, sheets, dry_run, file_meta):
    txs = result.get("transactions", [])
    pat = result.get("patrimoine", [])
    n   = len(txs) + len(pat)

    print(f"  -> {len(txs)} tx, {len(pat)} snapshots")

    if not dry_run:
        if txs:
            sheets.append_transactions(txs)
        if pat:
            sheets.append_patrimoine(pat)
        sheets.mark_file_processed(
            file_id   = result.get("file_id", file_meta["id"]),
            file_name = result.get("file_name", file_meta["name"]),
            source    = result.get("source", "unknown"),
            nb_lignes = n,
        )
        print(f"  [OK] Upload termine")
    else:
        print(f"  [DRY] Skip (dry run)")

    return len(txs), len(pat)


if __name__ == "__main__":
    dry = "--dry" in sys.argv
    run_pipeline(dry_run=dry)
