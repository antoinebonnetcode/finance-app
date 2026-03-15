"""
Pipeline Finance — IBKR + Fortuneo PEA
Usage : python main.py [--dry]

Sources :
  - IBKR        : API Flex (XML -> Drive + parsing)
  - Fortuneo PEA: scraping Chrome (clavier virtuel -> portefeuille)
"""

import sys
from datetime import datetime

from config import GOOGLE_DRIVE_FOLDERS, SHEETS_ID
from drive_client import DriveClient
from sheets_client import SheetsClient
from parse_ibkr import parse_ibkr


def run_pipeline(dry_run=False):
    print(f"\n{'='*60}")
    print(f"  PIPELINE FINANCE — IBKR + FORTUNEO PEA")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"{'='*60}\n")

    drive  = DriveClient()
    sheets = SheetsClient(SHEETS_ID)

    processed = sheets.get_processed_files()
    print(f"[INFO] {len(processed)} fichiers deja traites\n")

    total_tx  = 0
    total_pat = 0

    # ── Fortuneo PEA (scraping Chrome) ───────────────────────────
    print("[SOURCE] Fortuneo PEA (navigateur Chrome)")
    print("-" * 50)
    if "FORTUNEO_PEA_BROWSER" not in processed:
        try:
            from fortuneo_pea_browser import fetch_fortuneo_pea_browser
            result = fetch_fortuneo_pea_browser()
            if result:
                n_tx, n_pat = _upload(
                    result, sheets, dry_run,
                    {"id": "FORTUNEO_PEA_BROWSER", "name": "fortuneo_pea_browser"}
                )
                total_tx  += n_tx
                total_pat += n_pat
            else:
                print("  [SKIP] Aucune donnee PEA recuperee")
        except ImportError:
            print("  [SKIP] fortuneo_pea_browser.py introuvable")
        except Exception as e:
            print(f"  [ERREUR] {e}")
    else:
        print("  [SKIP] Fortuneo PEA deja traite ce cycle")
    print()

    # ── IBKR (API Flex) ──────────────────────────────────────────
    print("[SOURCE] IBKR (API Flex)")
    print("-" * 50)
    if "IBKR_API" not in processed:
        folder_ibkr = GOOGLE_DRIVE_FOLDERS.get("ibkr")
        result = parse_ibkr(
            drive_client=drive,
            folder_ibkr=folder_ibkr,
        )
        if result:
            n_tx, n_pat = _upload(
                result, sheets, dry_run,
                {"id": "IBKR_API", "name": "ibkr_flex_api"}
            )
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
