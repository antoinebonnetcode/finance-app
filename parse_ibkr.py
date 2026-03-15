"""
Parser IBKR — deux modes :
  1. API Flex (automatique) : appel REST -> XML
  2. Fichier CSV Activity Statement (fallback manuel)

Sections traitees :
  Open Positions       -> PATRIMOINE (titres)
  Cash Balances        -> PATRIMOINE (liquidites par devise)
  Trades               -> TRANSACTIONS (nature=epargne)
  Deposits/Withdrawals -> TRANSACTIONS (nature=epargne)
  Dividends            -> TRANSACTIONS (nature=revenu)
  Interest             -> TRANSACTIONS (nature=revenu)
"""

import io
import re
import csv
import time
import urllib.request
from datetime import datetime

from utils import make_id, parse_date_fr, clean_amount, normalize_libelle, to_eur
from config import IBKR_FLEX_TOKEN, IBKR_FLEX_QUERY_ID, COMPTE_ENTITE

COMPTE_ID = "IBKR_antoine"
ENTITE    = COMPTE_ENTITE.get(COMPTE_ID, "perso")

FLEX_REQUEST_URL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"
FLEX_FETCH_URL   = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement"


# ─── ENTRYPOINT ──────────────────────────────────────────────────

def parse_ibkr(file_bytes=None, file_id=None, file_name=None,
               drive_client=None, folder_ibkr=None, **kwargs):
    if file_bytes:
        print("    [IBKR] Mode fichier CSV")
        return _parse_csv(file_bytes, file_id or "IBKR_CSV", file_name or "ibkr.csv")

    print("    [IBKR] Mode API Flex")
    if not IBKR_FLEX_TOKEN or IBKR_FLEX_TOKEN == "VOTRE_TOKEN_IBKR":
        print("    [WARN] IBKR_FLEX_TOKEN non configure")
        return None

    xml_bytes = _fetch_flex_xml()
    if not xml_bytes:
        return None

    # Sauvegarde brute du XML dans Drive (obligatoire si DRIVE_FOLDER_IBKR configure)
    if drive_client and folder_ibkr:
        try:
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = f"ibkr_flex_{ts}.xml"
            drive_client.upload_file(folder_ibkr, name, xml_bytes, "application/xml")
            print(f"    [IBKR] XML brut sauvegarde dans Drive : {name}")
        except Exception as e:
            print(f"    [IBKR] ATTENTION : echec sauvegarde XML dans Drive : {e}")
    else:
        print("    [IBKR] DRIVE_FOLDER_IBKR non configure — XML brut non sauvegarde")

    return _parse_xml(xml_bytes, "IBKR_API", "ibkr_flex_api")


# ─── API FLEX ────────────────────────────────────────────────────

def _fetch_flex_xml():
    try:
        url1 = f"{FLEX_REQUEST_URL}?t={IBKR_FLEX_TOKEN}&q={IBKR_FLEX_QUERY_ID}&v=3"
        with urllib.request.urlopen(url1, timeout=30) as r:
            xml1 = r.read().decode()

        ref_code = re.search(r"<ReferenceCode>(.+?)</ReferenceCode>", xml1)
        if not ref_code:
            print(f"    [IBKR] Pas de ReferenceCode : {xml1[:200]}")
            return None

        ref = ref_code.group(1)
        print(f"    [IBKR] ReferenceCode={ref}, attente 10s...")
        time.sleep(10)

        url2 = f"{FLEX_FETCH_URL}?t={IBKR_FLEX_TOKEN}&q={ref}&v=3"
        with urllib.request.urlopen(url2, timeout=60) as r:
            return r.read()

    except Exception as e:
        print(f"    [IBKR] Erreur API Flex : {e}")
        return None


# ─── PARSE XML ───────────────────────────────────────────────────

def _parse_xml(xml_bytes, file_id, file_name):
    transactions = []
    patrimoine   = []

    try:
        from ibflex import parser as ibflex_parser
        stmt = ibflex_parser.parse(io.BytesIO(xml_bytes))
        _extract_ibflex(stmt, transactions, patrimoine)
    except ImportError:
        print("    [WARN] ibflex non installe, parsing XML direct")
        _extract_xml_direct(xml_bytes, transactions, patrimoine)
    except Exception as e:
        print(f"    [IBKR] Erreur ibflex : {e}, fallback XML direct")
        _extract_xml_direct(xml_bytes, transactions, patrimoine)

    print(f"    [IBKR] {len(transactions)} tx, {len(patrimoine)} positions")
    return {"transactions": transactions, "patrimoine": patrimoine,
            "file_id": file_id, "file_name": file_name, "source": "ibkr"}


def _extract_xml_direct(xml_bytes, transactions, patrimoine):
    xml = xml_bytes.decode("utf-8", errors="replace")

    # Open positions (titres : actions, ETFs, obligations, etc.)
    for m in re.finditer(r"<OpenPosition\s([^>]+?)/>", xml, re.DOTALL):
        snap = _position_to_patrimoine(_parse_attrs(m.group(1)))
        if snap:
            patrimoine.append(snap)

    # Cash balances par devise
    for m in re.finditer(r"<CashBalance\s([^>]+?)/>", xml, re.DOTALL):
        snap = _cash_balance_to_patrimoine(_parse_attrs(m.group(1)))
        if snap:
            patrimoine.append(snap)

    for m in re.finditer(r"<Trade\s([^>]+?)/>", xml, re.DOTALL):
        tx = _trade_to_tx(_parse_attrs(m.group(1)))
        if tx:
            transactions.append(tx)

    for m in re.finditer(r"<CashTransaction\s([^>]+?)/>", xml, re.DOTALL):
        tx = _cash_to_tx(_parse_attrs(m.group(1)))
        if tx:
            transactions.append(tx)


def _parse_attrs(s):
    return {m.group(1): m.group(2) for m in re.finditer(r'(\w+)="([^"]*)"', s)}


def _position_to_patrimoine(a):
    try:
        symbol    = a.get("symbol", "")
        currency  = a.get("currency", "EUR")
        mv        = clean_amount(a.get("markPrice", 0))
        qty       = clean_amount(a.get("position", 0))
        value_lc  = mv * qty
        value_eur = to_eur(value_lc, currency)
        isin      = a.get("isin", "")
        desc      = a.get("description", symbol)
        cost_lc   = clean_amount(a.get("costBasisMoney", 0))
        pnl_lc    = clean_amount(a.get("fifoPnlUnrealized", 0))
        if not symbol or value_eur == 0:
            return None
        return {
            "date_snapshot":       datetime.today().strftime("%Y-%m-%d"),
            "entite":              ENTITE,
            "poste":               f"IBKR_{isin or symbol}",
            "classe_actif":        "actif_financier_cto",
            "valeur_eur":          value_eur,
            "devise_origine":      currency,
            "quantite":            qty,
            "prix_unitaire":       mv,
            "source_valorisation": "ibkr_flex",
            "isin":                isin,
            "description":         desc,
            "pv_latente_eur":      to_eur(pnl_lc, currency),
            "cout_base_eur":       to_eur(cost_lc, currency),
            "commentaire":         "",
        }
    except Exception:
        return None


def _cash_balance_to_patrimoine(a):
    try:
        currency = a.get("currency", "")
        if currency in ("BASE_SUMMARY", "BASE", ""):
            return None
        ending_cash = clean_amount(a.get("endingCash", a.get("endingSettledCash", 0)))
        if ending_cash == 0:
            return None
        value_eur = to_eur(ending_cash, currency)
        return {
            "date_snapshot":       datetime.today().strftime("%Y-%m-%d"),
            "entite":              ENTITE,
            "poste":               f"IBKR_CASH_{currency}",
            "classe_actif":        "liquidite",
            "valeur_eur":          value_eur,
            "devise_origine":      currency,
            "quantite":            ending_cash,
            "prix_unitaire":       to_eur(1, currency),
            "source_valorisation": "ibkr_flex",
            "isin":                "",
            "description":         f"IBKR Cash {currency}",
            "pv_latente_eur":      0,
            "cout_base_eur":       value_eur,
            "commentaire":         "",
        }
    except Exception:
        return None


def _trade_to_tx(a):
    try:
        date_raw = a.get("tradeDate", a.get("dateTime", ""))
        symbol   = a.get("symbol", "")
        currency = a.get("currency", "EUR")
        proceeds = clean_amount(a.get("proceeds", 0))
        comm     = clean_amount(a.get("ibCommission", 0))
        bs       = a.get("buySell", "")
        if not date_raw or not symbol:
            return None
        date    = parse_date_fr(date_raw)
        libelle = f"IBKR TRADE {bs} {symbol}"
        return _build_tx(make_id(COMPTE_ID, date, libelle, proceeds),
                         date, "ibkr", libelle, proceeds, currency,
                         to_eur(proceeds, currency), "epargne",
                         f"commission={comm}")
    except Exception:
        return None


def _cash_to_tx(a):
    try:
        date_raw = a.get("dateTime", a.get("settleDate", ""))
        tx_type  = a.get("type", a.get("transactionType", ""))
        currency = a.get("currency", "EUR")
        montant  = clean_amount(a.get("amount", 0))
        desc     = a.get("description", tx_type)
        if not date_raw or montant == 0:
            return None
        date    = parse_date_fr(date_raw)
        libelle = f"IBKR {tx_type} {desc}"[:200]
        nature  = ("revenu" if "DIVIDEND" in tx_type.upper() or "INTEREST" in tx_type.upper()
                   else "depense" if "WITHHOLDING" in tx_type.upper()
                   else "epargne")
        return _build_tx(make_id(COMPTE_ID, date, libelle, montant),
                         date, "ibkr", libelle, montant, currency,
                         to_eur(montant, currency), nature, "")
    except Exception:
        return None


# ─── PARSE CSV IBKR (Activity Statement) ─────────────────────────

def _parse_csv(file_bytes, file_id, file_name):
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
                "file_id": file_id, "file_name": file_name, "source": "ibkr"}

    sections = _split_sections(text)

    for section_name, rows in sections.items():
        sn = section_name.lower()
        if "open position" in sn:
            for row in rows:
                s = _csv_position(row)
                if s:
                    patrimoine.append(s)
        elif "trade" in sn and "statement" not in sn:
            for row in rows:
                tx = _csv_trade(row)
                if tx:
                    transactions.append(tx)
        elif "deposit" in sn or "withdrawal" in sn:
            for row in rows:
                tx = _csv_cash(row, "depot_retrait")
                if tx:
                    transactions.append(tx)
        elif "dividend" in sn:
            for row in rows:
                tx = _csv_cash(row, "dividende")
                if tx:
                    transactions.append(tx)
        elif "interest" in sn:
            for row in rows:
                tx = _csv_cash(row, "interet")
                if tx:
                    transactions.append(tx)

    print(f"    [IBKR CSV] {len(transactions)} tx, {len(patrimoine)} positions")
    return {"transactions": transactions, "patrimoine": patrimoine,
            "file_id": file_id, "file_name": file_name, "source": "ibkr"}


def _split_sections(text):
    sections = {}
    current_section = None
    current_headers = None
    current_rows    = []

    for line in text.splitlines():
        if not line.strip():
            continue
        parts = list(csv.reader([line]))[0]
        if len(parts) < 2:
            continue

        section_name = parts[0].strip()
        row_type     = parts[1].strip()

        if row_type == "Header":
            if current_section and current_rows:
                sections[current_section] = current_rows
            current_section = section_name
            current_headers = parts[2:]
            current_rows    = []
        elif row_type == "Data" and current_headers is not None:
            if section_name == current_section:
                current_rows.append(dict(zip(current_headers, parts[2:])))

    if current_section and current_rows:
        sections[current_section] = current_rows

    return sections


def _csv_position(row):
    try:
        symbol    = row.get("Symbol", row.get("Description", "")).strip()
        currency  = row.get("Currency", "EUR").strip()
        qty       = clean_amount(row.get("Quantity", 0) or 0)
        value_lc  = clean_amount(row.get("Value", row.get("Mark Price", 0)) or 0)
        prix      = clean_amount(row.get("Mark Price", row.get("Close Price", 0)) or 0)
        isin      = row.get("ISIN", "").strip()
        desc      = row.get("Description", symbol).strip()
        cost_lc   = clean_amount(row.get("Cost Basis", row.get("Basis", 0)) or 0)
        pnl_lc    = clean_amount(row.get("Unrealized P/L", row.get("Unrealized PnL", 0)) or 0)
        value_eur = to_eur(value_lc, currency)
        if not symbol or value_eur == 0:
            return None
        return {
            "date_snapshot":       datetime.today().strftime("%Y-%m-%d"),
            "entite":              ENTITE,
            "poste":               f"IBKR_{isin or symbol}",
            "classe_actif":        "actif_financier_cto",
            "valeur_eur":          value_eur,
            "devise_origine":      currency,
            "quantite":            qty,
            "prix_unitaire":       prix,
            "source_valorisation": "ibkr_csv",
            "isin":                isin,
            "description":         desc,
            "pv_latente_eur":      to_eur(pnl_lc, currency),
            "cout_base_eur":       to_eur(cost_lc, currency),
            "commentaire":         "",
        }
    except Exception:
        return None


def _csv_trade(row):
    try:
        date_raw = row.get("Date/Time", row.get("TradeDate", "")).strip()
        symbol   = row.get("Symbol", "").strip()
        currency = row.get("Currency", "EUR").strip()
        proceeds = clean_amount(row.get("Proceeds", 0) or 0)
        comm     = clean_amount(row.get("Comm/Fee", row.get("Commission", 0)) or 0)
        bs       = row.get("Buy/Sell", "").strip()
        if not date_raw or not symbol:
            return None
        date    = parse_date_fr(date_raw)
        libelle = f"IBKR TRADE {bs} {symbol}"
        return _build_tx(make_id(COMPTE_ID, date, libelle, proceeds),
                         date, "ibkr", libelle, proceeds, currency,
                         to_eur(proceeds, currency), "epargne",
                         f"commission={comm}")
    except Exception:
        return None


def _csv_cash(row, subtype):
    try:
        date_raw = row.get("Date", row.get("Settle Date", "")).strip()
        desc     = row.get("Description", subtype).strip()
        currency = row.get("Currency", "EUR").strip()
        montant  = clean_amount(row.get("Amount", 0) or 0)
        if not date_raw or montant == 0:
            return None
        date    = parse_date_fr(date_raw)
        libelle = f"IBKR {subtype.upper()} {desc}"[:200]
        nature  = "revenu" if subtype in ("dividende", "interet") else "epargne"
        return _build_tx(make_id(COMPTE_ID, date, libelle, montant),
                         date, "ibkr", libelle, montant, currency,
                         to_eur(montant, currency), nature, "")
    except Exception:
        return None


# ─── IBFLEX ──────────────────────────────────────────────────────

def _extract_ibflex(stmt, transactions, patrimoine):
    today = datetime.today().strftime("%Y-%m-%d")
    for account_stmt in stmt.FlexStatements:
        if hasattr(account_stmt, "OpenPositions"):
            for pos in account_stmt.OpenPositions:
                try:
                    currency  = str(pos.currency or "EUR")
                    qty       = float(pos.position or 0)
                    prix      = float(pos.markPrice or 0)
                    value_eur = to_eur(prix * qty, currency)
                    isin      = str(getattr(pos, "isin", "") or "")
                    symbol    = str(pos.symbol or "")
                    desc      = str(getattr(pos, "description", "") or symbol)
                    cost_lc   = float(getattr(pos, "costBasisMoney", 0) or 0)
                    pnl_lc    = float(getattr(pos, "fifoPnlUnrealized", 0) or 0)
                    if value_eur == 0:
                        continue
                    patrimoine.append({
                        "date_snapshot":       today,
                        "entite":              ENTITE,
                        "poste":               f"IBKR_{isin or symbol}",
                        "classe_actif":        "actif_financier_cto",
                        "valeur_eur":          value_eur,
                        "devise_origine":      currency,
                        "quantite":            qty,
                        "prix_unitaire":       prix,
                        "source_valorisation": "ibkr_flex_ibflex",
                        "isin":                isin,
                        "description":         desc,
                        "pv_latente_eur":      to_eur(pnl_lc, currency),
                        "cout_base_eur":       to_eur(cost_lc, currency),
                        "commentaire":         "",
                    })
                except Exception:
                    continue

        if hasattr(account_stmt, "CashBalances"):
            for cb in account_stmt.CashBalances:
                try:
                    currency = str(cb.currency or "")
                    if currency in ("BASE_SUMMARY", "BASE", ""):
                        continue
                    ending_cash = float(getattr(cb, "endingCash",
                                        getattr(cb, "endingSettledCash", 0)) or 0)
                    if ending_cash == 0:
                        continue
                    value_eur = to_eur(ending_cash, currency)
                    patrimoine.append({
                        "date_snapshot":       today,
                        "entite":              ENTITE,
                        "poste":               f"IBKR_CASH_{currency}",
                        "classe_actif":        "liquidite",
                        "valeur_eur":          value_eur,
                        "devise_origine":      currency,
                        "quantite":            ending_cash,
                        "prix_unitaire":       to_eur(1, currency),
                        "source_valorisation": "ibkr_flex_ibflex",
                        "isin":                "",
                        "description":         f"IBKR Cash {currency}",
                        "pv_latente_eur":      0,
                        "cout_base_eur":       value_eur,
                        "commentaire":         "",
                    })
                except Exception:
                    continue

        if hasattr(account_stmt, "Trades"):
            for trade in account_stmt.Trades:
                try:
                    currency = str(trade.currency or "EUR")
                    proceeds = float(trade.proceeds or 0)
                    date     = str(trade.tradeDate)[:10]
                    symbol   = str(trade.symbol or "")
                    bs       = str(getattr(trade, "buySell", ""))
                    libelle  = f"IBKR TRADE {bs} {symbol}"
                    transactions.append(
                        _build_tx(make_id(COMPTE_ID, date, libelle, proceeds),
                                  date, "ibkr", libelle, proceeds, currency,
                                  to_eur(proceeds, currency), "epargne", "")
                    )
                except Exception:
                    continue

        if hasattr(account_stmt, "CashTransactions"):
            for ct in account_stmt.CashTransactions:
                try:
                    currency = str(ct.currency or "EUR")
                    amount   = float(ct.amount or 0)
                    date     = str(ct.dateTime)[:10]
                    tx_type  = str(getattr(ct, "type", "") or "")
                    desc     = str(ct.description or tx_type)[:100]
                    libelle  = f"IBKR {tx_type} {desc}"[:200]
                    nature   = ("revenu" if "DIVIDEND" in tx_type.upper() or
                                "INTEREST" in tx_type.upper() else "epargne")
                    transactions.append(
                        _build_tx(make_id(COMPTE_ID, date, libelle, amount),
                                  date, "ibkr", libelle, amount, currency,
                                  to_eur(amount, currency), nature, "")
                    )
                except Exception:
                    continue


# ─── BUILDER ─────────────────────────────────────────────────────

def _build_tx(tx_id, date, source, libelle_brut, montant, devise,
              montant_eur, nature, commentaire):
    return {
        "id":             tx_id,
        "date":           date,
        "date_valeur":    date,
        "source":         source,
        "entite":         ENTITE,
        "compte_id":      COMPTE_ID,
        "libelle_brut":   libelle_brut,
        "libelle_clean":  normalize_libelle(libelle_brut),
        "montant":        montant,
        "devise":         devise,
        "montant_eur":    montant_eur,
        "nature":         nature,
        "categorie":      "",
        "sous_categorie": "",
        "deductible_ir":  "non",
        "contre_partie":  "",
        "statut":         "brut",
        "flag_doublon":   "",
        "commentaire":    commentaire,
    }
