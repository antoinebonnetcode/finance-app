"""
Fortuneo PEA — récupération automatique via navigateur Chrome (Selenium).

Flux :
  1. Connexion à mabanque.fortuneo.fr
  2. Saisie du code confidentiel sur le clavier virtuel
  3. Navigation vers le portefeuille PEA
  4. Extraction du tableau des positions
  5. Retour au format patrimoine standard

Variables d'environnement requises :
  FORTUNEO_LOGIN     — identifiant (numéro client ou e-mail)
  FORTUNEO_PASSWORD  — code confidentiel (chiffres uniquement)
"""

import re
import time
from datetime import datetime

from utils import clean_amount, to_eur, parse_date_fr
from config import FORTUNEO_LOGIN, FORTUNEO_PASSWORD
from parse_fortuneo_metrobank import _pea_build, PEA_ENTITE, _strip_acc

FORTUNEO_BASE_URL = "https://mabanque.fortuneo.fr"
FORTUNEO_LOGIN_URL = f"{FORTUNEO_BASE_URL}/fr/identification.jsp"


# ─── ENTRYPOINT ──────────────────────────────────────────────────

def fetch_fortuneo_pea_browser():
    """
    Lance Chrome headless, se connecte à Fortuneo et extrait le portefeuille PEA.
    Retourne un dict compatible avec le pipeline (transactions=[], patrimoine=[...]).
    Retourne None en cas d'echec.
    """
    if not FORTUNEO_LOGIN or not FORTUNEO_PASSWORD:
        print("    [Fortuneo PEA] FORTUNEO_LOGIN ou FORTUNEO_PASSWORD non configure — skip")
        return None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
    except ImportError:
        print("    [Fortuneo PEA] selenium non installe (pip install selenium)")
        return None

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=fr-FR")
    # Reduce bot-detection fingerprint
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # webdriver-manager auto-downloads the matching ChromeDriver
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
        )
    except Exception:
        # Fallback: assume chromedriver is already in PATH
        driver = webdriver.Chrome(options=options)

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    try:
        snapshots = _run_session(driver)
        if snapshots is None:
            return None
        print(f"    [Fortuneo PEA] {len(snapshots)} position(s) extraite(s)")
        return {
            "transactions": [],
            "patrimoine":   snapshots,
            "file_id":      "FORTUNEO_PEA_BROWSER",
            "file_name":    "fortuneo_pea_browser",
            "source":       "fortuneo_pea",
        }
    except Exception as e:
        print(f"    [Fortuneo PEA] Erreur session : {e}")
        _save_screenshot(driver, "fortuneo_error_session")
        return None
    finally:
        driver.quit()


# ─── SESSION ─────────────────────────────────────────────────────

def _run_session(driver):
    from selenium.webdriver.support.ui import WebDriverWait
    wait = WebDriverWait(driver, 25)

    print("    [Fortuneo PEA] Ouverture page de connexion...")
    driver.get(FORTUNEO_LOGIN_URL)
    time.sleep(2)

    if not _login(driver, wait):
        return None

    print("    [Fortuneo PEA] Connexion reussie — navigation PEA...")
    time.sleep(3)

    return _navigate_and_extract_pea(driver, wait)


# ─── LOGIN ───────────────────────────────────────────────────────

def _login(driver, wait):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    # ── 1. Saisie identifiant ──────────────────────────────────
    login_field = _find_element(driver, [
        "input#numClient",
        "input[name='login']",
        "input[id*='login']",
        "input[id*='identifiant']",
        "input[id*='client']",
        "input[type='text']",
    ])
    if not login_field:
        print("    [Fortuneo PEA] Champ identifiant non trouve")
        _save_screenshot(driver, "fortuneo_error_login_field")
        return False

    login_field.clear()
    login_field.send_keys(str(FORTUNEO_LOGIN))
    print(f"    [Fortuneo PEA] Identifiant saisi")

    # ── 2. Validation intermediaire (si bouton "Suivant" existe) ──
    _try_click(driver, [
        "button[id*='next']",
        "button[id*='suivant']",
        "input[type='submit'][value*='Suivant']",
    ])
    time.sleep(1)

    # ── 3. Clavier virtuel : code confidentiel ─────────────────
    print("    [Fortuneo PEA] Recherche du clavier virtuel...")
    keyboard = _find_virtual_keyboard(driver)

    if keyboard:
        ok = _click_digits(driver, keyboard, str(FORTUNEO_PASSWORD))
        if not ok:
            print("    [Fortuneo PEA] Echec saisie code sur clavier virtuel")
            _save_screenshot(driver, "fortuneo_error_keyboard")
            return False
    else:
        print("    [Fortuneo PEA] Clavier virtuel absent — tentative saisie directe")
        pw_field = _find_element(driver, [
            "input[type='password']",
            "input[name='password']",
            "input[id*='password']",
            "input[id*='code']",
            "input[id*='motdepasse']",
        ])
        if pw_field:
            pw_field.send_keys(str(FORTUNEO_PASSWORD))
        else:
            print("    [Fortuneo PEA] Aucun champ de saisie du mot de passe trouve")
            _save_screenshot(driver, "fortuneo_error_nopassword")
            return False

    # ── 4. Soumission ──────────────────────────────────────────
    _try_click(driver, [
        "button[type='submit']",
        "input[type='submit']",
        "button[id*='submit']",
        "button[id*='connexion']",
        "button[id*='valider']",
        ".btn-connexion",
        "button.submit",
        "form button",
    ])
    time.sleep(4)

    # ── 5. Vérification connexion ──────────────────────────────
    if FORTUNEO_LOGIN_URL in driver.current_url or "identification" in driver.current_url:
        print("    [Fortuneo PEA] Connexion echouee (toujours sur page login)")
        _save_screenshot(driver, "fortuneo_error_login_failed")
        return False

    return True


def _find_virtual_keyboard(driver):
    """Retourne l'element clavier virtuel ou None."""
    from selenium.webdriver.common.by import By

    selectors = [
        "#pave-clavier",
        "#clavier_virtuel",
        ".clavier-num",
        ".clavier-virtuel",
        "[class*='clavier']",
        "[id*='clavier']",
        "[class*='keypad']",
        "[id*='keypad']",
        "[class*='keyboard']",
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            # Verify it contains clickable digit elements
            children = el.find_elements(By.XPATH, ".//*[self::button or self::a or self::li or self::span or self::td]")
            if len(children) >= 10:
                return el
        except Exception:
            continue
    return None


def _click_digits(driver, keyboard, code):
    """Clique sur chaque chiffre du code dans le clavier virtuel."""
    from selenium.webdriver.common.by import By

    for digit in code:
        clicked = False

        # Strategy 1 : data attributes
        for attr in (f"data-value='{digit}'", f"data-touche='{digit}'",
                     f"data-key='{digit}'", f"data-code='{digit}'"):
            try:
                btn = keyboard.find_element(By.CSS_SELECTOR, f"[{attr}]")
                btn.click()
                clicked = True
                break
            except Exception:
                continue

        if not clicked:
            # Strategy 2 : text content via XPath
            try:
                btn = keyboard.find_element(
                    By.XPATH,
                    f".//*[self::button or self::a or self::li or self::span or self::td"
                    f"][normalize-space(text())='{digit}']"
                )
                btn.click()
                clicked = True
            except Exception:
                pass

        if not clicked:
            # Strategy 3 : iterate all children and match stripped text
            children = keyboard.find_elements(
                By.XPATH,
                ".//*[self::button or self::a or self::li or self::span or self::td]"
            )
            for child in children:
                try:
                    if child.text.strip() == digit:
                        child.click()
                        clicked = True
                        break
                except Exception:
                    continue

        if not clicked:
            print(f"    [WARN] Chiffre '{digit}' introuvable sur le clavier virtuel")
            return False

        time.sleep(0.15)

    return True


# ─── NAVIGATION & EXTRACTION PEA ────────────────────────────────

def _navigate_and_extract_pea(driver, wait):
    from selenium.webdriver.common.by import By

    # Try to find & click PEA account link
    pea_link = _find_pea_link(driver)
    if pea_link:
        try:
            pea_link.click()
            time.sleep(3)
        except Exception:
            pass
    else:
        # Fallback direct URLs
        for path in [
            "/fr/private/mes-comptes/portefeuille-titres.jsp",
            "/fr/private/mes-comptes/synthese-mes-comptes.jsp",
            "/fr/private/tableau-de-bord.jsp",
        ]:
            try:
                driver.get(FORTUNEO_BASE_URL + path)
                time.sleep(3)
                if _has_portfolio_table(driver):
                    break
            except Exception:
                continue

    snapshots = _extract_portfolio(driver)

    # If still empty, try clicking PEA in account list on current page
    if not snapshots:
        _save_screenshot(driver, "fortuneo_debug_pea_page")
        pea_link = _find_pea_link(driver)
        if pea_link:
            try:
                pea_link.click()
                time.sleep(3)
                snapshots = _extract_portfolio(driver)
            except Exception:
                pass

    return snapshots


def _find_pea_link(driver):
    from selenium.webdriver.common.by import By

    # XPath: links whose text or href contains PEA
    for xpath in [
        "//a[contains(translate(text(),'pea','PEA'),'PEA')]",
        "//a[contains(@href,'pea') or contains(@href,'PEA')]",
        "//a[contains(@title,'PEA')]",
        "//*[contains(@class,'pea') or contains(@id,'pea')]//a",
    ]:
        try:
            return driver.find_element(By.XPATH, xpath)
        except Exception:
            continue
    return None


def _has_portfolio_table(driver):
    """Retourne True si la page contient un tableau de portefeuille."""
    src = driver.page_source.lower()
    return "valorisation" in src or "portefeuille" in src or "isin" in src


# ─── EXTRACTION TABLEAU ──────────────────────────────────────────

def _extract_portfolio(driver):
    """Extrait les positions du portefeuille PEA depuis la page courante."""
    from selenium.webdriver.common.by import By

    today = datetime.today().strftime("%Y-%m-%d")
    snapshots = []

    # Try to detect snapshot date from page
    date_snapshot = _detect_page_date(driver) or today

    tables = driver.find_elements(By.TAG_NAME, "table")
    for table in tables:
        rows = table.find_elements(By.TAG_NAME, "tr")
        if len(rows) < 2:
            continue

        headers, header_idx = _detect_portfolio_headers(rows)
        if not headers:
            continue

        for row in rows[header_idx + 1:]:
            cells = row.find_elements(By.XPATH, "td | th")
            texts = [c.text.strip() for c in cells]
            snap = _map_row_to_snap(texts, headers, date_snapshot)
            if snap:
                snapshots.append(snap)

    if not snapshots:
        snapshots = _extract_from_html_source(driver.page_source, date_snapshot)

    return snapshots


def _detect_portfolio_headers(rows):
    """Trouve la ligne d'en-tete du tableau portefeuille.
    Retourne (headers_list_normalized, index) ou (None, -1)."""
    for i, row in enumerate(rows[:6]):
        cells = row.find_elements("xpath", "td | th")
        texts = [_strip_acc(c.text or "") for c in cells]
        joined = " ".join(texts)
        if ("valorisation" in joined or "valeur" in joined) and \
           ("libelle" in joined or "isin" in joined or "titre" in joined):
            return texts, i
    return None, -1


def _map_row_to_snap(cells, headers, date_snapshot):
    """Mappe une ligne de tableau (liste de textes) en snapshot patrimoine."""
    try:
        def get(*keys):
            for k in keys:
                for i, h in enumerate(headers):
                    if k in h and i < len(cells) and cells[i]:
                        return cells[i]
            return ""

        libelle      = get("libelle", "libelle", "titre", "valeur", "designation", "nom")
        isin_raw     = get("isin", "code")
        valorisation = clean_amount(get("valorisation", "valeur totale", "montant") or 0)
        cours        = clean_amount(get("cours", "dernier cours", "prix") or 0)
        quantite     = clean_amount(get("quantite", "qte", "nombre") or 0)
        pv           = clean_amount(get("+/-", "plus", "perte", "gain", "p/l") or 0)

        isin = re.sub(r"[^A-Z0-9]", "", str(isin_raw).upper())
        if len(isin) != 12:
            isin = ""

        if not libelle or valorisation == 0:
            return None

        return _pea_build(date_snapshot, libelle.strip(), valorisation,
                          isin, cours, quantite, pv)
    except Exception:
        return None


def _detect_page_date(driver):
    """Cherche une date dans le contenu de la page (ex: 'Portefeuille au 03/03/2026')."""
    text = driver.find_element("tag name", "body").text if driver else ""
    m = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", text or "")
    if m:
        return parse_date_fr(m.group(1))
    return None


def _extract_from_html_source(html, date_snapshot):
    """Fallback regex : extrait les positions en cherchant les codes ISIN dans le HTML."""
    snapshots = []
    isin_re = re.compile(r"\b([A-Z]{2}[A-Z0-9]{9}[0-9])\b")
    amount_re = re.compile(r"[\d\s]+[,\.]\d{2}")

    clean_html = re.sub(r"<[^>]+>", " ", html)
    clean_html = re.sub(r"\s+", " ", clean_html)

    for m in isin_re.finditer(clean_html):
        isin = m.group(1)
        ctx = clean_html[max(0, m.start() - 300): m.end() + 300]
        amounts = [clean_amount(a) for a in amount_re.findall(ctx) if clean_amount(a) > 0]
        if not amounts:
            continue
        # Largest amount is most likely the valorisation
        valorisation = max(amounts)
        if valorisation < 1:
            continue
        snapshots.append(_pea_build(date_snapshot, f"Position {isin}",
                                    valorisation, isin, 0, 0, 0))

    return snapshots


# ─── HELPERS ─────────────────────────────────────────────────────

def _find_element(driver, selectors):
    """Retourne le premier element trouve parmi les selecteurs CSS."""
    from selenium.webdriver.common.by import By
    for sel in selectors:
        try:
            return driver.find_element(By.CSS_SELECTOR, sel)
        except Exception:
            continue
    return None


def _try_click(driver, selectors):
    """Clique sur le premier element trouve (silencieux si non trouve)."""
    el = _find_element(driver, selectors)
    if el:
        try:
            el.click()
        except Exception:
            pass


def _save_screenshot(driver, name):
    try:
        path = f"/tmp/{name}_{datetime.now().strftime('%H%M%S')}.png"
        driver.save_screenshot(path)
        print(f"    [DEBUG] Screenshot : {path}")
    except Exception:
        pass
