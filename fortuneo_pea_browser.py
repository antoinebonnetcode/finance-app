"""
Fortuneo PEA — récupération automatique via navigateur Chrome (Selenium).

Flux :
  1. Connexion à mabanque.fortuneo.fr (clavier virtuel PIN)
  2. Clic sur "PEA" dans le menu gauche
  3. Clic sur "Portefeuille et carnet d'ordres"
  4. Clic sur "Export Excel®" (bouton bas-droite du tableau)
  5. Lecture du fichier XLS téléchargé et parsing via parse_fortuneo_pea

Variables d'environnement requises :
  FORTUNEO_LOGIN     — numéro client (visible en haut à droite quand connecté)
  FORTUNEO_PASSWORD  — code confidentiel (chiffres uniquement, ex: "123456")
"""

import glob
import os
import re
import time
from datetime import datetime

from config import FORTUNEO_LOGIN, FORTUNEO_PASSWORD
from parse_fortuneo_metrobank import parse_fortuneo_pea

FORTUNEO_BASE  = "https://mabanque.fortuneo.fr"
FORTUNEO_LOGIN_URL = f"{FORTUNEO_BASE}/fr/identification.jsp"

DL_DIR = "/tmp/fortuneo_dl"


# ─── ENTRYPOINT ──────────────────────────────────────────────────

def fetch_fortuneo_pea_browser():
    """
    Lance Chrome headless, se connecte à Fortuneo, exporte le portefeuille PEA
    en Excel, et retourne les données au format pipeline.
    """
    if not FORTUNEO_LOGIN or not FORTUNEO_PASSWORD:
        print("    [Fortuneo PEA] FORTUNEO_LOGIN / FORTUNEO_PASSWORD non configurés — skip")
        return None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
    except ImportError:
        print("    [Fortuneo PEA] selenium non installé (pip install selenium)")
        return None

    os.makedirs(DL_DIR, exist_ok=True)

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=fr-FR")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # Configure download directory (no dialog, auto-save)
    options.add_experimental_option("prefs", {
        "download.default_directory":        DL_DIR,
        "download.prompt_for_download":      False,
        "download.directory_upgrade":        True,
        "safebrowsing.enabled":              True,
        "plugins.always_open_pdf_externally": True,
    })

    try:
        from webdriver_manager.chrome import ChromeDriverManager
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
        )
    except Exception:
        driver = webdriver.Chrome(options=options)

    # Remove webdriver flag
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    try:
        result = _run(driver)
        return result
    except Exception as e:
        print(f"    [Fortuneo PEA] Erreur : {e}")
        _screenshot(driver, "fortuneo_error_session")
        return None
    finally:
        driver.quit()


# ─── SESSION PRINCIPALE ───────────────────────────────────────────

def _run(driver):
    from selenium.webdriver.support.ui import WebDriverWait
    wait = WebDriverWait(driver, 25)

    # Purge tout fichier xls précédent dans le dossier de téléchargement
    for f in glob.glob(os.path.join(DL_DIR, "*.xls*")):
        os.remove(f)

    # 1. Connexion
    print("    [Fortuneo PEA] Connexion en cours...")
    driver.get(FORTUNEO_LOGIN_URL)
    time.sleep(2)

    if not _login(driver, wait):
        return None

    print("    [Fortuneo PEA] Connecté — navigation PEA...")
    time.sleep(3)

    # 2. Navigation vers le portefeuille PEA
    if not _navigate_to_pea_portfolio(driver, wait):
        return None

    # 3. Export Excel
    print("    [Fortuneo PEA] Téléchargement Export Excel...")
    if not _click_export_excel(driver, wait):
        return None

    # 4. Attendre le fichier téléchargé
    xls_path = _wait_for_download(DL_DIR, timeout=30)
    if not xls_path:
        print("    [Fortuneo PEA] Fichier XLS non reçu dans les délais")
        _screenshot(driver, "fortuneo_error_download")
        return None

    print(f"    [Fortuneo PEA] Fichier reçu : {os.path.basename(xls_path)}")

    # 5. Parser avec le parseur existant
    with open(xls_path, "rb") as fh:
        file_bytes = fh.read()

    result = parse_fortuneo_pea(
        file_bytes=file_bytes,
        file_id="FORTUNEO_PEA_BROWSER",
        file_name=os.path.basename(xls_path),
    )
    # Surcharge file_id/source pour le dédup pipeline
    if result:
        result["file_id"]   = "FORTUNEO_PEA_BROWSER"
        result["file_name"] = "fortuneo_pea_browser"
        result["source"]    = "fortuneo_pea"

    return result


# ─── LOGIN ───────────────────────────────────────────────────────

def _login(driver, wait):
    from selenium.webdriver.common.by import By

    # ── Identifiant ───────────────────────────────────────────────
    login_field = _find(driver, [
        "input#numClient",
        "input[name='login']",
        "input[name='j_username']",
        "input[id*='identifiant']",
        "input[id*='login']",
        "input[id*='client']",
        "input[type='text']",
    ])
    if not login_field:
        print("    [Fortuneo PEA] Champ identifiant introuvable")
        _screenshot(driver, "fortuneo_error_no_login_field")
        return False

    login_field.clear()
    login_field.send_keys(str(FORTUNEO_LOGIN))
    print("    [Fortuneo PEA] Identifiant saisi")

    # Bouton "Suivant" intermédiaire (certaines versions du site)
    _click(driver, [
        "button[id*='next']", "button[id*='suivant']",
        "input[type='submit'][value*='Suivant']",
        "input[type='submit'][value*='Continuer']",
    ])
    time.sleep(1)

    # ── Clavier virtuel ────────────────────────────────────────────
    print("    [Fortuneo PEA] Recherche clavier virtuel...")
    keyboard = _find_keyboard(driver)

    if keyboard:
        ok = _type_on_keyboard(driver, keyboard, str(FORTUNEO_PASSWORD))
        if not ok:
            _screenshot(driver, "fortuneo_error_keyboard")
            return False
        print("    [Fortuneo PEA] Code confidentiel saisi sur clavier virtuel")
    else:
        # Fallback saisie directe (peu probable sur Fortuneo)
        print("    [Fortuneo PEA] Clavier virtuel absent — saisie directe")
        pw = _find(driver, [
            "input[type='password']", "input[name='password']",
            "input[id*='password']", "input[id*='motdepasse']",
        ])
        if pw:
            pw.send_keys(str(FORTUNEO_PASSWORD))
        else:
            print("    [Fortuneo PEA] Aucun champ mot de passe trouvé")
            _screenshot(driver, "fortuneo_error_no_password")
            return False

    # ── Soumission ────────────────────────────────────────────────
    _click(driver, [
        "button[type='submit']",
        "input[type='submit']",
        "button[id*='submit']",
        "button[id*='valider']",
        "button[id*='connexion']",
        ".btn-connexion",
        "form button",
    ])
    time.sleep(4)

    # Vérification
    if "identification" in driver.current_url or FORTUNEO_LOGIN_URL in driver.current_url:
        print("    [Fortuneo PEA] Connexion échouée (toujours sur page login)")
        _screenshot(driver, "fortuneo_error_login_failed")
        return False

    return True


def _find_keyboard(driver):
    """Retourne l'élément clavier virtuel, ou None."""
    from selenium.webdriver.common.by import By
    selectors = [
        "#pave-clavier", "#clavier_virtuel", ".clavier-num",
        ".clavier-virtuel", "[class*='clavier']", "[id*='clavier']",
        "[class*='keypad']", "[id*='keypad']",
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            children = el.find_elements(
                By.XPATH,
                ".//*[self::button or self::a or self::li or self::span or self::td]"
            )
            if len(children) >= 9:
                return el
        except Exception:
            continue
    return None


def _type_on_keyboard(driver, keyboard, code):
    """Clique sur chaque chiffre du code PIN dans le clavier virtuel."""
    from selenium.webdriver.common.by import By

    for digit in code:
        clicked = False

        # Stratégie 1 : attribut data-*
        for attr in (f"data-value='{digit}'", f"data-touche='{digit}'",
                     f"data-key='{digit}'", f"data-code='{digit}'"):
            try:
                btn = keyboard.find_element(By.CSS_SELECTOR, f"[{attr}]")
                btn.click()
                clicked = True
                break
            except Exception:
                continue

        # Stratégie 2 : texte visible via XPath
        if not clicked:
            try:
                btn = keyboard.find_element(
                    By.XPATH,
                    f".//*[self::button or self::a or self::li or self::span or self::td"
                    f"][normalize-space(.)='{digit}']"
                )
                btn.click()
                clicked = True
            except Exception:
                pass

        # Stratégie 3 : itération sur tous les enfants
        if not clicked:
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
            print(f"    [WARN] Chiffre '{digit}' introuvable sur le clavier")
            return False
        time.sleep(0.15)

    return True


# ─── NAVIGATION VERS LE PORTEFEUILLE PEA ─────────────────────────

def _navigate_to_pea_portfolio(driver, wait):
    """
    Clique sur 'PEA' dans le menu gauche puis sur
    'Portefeuille et carnet d'ordres'.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    # ── Clic sur "PEA" dans la sidebar ────────────────────────────
    pea_clicked = False
    # XPath : lien dont le texte contient "PEA" (et éventuellement un numéro)
    for xpath in [
        "//ul[contains(@class,'compte') or contains(@class,'menu') or contains(@class,'nav')]//a[contains(normalize-space(.),'PEA')]",
        "//a[contains(normalize-space(.),'PEA') and not(contains(normalize-space(.),'Résumé'))]",
        "//*[@id='comptes-menu']//a[contains(.,'PEA')]",
        "//div[contains(@class,'sidebar') or contains(@class,'lateral')]//a[contains(.,'PEA')]",
        "//a[contains(.,'PEA')]",
    ]:
        try:
            el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            el.click()
            pea_clicked = True
            print("    [Fortuneo PEA] Clic 'PEA' réussi")
            time.sleep(2)
            break
        except Exception:
            continue

    if not pea_clicked:
        print("    [Fortuneo PEA] Lien 'PEA' introuvable dans le menu")
        _screenshot(driver, "fortuneo_error_no_pea_link")
        return False

    # ── Clic sur "Portefeuille et carnet d'ordres" ────────────────
    portfolio_clicked = False
    for xpath in [
        "//a[contains(normalize-space(.),'Portefeuille et carnet')]",
        "//a[contains(normalize-space(.),'Portefeuille')]",
        "//a[contains(@href,'portefeuille')]",
        "//a[contains(@href,'carnet')]",
    ]:
        try:
            el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            el.click()
            portfolio_clicked = True
            print("    [Fortuneo PEA] Clic 'Portefeuille et carnet d'ordres' réussi")
            time.sleep(3)
            break
        except Exception:
            continue

    if not portfolio_clicked:
        # Le portefeuille est peut-être déjà affiché après clic PEA
        if "portefeuille" in driver.current_url.lower() or \
           "Export Excel" in driver.page_source:
            print("    [Fortuneo PEA] Portefeuille déjà visible")
        else:
            # Tentative via URL directe
            try:
                driver.get(f"{FORTUNEO_BASE}/fr/prive/default.jsp?ANav=1")
                time.sleep(3)
            except Exception:
                pass

    return True


# ─── EXPORT EXCEL ────────────────────────────────────────────────

def _click_export_excel(driver, wait):
    """Clique sur le bouton 'Export Excel®' dans la page portefeuille."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    for xpath in [
        "//a[contains(normalize-space(.),'Export Excel')]",
        "//button[contains(normalize-space(.),'Export Excel')]",
        "//a[contains(@class,'export') and contains(translate(.,'excel','EXCEL'),'EXCEL')]",
        "//a[contains(@href,'excel') or contains(@href,'xls')]",
        "//*[contains(@class,'excel')]",
    ]:
        try:
            el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            el.click()
            print("    [Fortuneo PEA] Clic 'Export Excel' réussi")
            return True
        except Exception:
            continue

    # Fallback : cherche par texte partiel dans tous les liens
    try:
        links = driver.find_elements(By.TAG_NAME, "a")
        for link in links:
            if "excel" in (link.text or "").lower() or \
               "export" in (link.text or "").lower():
                link.click()
                print("    [Fortuneo PEA] Export Excel cliqué (fallback liens)")
                return True
    except Exception:
        pass

    print("    [Fortuneo PEA] Bouton 'Export Excel' introuvable")
    _screenshot(driver, "fortuneo_error_no_export_button")
    return False


# ─── ATTENTE TÉLÉCHARGEMENT ──────────────────────────────────────

def _wait_for_download(directory, timeout=30):
    """Attend qu'un fichier XLS/XLSX apparaisse dans directory. Retourne son chemin."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = (
            glob.glob(os.path.join(directory, "*.xls"))
            + glob.glob(os.path.join(directory, "*.xlsx"))
        )
        # Exclude Chrome temp files (.crdownload)
        files = [f for f in files if not f.endswith(".crdownload")]
        if files:
            # Wait a tiny bit to ensure the file is fully written
            time.sleep(1)
            return max(files, key=os.path.getmtime)
        time.sleep(1)
    return None


# ─── HELPERS ─────────────────────────────────────────────────────

def _find(driver, selectors):
    from selenium.webdriver.common.by import By
    for sel in selectors:
        try:
            return driver.find_element(By.CSS_SELECTOR, sel)
        except Exception:
            continue
    return None


def _click(driver, selectors):
    el = _find(driver, selectors)
    if el:
        try:
            el.click()
        except Exception:
            pass


def _screenshot(driver, name):
    try:
        path = f"/tmp/{name}_{datetime.now().strftime('%H%M%S')}.png"
        driver.save_screenshot(path)
        print(f"    [DEBUG] Screenshot : {path}")
    except Exception:
        pass
