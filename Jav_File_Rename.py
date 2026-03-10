import os
import re
import time
import random
import threading
import requests
from bs4 import BeautifulSoup, NavigableString
import tkinter as tk
from tkinter import filedialog, ttk, font as tkfont
from urllib.parse import urljoin, quote_plus
from concurrent.futures import ThreadPoolExecutor

# ─────────────────────────────────────────────────────────────────────────────
#  JAV FILE RENAMER
#  Automatically renames JAV video files using metadata from online databases.
#
#  ⚠  COMPATIBILITY NOTICE:
#  This program is designed for MANUFACTURER-CENSORED JAVs only.
#  These are titles identified by a studio 番号 (catalogue number), e.g.:
#    MIGD-123, DASD-456, PIYO-114, HNDS-011, DOKS-612, etc.
#
#  It will NOT work for manufacturer-uncensored titles from studios such as:
#    i-pondo, Heyzo, Caribbeancom, 1Pondo, etc.
#  Uncensored titles use non-standard ID formats not supported by the scrapers.
# ─────────────────────────────────────────────────────────────────────────────

# undetected_chromedriver: launches real Chrome to bypass Cloudflare Bot Management.
# Requires Google Chrome to be installed. Gracefully disabled if not available.
UC_AVAILABLE = False
UC_IMPORT_ERROR = ""
try:
    # Python 3.12+ removed distutils; shim it from setuptools before uc imports it
    import sys
    if 'distutils' not in sys.modules:
        try:
            import setuptools._distutils as _dt
            sys.modules['distutils'] = _dt
            # also patch sub-modules uc commonly needs
            import setuptools._distutils.version as _dtv
            sys.modules['distutils.version'] = _dtv
        except Exception:
            pass

    import undetected_chromedriver as uc
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    UC_AVAILABLE = True
except Exception as _uc_err:
    UC_IMPORT_ERROR = str(_uc_err)

# ─────────────────────────────────────────────
#  USER CONFIG  ← edit this section to customise the app
# ─────────────────────────────────────────────

# Path where Master_Log.csv will be stored.
# Change this to any folder you prefer — the folder will be created automatically.
LOG_DIR = r"C:\Users\Administrator\Desktop\Python Projects\Renaming log"

# ─────────────────────────────────────────────
# --- STEALTH & THROTTLING CONFIG ---
MIN_REQUEST_DELAY = 1.5
MAX_REQUEST_DELAY = 3.5
FILE_COOLDOWN_MIN = 1.0
FILE_COOLDOWN_MAX = 2.5
MAX_WORKERS = 1
MAX_RETRIES = 3

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0'
]

# ─────────────────────────────────────────────
#  SCRAPER  (unchanged)
# ─────────────────────────────────────────────
class SkipDuplicateRenamer:
    def __init__(self):
        self.session    = requests.Session()
        self._uc_driver = None
        self._uc_lock   = threading.Lock()
        self._uc_failed = False
        self.cache      = {}
        self.log_fn     = None
        self.always_log_fn = None
        self._parts_seen = set()
        self._parts_lock = threading.Lock()
        self.stop_event  = threading.Event()   # set by UI Stop button

    def _sleep(self, seconds):
        """Interruptible sleep — returns immediately if stop_event is set."""
        self.stop_event.wait(seconds)

    def _stopped(self):
        return self.stop_event.is_set()

    def reset_run_state(self):
        """Call before each rename run to clear per-run tracking state."""
        with self._parts_lock:
            self._parts_seen.clear()

    def _always_log(self, msg, tag="info"):
        """Log regardless of verbose mode — for critical status messages."""
        if self.always_log_fn:
            self.always_log_fn(f"  ↳ {msg}", tag)

    def close(self):
        """Cleanly shut down the Chrome driver if it was started."""
        if self._uc_driver:
            try:
                self._uc_driver.quit()
            except Exception:
                pass
            self._uc_driver = None

    def _get_uc_driver(self):
        """
        Return (or lazily create) a shared undetected Chrome driver.
        Chrome is launched once, visits the JAVLibrary homepage to get a
        cf_clearance cookie, then reused for all subsequent lookups.
        """
        with self._uc_lock:
            if self._uc_driver is not None:
                return self._uc_driver
            if self._uc_failed:
                return None
            if not UC_AVAILABLE:
                self._always_log(f"[JAVLIB] import failed: {UC_IMPORT_ERROR}  —  "
                                 "run: pip install undetected-chromedriver selenium", "error")
                self._uc_failed = True
                return None

            self._always_log("[JAVLIB] launching Chrome to bypass Cloudflare…", "info")
            opts = uc.ChromeOptions()
            opts.add_argument('--lang=zh-CN,zh;q=0.9,en;q=0.8')
            # Start at minimal size — just enough to show the Cloudflare checkbox
            opts.add_argument('--window-size=360,200')
            opts.add_argument('--window-position=30,30')
            opts.add_argument('--no-first-run')
            opts.add_argument('--no-default-browser-check')

            # ── Detect Chrome major version (most reliable: Windows registry) ─
            chrome_version = None

            # Method 1: Windows registry (works even if chrome.exe isn't on PATH)
            try:
                import winreg
                for reg_path in [
                    r"SOFTWARE\Google\Chrome\BLBeacon",
                    r"SOFTWARE\Wow6432Node\Google\Chrome\BLBeacon",
                ]:
                    for hive in [winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE]:
                        try:
                            key = winreg.OpenKey(hive, reg_path)
                            ver_str, _ = winreg.QueryValueEx(key, "version")
                            winreg.CloseKey(key)
                            chrome_version = int(ver_str.split('.')[0])
                            self._always_log(f"[JAVLIB] Chrome {ver_str} detected via registry → version_main={chrome_version}", "info")
                            break
                        except Exception:
                            continue
                    if chrome_version:
                        break
            except Exception:
                pass

            # Method 2: subprocess fallback (macOS / Linux / chrome on PATH)
            if not chrome_version:
                import subprocess, re as _re
                chrome_bins = [
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                    "google-chrome", "google-chrome-stable", "chromium",
                ]
                for cb in chrome_bins:
                    try:
                        out = subprocess.check_output(
                            [cb, "--version"],
                            stderr=subprocess.STDOUT,
                            timeout=5,
                            shell=False,
                        ).decode(errors="ignore")
                        m = _re.search(r'(\d+)\.', out)
                        if m:
                            chrome_version = int(m.group(1))
                            self._always_log(f"[JAVLIB] Chrome {chrome_version} detected via binary", "info")
                            break
                    except Exception:
                        continue

            if not chrome_version:
                self._always_log("[JAVLIB] ⚠ Could not detect Chrome version — driver mismatch may occur", "skip")

            try:
                kwargs = dict(options=opts, headless=False, use_subprocess=True)
                if chrome_version:
                    kwargs['version_main'] = chrome_version
                driver = uc.Chrome(**kwargs)
                # Enforce minimal size after launch (flag alone isn't always respected)
                driver.set_window_rect(x=30, y=30, width=360, height=200)
            except Exception as e:
                self._always_log(f"[JAVLIB] Chrome failed to start: {e.msg if hasattr(e,'msg') else str(e).splitlines()[0]}", "error")
                self._uc_failed = True
                return None

            # ── Homepage warm-up: let Cloudflare issue cf_clearance ──────
            self._always_log("[JAVLIB] Chrome launched — warming up on homepage…", "info")
            try:
                driver.get("https://www.javlibrary.com/cn/")
                # Zoom out so the CF checkbox widget fits in the tiny window
                try:
                    driver.execute_script("document.body.style.zoom='0.7'")
                except Exception:
                    pass
                # Re-apply tiny size in case page load resized the window
                try:
                    driver.set_window_rect(x=30, y=30, width=360, height=200)
                except Exception:
                    pass
                # Wait until the page has actual content (not a CF challenge)
                try:
                    WebDriverWait(driver, 20).until(
                        lambda d: 'just a moment' not in (d.title or '').lower()
                                  and len(d.page_source) > 5000
                    )
                except Exception:
                    pass
                # Set age-gate cookies now that the domain is loaded
                for name, value in [('over18', '18'), ('age_check', '1'),
                                     ('locale', 'zh'), ('adc', '1')]:
                    try:
                        driver.add_cookie({'name': name, 'value': value,
                                           'domain': 'www.javlibrary.com'})
                    except Exception:
                        pass
                self._sleep(random.uniform(1.5, 2.5))
                self._always_log("[JAVLIB] Chrome ready ✓", "info")
                self._uc_driver = driver
            except Exception as e:
                self._always_log(f"[JAVLIB] homepage warm-up failed: {e}", "error")
                try:
                    driver.quit()
                except Exception:
                    pass
                self._uc_failed = True

            return self._uc_driver

    def _uc_get_html(self, url, wait_css=None, timeout=20):
        """
        Navigate Chrome to url, wait for wait_css to appear, return (html, final_url).
        Also detects if a Cloudflare challenge page is shown and waits it out.
        """
        driver = self._get_uc_driver()
        if driver is None:
            return None, None
        try:
            driver.get(url)
            # Wait for CF challenge to clear if it appears
            try:
                WebDriverWait(driver, 12).until(
                    lambda d: 'just a moment' not in (d.title or '').lower()
                )
            except Exception:
                pass

            if wait_css:
                try:
                    WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                    )
                except Exception:
                    pass   # timeout — parse whatever loaded
            else:
                self._sleep(random.uniform(1.5, 2.5))

            self._vlog(f"[JAVLIB] page title: {driver.title!r}  url: {driver.current_url}", "info")
            return driver.page_source, driver.current_url
        except Exception as e:
            self._always_log(f"[JAVLIB] Chrome navigation error: {e}", "error")
            return None, None

    def _vlog(self, msg, tag="info"):
        """Emit a verbose diagnostic line if a log callback is registered."""
        if self.log_fn:
            self.log_fn(f"  ↳ {msg}", tag)

    def get_headers(self, referer=None):
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cookie': 'adc=1; locale=zh'
        }
        if referer:
            headers['Referer'] = referer
        return headers

    def fix_url(self, url):
        if not url: return None
        if url.startswith('//'): return 'https:' + url
        if not url.startswith('http'): return urljoin('https://avmoo.website', url)
        return url

    def safe_get(self, url, referer=None):
        url = self.fix_url(url)
        self._vlog(f"GET {url}", "info")
        for i in range(MAX_RETRIES):
            if self._stopped():
                return None
            try:
                self._sleep(random.uniform(MIN_REQUEST_DELAY, MAX_REQUEST_DELAY))
                res = self.session.get(url, headers=self.get_headers(referer), timeout=15)
                self._vlog(f"  → HTTP {res.status_code}  final_url={res.url}", "info")
                if res.status_code == 200: return res
                self._vlog(f"  → Non-200, retry {i+1}/{MAX_RETRIES}", "skip")
            except Exception as e:
                self._vlog(f"  → Request error (attempt {i+1}): {e}", "error")
                continue
        self._vlog(f"  → All {MAX_RETRIES} attempts failed for {url}", "error")
        return None

    def sanitize_filename(self, name, max_length=210):
        clean = re.sub(r'[\\/:*?"<>|]', ' ', name).strip()
        clean = re.sub(r'\s+', ' ', clean)
        return (clean[:max_length] + '..') if len(clean) > max_length else clean

    def parse_avmoo_movie_page(self, soup, query_id):
        raw_page_title = soup.title.string if soup.title else ""
        actors = [el.get_text(strip=True) for el in soup.select('#avatar-waterfall a.avatar-box span')]
        clean_title = raw_page_title
        clean_title = re.sub(re.escape(query_id), '', clean_title, flags=re.IGNORECASE)
        for actor in actors:
            clean_title = clean_title.replace(actor, '')
        clean_title = re.sub(r' - AVMOO$', '', clean_title).strip()
        self._vlog(f"  parsed title='{clean_title}'  actors={actors[:4]}", "info")
        return {"title": clean_title, "actors": actors[:4]}

    def fetch_from_avmoo(self, query_id):
        search_url = f"https://avmoo.website/cn/search/{query_id}"
        self._vlog(f"[AVMOO] searching '{query_id}'  url={search_url}", "info")
        res = self.safe_get(search_url)
        if not res:
            self._vlog("[AVMOO] no response — request failed entirely", "error")
            return None

        # Sometimes AVMOO redirects directly to the movie page
        if "/movie/" in res.url:
            self._vlog(f"[AVMOO] redirected straight to movie page: {res.url}", "info")
            return self.parse_avmoo_movie_page(BeautifulSoup(res.text, 'html.parser'), query_id)

        soup = BeautifulSoup(res.text, 'html.parser')

        # --- Primary strategy: mirror the exact XPath the user identified ---
        # XPath: html/body/div[2]/div/div/div/a
        # body > second div > div > div > div > a  (the search results wrapper)
        movie_url = None

        body_divs = soup.body.find_all('div', recursive=False) if soup.body else []
        container = body_divs[1] if len(body_divs) >= 2 else None   # div[2] = index 1

        if container:
            # Walk down: div > div > div > a  (collect ALL candidate anchors)
            candidates = container.select('div > div > div > a[href*="/movie/"]')
            self._vlog(f"[AVMOO] XPath strategy found {len(candidates)} movie anchor(s)", "info")

            # Prefer the anchor whose visible ID text exactly matches query_id
            norm_query = re.sub(r'[-\s]', '', query_id).upper()
            for a in candidates:
                href = a.get('href', '')
                link_text = re.sub(r'[-\s]', '', a.get_text()).upper()
                # Also check the video-title span inside the box if present
                title_el = a.select_one('.video-title, .title, strong, p')
                title_text = re.sub(r'[-\s]', '', title_el.get_text()).upper() if title_el else ''
                if norm_query in link_text or norm_query in title_text or norm_query in href.upper():
                    movie_url = self.fix_url(href)
                    self._vlog(f"[AVMOO] matched by ID text: {href}", "info")
                    break

            # Fallback: just take the first result in that container
            if not movie_url and candidates:
                movie_url = self.fix_url(candidates[0].get('href', ''))
                self._vlog(f"[AVMOO] no exact ID match — using first result: {movie_url}", "skip")
        else:
            self._vlog("[AVMOO] could not locate body > div[2] container", "skip")

        # --- Secondary strategy: any a[href*=/movie/] anywhere on the page ---
        if not movie_url:
            fallback = soup.select_one('a.movie-box, a[href*="/movie/"]')
            if fallback:
                movie_url = self.fix_url(fallback.get('href', ''))
                self._vlog(f"[AVMOO] fallback selector found: {movie_url}", "skip")

        if not movie_url:
            sample_links = [a.get('href', '') for a in soup.select('a[href]')[:12]]
            self._vlog(f"[AVMOO] no movie link found at all. Sample hrefs: {sample_links}", "error")
            return None

        self._vlog(f"[AVMOO] fetching detail page: {movie_url}", "info")
        res_detail = self.safe_get(movie_url, referer=search_url)
        if res_detail:
            return self.parse_avmoo_movie_page(BeautifulSoup(res_detail.text, 'html.parser'), query_id)
        else:
            self._vlog("[AVMOO] detail page request failed", "error")
            return None

    def fetch_from_javdb(self, query_id):
        search_url = f"https://javdb.com/search?q={query_id}&f=all"
        self._vlog(f"[JAVDB] searching '{query_id}'", "info")
        res = self.safe_get(search_url)
        if not res:
            self._vlog("[JAVDB] no response", "error")
            return None
        soup = BeautifulSoup(res.text, 'html.parser')
        item = soup.select_one('.movie-list .item')
        if not item:
            self._vlog("[JAVDB] no .movie-list .item found on search page", "skip")
            return None
        res_detail = self.safe_get(f"https://javdb.com{item.select_one('a')['href']}", referer=search_url)
        if not res_detail:
            self._vlog("[JAVDB] detail page request failed", "error")
            return None
        d_soup = BeautifulSoup(res_detail.text, 'html.parser')
        title = d_soup.select_one('h2.title').get_text(strip=True)
        if "顯示原標題" in title: title = title.split("顯示原標題")[-1].strip()
        title = re.sub(re.escape(query_id), '', title, flags=re.IGNORECASE).strip()
        actors = []
        for block in d_soup.find_all('div', class_='panel-block'):
            if '演員' in block.get_text():
                for a in block.find_all('a'):
                    name = a.get_text(strip=True)
                    if '♂' not in (a.next_sibling or ''): actors.append(name)
                break
        self._vlog(f"[JAVDB] title='{title}'  actors={actors[:4]}", "info")
        return {"title": title, "actors": actors[:4]}

    def _is_javlib_movie_page(self, url, soup):
        """
        Return True when the page we landed on IS a movie detail page.
        JAVLibrary redirects the ID-search to a randomised slug like
        /cn/javmefiqte.html  — there is no fixed pattern in the URL itself.
        Instead we detect a movie page by the presence of its key DOM elements.
        """
        return bool(soup.select_one('#video_title') or soup.select_one('#video_cast'))

    def fetch_from_javlibrary(self, query_id):
        """
        Scrape javlibrary.com/cn using a real Chrome browser (undetected_chromedriver)
        to bypass Cloudflare Bot Management.

        Flow:
          1. Chrome navigates to /cn/vl_searchbyid.php?keyword=<ID>
             → site redirects to a randomised slug e.g. /cn/javmefiqte.html
          2. If the landed page is a movie detail page → parse directly.
          3. If it's a multi-result listing → find matching entry, navigate to it.

        Title:  #video_title a
        Actors: #video_cast span.cast span.star a
        """
        if not UC_AVAILABLE:
            self._always_log(f"[JAVLIB] undetected_chromedriver import failed: {UC_IMPORT_ERROR}", "error")
            return None

        if self._uc_failed:
            self._always_log("[JAVLIB] Chrome previously failed to start — skipping", "error")
            return None

        JAVLIB_BASE = "https://www.javlibrary.com"
        search_url  = f"{JAVLIB_BASE}/cn/vl_searchbyid.php?keyword={quote_plus(query_id)}"
        self._always_log(f"[JAVLIB] searching '{query_id}'", "info")
        self._vlog(f"[JAVLIB] url={search_url}", "info")

        # ── Navigate to search URL ───────────────────────────────────────
        html, final_url = self._uc_get_html(
            search_url,
            wait_css='#video_title, #video_cast, div.videos div.video'
        )

        if not html:
            self._always_log("[JAVLIB] Chrome failed to load page", "error")
            return None

        self._vlog(f"[JAVLIB] landed on: {final_url}", "info")
        soup = BeautifulSoup(html, 'html.parser')

        # ── Cloudflare still blocking (extremely unlikely with real Chrome) ──
        if soup.title and 'just a moment' in (soup.title.string or '').lower():
            self._vlog("[JAVLIB] Cloudflare challenge still showing — try again later", "error")
            return None

        # ── Case 1: already on movie detail page ─────────────────────────
        detail_soup = None
        if self._is_javlib_movie_page(final_url, soup):
            self._vlog(f"[JAVLIB] redirect → movie page: {final_url}", "info")
            detail_soup = soup

        # ── Case 2: multi-result search listing ─────────────────────────
        else:
            norm_query = re.sub(r'[-\s]', '', query_id).upper()
            candidates = soup.select('div.videos div.video a[href]')
            self._vlog(f"[JAVLIB] search listing — {len(candidates)} result(s)", "info")

            movie_href = None
            for a in candidates:
                id_el   = a.select_one('div.id')
                id_text = re.sub(r'[-\s]', '', id_el.get_text()).upper() if id_el else ''
                if norm_query == id_text or norm_query in id_text:
                    movie_href = a['href']
                    self._vlog(f"[JAVLIB] exact ID match: {movie_href}", "info")
                    break
            if not movie_href and candidates:
                movie_href = candidates[0]['href']
                self._vlog(f"[JAVLIB] first result fallback: {movie_href}", "skip")
            if not movie_href:
                self._vlog("[JAVLIB] no results in listing", "skip")
                return None

            detail_url = urljoin(JAVLIB_BASE + '/cn/', movie_href)
            self._vlog(f"[JAVLIB] navigating to detail: {detail_url}", "info")
            html_d, _ = self._uc_get_html(
                detail_url,
                wait_css='#video_title, #video_cast'
            )
            if not html_d:
                self._vlog("[JAVLIB] detail page failed to load", "error")
                return None
            detail_soup = BeautifulSoup(html_d, 'html.parser')

        # ── Parse title and actors ───────────────────────────────────────
        if not detail_soup:
            self._vlog("[JAVLIB] no detail page to parse", "error")
            return None

        title = ''
        title_el = (detail_soup.select_one('#video_title a') or
                    detail_soup.select_one('h3.post-title a') or
                    detail_soup.select_one('#video_title'))
        if title_el:
            title = title_el.get_text(strip=True)
            title = re.sub(re.escape(query_id), '', title, flags=re.IGNORECASE).strip()

        actors = []
        cast_div = detail_soup.select_one('#video_cast')
        if cast_div:
            for a_tag in cast_div.select('span.cast span.star a'):
                name = a_tag.get_text(strip=True)
                if name:
                    actors.append(name)

        self._vlog(f"[JAVLIB] title='{title}'  actors={actors[:4]}", "info")
        return {"title": title, "actors": actors[:4]}

    def process_file(self, folder, filename, source_order=None, force_rename=False):
        """
        source_order: [(key, enabled), ...] in priority order.
        force_rename:  if False, skip files already containing '[r]' in their stem.
        """
        if source_order is None:
            source_order = [("avmoo", True), ("javlibrary", True), ("javdb", False)]

        # Bail immediately if stop was requested
        if self._stopped():
            return ("stopped", "", filename, "")

        RENAMED_TAG = "[r]"

        # ── Step 0: renamed-indicator check ─────────────────────────────
        stem_check = os.path.splitext(filename)[0]
        already_renamed = stem_check.endswith(RENAMED_TAG)

        if already_renamed and not force_rename:
            self._vlog(f"skipping already-renamed file: {filename}", "skip")
            return ("skipped_renamed", filename, filename, filename)

        # ── Step 1: extract raw ID and detect multi-part suffix ──────────
        #
        # Multi-part indicators (converted to _1, _2, _3 … in output):
        #   Trailing letter:   PPT090A  → base=PPT-090, part=1
        #                      PPT090B  → base=PPT-090, part=2
        #   Underscore+digit:  PPT-090_1 / PPT-090_A → part=1
        #   Hyphen+digit:      PPT-090-2             → part=2
        #
        # The part suffix is stripped BEFORE searching so the lookup always
        # uses the clean base ID (e.g. PPT-090, not PPT-090A).

        stem = os.path.splitext(filename)[0]
        ext  = os.path.splitext(filename)[1]

        # Strip any existing [r] tag before parsing — prevents ID extraction failures
        # and ensures force-rename doesn't double-add the tag
        if stem.endswith(RENAMED_TAG):
            stem = stem[:-len(RENAMED_TAG)].rstrip()

        part_suffix = ""   # will become "_1", "_2", etc. if detected

        # Pattern: letters (2-6) + optional separator + digits (2-8) + optional Z
        #          + optional part marker (A-D / _A-_D / -A-D / _1-_9 / -1-9)
        # Matches standard IDs (PIYO-114, CHUC-155) AND T##-### style (T28-504).
        # Group 1 = label (letters, or letter+digits like T28)
        # Group 2 = number portion
        id_pattern = re.compile(
            r'([a-zA-Z]{1,6}\d{0,4})\s*[-]\s*(\d{2,8}[zZ]?)'   # label-number (dash required for T28-504)
            r'|'
            r'([a-zA-Z]{2,6})\s*(\d{2,8}[zZ]?)',                 # label+number (no dash, e.g. PIYO114)
            re.IGNORECASE
        )
        match = id_pattern.search(stem)
        if not match:
            self._vlog(f"no ID pattern matched in filename: {filename}", "skip")
            return "skip"

        # Determine which alternative matched
        if match.group(1) and match.group(2):
            raw_letters = match.group(1)
            raw_digits  = match.group(2)
        else:
            raw_letters = match.group(3)
            raw_digits  = match.group(4)
        # Extract part marker by searching for suffix immediately after the matched number
        part_marker = ""
        part_pattern = re.compile(
            re.escape(raw_letters) + r'\s*[-]?\s*' + re.escape(raw_digits) + r'([_\-]?([A-Da-d]|\d))',
            re.IGNORECASE
        )
        pm = part_pattern.search(stem)
        if pm:
            part_marker = pm.group(1) or ""

        # Normalise the part marker → integer part index
        part_num = None
        if part_marker:
            # Strip leading separator
            clean_marker = part_marker.lstrip("_-").upper()
            if clean_marker.isdigit():
                part_num = int(clean_marker)
            elif len(clean_marker) == 1 and clean_marker.isalpha():
                # A→1, B→2, C→3, D→4
                part_num = ord(clean_marker) - ord('A') + 1

        if part_num is not None:
            part_suffix = f"_{part_num}"
            self._vlog(f"multi-part detected: marker='{part_marker}' → suffix='{part_suffix}'", "info")

        base_id = f"{raw_letters.upper()}-{raw_digits.upper()}"

        # ── Validate multi-part suffix ────────────────────────────────────
        # Rule: only treat part N as genuine if ALL of parts 1..(N-1) have
        # already been processed in this run.  If predecessors are missing
        # (e.g. a file title that happens to end with "C"), strip the suffix.
        if part_num is not None and part_num >= 2:
            with self._parts_lock:
                predecessors_seen = all(
                    (base_id, p) in self._parts_seen
                    for p in range(1, part_num)
                )
            if not predecessors_seen:
                missing = [p for p in range(1, part_num)
                           if (base_id, p) not in self._parts_seen]
                self._vlog(
                    f"multi-part suffix '{part_marker}' ignored — "
                    f"predecessor part(s) {missing} not yet seen for {base_id}",
                    "skip"
                )
                part_num    = None
                part_suffix = ""

        # Register this part as seen (after validation so false positives aren't recorded)
        if part_num is not None:
            with self._parts_lock:
                self._parts_seen.add((base_id, part_num))
        self._vlog(f"extracted ID: {base_id}  part_suffix: '{part_suffix}'  from: {filename}", "info")

        is_ub = any(x in filename.lower() for x in ["uncensored", "[ub]", "leaked"])
        # Build display ID: PPT-090_1 [UB]  or  PPT-090_1
        full_id_display = f"{base_id}{part_suffix}"
        if is_ub:
            full_id_display += " [UB]"

        fetch_map = {
            "avmoo":      self.fetch_from_avmoo,
            "javlibrary": self.fetch_from_javlibrary,
            "javdb":      self.fetch_from_javdb,
        }

        successful_source = None   # track which source provided data

        data = self.cache.get(base_id)
        if not data:
            for key, enabled in source_order:
                if self._stopped():
                    return ("stopped", base_id, filename, "")
                if not enabled:
                    self._vlog(f"[{key.upper()}] skipped (disabled)", "skip")
                    continue
                fn = fetch_map.get(key)
                if fn:
                    self._vlog(f"trying source: {key.upper()}", "info")
                    data = fn(base_id)
                    if data:
                        successful_source = key.upper()
                        self._vlog(f"found via {key.upper()}", "info")
                        break

        if data:
            self.cache[base_id] = data
            actors_str = " ".join(data.get('actors', [])).strip()
            title = data.get('title', '')
            new_name = f"{full_id_display} {actors_str} ｜ {title}" if actors_str else f"{full_id_display} {title}"
            new_name_tagged = self.sanitize_filename(new_name) + RENAMED_TAG
            target_path = os.path.join(folder, new_name_tagged + ext)
            original_path = os.path.join(folder, filename)
            try:
                os.rename(original_path, target_path)
                return ("ok", base_id, filename, new_name_tagged + ext, successful_source)
            except FileExistsError:
                return ("skip_exists", base_id, filename, new_name_tagged + ext)
            except Exception as e:
                return ("error", base_id, filename, "", str(e))
        else:
            return ("not_found", base_id, filename, "")


# ─────────────────────────────────────────────
#  UI
# ─────────────────────────────────────────────
DARK_BG     = "#0f1117"
PANEL_BG    = "#181c27"
CARD_BG     = "#1e2333"
ACCENT      = "#4f8ef7"
ACCENT2     = "#7c3aed"
SUCCESS     = "#22c55e"
WARNING     = "#f59e0b"
ERROR_CLR   = "#ef4444"
MUTED       = "#64748b"
TEXT_MAIN   = "#e2e8f0"
TEXT_SUB    = "#94a3b8"
BORDER      = "#2d3348"
TOGGLE_ON   = "#4f8ef7"
TOGGLE_OFF  = "#2d3348"


class ToggleSwitch(tk.Canvas):
    def __init__(self, parent, variable, command=None, **kwargs):
        kwargs.setdefault('bg', PANEL_BG)
        super().__init__(parent, width=44, height=24,
                         highlightthickness=0, cursor="hand2", **kwargs)
        self.variable = variable
        self.command  = command
        self._draw()
        self.bind("<Button-1>", self._toggle)
        self.variable.trace_add("write", lambda *_: self._draw())

    def _draw(self):
        self.delete("all")
        on = bool(self.variable.get())
        track_color = TOGGLE_ON if on else TOGGLE_OFF
        # track
        self.create_rounded_rect(2, 4, 42, 20, radius=8, fill=track_color, outline="")
        # knob
        x = 30 if on else 14
        self.create_oval(x - 9, 3, x + 9, 21, fill=TEXT_MAIN, outline="")

    def create_rounded_rect(self, x1, y1, x2, y2, radius=10, **kwargs):
        points = [
            x1 + radius, y1,
            x2 - radius, y1,
            x2, y1,
            x2, y1 + radius,
            x2, y2 - radius,
            x2, y2,
            x2 - radius, y2,
            x1 + radius, y2,
            x1, y2,
            x1, y2 - radius,
            x1, y1 + radius,
            x1, y1,
        ]
        return self.create_polygon(points, smooth=True, **kwargs)

    def _toggle(self, _=None):
        self.variable.set(not self.variable.get())
        if self.command:
            self.command()


class SourcePriorityList(tk.Frame):
    """
    Drag-and-drop priority list for scrape sources.
    Each row:  ⠿ drag-handle │ badge (1st/2nd/3rd) │ label │ toggle ON/OFF
    Dragging a row up/down reorders priority.
    get_ordered_sources() → [(key, enabled), ...] in priority order.
    """

    SOURCES = [
        ("javlibrary", "JAVLibrary", True),
        ("avmoo",      "AVMOO",      False),
        # NOTE: JAVDB is under development and will be released in the next update.
        # The toggle is intentionally locked OFF and greyed out in the UI.
        ("javdb",      "JAVDB",      False),
    ]

    # Sources locked to OFF — toggle is displayed but disabled
    LOCKED_OFF = {"javdb"}

    ROW_H   = 44
    DRAG_BG = "#252b3b"

    def __init__(self, parent, **kwargs):
        super().__init__(parent, bg=CARD_BG, **kwargs)

        # State: list of [key, label, BooleanVar]
        self._items = [[k, lbl, tk.BooleanVar(value=default)]
                       for k, lbl, default in self.SOURCES]

        self._drag_item  = None   # index being dragged
        self._drag_start = 0      # Y coord where drag began
        self._rows       = []     # list of row Frame widgets (in current order)

        self._render()

    # ── public ──────────────────────────────────────────────────────────
    def get_ordered_sources(self):
        """Return [(key, enabled_bool), ...] in current priority order."""
        return [(item[0], item[2].get()) for item in self._items]

    # ── rendering ───────────────────────────────────────────────────────
    def _render(self):
        for w in self.winfo_children():
            w.destroy()
        self._rows = []

        for idx, (key, lbl, var) in enumerate(self._items):
            row = self._make_row(idx, key, lbl, var)
            row.pack(fill="x", pady=(0, 4))
            self._rows.append(row)

    def _make_row(self, idx, key, lbl, var):
        locked = key in self.LOCKED_OFF
        row_bg = "#181c27" if locked else CARD_BG   # darker bg for locked rows
        fg_col = "#404660" if locked else TEXT_MAIN  # dimmed text

        row = tk.Frame(self, bg=row_bg,
                       highlightbackground=BORDER, highlightthickness=1)

        # ── drag handle (hidden/inactive for locked rows) ───────────────
        handle = tk.Label(row, text="⠿" if not locked else "  ",
                          font=("Courier New", 14),
                          bg=row_bg, fg="#303550" if locked else MUTED,
                          cursor="arrow" if locked else "fleur",
                          padx=10, pady=8)
        handle.pack(side="left")

        # ── priority badge ─────────────────────────────────────────────
        badge_texts  = ["1ST", "2ND", "3RD"]
        badge_colors = [ACCENT, "#a855f7", MUTED]
        badge_bg     = "#282c3a" if locked else (badge_colors[idx] if idx < 3 else MUTED)
        badge = tk.Label(row,
                         text=badge_texts[idx] if idx < 3 else f"#{idx+1}",
                         font=("Courier New", 7, "bold"),
                         bg=badge_bg,
                         fg="#404660" if locked else "white",
                         padx=5, pady=2, width=4)
        badge.pack(side="left", padx=(0, 8), pady=10)

        # ── source label ───────────────────────────────────────────────
        source_icons = {"avmoo": "🎬", "javlibrary": "📚", "javdb": "🔗"}
        label_text = f"{source_icons.get(key, '◆')}  {lbl}"
        if locked:
            label_text += "  (coming soon)"
        tk.Label(row,
                 text=label_text,
                 font=("Courier New", 9, "bold"),
                 bg=row_bg, fg=fg_col).pack(side="left", fill="x", expand=True)

        # ── ON/OFF toggle (disabled + forced OFF for locked sources) ────
        if locked:
            var.set(False)   # ensure always OFF
        ts = ToggleSwitch(row, variable=var, bg=row_bg)
        if locked:
            ts.configure(state="disabled" if hasattr(ts, 'configure') else None)
            # Directly disable all canvas interactions
            ts.unbind("<Button-1>")
            ts.unbind("<ButtonRelease-1>")
        ts.pack(side="right", padx=10)

        # ── bind drag only for non-locked rows ─────────────────────────
        if not locked:
            for widget in (handle, row):
                widget.bind("<ButtonPress-1>",   lambda e, i=idx: self._drag_start_cb(e, i))
                widget.bind("<B1-Motion>",        self._drag_motion)
                widget.bind("<ButtonRelease-1>",  self._drag_release)

        return row

    # ── drag logic ───────────────────────────────────────────────────────
    def _drag_start_cb(self, event, idx):
        self._drag_item  = idx
        self._drag_start = event.y_root

    def _drag_motion(self, event):
        if self._drag_item is None:
            return
        delta = event.y_root - self._drag_start
        # Move if dragged more than half a row height
        if abs(delta) < self.ROW_H // 2:
            return

        direction = 1 if delta > 0 else -1
        new_idx = self._drag_item + direction

        if 0 <= new_idx < len(self._items):
            # swap
            self._items[self._drag_item], self._items[new_idx] = \
                self._items[new_idx], self._items[self._drag_item]
            self._drag_item  = new_idx
            self._drag_start = event.y_root
            self._render()

    def _drag_release(self, event):
        self._drag_item = None


class MultiSelectDialog(tk.Toplevel):
    """
    Custom dialog that lets the user build up a mixed list of folders and files.
    Folders are scanned at runtime (respecting the subfolders toggle).
    Files are added individually (multi-select per dialog open).
    """
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Select Folders / Files")
        self.configure(bg=DARK_BG)
        self.resizable(True, True)
        self.geometry("560x420")
        self.minsize(480, 320)
        self.transient(parent)
        self.grab_set()

        self._items = []   # list of (kind, path) where kind = 'folder' or 'file'
        self.result  = None  # set to list on OK, None on cancel

        self._build()
        self.protocol("WM_DELETE_WINDOW", self._cancel)

    def _build(self):
        # ── toolbar ─────────────────────────────────────────────────────
        tb = tk.Frame(self, bg=CARD_BG, pady=8)
        tb.pack(fill="x", padx=12)

        def btn(text, cmd):
            return tk.Button(tb, text=text, command=cmd,
                             bg=ACCENT, fg="white", relief="flat",
                             font=("Courier New", 8, "bold"),
                             cursor="hand2", padx=10, pady=4,
                             activebackground="#3a7de8", activeforeground="white")

        btn("📁  Add Folder",  self._add_folder).pack(side="left", padx=(0, 6))
        btn("🗂  Add Files",   self._add_files).pack(side="left", padx=(0, 6))
        btn("✕  Remove",       self._remove_selected).pack(side="left")
        btn("🗑  Clear All",   self._clear).pack(side="left", padx=(6, 0))

        # ── list box ────────────────────────────────────────────────────
        list_frame = tk.Frame(self, bg=DARK_BG)
        list_frame.pack(fill="both", expand=True, padx=12, pady=(8, 0))

        sb = ttk.Scrollbar(list_frame, orient="vertical")
        self.listbox = tk.Listbox(
            list_frame, selectmode="extended",
            bg="#0d1018", fg=TEXT_MAIN,
            font=("Courier New", 8),
            relief="flat", activestyle="none",
            selectbackground=ACCENT, selectforeground="white",
            highlightthickness=0,
            yscrollcommand=sb.set
        )
        sb.config(command=self.listbox.yview)
        sb.pack(side="right", fill="y")
        self.listbox.pack(side="left", fill="both", expand=True)

        # ── count label ─────────────────────────────────────────────────
        self._count_var = tk.StringVar(value="0 items")
        tk.Label(self, textvariable=self._count_var,
                 font=("Courier New", 8), bg=DARK_BG, fg=MUTED
                 ).pack(anchor="w", padx=14, pady=(4, 0))

        # ── OK / Cancel ─────────────────────────────────────────────────
        ok_row = tk.Frame(self, bg=DARK_BG, pady=10)
        ok_row.pack(fill="x")
        tk.Button(ok_row, text="  ✔  OK  ", command=self._ok,
                  bg=SUCCESS, fg="white", relief="flat",
                  font=("Courier New", 9, "bold"),
                  cursor="hand2", padx=14, pady=6,
                  activebackground="#16a34a", activeforeground="white"
                  ).pack(side="right", padx=(0, 12))
        tk.Button(ok_row, text="  Cancel  ", command=self._cancel,
                  bg=CARD_BG, fg=TEXT_SUB, relief="flat",
                  font=("Courier New", 9),
                  cursor="hand2", padx=14, pady=6
                  ).pack(side="right", padx=(0, 6))

    def _refresh_list(self):
        self.listbox.delete(0, "end")
        for kind, path in self._items:
            icon = "📁" if kind == "folder" else "🗂"
            self.listbox.insert("end", f" {icon}  {path}")
        n = len(self._items)
        self._count_var.set(f"{n} item{'s' if n != 1 else ''} selected")

    def _add_folder(self):
        path = filedialog.askdirectory(title="Add folder", parent=self)
        if path and ("folder", path) not in self._items:
            self._items.append(("folder", path))
            self._refresh_list()

    def _add_files(self):
        paths = filedialog.askopenfilenames(
            title="Add files", parent=self,
            filetypes=[("Video files", "*.mp4 *.mkv *.avi *.wmv *.mov *.m4v *.ts *.flv"),
                       ("All files", "*.*")]
        )
        for p in paths:
            if ("file", p) not in self._items:
                self._items.append(("file", p))
        self._refresh_list()

    def _remove_selected(self):
        for idx in reversed(self.listbox.curselection()):
            del self._items[idx]
        self._refresh_list()

    def _clear(self):
        self._items.clear()
        self._refresh_list()

    def _ok(self):
        self.result = list(self._items)
        self.destroy()

    def _cancel(self):
        self.result = None
        self.destroy()


class LogWindow(tk.Toplevel):
    """
    Separate, resizable window for progress bar + verbose log.
    Opened automatically when START is clicked; stays open after done.
    """
    def __init__(self, parent):
        super().__init__(parent)
        self.title("Progress & Log")
        self.configure(bg=DARK_BG)
        self.resizable(True, True)
        self.geometry("720x480")
        self.minsize(480, 300)
        self.protocol("WM_DELETE_WINDOW", self._on_close_request)
        self._allow_close = False   # block accidental close while running

        # ── counter + status ────────────────────────────────────────────
        top = tk.Frame(self, bg=DARK_BG, pady=10)
        top.pack(fill="x", padx=16)

        self.progress_label = tk.Label(top, text="0 / 0  files",
                                       font=("Courier New", 10, "bold"),
                                       bg=DARK_BG, fg=TEXT_MAIN)
        self.progress_label.pack(side="left")

        self.status_badge = tk.Label(top, text="idle",
                                     font=("Courier New", 8),
                                     bg=DARK_BG, fg=MUTED)
        self.status_badge.pack(side="right")

        # ── progress bar ────────────────────────────────────────────────
        self.pbar_canvas = tk.Canvas(self, height=10, bg="#252b3b",
                                     highlightthickness=0, bd=0)
        self.pbar_canvas.pack(fill="x", padx=16, pady=(0, 8))
        self.pbar_canvas.bind("<Configure>", lambda _: self._update_pbar(force=True))
        self._pbar_pct = 0.0

        # ── log box ─────────────────────────────────────────────────────
        log_frame = tk.Frame(self, bg=DARK_BG)
        log_frame.pack(fill="both", expand=True, padx=16, pady=(0, 10))

        hscroll = ttk.Scrollbar(log_frame, orient="horizontal")
        vscroll = ttk.Scrollbar(log_frame, orient="vertical")

        self.log_box = tk.Text(log_frame,
                               bg="#0d1018", fg=TEXT_SUB,
                               font=("Courier New", 8),
                               relief="flat", state="disabled",
                               wrap="none",
                               selectbackground=ACCENT, selectforeground=TEXT_MAIN,
                               xscrollcommand=hscroll.set,
                               yscrollcommand=vscroll.set)

        vscroll.config(command=self.log_box.yview)
        hscroll.config(command=self.log_box.xview)
        hscroll.pack(side="bottom", fill="x")
        vscroll.pack(side="right",  fill="y")
        self.log_box.pack(side="left", fill="both", expand=True)

        # colour tags
        self.log_box.tag_config("ok",       foreground=SUCCESS)
        self.log_box.tag_config("skip",     foreground=WARNING)
        self.log_box.tag_config("error",    foreground=ERROR_CLR)
        self.log_box.tag_config("notfound", foreground=MUTED)
        self.log_box.tag_config("info",     foreground=ACCENT)

        # ── clear button ─────────────────────────────────────────────────
        tk.Button(self, text="  🗑  Clear Log  ",
                  command=self.clear_log,
                  bg=CARD_BG, fg=TEXT_SUB, relief="flat",
                  font=("Courier New", 8), cursor="hand2",
                  padx=8, pady=4).pack(anchor="e", padx=16, pady=(0, 8))

    def log(self, msg, tag=""):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", msg + "\n", tag)
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def clear_log(self):
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")

    def _update_pbar(self, pct=None, force=False):
        if pct is not None:
            self._pbar_pct = pct
        w = self.pbar_canvas.winfo_width()
        if w < 2:
            return
        self.pbar_canvas.delete("all")
        self.pbar_canvas.create_rectangle(0, 0, w, 10, fill="#252b3b", outline="")
        fill_w = int(w * min(self._pbar_pct, 1.0))
        if fill_w > 0:
            self.pbar_canvas.create_rectangle(0, 0, fill_w, 10, fill=ACCENT, outline="")

    def set_status(self, text, color=MUTED):
        self.status_badge.configure(text=text, fg=color)

    def set_running(self, running):
        """While running, prevent the window from being closed."""
        self._allow_close = not running

    def _on_close_request(self):
        if not self._allow_close:
            self.log("⚠  Cannot close while renaming is in progress.", tag="skip")
        else:
            self.withdraw()   # hide rather than destroy so it can be re-shown


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("File Renamer")
        self.configure(bg=DARK_BG)
        self.resizable(False, True)
        self.geometry("640x720")
        self.minsize(640, 680)

        self.renamer = SkipDuplicateRenamer()
        self._selected_items   = []
        self._selection_label  = tk.StringVar(value="")
        self._folder_path      = ""
        self._selected_folders = []
        self.include_subdir   = tk.BooleanVar(value=True)
        self.force_rename     = tk.BooleanVar(value=False)
        self.skip_bubing      = tk.BooleanVar(value=True)
        self._running        = False
        self._stop_event     = threading.Event()
        self._cf_alert_open  = False   # prevent stacking duplicate alert windows

        self._build_ui()
        # Log window — created hidden, shown when START is clicked
        self.log_win = LogWindow(self)
        self.log_win.withdraw()
        self.after(200, self._startup_check)
        self.after(60_000, self._cf_monitor_tick)

    # ── layout ──────────────────────────────────────────────────────────
    def _build_ui(self):
        # ── Button bar MUST be packed first so tkinter reserves its space
        # before any expand=True content is laid out above it.
        btn_bar = tk.Frame(self, bg=DARK_BG, pady=14)
        btn_bar.pack(side="bottom", fill="x")

        inner_bar = tk.Frame(btn_bar, bg=DARK_BG)
        inner_bar.pack()

        self.run_btn = self._btn(inner_bar, "  ▶  START RENAMING  ", self._start, style="primary", big=True)
        self.run_btn.pack(side="left", padx=(0, 8))

        self.stop_btn = self._btn(inner_bar, "  ■  STOP  ", self._stop, style="danger", big=True)
        self.stop_btn.pack(side="left")
        self.stop_btn.configure(state="disabled")

        # Header
        hdr = tk.Frame(self, bg=DARK_BG, pady=20)
        hdr.pack(fill="x", padx=28)
        tk.Label(hdr, text="FILE  RENAMER", font=("Courier New", 18, "bold"),
                 bg=DARK_BG, fg=ACCENT).pack(side="left")
        tk.Label(hdr, text="JAV metadata enrichment tool",
                 font=("Courier New", 9), bg=DARK_BG, fg=MUTED).pack(side="left", padx=(10, 0), pady=(6, 0))

        # ── Selection card ──────────────────────────────────────────────
        card = self._card(self, label="TARGET  FILES  /  FOLDER")
        card.pack(fill="x", padx=28, pady=(0, 12))

        inner = tk.Frame(card, bg=CARD_BG)
        inner.pack(fill="x", padx=16, pady=(0, 16))

        # path/summary display
        self.path_entry = tk.Entry(
            inner, textvariable=self._selection_label,
            bg="#252b3b", fg=TEXT_MAIN, insertbackground=TEXT_MAIN,
            relief="flat", font=("Courier New", 9),
            readonlybackground="#252b3b", state="readonly"
        )
        self.path_entry.pack(fill="x", ipady=8, ipadx=8)

        # single browse button + clear
        btn_row = tk.Frame(inner, bg=CARD_BG)
        btn_row.pack(fill="x", pady=(8, 0))

        browse_btn = self._btn(btn_row, "  📂  Browse…  ", self._browse, style="primary")
        browse_btn.pack(side="left", padx=(0, 6))

        clear_btn = self._btn(btn_row, "✕  Clear", self._clear_selection, style="secondary")
        clear_btn.pack(side="left")

        # drag-drop hint
        tk.Label(inner, text="or drag & drop files / a folder here",
                 font=("Courier New", 8), bg=CARD_BG, fg=MUTED).pack(anchor="w", pady=(6, 0))
        self._setup_drag_drop(inner)

        # ── Options card ────────────────────────────────────────────────
        opt_card = self._card(self, label="OPTIONS")
        opt_card.pack(fill="x", padx=28, pady=(0, 12))

        opt_inner = tk.Frame(opt_card, bg=CARD_BG)
        opt_inner.pack(fill="x", padx=16, pady=(0, 16))

        # ── Source priority list ────────────────────────────────────────
        tk.Label(opt_inner,
                 text="SOURCES  ·  drag ⠿ to reorder priority",
                 font=("Courier New", 7, "bold"),
                 bg=CARD_BG, fg=MUTED).pack(anchor="w", pady=(0, 6))

        self.source_list = SourcePriorityList(opt_inner)
        self.source_list.pack(fill="x")

        sep2 = tk.Frame(opt_inner, bg=BORDER, height=1)
        sep2.pack(fill="x", pady=10)

        self._toggle_row(opt_inner,
            icon="📂",
            title="Include Sub-folders",
            subtitle="Recursively rename files inside all sub-directories",
            variable=self.include_subdir)

        sep3 = tk.Frame(opt_inner, bg=BORDER, height=1)
        sep3.pack(fill="x", pady=10)

        self._toggle_row(opt_inner,
            icon="⚡",
            title="Force Rename",
            subtitle="Re-process files already marked [r]. OFF = skip already-renamed files",
            variable=self.force_rename)

        sep4 = tk.Frame(opt_inner, bg=BORDER, height=1)
        sep4.pack(fill="x", pady=10)

        self._toggle_row(opt_inner,
            icon="🚫",
            title="Skip 步兵 Folders",
            subtitle="Exclude all files inside any folder named 步兵 from renaming",
            variable=self.skip_bubing)

    # ── helpers ─────────────────────────────────────────────────────────
    def _card(self, parent, label=""):
        outer = tk.Frame(parent, bg=CARD_BG, bd=0)
        outer.configure(highlightbackground=BORDER, highlightthickness=1)
        if label:
            tk.Label(outer, text=f"  {label}  ",
                     font=("Courier New", 7, "bold"),
                     bg=CARD_BG, fg=MUTED).pack(anchor="w", padx=12, pady=(10, 6))
        return outer

    def _btn(self, parent, text, cmd, style="secondary", big=False):
        size = 10 if big else 9
        pad  = 12 if big else 8
        if style == "primary":
            b = tk.Button(parent, text=text, command=cmd,
                          bg=ACCENT, fg="white", activebackground="#3b6fd4",
                          activeforeground="white", relief="flat",
                          font=("Courier New", size, "bold"),
                          cursor="hand2", padx=pad, pady=6)
        elif style == "danger":
            b = tk.Button(parent, text=text, command=cmd,
                          bg=ERROR_CLR, fg="white", activebackground="#c53030",
                          activeforeground="white", relief="flat",
                          font=("Courier New", size, "bold"),
                          cursor="hand2", padx=pad, pady=6)
        else:
            b = tk.Button(parent, text=text, command=cmd,
                          bg=CARD_BG, fg=TEXT_MAIN, activebackground=BORDER,
                          activeforeground=TEXT_MAIN, relief="flat",
                          font=("Courier New", size),
                          cursor="hand2", padx=pad, pady=6)
        return b

    def _toggle_row(self, parent, icon, title, subtitle, variable):
        row = tk.Frame(parent, bg=CARD_BG)
        row.pack(fill="x", pady=4)

        left = tk.Frame(row, bg=CARD_BG)
        left.pack(side="left", fill="x", expand=True)

        tk.Label(left, text=f"{icon}  {title}",
                 font=("Courier New", 9, "bold"),
                 bg=CARD_BG, fg=TEXT_MAIN).pack(anchor="w")
        tk.Label(left, text=subtitle,
                 font=("Courier New", 8),
                 bg=CARD_BG, fg=MUTED).pack(anchor="w")

        ToggleSwitch(row, variable=variable).pack(side="right", padx=(8, 0))

    def _setup_drag_drop(self, widget):
        """Best-effort drag and drop (works if tkinterdnd2 is installed)."""
        try:
            from tkinterdnd2 import DND_FILES
            widget.drop_target_register(DND_FILES)
            widget.dnd_bind('<<Drop>>', lambda e: self._handle_drop(e.data))
        except Exception:
            pass  # silently skip if not available

    def _handle_drop(self, data):
        import shlex
        try:
            paths = shlex.split(data)
        except Exception:
            paths = [data.strip().strip("{}")]

        folders = [p for p in paths if os.path.isdir(p)]
        files   = [p for p in paths if os.path.isfile(p)]

        if folders:
            self._selected_folders = list(set(getattr(self, '_selected_folders', []) + folders))
        if files:
            new_items = [(os.path.dirname(f), os.path.basename(f)) for f in files]
            self._selected_items = list({(d,n) for d,n in self._selected_items + new_items})

        parts = []
        if self._selected_folders:
            parts.append(f"📁 {len(self._selected_folders)} folder{'s' if len(self._selected_folders)>1 else ''}")
        if self._selected_items:
            parts.append(f"🗂 {len(self._selected_items)} file{'s' if len(self._selected_items)>1 else ''}")
        if parts:
            self._selection_label.set("  +  ".join(parts))
            self._log(f"📂  Dropped: {', '.join(parts)}", tag="info")

    # ── Cloudflare challenge monitor ─────────────────────────────────────
    CF_MONITOR_INTERVAL = 60_000    # 1 minute in milliseconds

    def _cf_monitor_tick(self):
        """Runs every 1 min via tkinter.after — checks if Chrome is blocked."""
        if self.renamer._uc_driver is not None:
            # Run the check in a thread so it doesn't freeze the UI
            threading.Thread(target=self._cf_check_thread, daemon=True).start()
        # Reschedule regardless
        self.after(self.CF_MONITOR_INTERVAL, self._cf_monitor_tick)

    def _cf_check_thread(self):
        """Background thread: reads Chrome page title and triggers alert if CF blocked."""
        try:
            driver = self.renamer._uc_driver
            if driver is None:
                return
            title = (driver.title or "").lower()
            src   = driver.page_source or ""
            challenged = (
                "just a moment" in title
                or "cf-challenge" in src
                or "cf_chl" in src
                or (len(src) < 3000 and "cloudflare" in src.lower())
            )
            if challenged and not self._cf_alert_open:
                self.after(0, self._cf_show_alert)
        except Exception:
            pass   # driver may be mid-navigation — skip this tick

    def _cf_show_alert(self):
        """Show a topmost notification window asking user to solve the CF challenge."""
        if self._cf_alert_open:
            return
        self._cf_alert_open = True

        win = tk.Toplevel(self)
        win.title("⚠  Cloudflare Check Required")
        win.configure(bg=DARK_BG)
        win.resizable(False, False)
        win.attributes("-topmost", True)   # always on top of all windows
        win.geometry("420x210")

        # Flash the taskbar / bring to front
        win.lift()
        win.focus_force()

        # ── content ─────────────────────────────────────────────────────
        tk.Label(win,
                 text="⚠",
                 font=("Courier New", 36),
                 bg=DARK_BG, fg=WARNING).pack(pady=(22, 0))

        tk.Label(win,
                 text="Cloudflare is blocking the Chrome window.",
                 font=("Courier New", 10, "bold"),
                 bg=DARK_BG, fg=TEXT_MAIN).pack(pady=(8, 2))

        tk.Label(win,
                 text='Please switch to Chrome and complete\nthe "Verify you are human" checkbox.',
                 font=("Courier New", 9),
                 bg=DARK_BG, fg=TEXT_SUB,
                 justify="center").pack()

        def _dismiss():
            self._cf_alert_open = False
            win.destroy()

        tk.Button(win,
                  text="  ✔  Got it — I'll handle it  ",
                  command=_dismiss,
                  bg=ACCENT, fg="white",
                  font=("Courier New", 9, "bold"),
                  relief="flat", cursor="hand2",
                  padx=14, pady=6,
                  activebackground="#3a7de8", activeforeground="white"
                  ).pack(pady=(14, 0))

        win.protocol("WM_DELETE_WINDOW", _dismiss)

    # ── startup check ────────────────────────────────────────────────────
    def _startup_check(self):
        """Run once on startup — verify optional dependencies and warn clearly."""
        self._log("─" * 52, tag="info")
        self._log("  FILE RENAMER  ready", tag="info")
        self._log("─" * 52, tag="info")

        if not UC_AVAILABLE:
            self._log("⚠  JAVLibrary requires undetected-chromedriver.", tag="skip")
            self._log(f"   Import error: {UC_IMPORT_ERROR}", tag="error")
            if 'distutils' in UC_IMPORT_ERROR:
                self._log("   Python 3.12+ removed 'distutils'. Fix with:", tag="skip")
                self._log("   pip install setuptools", tag="error")
                self._log("   Then restart this app.", tag="skip")
            else:
                self._log("   If not installed, run:", tag="skip")
                self._log("   pip install undetected-chromedriver selenium setuptools", tag="error")
                self._log("   Then restart this app.", tag="skip")
        else:
            self._log("✅  undetected-chromedriver  found", tag="ok")

        # Check Google Chrome is reachable
        if UC_AVAILABLE:
            try:
                import subprocess, shutil
                chrome_paths = [
                    shutil.which("google-chrome"),
                    shutil.which("chrome"),
                    shutil.which("chromium"),
                    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                ]
                found = any(
                    p and __import__('os').path.exists(p) for p in chrome_paths
                )
                if found:
                    self._log("✅  Google Chrome  found", tag="ok")
                else:
                    self._log("⚠  Google Chrome not found in common locations.", tag="skip")
                    self._log("   Please install Chrome from https://www.google.com/chrome/", tag="error")
            except Exception:
                pass

        self._log("─" * 52, tag="info")

    def _browse(self):
        dlg = MultiSelectDialog(self)
        self.wait_window(dlg)
        if dlg.result is None:
            return   # cancelled
        if not dlg.result:
            return   # nothing added

        # Expand folders into (folder, filename) pairs; files added directly
        items = []
        folders_added = []
        files_added   = []
        for kind, path in dlg.result:
            if kind == "folder":
                folders_added.append(path)
                self._folder_path = path   # keep last folder for subdir toggle
            else:
                files_added.append(path)
                items.append((os.path.dirname(path), os.path.basename(path)))

        # Store as mixed list — folders will be expanded at run-time by _collect_files
        self._selected_items  = items
        self._selected_folders = folders_added

        # Build display label
        parts = []
        if folders_added:
            parts.append(f"📁 {len(folders_added)} folder{'s' if len(folders_added)>1 else ''}")
        if files_added:
            parts.append(f"🗂 {len(files_added)} file{'s' if len(files_added)>1 else ''}")
        self._selection_label.set("  +  ".join(parts))

        # Log summary
        self._log(f"📂  Selection: {', '.join(parts)}", tag="info")
        for p in folders_added:
            self._log(f"   📁 {p}", tag="info")
        for p in files_added[:5]:
            self._log(f"   🗂 {os.path.basename(p)}", tag="info")
        if len(files_added) > 5:
            self._log(f"   … and {len(files_added)-5} more files", tag="info")

    def _clear_selection(self):
        self._selected_items   = []
        self._selected_folders = []
        self._folder_path      = ""
        self._selection_label.set("")
        self._log("✕  Selection cleared.", tag="skip")

    def _log(self, msg, tag=""):
        self.log_win.log(msg, tag)

    def _update_pbar(self, pct=None, force=False):
        self.log_win._update_pbar(pct, force)

    def _set_status(self, text, color=MUTED):
        self.log_win.set_status(text, color)

    def _collect_files(self):
        """Return list of (folder, filename) to process."""
        skip_bubing = self.skip_bubing.get()
        files = []
        # Scan any selected folders
        for folder in getattr(self, '_selected_folders', []):
            if self.include_subdir.get():
                for root, _, fnames in os.walk(folder):
                    # Skip any directory named 步兵 (check every component of the path)
                    if skip_bubing and '步兵' in root.replace('\\', '/').split('/'):
                        continue
                    for f in fnames:
                        files.append((root, f))
            else:
                for f in os.listdir(folder):
                    if os.path.isfile(os.path.join(folder, f)):
                        files.append((folder, f))
        # Add any directly selected files — apply 步兵 filter here too
        for fldr, fname in self._selected_items:
            if skip_bubing and '步兵' in fldr.replace('\\', '/').split('/'):
                continue
            files.append((fldr, fname))
        return files

    def _start(self):
        if not self._selected_items and not self._selected_folders:
            self._log("⚠  No files or folders selected.", tag="error")
            return
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self.renamer.stop_event = self._stop_event
        # Verbose always ON
        _log_cb = lambda m, t: self.after(0, lambda msg=m, tag=t: self._log(msg, tag))
        self.renamer.always_log_fn = _log_cb
        self.renamer.log_fn        = _log_cb
        # Show the log window next to the main window
        self.log_win.set_running(True)
        self.log_win.deiconify()
        self.log_win.lift()
        x = self.winfo_x() + self.winfo_width() + 10
        y = self.winfo_y()
        self.log_win.geometry(f"720x480+{x}+{y}")
        self.run_btn.configure(state="disabled", bg="#2d3348", text="  ⏳  RUNNING…  ")
        self.stop_btn.configure(state="normal")

        # ── Pre-launch Chrome if JAVLibrary is enabled ───────────────────
        # Do this in a thread so the UI stays responsive
        source_order = self.source_list.get_ordered_sources()
        javlib_enabled = any(k == "javlibrary" and en for k, en in source_order)
        if javlib_enabled:
            threading.Thread(target=self._ensure_chrome_ready, daemon=True).start()
        else:
            threading.Thread(target=self._run_rename, daemon=True).start()

    def _ensure_chrome_ready(self):
        """
        Called before renaming starts when JAVLibrary is enabled.
        Ensures Chrome is launched (or relaunched after a previous failure),
        brings the Chrome window to the foreground, and waits for any CF
        challenge to be resolved before kicking off the rename thread.
        """
        # Reset failure flag so a previous crash doesn't block this run
        self.renamer._uc_failed = False

        self.after(0, lambda: self._log("[JAVLIB] initialising Chrome before run…", tag="info"))
        driver = self.renamer._get_uc_driver()   # launches + warms up if not already running

        if driver is None:
            self.after(0, lambda: self._log(
                "[JAVLIB] ⚠ Chrome could not be started — proceeding without JAVLibrary",
                tag="error"))
            threading.Thread(target=self._run_rename, daemon=True).start()
            return

        # ── Bring Chrome window to the foreground ────────────────────────
        try:
            driver.switch_to.window(driver.current_window_handle)
            # Windows: use pygetwindow or ctypes to raise the window
            try:
                import ctypes
                hwnd = int(driver.current_window_handle, 16)
                ctypes.windll.user32.SetForegroundWindow(hwnd)
            except Exception:
                pass   # non-Windows or ctypes unavailable — Chrome still visible
        except Exception:
            pass

        # ── Check for Cloudflare challenge ───────────────────────────────
        try:
            title      = (driver.title or "").lower()
            src        = driver.page_source or ""
            challenged = (
                "just a moment" in title
                or "cf-challenge" in src
                or "cf_chl"       in src
                or (len(src) < 3000 and "cloudflare" in src.lower())
            )
            if challenged:
                self.after(0, lambda: self._log(
                    "[JAVLIB] ⚠ Cloudflare challenge detected — complete the checkbox in the Chrome window",
                    tag="error"))
                self.after(0, self._cf_show_alert)
                # Wait up to 120 s (24 × 5 s) for the challenge to clear
                for _ in range(24):
                    self.renamer._sleep(5)
                    if self._stop_event.is_set():
                        threading.Thread(target=self._run_rename, daemon=True).start()
                        return
                    try:
                        t = (driver.title or "").lower()
                        s = driver.page_source or ""
                        if "just a moment" not in t and len(s) > 3000:
                            self.after(0, lambda: self._log(
                                "[JAVLIB] ✅ Cloudflare cleared — starting rename", tag="ok"))
                            # Dismiss CF alert if still open
                            self._cf_alert_open = False
                            break
                    except Exception:
                        break
            else:
                self.after(0, lambda: self._log("[JAVLIB] Chrome ready ✓", tag="ok"))
        except Exception:
            pass

        threading.Thread(target=self._run_rename, daemon=True).start()


    def _stop(self):
        if self._running:
            self._stop_event.set()
            self.stop_btn.configure(state="disabled", text="  ⏳  STOPPING…  ")
            self._set_status("stopping…", WARNING)
            self._log("⛔  Stop requested — finishing current file…", tag="skip")

    def _run_rename(self):
        files = self._collect_files()
        total_collected = len(files)
        source_order = self.source_list.get_ordered_sources()
        force_rename = self.force_rename.get()
        self.renamer.reset_run_state()

        # ── Batch pre-filter: instantly separate already-renamed from pending ──
        # This is a pure stem-string check (no I/O, no sleep) so it runs
        # at full speed regardless of collection size.
        RENAMED_TAG = "[r]"
        if force_rename:
            to_process = files
            skipped_pre = []
        else:
            to_process   = []
            skipped_pre  = []
            for folder_f in files:
                _, fname = folder_f
                stem = os.path.splitext(fname)[0]
                if stem.endswith(RENAMED_TAG):
                    skipped_pre.append(fname)
                else:
                    to_process.append(folder_f)

        total     = len(to_process)
        completed = 0

        order_str = " → ".join(f"{k.upper()}{'✓' if en else '✗'}" for k, en in source_order)
        self.after(0, lambda: self.log_win.progress_label.configure(text=f"0 / {total}  files"))
        self.after(0, lambda: self._update_pbar(0.0))
        self.after(0, lambda: self._set_status("running…", ACCENT))
        self.after(0, lambda: self._log(
            f"▶ Starting — {total_collected} collected  |  "
            f"{len(skipped_pre)} already renamed  |  "
            f"{total} to process  |  sources: {order_str}", tag="info"))

        # Log pre-filtered skips in one burst (no cooldown)
        for fname in skipped_pre:
            self.after(0, lambda f=fname: self._log(f"⏭   {f}  Skipped (already renamed [r]).", tag="skip"))

        # Thread-safe accumulators
        failures     = []
        failures_lock = threading.Lock()
        log_entries  = []
        log_lock     = threading.Lock()

        def process_one(folder_f):
            nonlocal completed
            if self._stop_event.is_set():
                return
            fldr, fname = folder_f
            result = self.renamer.process_file(fldr, fname, source_order=source_order, force_rename=force_rename)
            if self._stop_event.is_set():
                return
            completed += 1
            pct = completed / total if total else 1.0

            # result format: (kind, base_id, orig_name, new_name, [source_or_error])
            kind      = result[0]
            base_id   = result[1] if len(result) > 1 else ""
            orig_name = result[2] if len(result) > 2 else fname
            new_name  = result[3] if len(result) > 3 else ""

            if kind == "ok":
                source   = result[4] if len(result) > 4 else ""
                msg, tag = f"✅  {base_id}  Renamed.", "ok"
                entry    = {"id": base_id, "original": orig_name, "renamed": new_name,
                            "status": "Success", "reason": "", "source": source}
            elif kind == "stopped":
                return
            elif kind == "skip_exists":
                msg, tag = f"⏭   {base_id}  Skipped (name already exists).", "skip"
                entry    = {"id": base_id, "original": orig_name, "renamed": new_name,
                            "status": "Skipped", "reason": "Name already exists", "source": ""}
            elif kind == "skipped_renamed":
                msg, tag = f"⏭   {base_id}  Skipped (already renamed [r]).", "skip"
                entry    = {"id": base_id, "original": orig_name, "renamed": orig_name,
                            "status": "Skipped", "reason": "Already renamed [r]", "source": ""}
            elif kind == "error":
                reason   = result[4] if len(result) > 4 else "Unknown error"
                msg, tag = f"❌  {base_id}  Error: {reason}", "error"
                entry    = {"id": base_id, "original": orig_name, "renamed": "",
                            "status": "Fail", "reason": f"Error — {reason}", "source": ""}
                with failures_lock:
                    failures.append((orig_name, f"Error — {reason}"))
            elif kind == "not_found":
                msg, tag = f"⚠   {base_id}  Not found.", "notfound"
                entry    = {"id": base_id, "original": orig_name, "renamed": "",
                            "status": "Fail", "reason": "Metadata not found on any source", "source": ""}
                with failures_lock:
                    failures.append((orig_name, "Metadata not found on any source"))
            else:
                msg, tag = f"—   {fname}  No ID matched.", "notfound"
                entry    = {"id": "", "original": fname, "renamed": "",
                            "status": "Fail", "reason": "No JAV ID pattern matched", "source": ""}
                with failures_lock:
                    failures.append((fname, "No JAV ID pattern matched"))

            with log_lock:
                log_entries.append(entry)

            label_txt = f"{completed} / {total}  files"
            self.after(0, lambda m=msg, t=tag: self._log(m, tag=t))
            self.after(0, lambda p=pct: self._update_pbar(p))
            self.after(0, lambda lt=label_txt: self.log_win.progress_label.configure(text=lt))
            self.renamer._sleep(random.uniform(FILE_COOLDOWN_MIN, FILE_COOLDOWN_MAX))

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for item in to_process:
                if self._stop_event.is_set():
                    break
                ex.submit(process_one, item)

        self.after(0, lambda: self._on_done(failures, log_entries))

    def _on_done(self, failures=None, log_entries=None):
        self._running = False
        self.log_win.set_running(False)
        self.run_btn.configure(state="normal", bg=ACCENT, text="  ▶  START RENAMING  ")
        self.stop_btn.configure(state="disabled", text="  ■  STOP  ")
        if self._stop_event.is_set():
            self._set_status("stopped ◼", ERROR_CLR)
            self._log("◼  Stopped by user.", tag="error")
        else:
            self._set_status("done ✓", SUCCESS)
            self._log("✔  All done.", tag="ok")

        # ── Write CSV renaming log ───────────────────────────────────────
        if log_entries:
            self._write_csv_log(log_entries)

        # ── Failure summary ─────────────────────────────────────────────
        if failures:
            self._log("", tag="info")
            self._log("─" * 52, tag="error")
            self._log(f"  ❌  FAILED TO RENAME — {len(failures)} file(s)", tag="error")
            self._log("─" * 52, tag="error")
            for fname, reason in failures:
                self._log(f"  • {fname}", tag="error")
                self._log(f"    ↳ {reason}", tag="notfound")
            self._log("─" * 52, tag="error")

    def _write_csv_log(self, entries):
        """Append renaming results to the Master_Log.csv — never overwrites existing records."""
        import csv
        from datetime import datetime

        CSV_PATH = os.path.join(LOG_DIR, "Master_Log.csv")
        FIELDS   = ["ID", "Original Name", "Renamed To", "Status", "Reason", "Source", "Processed At"]
        timestamp = datetime.now().strftime("%d/%m/%Y:%H/%M/%S")
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            file_exists = os.path.isfile(CSV_PATH)
            with open(CSV_PATH, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                if not file_exists:
                    writer.writeheader()
                for e in entries:
                    writer.writerow({
                        "ID":             e.get("id", ""),
                        "Original Name":  e.get("original", ""),
                        "Renamed To":     e.get("renamed", ""),
                        "Status":         e.get("status", ""),
                        "Reason":         e.get("reason", ""),
                        "Source":         e.get("source", ""),
                        "Processed At":   timestamp,
                    })
            self._log(f"📄  Master log updated → {CSV_PATH}", tag="info")
        except Exception as ex:
            self._log(f"⚠  Could not save Master_Log.csv: {ex}", tag="error")


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    app = App()
    def _on_close():
        app.renamer.close()   # shut down Chrome driver if running
        app.destroy()
    app.protocol("WM_DELETE_WINDOW", _on_close)
    app.mainloop()
