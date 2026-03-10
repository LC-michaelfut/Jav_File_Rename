# JAV File Renamer

A desktop application for automatically renaming Japanese Adult Video (JAV) files using metadata scraped from online databases.

> ⚠ **Compatibility Notice**
> This program works with **manufacturer-censored JAVs only** — titles that carry a studio catalogue number (番号), such as `MIGD-123`, `DASD-456`, `PIYO-114`, `HNDS-011`, `DOKS-612`, etc.
>
> It will **not** work for manufacturer-uncensored titles from studios such as **i-pondo, Heyzo, Caribbeancom, 1Pondo**, etc. These titles use non-standard ID formats that are not supported by the metadata scrapers.

---

## Features

> Supports **censored JAVs with studio 番号** (e.g. MIGD, DASD, PIYO) only. Uncensored studios (i-pondo, Heyzo, etc.) are not supported.

- **Multi-source scraping** — JAVLibrary, AVMOO, JAVDB with draggable priority ordering
- **Cloudflare bypass** — Uses `undetected-chromedriver` with a real Chrome browser to access JAVLibrary
- **Smart ID extraction** — Supports standard (`PIYO-114`), no-dash (`PIYO114`), site-prefixed (`kpxvs.com-PIYO114`), and `T##-###` formats
- **Multi-part detection** — Recognises `A/B/C` or `_1/_2/_3` suffixes and converts them to a unified `_1/_2/_3` format
- **Rename indicator** — Appends `[r]` to successfully renamed files; skips already-renamed files by default
- **Force Rename** — Re-processes `[r]`-tagged files when needed
- **Multi-selection** — Select multiple folders and/or individual files via a custom picker dialog
- **步兵 skip toggle** — Excludes files inside any folder named `步兵` (ON by default)
- **Separate log window** — Resizable progress & verbose log window that opens on start
- **Cloudflare monitor** — Checks every 60 seconds if Chrome needs a manual human verification and pops up an alert
- **Master log** — Appends every run's results to a persistent `Master_Log.csv`
- **Failure summary** — Displays a per-run summary of all failed files and their reasons
- **Stop button** — Instantly interruptible via a shared threading event

---

## Requirements

### Python
Python 3.9+ (tested on 3.12)

### Python packages
```
requests
beautifulsoup4
undetected-chromedriver
selenium
setuptools
tkinterdnd2          # optional — enables drag & drop folder/file support
```

Install all required packages:
```bash
pip install requests beautifulsoup4 undetected-chromedriver selenium setuptools
```

Install optional drag & drop support:
```bash
pip install tkinterdnd2
```

### Google Chrome
A working installation of **Google Chrome** is required for JAVLibrary scraping (Cloudflare bypass).

Download: https://www.google.com/chrome/

---

## Installation

```bash
git clone https://github.com/YOUR_USERNAME/jav-file-renamer.git
cd jav-file-renamer
pip install -r requirements.txt
python Jav_File_Rename.py
```

---

## Usage

1. Launch the app: `python Jav_File_Rename.py`
2. Click **📂 Browse…** to select one or more folders and/or individual files
3. Configure sources and options in the **OPTIONS** panel
4. Click **▶ START RENAMING**
   - If JAVLibrary is enabled, Chrome will launch automatically and warm up
   - If a Cloudflare challenge appears, a pop-up notification will remind you to complete it manually
5. The **Progress & Log** window will open showing real-time verbose output
6. After completion, results are appended to `Master_Log.csv`

---

## Output Format

Successfully renamed files follow this pattern:
```
{ID}{part_suffix} {actors} ｜ {title}[r].{ext}
```

Examples:
```
PIYO-114 Actress Name ｜ Movie Title[r].mp4
T28-504 Actress Name ｜ Movie Title[r].mp4
PPT-090_1 Actress Name ｜ Movie Title[r].mp4   ← multi-part file
```

---

## Source Priority

Sources are shown in a drag-and-drop list. Drag the `⠿` handle to reorder. Each source can be toggled ON/OFF independently.

| Source | Default | Status |
|--------|---------|--------|
| JAVLibrary | ✅ ON (1st) | Available |
| AVMOO | ❌ OFF (2nd) | Available |
| JAVDB | ❌ OFF (3rd) | **Under development — coming in next update.** Toggle is locked and greyed out. |

---

## Options

| Option | Default | Description |
|--------|---------|-------------|
| Include Sub-folders | ON | Recursively scan subdirectories |
| Force Rename | OFF | Re-process files already marked `[r]` |
| Skip 步兵 Folders | ON | Exclude files inside folders named `步兵` |

---

## Master Log

All runs are appended to a single `Master_Log.csv` file. The storage path is configured via the `LOG_DIR` constant near the top of `Jav_File_Rename.py`:

```python
# ─────────────────────────────────────────────
#  USER CONFIG  ← edit this section to customise the app
# ─────────────────────────────────────────────

# Path where Master_Log.csv will be stored.
# Change this to any folder you prefer — the folder will be created automatically.
LOG_DIR = r"C:\Users\Administrator\Desktop\Python Projects\Renaming log"
```

**To change the log location**, edit `LOG_DIR` — it's the only line you need to touch. The folder will be created automatically if it doesn't exist.

Default path:
```
C:\Users\Administrator\Desktop\Python Projects\Renaming log\Master_Log.csv
```

Columns: `ID` · `Original Name` · `Renamed To` · `Status` · `Reason` · `Source` · `Processed At`

---

## Cloudflare Notes

JAVLibrary uses Cloudflare Bot Management. The app handles this by:
- Launching a real Chrome browser via `undetected-chromedriver`
- Reusing the same Chrome session across all files (avoids repeated CF challenges)
- Monitoring every 60 seconds and alerting you if a challenge appears mid-run
- Checking for an active CF challenge when **▶ START RENAMING** is clicked

If Chrome version mismatches cause errors, the app reads your installed Chrome version from the Windows registry and passes `version_main` to force the correct ChromeDriver download.

---

## File Structure

```
jav-file-renamer/
├── Jav_File_Rename.py   # Main application
├── requirements.txt
├── .gitignore
└── README.md
```

---

## License

MIT License — see [LICENSE](LICENSE)
