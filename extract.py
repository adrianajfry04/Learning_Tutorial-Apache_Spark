"""
extract.py — Download Brazilian School Census (Censo Escolar) data.

Usage:
    python extract.py           # downloads the default year (2021)
    python extract.py 2020      # downloads a specific year
"""

import os
import sys
import zipfile
import requests

# ── Config ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "data")

DEFAULT_YEAR = 2021
BASE_URL     = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{year}.zip"

# The main CSV inside the zip lives under: {year}/dados/ESCOLAS.CSV
# (the exact filename may vary by year, but ESCOLAS is always present)
TARGET_CSV_NAME = "ESCOLAS.CSV"
OUTPUT_CSV      = os.path.join(DATA_DIR, TARGET_CSV_NAME)


def download_file(url: str, dest: str) -> None:
    """Stream-download a file with a simple progress indicator."""
    print(f"  Downloading: {url}")
    with requests.get(url, stream=True, verify=False, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  Progress: {pct:3d}%  ({downloaded >> 20} MB / {total >> 20} MB)", end="", flush=True)
    print()


def extract_schools_csv(zip_path: str, year: int) -> None:
    """Find and extract the ESCOLAS CSV from the downloaded zip."""
    print(f"  Extracting from: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as z:
        # Find any file whose name matches TARGET_CSV_NAME (case-insensitive)
        candidates = [
            name for name in z.namelist()
            if os.path.basename(name).upper() == TARGET_CSV_NAME
        ]
        if not candidates:
            sys.exit(
                f"[ERROR] Could not find '{TARGET_CSV_NAME}' inside the zip.\n"
                f"        Contents: {z.namelist()[:20]}"
            )

        chosen = candidates[0]
        print(f"  Found: {chosen}  →  {OUTPUT_CSV}")
        with z.open(chosen) as src, open(OUTPUT_CSV, "wb") as dst:
            dst.write(src.read())


def main():
    year = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_YEAR
    url  = BASE_URL.format(year=year)
    zip_dest = os.path.join(DATA_DIR, f"microdados_censo_escolar_{year}.zip")

    os.makedirs(DATA_DIR, exist_ok=True)

    # ── Step 1: check if CSV already exists ──────────────────────────────────
    if os.path.isfile(OUTPUT_CSV):
        size_mb = os.path.getsize(OUTPUT_CSV) >> 20
        print(f"[✓] Data already present: {OUTPUT_CSV}  ({size_mb} MB)")
        print("    Delete it and re-run to download fresh data.")
        return

    # ── Step 2: download zip ──────────────────────────────────────────────────
    if not os.path.isfile(zip_dest):
        print(f"[1/2] Downloading {year} census data...")
        try:
            download_file(url, zip_dest)
        except requests.HTTPError as e:
            sys.exit(f"[ERROR] Download failed: {e}\n"
                     f"        Try a different year, e.g.  python extract.py 2020")
    else:
        print(f"[1/2] Zip already downloaded: {zip_dest}")

    # ── Step 3: extract CSV ──────────────────────────────────────────────────
    print(f"[2/2] Extracting {TARGET_CSV_NAME}...")
    extract_schools_csv(zip_dest, year)

    # Clean up the large zip to save disk space
    os.remove(zip_dest)
    print(f"      Removed zip to free disk space.")

    size_mb = os.path.getsize(OUTPUT_CSV) >> 20
    print(f"\n[✓] Done!  {OUTPUT_CSV}  ({size_mb} MB)")
    print("    Run  python transform_load.py  to load data into PostgreSQL.")


if __name__ == "__main__":
    main()