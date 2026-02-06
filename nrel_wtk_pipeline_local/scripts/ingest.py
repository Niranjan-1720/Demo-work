
import os
import sys
import json
from pathlib import Path
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NREL_API_KEY")
EMAIL = os.getenv("NREL_EMAIL")  # Add your email in .env
WKT = os.getenv("WTK_WKT", "POINT(-104.9903 39.7392)")  # Example: Denver, CO
YEARS = os.getenv("WTK_YEARS", "2013")  # Example: single year or comma-separated
OUT_DIR = Path(os.getenv("WTK_OUT_DIR", "data"))

if not API_KEY or not EMAIL:
    print("[ERROR] Missing API key or email in .env")
    sys.exit(1)

url = "https://developer.nrel.gov/api/wind-toolkit/v2/wind/wtk-download.json"
params = {
    "api_key": API_KEY,
    "email": EMAIL,
    "wkt": WKT,
    "years": YEARS
}

print(f"[INFO] Requesting: {url}")
print(f"[INFO] Params: {params}")

response = requests.get(url, params=params)

print(f"[INFO] HTTP {response.status_code}")
if response.status_code == 200:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = OUT_DIR / f"wtk_raw_{ts}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"[INFO] Data saved to {out_path}")
    
else:
    print("[ERROR] Failed:", response.text)

# After saving metadata JSON
metadata = response.json()
download_url = metadata.get("outputs", {}).get("downloadUrl")

if download_url:
    print(f"[INFO] Download URL: {download_url}")
    # Download the actual data file
    file_resp = requests.get(download_url, stream=True)
    if file_resp.status_code == 200:
        zip_path = OUT_DIR / f"wtk_data_{ts}.zip"
        with open(zip_path, "wb") as f:
            for chunk in file_resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[INFO] Data file downloaded: {zip_path}")
    else:
        print(f"[ERROR] Failed to download data file: {file_resp.status_code}")
else:
    print("[WARN] No download URL found in response.")
