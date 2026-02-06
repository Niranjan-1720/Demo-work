
# config/settings.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env at project root
load_dotenv()

# --- Project & Data Dirs ---
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parent.parent))
DATA_DIR = Path(os.getenv("WTK_OUT_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = RAW_DIR / "processed"
EXTRACT_DIR = RAW_DIR / "extracted"

# Ensure directories exist
for d in (DATA_DIR, RAW_DIR, PROCESSED_DIR, EXTRACT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# --- MySQL Settings ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "nkr")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "neeru@143")
MYSQL_DB = os.getenv("MYSQL_DB", "neeru")

# Optional: enable faster loading via LOAD DATA LOCAL INFILE
MYSQL_LOCAL_INFILE = os.getenv("MYSQL_LOCAL_INFILE", "false").lower() in {"1", "true", "yes"}
