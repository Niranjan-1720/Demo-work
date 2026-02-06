
# NREL Wind Toolkit (WTK) – Local Ingestion Pipeline (Airflow + MySQL)

Run entirely on your Windows machine (via Docker Desktop) or Linux/Mac. Author, run, and test in VS Code.

## What you get
- Python ingestion library to call NREL Wind Toolkit APIs (build URLs/params, CSV direct & async two-step)
- Integrated rate limiting and in-flight request cap
- Local storage of raw CSV in `data/raw/`
- MySQL schema for raw, cleansed, and aggregated tables + loaders
- Airflow DAG with tasks: **extract → load_raw → transform → aggregate → quality**
- Daily schedule & backfill-ready configuration

---
## 1) Prereqs
- **Docker Desktop** installed and running (Windows/macOS/Linux)
- **VS Code**
- **NREL API key** from https://developer.nrel.gov/signup/

> Windows native Airflow is not supported; use Docker/WSL. This project uses Docker Compose (official Airflow images).

---
## 2) Configure environment
1. Copy `.env.example` → `.env` and fill in:
   - `NREL_API_KEY`
   - Your user metadata (for async jobs): `USER_FULL_NAME`, `USER_EMAIL`, `USER_AFFILIATION`, `USER_REASON`
   - Choose dataset via `WTK_DATASET_PATH` (e.g., `wind-toolkit/v2/wind/india-wind-download` or `wind-toolkit/v2/wind/wtk-download`)
   - `WKT` (POINT lon lat) – e.g., `POINT(77.5946 12.9716)`
   - `ATTRIBUTES`, `YEARS`, `INTERVAL`, `UTC`, `LEAP_DAY`
   - MySQL creds (already defaulted for local compose)

---
## 3) Start stack
```bash
# From repository root
docker compose up -d airflow-init
# then
docker compose up -d
```
- Airflow UI: http://localhost:8080 (user: `airflow` / pass: `airflow`)
- MySQL: `localhost:3306`, db=`wtk`, user=`wtk`, pass=`wtk_password`

The DAG `nrel_wtk_ingest` will be visible. Unpause it to run on schedule; or trigger manually.

---
## 4) Develop & run in VS Code (without Airflow)
Create a Python virtual env, then:
```bash
pip install -r requirements.txt
cp .env.example .env  # and fill values
python run_once.py
```
This downloads a CSV for the configured year and WKT and loads→transforms→aggregates in MySQL.

---
## 5) Backfill
Set `start_date` and `catchup=True` in `airflow/dags/wtk_download_dag.py` (or use the UI to backfill). You can also set `YEARS` to a comma list to request multiple CSVs.

---
## Notes on limits
- CSV (single point/year) lane enforces **1 req/sec**, **10,000/day**, **≤20 in-flight**.
- Non-CSV (async) lane enforces **1 req/2 sec**, **2000/day**, **≤20 in-flight**.

---
## Troubleshooting
- If Airflow containers keep restarting, ensure Docker has ≥4GB RAM.
- On Windows, keep everything inside your user folder to avoid permission issues.
- If MySQL port 3306 is busy, stop other MySQL services or change the mapped port in compose.

---
## Security
- API key is **not** hard-coded; injected via environment variables.

---
## References
- NREL Wind Toolkit API docs (WTK V2, India, guide) – see the accompanying write-up in your workspace or code comments.
