
# scripts/mysql_load.py
import csv
import re
import sys
import zipfile
from pathlib import Path
from typing import List, Iterable

# Try to import settings; if not, add project root to sys.path
try:
    from config import settings
except ModuleNotFoundError:
    import sys as _sys
    from pathlib import Path as _Path
    root = _Path(__file__).resolve().parent.parent
    _sys.path.append(str(root))
    from config import settings

import mysql.connector


# ------------------ Utils ------------------
def sanitize_identifier(name: str) -> str:
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name.strip())
    if re.match(r"^[0-9]", name):
        name = "_" + name
    return name[:64]


def infer_mysql_type(sample_values: List[str]) -> str:
    def is_int(v: str) -> bool:
        try:
            int(v)
            return True
        except:
            return False

    def is_float(v: str) -> bool:
        try:
            float(v)
            return not is_int(v)
        except:
            return False

    clean = [v for v in sample_values if v not in (None, "", "NA")]
    if not clean:
        return "TEXT"
    ints = sum(is_int(v) for v in clean)
    floats = sum(is_float(v) for v in clean)
    if ints == len(clean):
        return "INT"
    if ints + floats == len(clean):
        return "DOUBLE"
    return "TEXT"


def connect_mysql():
    try:
        conn = mysql.connector.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DB,
            autocommit=False,
            allow_local_infile=settings.MYSQL_LOCAL_INFILE,
        )
        return conn
    except mysql.connector.Error as e:
        print(f"[ERROR] MySQL connection failed: {e}")
        sys.exit(1)


# ------------------ ZIP & CSV ------------------
def find_latest_zip() -> Path:
    # Search in data/raw and data root for wtk_data_*.zip
    candidates = []
    candidates.extend(settings.RAW_DIR.glob("wtk_data_*.zip"))
    candidates.extend(settings.DATA_DIR.glob("wtk_data_*.zip"))
    candidates = sorted(candidates, reverse=True)
    if not candidates:
        print(f"[ERROR] No ZIPs found in {settings.RAW_DIR} or {settings.DATA_DIR}.")
        sys.exit(1)
    return candidates[0]


def unzip_data(zip_path: Path, extract_to: Path) -> None:
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)
    print(f"[INFO] Extracted: {zip_path} -> {extract_to}")


def find_csv_files(root: Path) -> List[Path]:
    return [p for p in root.rglob("*.csv")]


# ------------------ Row length safety helpers ------------------
def fix_row_to_header(row: List[str], header_len: int) -> List[str]:
    """Ensure row has exactly header_len columns: truncate extras, pad missing with empty strings."""
    if len(row) > header_len:
        return row[:header_len]
    if len(row) < header_len:
        return row + [""] * (header_len - len(row))
    return row


def safe_reader_rows(reader: Iterable[List[str]], header_len: int):
    """
    Yield rows fixed to header length.
    Tracks and logs how many rows were truncated/padded.
    """
    truncated = 0
    padded = 0
    total = 0
    for row in reader:
        total += 1
        if len(row) != header_len:
            # Decide strategy: fix (truncate/pad) instead of skip
            fixed = fix_row_to_header(row, header_len)
            truncated += int(len(row) > header_len)
            padded += int(len(row) < header_len)
            yield fixed
        else:
            yield row
    print(f"[INFO] Row normalization: total={total}, truncated={truncated}, padded={padded}")


# ------------------ DDL & Load ------------------
def create_table_if_not_exists(conn, table_name: str, headers: List[str], sample_rows: List[List[str]]):
    cursor = conn.cursor()

    # Ensure sample rows are safe length
    header_len = len(headers)
    normalized_samples = [fix_row_to_header(r, header_len) for r in sample_rows]

    # Collect samples per column
    samples_per_col = [[] for _ in headers]
    for row in normalized_samples[:100]:
        for i, val in enumerate(row):
            samples_per_col[i].append(val)

    col_defs = []
    for h, samples in zip(headers, samples_per_col):
        col_name = sanitize_identifier(h)
        col_type = infer_mysql_type(samples)
        col_defs.append(f"`{col_name}` {col_type}")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(col_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        cursor.execute(ddl)
        conn.commit()
        print(f"[INFO] Ensured table exists: {table_name}")
    except mysql.connector.Error as e:
        print(f"[ERROR] Failed to create table `{table_name}`: {e}")
        conn.rollback()
        cursor.close()
        sys.exit(1)
    cursor.close()


def bulk_insert(conn, table_name: str, headers: List[str], rows_iter, chunk_size: int = 5000):
    cursor = conn.cursor()
    cols = ", ".join([f"`{sanitize_identifier(h)}`" for h in headers])
    placeholders = ", ".join(["%s"] * len(headers))
    sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"

    batch = []
    total = 0
    for row in rows_iter:
        batch.append(row)
        if len(batch) >= chunk_size:
            try:
                cursor.executemany(sql, batch)
                conn.commit()
                total += len(batch)
                print(f"[INFO] Inserted {total} records so far...")
            except mysql.connector.Error as e:
                print(f"[ERROR] Insert failed: {e}")
                conn.rollback()
                cursor.close()
                sys.exit(1)
            batch = []

    if batch:
        try:
            cursor.executemany(sql, batch)
            conn.commit()
            total += len(batch)
            print(f"[INFO] Inserted total {total} records.")
        except mysql.connector.Error as e:
            print(f"[ERROR] Final insert failed: {e}")
            conn.rollback()
            cursor.close()
            sys.exit(1)

    cursor.close()


def load_csv_into_mysql(conn, csv_path: Path, table_name: str, delimiter: str = ","):
    print(f"[INFO] Loading CSV: {csv_path}")

    # Read header and sample rows using UTF-8 with BOM handling
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)  # header line
        header_len = len(headers)

        # Collect sample rows (normalized)
        sample_rows = []
        for i, row in enumerate(reader):
            sample_rows.append(fix_row_to_header(row, header_len))
            if i >= 200:
                break

    # Create table with safe samples
    create_table_if_not_exists(conn, table_name, headers, sample_rows)

    # Re-read CSV and insert all rows safely
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)  # skip header
        header_len = len(headers)
        normalized_iter = safe_reader_rows(reader, header_len)
        bulk_insert(conn, table_name, headers, normalized_iter)


# ------------------ Main ------------------
def main():
    latest_zip = find_latest_zip()
    unzip_data(latest_zip, settings.EXTRACT_DIR)

    csv_files = find_csv_files(settings.EXTRACT_DIR)
    if not csv_files:
        print("[ERROR] No CSV files found in extracted data.")
        sys.exit(1)

    conn = connect_mysql()
    for csv_path in csv_files:
        # table_name = sanitize_identifier(f"wtk_{csv_path.stem}")
        FIXED_TABLE_NAME = "wtk_raw_data"
        load_csv_into_mysql(conn, csv_path, FIXED_TABLE_NAME)
    conn.close()
    print("[INFO] All done.")


if __name__ == "__main__":
    main()
