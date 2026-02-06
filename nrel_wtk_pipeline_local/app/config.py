
import os
from dataclasses import dataclass

@dataclass
class Settings:
    api_key: str = os.getenv('NREL_API_KEY', '')
    dataset_path: str = os.getenv('WTK_DATASET_PATH', 'wind-toolkit/v2/wind/wtk-download')
    wkt: str = os.getenv('WKT', '')
    attributes: str = os.getenv('ATTRIBUTES', '')
    years: str = os.getenv('YEARS', '')
    interval: int = int(os.getenv('INTERVAL', '60'))
    utc: str = os.getenv('UTC', 'true')
    leap_day: str = os.getenv('LEAP_DAY', 'false')

    # Async/metadata
    full_name: str = os.getenv('USER_FULL_NAME', '')
    email: str = os.getenv('USER_EMAIL', '')
    affiliation: str = os.getenv('USER_AFFILIATION', '')
    reason: str = os.getenv('USER_REASON', '')

    # Storage
    data_dir: str = os.getenv('DATA_DIR', './data')
    raw_dir: str = os.getenv('RAW_DIR', './data/raw')
    processed_dir: str = os.getenv('PROCESSED_DIR', './data/processed')

    # Rate limiting
    rate_state_file: str = os.getenv('RATE_STATE_FILE', './data/rate_state.json')

    # MySQL
    mysql_host: str = os.getenv('MYSQL_HOST', 'localhost')
    mysql_port: int = int(os.getenv('MYSQL_PORT', '3306'))
    mysql_user: str = os.getenv('MYSQL_USER', 'wtk')
    mysql_password: str = os.getenv('MYSQL_PASSWORD', 'wtk_password')
    mysql_db: str = os.getenv('MYSQL_DB', 'wtk')

settings = Settings()
