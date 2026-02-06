
import os
import time
from typing import Dict
import requests
from .config import settings
from .rate_limit import RateLimiter, RequestType
from .storage import ensure_dirs, raw_file_path, parse_point_wkt

BASE_URL = 'https://developer.nrel.gov/api'

class NRELClient:
    def __init__(self, limiter: RateLimiter):
        self.limiter = limiter

    def _dataset_slug(self) -> str:
        # e.g., wind-toolkit/v2/wind/india-wind-download -> india-wind-download
        return settings.dataset_path.strip('/').split('/')[-1]

    def build_params(self, year: int) -> Dict[str, str]:
        params = {
            'api_key': settings.api_key,
            'wkt': settings.wkt,
            'attributes': settings.attributes,
            'names': str(year),
            'interval': str(settings.interval),
            'utc': settings.utc,
            'leap_day': settings.leap_day,
        }
        return {k: v for k, v in params.items() if v not in (None, '', [])}

    def download_csv_point_year(self, year: int, out_dir: str) -> str:
        """Direct CSV path (single POINT, single year)."""
        params = self.build_params(year)
        # CSV direct
        path = f"/{settings.dataset_path}.csv"
        url = f"{BASE_URL}/{path.lstrip('/')}"

        # Rate limit: CSV lane
        self.limiter.acquire(RequestType.CSV)
        try:
            resp = requests.get(url, params=params, stream=True, timeout=60)
            resp.raise_for_status()
            lon, lat = parse_point_wkt(settings.wkt)
            ensure_dirs(out_dir)
            out_f = raw_file_path(out_dir, self._dataset_slug(), year, lon, lat)
            with open(out_f, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=1024*64):
                    if chunk:
                        f.write(chunk)
            return out_f
        finally:
            self.limiter.release(RequestType.CSV)

    def request_async_zip(self, years: str) -> Dict:
        """Initiate asynchronous request (JSON ack). Often delivers downloadUrl by email."""
        params = {
            'api_key': settings.api_key,
            'wkt': settings.wkt,
            'attributes': settings.attributes,
            'names': years,
            'interval': str(settings.interval),
            'utc': settings.utc,
            'leap_day': settings.leap_day,
            'full_name': settings.full_name,
            'email': settings.email,
            'affiliation': settings.affiliation,
            'reason': settings.reason,
        }
        url = f"{BASE_URL}/{settings.dataset_path}.json"
        self.limiter.acquire(RequestType.NONCSV)
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        finally:
            self.limiter.release(RequestType.NONCSV)
