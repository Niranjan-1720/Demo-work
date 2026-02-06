
import json
import os
import threading
import time
from datetime import datetime
from enum import Enum
from pathlib import Path

class RequestType(str, Enum):
    CSV = 'csv'
    NONCSV = 'noncsv'

class RateLimiter:
    """Simple rate limiter with per-day quotas and per-request pacing, plus in-flight cap."""
    def __init__(self, state_file: str, in_flight_limit: int = 20):
        self.state_file = state_file
        self.lock = threading.Lock()
        self.in_flight_sem = threading.Semaphore(in_flight_limit)
        Path(state_file).parent.mkdir(parents=True, exist_ok=True)
        if not os.path.exists(state_file):
            with open(state_file, 'w') as f:
                json.dump({}, f)

    def _load(self):
        try:
            with open(self.state_file, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save(self, data):
        with open(self.state_file, 'w') as f:
            json.dump(data, f)

    def _get_limits(self, req_type: RequestType):
        if req_type == RequestType.CSV:
            return { 'min_interval_sec': 1.0, 'daily_quota': 10000 }
        else:
            return { 'min_interval_sec': 2.0, 'daily_quota': 2000 }

    def acquire(self, req_type: RequestType):
        limits = self._get_limits(req_type)
        today = datetime.utcnow().strftime('%Y-%m-%d')
        with self.lock:
            state = self._load()
            day = state.get(today, { 'csv_count': 0, 'noncsv_count': 0, 'last_csv_ts': 0.0, 'last_noncsv_ts': 0.0 })
            key = 'csv_count' if req_type == RequestType.CSV else 'noncsv_count'
            last_key = 'last_csv_ts' if req_type == RequestType.CSV else 'last_noncsv_ts'
            if day[key] >= limits['daily_quota']:
                raise RuntimeError(f"Daily quota exceeded for {req_type} (limit {limits['daily_quota']})")
            # Pacing
            elapsed = time.time() - float(day.get(last_key, 0.0))
            sleep_for = max(0.0, limits['min_interval_sec'] - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)
            # Update timestamps now (pre-acquire) to ensure spacing
            day[last_key] = time.time()
            state[today] = day
            self._save(state)
        # In-flight cap
        self.in_flight_sem.acquire()

    def release(self, req_type: RequestType):
        today = datetime.utcnow().strftime('%Y-%m-%d')
        with self.lock:
            state = self._load()
            day = state.get(today, { 'csv_count': 0, 'noncsv_count': 0 })
            key = 'csv_count' if req_type == RequestType.CSV else 'noncsv_count'
            day[key] = int(day.get(key, 0)) + 1
            state[today] = day
            self._save(state)
        self.in_flight_sem.release()
