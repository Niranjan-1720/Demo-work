
import os
from pathlib import Path
from typing import Tuple

def ensure_dirs(*dirs):
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)

def raw_file_path(raw_dir: str, dataset_slug: str, year: int, lon: float, lat: float) -> str:
    fname = f"{dataset_slug}_{year}_{lon:.4f}_{lat:.4f}.csv"
    return str(Path(raw_dir) / fname)

def parse_point_wkt(wkt: str) -> Tuple[float, float]:
    # Very small parser for 'POINT(lon lat)'
    wkt = wkt.strip()
    if not wkt.upper().startswith('POINT(') or not wkt.endswith(')'):
        raise ValueError('WKT must be POINT(lon lat)')
    inner = wkt[wkt.find('(')+1: -1]
    parts = inner.strip().split()
    lon = float(parts[0])
    lat = float(parts[1])
    return lon, lat
