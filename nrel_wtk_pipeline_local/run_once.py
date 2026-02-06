
import os
from app.config import settings
from app.rate_limit import RateLimiter
from app.nrel_client import NRELClient
from app.mysql_loader import load_csv_to_raw, transform_to_cleansed, aggregate_daily, quality_checks

if __name__ == '__main__':
    print('Settings:', settings)
    limiter = RateLimiter(settings.rate_state_file, in_flight_limit=20)
    client = NRELClient(limiter)
    years = [int(y) for y in str(settings.years).split(',') if y]
    for y in years:
        f = client.download_csv_point_year(y, settings.raw_dir)
        print('Downloaded:', f)
        load_csv_to_raw(f)
    transform_to_cleansed()
    aggregate_daily()
    quality_checks()
    print('Done.')
