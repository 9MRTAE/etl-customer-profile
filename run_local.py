"""
run_local.py
============
Local development and repair runner.

Usage:
    python run_local.py --flow all --start 2025-06-01 --end 2025-06-30
    python run_local.py --flow dim_pmscompany --start 2025-06-15
    python run_local.py --flow dim_pmscompany --full
    python run_local.py --flow dim_party_1 --start 2025-06-01  # sub-flows use _n suffix
"""

import argparse
import os
from datetime import datetime, timedelta

os.environ.setdefault("CI_COMMIT_BRANCH", "develop")

from flows.flow_factory import ALL_FLOWS

parser = argparse.ArgumentParser(description="etl-pms-customer local runner")
parser.add_argument("--flow",  default="all", help="Flow key from ALL_FLOWS, or 'all'")
parser.add_argument("--start", default=None,  help="Start date YYYY-MM-DD")
parser.add_argument("--end",   default=None,  help="End date YYYY-MM-DD (default=start)")
parser.add_argument("--full",  action="store_true", help="Full data scan (job_full_data=1)")
args = parser.parse_args()

yesterday = datetime.today() + timedelta(hours=7, days=-1)
default_year  = yesterday.strftime("%Y")
default_month = str(int(yesterday.strftime("%m")))
default_day   = str(int(yesterday.strftime("%d")))


def run_date_range(flow_key: str, start_date: str, end_date: str, full: bool):
    flow_fn = ALL_FLOWS[flow_key]
    if full:
        print(f"  [FULL] {flow_key}")
        flow_fn(job_year="None", job_month="None", job_day="None", job_full_data="1")
        return

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end     = datetime.strptime(end_date,   "%Y-%m-%d")
    while current <= end:
        y = current.strftime("%Y")
        m = str(int(current.strftime("%m")))
        d = str(int(current.strftime("%d")))
        print(f"  {flow_key}  {y}-{m}-{d}")
        flow_fn(job_year=y, job_month=m, job_day=d, job_full_data="0")
        current += timedelta(days=1)


# Resolve date range
if args.full:
    start_date = end_date = None
elif args.start:
    start_date = args.start
    end_date   = args.end or args.start
else:
    # Default: yesterday
    start_date = yesterday.strftime("%Y-%m-%d")
    end_date   = start_date

# Resolve flow list
if args.flow == "all":
    flow_keys = list(ALL_FLOWS.keys())
else:
    if args.flow not in ALL_FLOWS:
        print(f"ERROR: unknown flow '{args.flow}'. Available: {list(ALL_FLOWS.keys())}")
        exit(1)
    flow_keys = [args.flow]

print(f"Running {len(flow_keys)} flow(s) | full={args.full} | {start_date} → {end_date}\n")
for key in flow_keys:
    run_date_range(key, start_date, end_date, args.full)
print("\nDone.")
