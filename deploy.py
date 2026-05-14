"""
deploy.py
=========
Register all flows in etl-pms-customer to Prefect Server.

Usage:
    python deploy.py          # register with schedule active (main branch)
    python deploy.py --paused # register all schedules as paused (maintenance)

CI/CD: called by scripts/register.sh inside the Docker container.
"""

import argparse
import os

from prefect.client.schemas.schedules import CronSchedule

from config_flows import (
    APP_ENV, APPLICATION_TYPE, CRON_SCHEDULE,
    PREFECT_WORK_POOL, PREFECT_WORK_QUEUE, PREFECT_IMAGE,
)
from flows.flow_registry import FLOW_REGISTRY
from flows.flow_factory import ALL_FLOWS

parser = argparse.ArgumentParser()
parser.add_argument("--paused", action="store_true", help="Deploy all schedules as paused")
args = parser.parse_args()

IS_PRODUCTION = APP_ENV == "main"
IS_PAUSED = args.paused or not IS_PRODUCTION


def get_cron(cfg) -> str | None:
    """Priority: cron_override → CRON_SCHEDULE → None (develop)"""
    if not IS_PRODUCTION:
        return None
    return cfg.cron_override or CRON_SCHEDULE


# Track seen dwh_table to match ALL_FLOWS key generation
_seen: dict[str, int] = {}

for cfg in FLOW_REGISTRY:
    _base = cfg.dwh_table
    _count = _seen.get(_base, 0)
    _seen[_base] = _count + 1
    _key = _base if _count == 0 else f"{_base}_{_count}"

    flow_fn = ALL_FLOWS[_key]
    flow_name = f"{APPLICATION_TYPE}_{cfg.dwh_table}"
    cron = get_cron(cfg)

    schedules = []
    if cron and not IS_PAUSED:
        schedules = [CronSchedule(cron=cron, timezone="UTC")]

    flow_fn.deploy(
        name=flow_name,
        work_pool_name=PREFECT_WORK_POOL,
        work_queue_name=PREFECT_WORK_QUEUE,
        image=PREFECT_IMAGE,
        schedules=schedules,
        tags=[APP_ENV, "etl-pms-customer", cfg.dwh_table],
    )
    schedule_info = cron if cron else "paused"
    print(f"  deployed: {flow_name}  schedule={schedule_info}")

print(f"\nRegistered {len(FLOW_REGISTRY)} deployments to Prefect ({APP_ENV})")
