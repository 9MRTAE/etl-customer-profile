"""
flows/flow_factory.py
=====================
Prefect v3 flow factory — mirrors etl-urb-customer pattern.

make_flow(cfg: FlowConfig) → @flow function
ALL_FLOWS: dict[str, Callable] — keyed by dwh_table name
"""

import os
from typing import Callable

from prefect import flow, get_run_logger

from config_flows import (
    APPLICATION_TYPE, CRON_SCHEDULE,
    JOB_YEAR, JOB_MONTH, JOB_DAY, JOB_FULL_DATA,
    PREFECT_WORK_POOL, PREFECT_WORK_QUEUE, PREFECT_IMAGE,
)
from flows.flow_registry import FLOW_REGISTRY
from flows.flow_config import FlowConfig
from tasks.tasks import extract_lake, extract_lake_join, extract_dwh, load


def make_flow(cfg: FlowConfig) -> Callable:
    """
    Build a Prefect v3 @flow for a given FlowConfig.
    The flow name is APPLICATION_TYPE + '_' + dwh_table.
    """
    flow_name = f"{APPLICATION_TYPE}_{cfg.dwh_table}"

    @flow(name=flow_name)
    def _flow(
        job_year:      str = JOB_YEAR,
        job_month:     str = JOB_MONTH,
        job_day:       str = JOB_DAY,
        job_full_data: str = cfg.default_full_data,
    ) -> None:
        logger = get_run_logger()
        logger.info(f"START {flow_name} | year={job_year} month={job_month} day={job_day} full={job_full_data}")

        # ── Extract all sources ───────────────────────────────────────────────
        p_data = []
        for src in cfg.sources:
            if src.is_dwh:
                df = extract_dwh(src.query)
            elif src.is_join:
                df = extract_lake_join(
                    p_source_type=src.source_type,
                    p_tablename=src.tablename,
                    p_groupby=src.groupby,
                    p_bucket_app=src.bucket_app,
                    p_columns=list(src.columns) if src.columns else None,
                )
            else:
                df = extract_lake(
                    p_source_type=src.source_type,
                    p_tablename=src.tablename,
                    p_bucket_app=src.bucket_app,
                    p_year=job_year,
                    p_month=job_month,
                    p_day=job_day,
                    p_columns=list(src.columns) if src.columns else None,
                    p_full_data=job_full_data,
                )
            p_data.append(df)

        # ── Transform ────────────────────────────────────────────────────────
        df_transformed = cfg.transform_fn(p_data)

        # ── Load ─────────────────────────────────────────────────────────────
        load(
            p_dataframe=df_transformed,
            p_table=cfg.dwh_table,
            p_primary_key=list(cfg.pk),
            p_schema=cfg.schema,
            p_insert=cfg.p_insert,
        )
        logger.info(f"END {flow_name}")

    return _flow


# ─── Build ALL_FLOWS dict from FLOW_REGISTRY ─────────────────────────────────
# Note: multiple FlowConfig may share the same dwh_table (e.g. dim_party has 4 sub-flows).
# We append suffix _<n> for duplicates so each has a unique dict key.
_seen: dict[str, int] = {}
ALL_FLOWS: dict[str, Callable] = {}

for _cfg in FLOW_REGISTRY:
    _base = _cfg.dwh_table
    _count = _seen.get(_base, 0)
    _seen[_base] = _count + 1
    _key = _base if _count == 0 else f"{_base}_{_count}"
    ALL_FLOWS[_key] = make_flow(_cfg)
