"""
flows/flow_config.py
====================
Standalone dataclasses for FlowConfig and SourceConfig.
Kept separate to avoid circular imports between flow_registry.py and registry/*.py.
"""

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class SourceConfig:
    """Config for a single GCS source table."""
    tablename: str                          # GCS table name (leaf)
    source_type: str                        # sub-folder (e.g. 'postgresql', '' for MSSQL)
    bucket_app: str                         # GCS application prefix (e.g. 'pmsmanagement')
    columns: tuple[str, ...] | None = None  # None = all columns
    groupby: tuple[str, ...] | None = None  # set for extract_lake_join
    is_join: bool = False                   # True → extract_lake_join (deduplicated latest)
    is_dwh: bool = False                    # True → extract_dwh (SQL query)
    query: str = ""                         # SQL for is_dwh=True sources


@dataclass(frozen=True)
class FlowConfig:
    """Config for one ETL flow → one BigQuery target table."""
    dwh_table: str                          # BQ table name (without schema)
    pk: tuple[str, ...]                     # primary key columns for upsert
    sources: tuple[SourceConfig, ...]       # ordered: matches transform p_data[i]
    transform_fn: Callable                  # @task transform function
    schema: str = "los"                     # BQ schema
    default_full_data: str = "0"            # "0"=incremental, "1"=full scan
    p_insert: int = 0                       # 0=upsert, 1=append
    cron_override: str | None = None        # per-flow schedule override
    origin: str = ""                        # legacy v1 repo reference
