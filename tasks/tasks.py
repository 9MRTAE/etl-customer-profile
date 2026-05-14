"""
tasks/tasks.py
==============
Prefect v3 generic @task wrappers — merged from:
  - etl-pmsmanagement/tasks/tasks.py
  - etl-iprop/tasks/tasks.py
  - etl-authentication/tasks/tasks.py
  - etl-homeservice/tasks/tasks.py
  - etl-mobileregister/tasks/tasks.py
  - etl-pdpa/tasks/tasks.py

Changes from Prefect v1 → v3
-----------------------------
- class Extract_Lake(Task): def run(...) → @task def extract_lake(...)
- prefect.context.get("logger")         → get_run_logger()
- Removed SlackTask / Alert_Notification (not supported in v3 OSS)
"""

import pandas as pd
from prefect import get_run_logger, task

from config import BUCKET_MSSQL
from tasks.main_components import ExtractSourceData, LoadSourceData


# ---------------------------------------------------------------------------
# Extract: GCS Lake (single partition or full)
# ---------------------------------------------------------------------------

@task(name="extract_lake")
def extract_lake(
    p_source_type: str,
    p_tablename: str,
    p_bucket_app: str = BUCKET_MSSQL,
    p_year: str = "None",
    p_month: str = "None",
    p_day: str = "None",
    p_columns: list | None = None,
    p_full_data: int | str = 0,
) -> pd.DataFrame:
    logger = get_run_logger()
    src = ExtractSourceData()
    df = src.fn_Get_Source_Lake(
        p_source_type, p_tablename, p_bucket_app, p_year, p_month, p_day, p_columns, p_full_data
    )
    logger.info(f"extract_lake [{p_tablename}]")
    if not df.empty:
        logger.info(f"Rows: {len(df)}, Cols: {df.shape[1]}")
        logger.info(pd.concat([df.iloc[:1], df.tail(1)]))
    return df


# ---------------------------------------------------------------------------
# Extract: GCS Lake (latest-partition join / dimension dedup)
# ---------------------------------------------------------------------------

@task(name="extract_lake_join")
def extract_lake_join(
    p_source_type: str,
    p_tablename: str,
    p_groupby,
    p_bucket_app: str = BUCKET_MSSQL,
    p_columns: list | None = None,
) -> pd.DataFrame:
    logger = get_run_logger()
    src = ExtractSourceData()
    df = src.fn_Gen_Source_Lake_Join(p_source_type, p_tablename, p_groupby, p_bucket_app, p_columns)
    logger.info(f"extract_lake_join [{p_tablename}]")
    if not df.empty:
        logger.info(f"Rows: {len(df)}, Cols: {df.shape[1]}")
    return df


# ---------------------------------------------------------------------------
# Extract: BigQuery DWH query
# ---------------------------------------------------------------------------

@task(name="extract_dwh")
def extract_dwh(p_query: str, p_schema: str = "los") -> pd.DataFrame:
    logger = get_run_logger()
    src = ExtractSourceData()
    df = src.fn_Get_DWH(p_query, p_schema)
    if not df.empty:
        logger.info(f"extract_dwh Rows: {len(df)}, Cols: {df.shape[1]}")
        logger.info(pd.concat([df.iloc[:1], df.tail(1)]))
    return df


# ---------------------------------------------------------------------------
# Load: BigQuery DWH (upsert or append)
# ---------------------------------------------------------------------------

@task(name="load")
def load(
    p_dataframe: pd.DataFrame,
    p_table: str,
    p_primary_key: list,
    p_schema: str = "los",
    p_insert: int = 0,
) -> None:
    logger = get_run_logger()
    logger.info(f"load [{p_table}] schema={p_schema} insert_mode={p_insert}")
    loader = LoadSourceData()
    loader.fn_Load_To_DWH(p_dataframe, p_table, p_primary_key, p_schema, p_insert)
