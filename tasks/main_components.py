"""
tasks/main_components.py
========================
Merged from:
  - etl-pmsmanagement/tasks/main_components.py
  - etl-iprop/tasks/main_components.py
  - etl-authentication/tasks/main_components.py
  - etl-homeservice/tasks/main_components.py
  - etl-mobileregister/tasks/main_components.py
  - etl-pdpa/tasks/main_components.py

Changes from Prefect v1 → v3
-----------------------------
- prefect.context.get("logger") → get_run_logger()
- service_account.Credentials.from_service_account_file(path) →
    SA key JSON from GCP Secret Manager via config._load_sa_key_json()
- BigQuery client uses BIGQUERY_CREDENTIAL from config
- GCSFS uses STORAGE_CREDENTIAL from config
"""

import re
from datetime import datetime, timedelta
from functools import lru_cache

import gcsfs
import numpy as np
import pandas as pd
import pandas_gbq
import pyarrow.parquet as pq
from google.cloud import bigquery
from prefect import get_run_logger

from config import (
    BIGQUERY_CREDENTIAL,
    BUCKET_LAKE,
    BUCKET_MSSQL,
    BUCKET_PARQUET,
    GCSFS,
    GCP_PROJECT,
)


# ---------------------------------------------------------------------------
# ConnectorDB
# ---------------------------------------------------------------------------

class ConnectorDB:
    """Database connectors.  BigQuery uses SA credentials from Secret Manager."""

    def fn_ConnectDWH(self, p_schema: str = "los") -> bigquery.Client:
        logger = get_run_logger()
        logger.info("Connect: BigQuery DWH")
        client = bigquery.Client(credentials=BIGQUERY_CREDENTIAL, project=GCP_PROJECT)
        logger.info("Connect: BIGQUERY OK")
        return client


# ---------------------------------------------------------------------------
# ExtractSourceData
# ---------------------------------------------------------------------------

class ExtractSourceData:
    """Read Parquet partitions from GCS Data Lake and query BigQuery DWH."""

    def __init__(self):
        self.connectdb = ConnectorDB()

    # ---- BigQuery -----------------------------------------------------------

    def fn_Get_DWH(self, p_query: str = "", p_schema: str = "los") -> pd.DataFrame:
        logger = get_run_logger()
        client = self.connectdb.fn_ConnectDWH()
        logger.info(p_query)
        df = client.query(p_query).to_arrow().to_pandas()
        if not df.empty:
            logger.info(f"Extract Rows: {len(df)}, Cols: {df.shape[1]}")
        return df

    # ---- GCS Lake (main read) -----------------------------------------------

    def fn_Get_Source_Lake(
        self,
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
        df = pd.DataFrame()
        p_full_data = int(p_full_data)

        # --- date predicate ---
        v_Today     = datetime.today()
        v_Yesterday = v_Today + timedelta(hours=7, days=-1)
        v_Year_Pred  = v_Yesterday.strftime("%Y")  if p_year  == "None" else p_year
        v_Month_Pred = v_Yesterday.strftime("%m").lstrip("0") if p_year == "None" else p_month
        v_Day_Pred   = v_Yesterday.strftime("%d").lstrip("0") if p_year == "None" else p_day

        if   (p_year != "None") and (p_month != "None") and (p_day != "None"):
            v_predicate = f"/calendar_year={v_Year_Pred}/month_no={v_Month_Pred}/day_of_month={v_Day_Pred}"
        elif (p_year != "None") and (p_month != "None"):
            v_predicate = f"/calendar_year={v_Year_Pred}/month_no={v_Month_Pred}"
        elif (p_year != "None"):
            v_predicate = f"/calendar_year={v_Year_Pred}"
        else:
            v_predicate = (
                f"/calendar_year={v_Year_Pred}/month_no={v_Month_Pred}/day_of_month={v_Day_Pred}"
                if p_full_data == 0 else ""
            )

        source_seg = f"/{p_source_type}" if p_source_type else ""
        v_path = f"{BUCKET_LAKE}/{BUCKET_PARQUET}/{p_bucket_app}{source_seg}/{p_tablename}{v_predicate}"
        logger.info(f"PATH: {v_path}")

        try:
            df = pq.read_table(
                source=v_path,
                partitioning="hive",
                columns=p_columns,
                filesystem=GCSFS,
            ).to_pandas()
            df = df.drop_duplicates()

            # inject partition columns when explicit date supplied
            if (p_year != "None") and (p_month != "None") and (p_day != "None"):
                df["calendar_year"] = v_Year_Pred
                df["month_no"]      = v_Month_Pred
                df["day_of_month"]  = v_Day_Pred
            elif (p_year != "None") and (p_month != "None"):
                df["calendar_year"] = v_Year_Pred
                df["month_no"]      = v_Month_Pred
            elif p_year != "None":
                df["calendar_year"] = v_Year_Pred
            else:
                if p_full_data == 0:
                    df["calendar_year"] = v_Year_Pred
                    df["month_no"]      = v_Month_Pred
                    df["day_of_month"]  = v_Day_Pred

        except Exception as e:
            if type(e).__name__ == "FileNotFoundError":
                logger.warning(f"FileNotFoundError: {v_path}")
                return df
            raise

        return df

    # ---- GCS Lake (latest-partition join) -----------------------------------

    def fn_Gen_Source_Lake_Join(
        self,
        p_source_type: str,
        p_tablename: str,
        p_groupby,
        p_bucket_app: str,
        p_columns: list | None = None,
    ) -> pd.DataFrame:
        df = self.fn_Get_Source_Lake(
            p_source_type, p_tablename, p_bucket_app, p_columns=p_columns, p_full_data=1
        )
        df = df.reset_index(drop=True)
        idx = (
            df.sort_values(["calendar_year", "month_no", "day_of_month"], ascending=False)
            .groupby(p_groupby)
            .head(1)
            .index
        )
        return df.loc[idx]

    # ---- GCS Lake (multi-year history) --------------------------------------

    def fn_Get_Source_Lake_Hist(
        self,
        p_source_type: str,
        p_tablename: str,
        p_groupby: str = "None",
        p_hisofyear: str = "None",
        p_numofyear: str = "10",
        p_columns: list | None = None,
        p_bucket_app: str = BUCKET_MSSQL,
    ) -> pd.DataFrame:
        logger = get_run_logger()
        p_hisofyear = datetime.now().strftime("%Y") if p_hisofyear == "None" else p_hisofyear
        source_seg = f"/{p_source_type}" if p_source_type else ""
        df = pd.DataFrame()

        try:
            for _ in range(int(p_numofyear)):
                v_path = (
                    f"{BUCKET_LAKE}/{BUCKET_PARQUET}/{p_bucket_app}{source_seg}"
                    f"/{p_tablename}/calendar_year={p_hisofyear}"
                )
                logger.info(f"History PATH: {v_path}")
                df_year = pq.read_table(
                    source=v_path, partitioning="hive", columns=p_columns, filesystem=GCSFS
                ).to_pandas()
                if df_year is not None:
                    df = pd.concat([df, df_year], ignore_index=True).drop_duplicates()
                p_hisofyear = int(p_hisofyear) - 1
        except Exception as e:
            if type(e).__name__ == "FileNotFoundError":
                logger.warning(f"FileNotFoundError history: {p_tablename}")
                return df
            raise
        return df


# ---------------------------------------------------------------------------
# LoadSourceData
# ---------------------------------------------------------------------------

class LoadSourceData:
    """Write DataFrames to BigQuery via upsert (temp-table pattern) or append."""

    def __init__(self):
        self.connectdb = ConnectorDB()
        self.cnn = self.connectdb.fn_ConnectDWH()

    def fn_Upsert_To_DWH(
        self, p_dataframe: pd.DataFrame, p_table: str, p_primary_key: list, p_schema: str
    ):
        """
        Upsert pattern:
          1. Write DataFrame → temp_table.<temp> with if_exists='replace'
             (pandas_gbq infers schema without partition/cluster spec — avoids
             BadRequest: Incompatible table partitioning specification when target
             table has date_id partitioning + clustering)
          2. DELETE matching PK rows from target using temp as filter
          3. (caller) INSERT from temp → target
          4. (caller) TRUNCATE temp

        CREATE TABLE ... LIKE is intentionally NOT used because it copies the
        partition/clustering spec onto temp_table dataset which BigQuery rejects
        when temp_table dataset has no default partitioning.
        """
        logger = get_run_logger()
        if p_dataframe.empty:
            return

        client     = self.cnn
        temp_table = f"temp_{p_table}"

        # Step 1 — write temp (replace = DROP+CREATE+INSERT, no partition spec)
        pandas_gbq.to_gbq(
            p_dataframe,
            destination_table=f"temp_table.{temp_table}",
            project_id=GCP_PROJECT,
            if_exists="replace",
            progress_bar=False,
            credentials=BIGQUERY_CREDENTIAL,
        )
        logger.info(f"Wrote {len(p_dataframe)} rows → temp_table.{temp_table}")

        # Step 2 — DELETE matching PK rows from target
        pk_conditions = " AND ".join(
            f"SS.{col} = TT.{col}" for col in p_primary_key
        )
        del_query = (
            f"DELETE FROM `{GCP_PROJECT}.{p_schema}.{p_table}` AS SS "
            f"WHERE EXISTS (SELECT 1 FROM `{GCP_PROJECT}.temp_table.{temp_table}` AS TT "
            f"WHERE {pk_conditions});"
        )
        logger.info(del_query)
        client.query(del_query).result()

    def _build_insert_select(
        self, client, p_schema: str, p_table: str, temp_table: str
    ) -> str:
        """
        Build INSERT INTO ... SELECT with explicit CAST for TIMESTAMP columns.

        pandas_gbq.to_gbq infers pd.Timestamp as DATETIME in the temp table,
        but the target table declares those columns as TIMESTAMP.
        BigQuery rejects SELECT * when DATETIME → TIMESTAMP without explicit cast.

        This reads the target table schema and wraps every TIMESTAMP column with
        CAST(col AS TIMESTAMP) in the SELECT, leaving all other columns as-is.
        """
        target_ref = client.dataset(p_schema).table(p_table)
        target_schema = client.get_table(target_ref).schema

        cols = []
        for field in target_schema:
            fname = f"`{field.name}`"
            
            cols = []
        for field in target_schema:
            fname = f"`{field.name}`"
            
            # ตรวจสอบ Data Type และทำการ Cast ให้ตรงกับตารางปลายทางเสมอ
            if field.field_type == "STRING":
                cols.append(f"CAST({fname} AS STRING) AS {fname}")
            elif field.field_type in ["NUMERIC", "BIGNUMERIC", "DECIMAL"]:
                cols.append(f"CAST({fname} AS {field.field_type}) AS {fname}")
            elif field.field_type == "TIMESTAMP":
                cols.append(f"CAST({fname} AS TIMESTAMP) AS {fname}")
            elif field.field_type == "DATE":
                cols.append(f"CAST({fname} AS DATE) AS {fname}")
            elif field.field_type == "INT64":
                cols.append(f"CAST({fname} AS INT64) AS {fname}")
            else:
                cols.append(fname)

        select_clause = ", ".join(cols)
        return (
            f"INSERT INTO `{GCP_PROJECT}.{p_schema}.{p_table}` "
            f"SELECT {select_clause} "
            f"FROM `{GCP_PROJECT}.temp_table.{temp_table}`"
        )

    def fn_Load_To_DWH(
        self,
        p_dataframe: pd.DataFrame,
        p_table: str,
        p_primary_key: list,
        p_schema: str,
        p_insert: int = 0,
    ):
        logger = get_run_logger()
        if p_dataframe.empty:
            logger.warning(f"Empty DataFrame — skipping load for {p_table}")
            return

        # เก็บค่าจำนวน Row และ Column ก่อนเริ่ม Load
        rows_count = len(p_dataframe)
        cols_count = len(p_dataframe.columns)

        client = self.cnn
        try:
            if p_insert == 0:
                # กรณี Upsert (Delete old, then Insert)
                self.fn_Upsert_To_DWH(p_dataframe, p_table, p_primary_key, p_schema)
                temp_table = f"temp_{p_table}"
                ins_query = self._build_insert_select(client, p_schema, p_table, temp_table)
                
                client.query(ins_query).result()
                client.query(f"TRUNCATE TABLE temp_table.{temp_table}").result()
                
                logger.info(f"Successfully Upserted data to {p_schema}.{p_table}")
            else:
                # กรณี Append (Insert only)
                pandas_gbq.to_gbq(
                    dataframe=p_dataframe,
                    destination_table=f"{p_schema}.{p_table}",
                    project_id=GCP_PROJECT,
                    if_exists="append",
                    progress_bar=False,
                    credentials=BIGQUERY_CREDENTIAL,
                )
                logger.info(f"Successfully Appended data to {p_schema}.{p_table}")

            # แสดง Log สรุปเฉพาะจำนวน
            logger.info(
                f"📊 [LOAD SUMMARY] Table: {p_schema}.{p_table} | "
                f"Total Rows: {rows_count:,} | "
                f"Total Columns: {cols_count}"
            )

        except Exception as e:
            logger.error(f"ERROR loading data to {p_table}: {e}")
            raise


# ---------------------------------------------------------------------------
# CommonSQL helpers (from etl-iprop)
# ---------------------------------------------------------------------------

class CommonSQL:
    def Get_DIM_ADDRESS(self) -> str:
        return """
            SELECT addr_id, party_id
            FROM (
                SELECT party_id, addr_id,
                       ROW_NUMBER() OVER(PARTITION BY party_id ORDER BY date_id DESC, addr_id DESC) AS rn
                FROM `your-gcp-project-id.los.dim_address`
                WHERE addr_type_id = 5
            ) T WHERE rn = 1;
        """

    def Get_DIM_PROJECT(self, param: str) -> str:
        return f"""
            SELECT project_id, project_code, TRIM(project_name) AS project_name
            FROM (
                SELECT project_id, project_code, TRIM(project_name) AS project_name,
                       ROW_NUMBER() OVER(PARTITION BY project_id ORDER BY DATE_ID DESC) AS rn
                FROM `your-gcp-project-id.los.dim_project`
                WHERE project_id IN ({param})
                UNION ALL
                SELECT '-1', '(not set)', '(not set)', 1
            ) T WHERE rn = 1
        """
