"""
tasks/tasks_pmscustomer_dwh_dim_pmscompany.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_dim_pmscompany
"""

import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_pmscompany")
def transform_dim_pmscompany(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df_final = p_data[0].copy()
        del p_data
        gc.collect()
        
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")

        # Handle Date ID
        updt_src = df_final["updated_at"].combine_first(df_final["created_at"])
        df_final["date_id"] = pd.to_datetime(
            updt_src.str[0:4] + 
            updt_src.str[5:7].str.zfill(2) + 
            updt_src.str[8:11].str.zfill(2)
        ).dt.date

        # 3. Column Mapping (Rename & Type Casting)
        rename_map = {
            "id": "company_id",
            "code": "company_cd",
            "name_th": "company_nm_th",
            "name_en": "company_nm_en",
            "created_at": "crtd_dttm",
            "created_by": "crtd_by",
            "updated_at": "updt_dttm",
            "updated_by": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Feature Engineering & Clean up
        df_final["company_id"] = df_final["company_id"].astype("Int64")
        df_final['company_nm_en'] = df_final['company_nm_en'].astype(str).replace('nan', '')
        df_final["rec_actv_flag"] = "1"
        
        # Datetime casting
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")

        # Handle created_by & updated_by nulls
        df_final["crtd_by"] = df_final.get("created_by", pd.NA)
        df_final["crtd_by"] = df_final["crtd_by"].astype(str).replace(['None', 'nan', '<NA>'], '')
        df_final["updt_by"] = df_final.get("updated_by", pd.NA)
        df_final["updt_by"] = df_final["updt_by"].astype(str).replace(['None', 'nan', '<NA>'], '')

        # 5. Select Final Columns (Schema Consistency)
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "company_id", "company_cd", "company_nm_th", "company_nm_en",
            "is_active", "rec_actv_flag", "crtd_by", "crtd_dttm", "updt_by", "updt_dttm"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_pmscompany Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final