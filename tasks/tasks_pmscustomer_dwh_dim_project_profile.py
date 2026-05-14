"""
tasks/tasks_pmscustomer_dwh_dim_project_profile.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_project_profile
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_project_profile")
def transform_dim_project_profile(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df_final = p_data[0].copy()
        del p_data
        gc.collect()

        df_final.columns = df_final.columns.str.lower()
        # ใช้มาตรฐาน UTC+7
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")
        
        # Date ID logic
        upd = df_final["ftupdatedate"].combine_first(df_final["ftcreatedate"])
        df_final["date_id"] = pd.to_datetime(
            upd.astype(str).str[0:4] + 
            upd.astype(str).str[5:7].str.zfill(2) + 
            upd.astype(str).str[8:11].str.zfill(2),
            errors="coerce"
        ).dt.date

        # 3. Rename Columns
        rename_map = {
            "fcid": "project_profile_id",
            "fcisactive": "project_status",
            "fcname": "segment_name",
            "ftcreatedate": "crtd_dttm",
            "fccreator": "crtd_by",
            "ftupdatedate": "updt_dttm",
            "fcupdateby": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # จัดการชื่อ Segment ไม่ให้เป็นค่า "nan"
        df_final["segment_name"] = df_final["segment_name"].apply(
            lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
        )

        df_final["start_selling_price"] = pd.array([0] * len(df_final), dtype="Int64")
        df_final["selling_price_per_square_meter"] = pd.array([0] * len(df_final), dtype="Int64")
        
        # Corporate Type Logic
        df_final["corporate_type"] = np.where(
            df_final["segment_name"].str.contains("มหาชน|PCL", na=False), "PCL", "LOCAL"
        )

        # Flag & Audit Info
        df_final["rec_actv_flag"] = "1"
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        df_final["updt_by"] = df_final["updt_by"].combine_first(df_final["crtd_by"])

        # 5. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "start_selling_price",
            "selling_price_per_square_meter", "project_status", "segment_name", "rec_actv_flag",
            "crtd_dttm", "crtd_by", "updt_dttm", "updt_by", "project_profile_id", "date_id", "corporate_type"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_project_profile Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final