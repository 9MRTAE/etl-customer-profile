"""
tasks/tasks_pmscustomer_dwh_fact_pmsinvitationhist_project.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_fact_pmsinvitationhist_project
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_pmsinvitationhist_project")
def transform_fact_pmsinvitationhist_project(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df_final = p_data[0].copy()
        del p_data
        gc.collect()

        # มาตรฐานเวลา UTC+7 (Bangkok)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        # สร้าง date_id จากวันที่สร้าง Log
        df_final["date_id"] = pd.to_datetime(df_final["created_at"], errors="coerce").dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "id": "record_id",
            "project_id": "pmsproject_id",
            "description": "desc",
            "target_email": "action_email"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # เคลียร์ค่าว่างและ 'nan' ในคอลัมน์ที่เป็น String สำคัญ
        str_cols = ["action_type", "desc", "action_email", "reason"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric Casting ให้เป็น Int64 เพื่อรองรับ Null values
        id_cols = ["record_id", "pmsproject_id"]
        for col in id_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors="coerce").astype("Int64")

        # 5. Flags & Audit Trails
        df_final["rec_actv_flag"] = "1"
        df_final["created_at"] = pd.to_datetime(df_final["created_at"], errors="coerce")
        
        # ข้อมูล History เป็น Insert-only จึงใช้เวลาสร้างเป็นทั้งจุดเริ่มและจุดล่าสุด
        df_final["crtd_dttm"] = df_final["created_at"]
        df_final["updt_dttm"] = df_final["created_at"]
        df_final["crtd_by"] = df_final.get("created_by", pd.NA)
        df_final["updt_by"] = df_final["crtd_by"]

        # 6. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "record_id", "pmsproject_id", "action_type", "desc", "action_email", "reason",
            "rec_actv_flag", "crtd_by", "crtd_dttm", "updt_by", "updt_dttm"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_pmsinvitationhist_project Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final