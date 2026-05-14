"""
tasks/tasks_pmscustomer_dwh_fact_pmsproject_features.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_fact_pmsproject_features
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_pmsproject_features")
def transform_fact_pmsproject_features(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Join
        df_pf = p_data[0].copy() # Project Features (Main)
        df_cp = p_data[1].copy().add_suffix("_company") # Company-Project Mapping
        del p_data
        gc.collect()

        # Join Feature status with Company information
        df_final = pd.merge(
            df_pf, 
            df_cp, 
            how="left", 
            left_on="project_id", 
            right_on="project_id_company"
        )
        
        # ล้างค่าว่างเบื้องต้น
        df_final = df_final.replace("", np.nan).copy()
        
        # มาตรฐานเวลา UTC+7 (Bangkok)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        # สร้าง date_id โดยเรียงลำดับความสำคัญจาก updated_at ก่อน created_at
        df_final["date_id"] = pd.to_datetime(
            df_final["updated_at"].combine_first(df_final["created_at"]), 
            errors="coerce"
        ).dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "enabled": "feature_enabled",
            "company_id_company": "company_id",
            "created_at": "crtd_dttm",
            "created_by": "crtd_by",
            "updated_at": "updt_dttm",
            "updated_by": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # จัดการข้อมูลที่เป็น Boolean String หรือสถานะ feature
        str_cols = ["feature_enabled", "crtd_by", "updt_by"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric Casting (ใช้ Int64 เพื่อความปลอดภัยของ ID)
        id_cols = ["project_id", "feature_id", "company_id"]
        for col in id_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors="coerce").astype("Int64")

        # 5. Flags & Datetime Casting
        df_final["rec_actv_flag"] = "1"
        for c in ["crtd_dttm", "updt_dttm"]:
            df_final[c] = pd.to_datetime(df_final[c], errors="coerce")

        # 6. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "project_id", "feature_id", "feature_enabled", "company_id",
            "rec_actv_flag", "crtd_by", "crtd_dttm", "updt_by", "updt_dttm"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_pmsproject_features Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final