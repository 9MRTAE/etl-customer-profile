"""
tasks/tasks_pmscustomer_dwh_fact_pmsuser_role.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_fact_pmsuser_role
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_pmsuser_role")
def transform_fact_pmsuser_role(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Internal Helper for Boolean Normalization
        def _norm_bool(v):
            if pd.isna(v): return None
            if isinstance(v, bool): return v
            s = str(v).strip().lower()
            if s in {"true", "1", "t", "y", "yes", "active"}: return True
            if s in {"false", "0", "f", "n", "no", "inactive"}: return False
            return None

        # 2. Initial Data Preparation
        df_final = p_data[0].copy()
        del p_data
        gc.collect()

        # มาตรฐานเวลา UTC+7 (Bangkok)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 3. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        # สร้าง date_id โดยอิงจาก updated_at เป็นหลักเพื่อให้ได้ข้อมูลล่าสุด
        df_final["date_id"] = pd.to_datetime(
            df_final["updated_at"].combine_first(df_final["created_at"]), 
            errors="coerce"
        ).dt.date

        # 4. Column Mapping & Renaming
        rename_map = {
            "id": "record_id",
            "company_id": "pmscompany_id",
            "project_id": "pmsproject_id",
            "created_at": "crtd_dttm",
            "created_by": "crtd_by",
            "updated_at": "updt_dttm",
            "updated_by": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 5. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # เคลียร์ค่าว่างในฟิลด์ String สำคัญ
        str_cols = ["crtd_by", "updt_by"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric Casting (ใช้ Int64 เพื่อความแม่นยำของ IDs)
        id_cols = ["record_id", "user_id", "role_id", "pmscompany_id", "pmsproject_id"]
        for col in id_cols:
            if col in df_final.columns:
                # เปลี่ยนจาก fillna(0) เป็นพยายามรักษา NULL ไว้ (ถ้าจำเป็น) 
                # แต่ตาม Logic เดิมของคุณคือเป็น 0 ผมจึงรักษาพฤติกรรมเดิมแต่ใช้ numeric casting ที่ปลอดภัยขึ้น
                df_final[col] = pd.to_numeric(df_final[col], errors="coerce").fillna(0).astype("Int64")

        # 6. Boolean & Audit Flags
        df_final["is_active"] = df_final["is_active"].apply(_norm_bool)
        df_final["rec_actv_flag"] = "1"
        
        for c in ["crtd_dttm", "updt_dttm"]:
            df_final[c] = pd.to_datetime(df_final[c], errors="coerce")

        # 7. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "record_id", "user_id", "role_id", "pmscompany_id", "pmsproject_id",
            "is_active", "rec_actv_flag", "crtd_by", "crtd_dttm", "updt_by", "updt_dttm"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_pmsuser_role Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final