"""
tasks/tasks_pmscustomer_dwh_fact_authuser.py
Origin: etl-authentication@prefect-v1 — etl_authentication_dwh_fact_authuser
"""

import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_authuser")
def transform_fact_authuser(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df_final = p_data[0].replace("", np.nan).copy()
        del p_data
        gc.collect()

        # ใช้มาตรฐาน UTC+7 (Bangkok Time)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")

        # สร้าง date_id จาก Year, Month, Day columns
        date_str = (
            df_final["calendar_year"].astype(str) + 
            df_final["month_no"].astype(str).str.zfill(2) + 
            df_final["day_of_month"].astype(str).str.zfill(2)
        )
        df_final["date_id"] = pd.to_datetime(date_str, errors="coerce").dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "id": "auth_id",
            "created_time": "crtd_dttm",
            "updated_time": "updt_dttm"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # จัดการข้อมูลเบอร์โทรศัพท์และรหัสประเทศ
        str_cols = ["phone_number", "country_code"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric & ID Casting
        df_final["auth_id"] = pd.to_numeric(df_final["auth_id"], errors="coerce").astype("Int64")
        
        # Boolean Mapping for is_deleted
        df_final["is_deleted"] = df_final["is_deleted"].astype(str).str.lower().replace(
            {"true": "1", "false": "0", "1": "1", "0": "0", "nan": "0"}
        )

        # Flag & Audit Columns
        df_final["rec_actv_flag"] = "1"
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        df_final["crtd_by"] = np.nan
        df_final["updt_by"] = np.nan

        # 5. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "auth_id", "mobile_user_id", "phone_number", "country_code",
            "is_deleted", "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_authuser Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final