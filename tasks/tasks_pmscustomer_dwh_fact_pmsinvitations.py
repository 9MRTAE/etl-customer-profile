"""
tasks/tasks_pmscustomer_dwh_fact_pmsinvitations.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_fact_pmsinvitations
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_pmsinvitations")
def transform_fact_pmsinvitations(p_data: list[pd.DataFrame]) -> pd.DataFrame:
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
        
        # สร้าง date_id จากวันที่สร้างคำเชิญ (created_at)
        df_final["date_id"] = pd.to_datetime(df_final["created_at"], errors="coerce").dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "id": "invite_id",
            "email": "invite_email",
            "status": "invite_status",
            "expires_at": "expired_dttm",
            "created_at": "crtd_dttm",
            "invited_by": "crtd_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # ป้องกันคำว่า 'nan' ในคอลัมน์สำคัญที่จะนำไปทำ Report หรือส่ง Notification
        str_cols = ["invite_email", "invite_status"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # # Numeric Casting ให้เป็น Int64 สำหรับ ID
        # if "invite_id" in df_final.columns:
        #     df_final["invite_id"] = pd.to_numeric(df_final["invite_id"], errors="coerce").astype("Int64")

        # 5. Datetime Casting & Audit Flags
        df_final["expired_dttm"] = pd.to_datetime(df_final["expired_dttm"], errors="coerce")
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["rec_actv_flag"] = "1"
        
        # สำหรับ Master Data นี้ ใช้เวลาสร้างเป็นค่าเริ่มต้นสำหรับ Update info
        df_final["updt_dttm"] = df_final["crtd_dttm"]
        df_final["updt_by"] = df_final["crtd_by"]

        # 6. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "invite_id", "invite_email", "invite_status", "expired_dttm", "rec_actv_flag",
            "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_pmsinvitations Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final