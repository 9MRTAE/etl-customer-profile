"""
tasks/tasks_pmscustomer_dwh_fact_userconsent.py
Origin: etl-pdpa@prefect-v1 — etl_pdpa_dwh_fact_userconsent
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_userconsent")
def transform_fact_userconsent(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Joins
        df_uc = p_data[0].copy()           # User Consent (Main)
        df_con = p_data[1].copy().add_suffix("_con") # Consent Topic/Status
        df_conver = p_data[2].copy().add_suffix("_conver") # Consent Version Info
        del p_data
        gc.collect()

        # Join User Consent with Topic and Version info
        df_final = pd.merge(df_uc, df_con, how="left", left_on="consent_id", right_on="id_con")
        df_final = pd.merge(df_final, df_conver, how="left", left_on="type_con", right_on="type_conver")
        
        # Replace empty strings with NaN for consistent cleaning
        df_final = df_final.replace("", np.nan).copy()
        
        # มาตรฐานเวลา UTC+7 (Bangkok)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        # สร้าง date_id จาก calendar parts
        date_str = (
            df_final["calendar_year"].astype(str) + 
            df_final["month_no"].astype(str).str.zfill(2) + 
            df_final["day_of_month"].astype(str).str.zfill(2)
        )
        df_final["date_id"] = pd.to_datetime(date_str, errors="coerce").dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "id": "uconsent_id",
            "type_con": "consent_type",
            "status_con": "consent_sts",
            "version_conver": "version_id",
            "version_con": "version_nm",
            "topic_con": "consent_topic",
            "status": "uconsent_sts",
            "created_date": "crtd_dttm",
            "created_by": "crtd_by",
            "updated_date": "updt_dttm",
            "updated_by": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # จัดการข้อมูล String สำคัญ ป้องกันข้อมูล 'nan' แสดงผลใน Report กฎหมาย
        str_cols = ["consent_type", "consent_sts", "version_nm", "consent_topic", "uconsent_sts", "crtd_by", "updt_by"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric Casting (Casting เป็น Int64 เพื่อรองรับ NULL)
        id_cols = ["uconsent_id", "user_id", "consent_id", "version_id"]
        for col in id_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors="coerce").astype("Int64")

        # 5. Flags & Datetime Casting
        df_final["rec_actv_flag"] = "1"
        for c in ["crtd_dttm", "updt_dttm"]:
            df_final[c] = pd.to_datetime(df_final[c], errors="coerce")

        # 6. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id", "uconsent_id",
            "user_id", "consent_id", "consent_type", "consent_sts", "version_id", "version_nm",
            "consent_topic", "uconsent_sts", "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_userconsent Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final