"""
tasks/tasks_pmscustomer_dwh_dim_pmsuserprofile.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_dim_pmsuserprofile
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_pmsuserprofile")
def transform_dim_pmsuserprofile(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    def _norm_bool(v):
        if pd.isna(v): return None
        if isinstance(v, bool): return v
        s = str(v).strip().lower()
        if s in {"true", "1", "t", "y", "yes", "active"}: return True
        if s in {"false", "0", "f", "n", "no", "inactive"}: return False
        return None

    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Join
        df_users = p_data[0].copy()
        df_profiles = p_data[1].copy().add_suffix("_profiles")
        del p_data
        gc.collect()

        # Merge Users with their Profiles
        df_final = pd.merge(df_users, df_profiles, how="left", left_on="id", right_on="id_profiles")
        df_final = df_final.replace("", np.nan).copy()
        
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        # Determine Date ID from source timestamps
        df_final["date_id"] = pd.to_datetime(
            df_final["created_at"].combine_first(df_final["updated_at"])
        ).dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "id": "user_id",
            "last_login_at": "last_login_dttm",
            "refresh_token_expired_at": "token_expired_dttm",
            "id_profiles": "profile_id",
            "first_name_profiles": "first_name",
            "last_name_profiles": "last_name",
            "country_code_profiles": "country_cd",
            "phone_number_profiles": "phone_number",
            "updated_by_profiles": "profile_updt_by",
            "updated_at_profiles": "profile_updt_dttm",
            "created_at": "crtd_dttm",
            "created_by": "crtd_by",
            "updated_at": "updt_dttm",
            "updated_by": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Type Transformation (เพิ่มเติมส่วนจัดการ String)
        string_cols = ["first_name", "last_name", "country_cd", "phone_number"]
        for col in string_cols:
            if col in df_final.columns:
                # แปลงเป็น string โดยจัดการค่า nan ให้เป็นค่าว่าง
                df_final[col] = df_final[col].apply(lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip())

        # Numeric Columns (คงเดิม)
        for col in ["user_id", "profile_id", "token_version"]:
            df_final[col] = pd.to_numeric(df_final[col], errors="coerce").astype("Int64")
        
        # Boolean Normalization
        df_final["is_active"] = df_final["is_active"].apply(_norm_bool)
        
        # Datetime Columns
        date_cols = [
            "last_login_dttm", "token_expired_dttm", "profile_updt_dttm", 
            "crtd_dttm", "updt_dttm"
        ]
        for col in date_cols:
            df_final[col] = pd.to_datetime(df_final[col], errors="coerce")

        # 5. Final Audit Columns & Selection
        df_final["rec_actv_flag"] = "1"
        
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "user_id", "email", "is_active", "last_login_dttm", "token_expired_dttm", "token_version",
            "profile_id", "first_name", "last_name", "country_cd", "phone_number",
            "profile_updt_by", "profile_updt_dttm", "rec_actv_flag", "crtd_by", "crtd_dttm", "updt_by", "updt_dttm"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_pmsuserprofile Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final