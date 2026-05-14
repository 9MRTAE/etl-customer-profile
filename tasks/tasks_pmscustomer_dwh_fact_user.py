"""
tasks/tasks_pmscustomer_dwh_fact_user.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_fact_user
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_user")
def transform_fact_user(p_data: list[pd.DataFrame]) -> pd.DataFrame:
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

        # มาตรฐานเวลา UTC+7 (Bangkok)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        # 3. Buddhist Era (พ.ศ.) & Date ID Handling
        def safe_bud(s):
            s = s.astype(str)
            return pd.Series(np.where(
                (s.str[0:4] > "2400") & (s != "nan") & (s != "None"),
                (s.str[0:4].astype(float) - 543).fillna(0).astype(int).astype(str) + s.str[4:],
                s
            ), index=s.index)

        # สร้าง date_id จาก updated_at หรือ created_at
        upd_raw = df_final["ftupdatedate"].combine_first(df_final["ftcreatedate"])
        # แปลงปี พ.ศ. ก่อนสร้าง date_id เพื่อความถูกต้อง
        upd_clean = safe_bud(upd_raw)
        df_final["date_id"] = pd.to_datetime(
            upd_clean.str[0:4] + upd_clean.str[5:7].str.zfill(2) + upd_clean.str[8:10].str.zfill(2),
            errors="coerce"
        ).dt.date

        # 4. Column Mapping & Renaming
        rename_map = {
            "fcid": "user_id",
            "fcusgid": "usergroup_id",
            "fccode": "user_cd",
            "fcname": "user_nm",
            "ftcreatedate": "crtd_dttm",
            "fccreator": "crtd_by",
            "ftupdatedate": "updt_dttm",
            "fcupdateby": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 5. Data Transformation & Handling "nan" Strings (ข้อ 4)
        str_cols = ["user_id", "usergroup_id", "user_cd", "user_nm", "crtd_by", "updt_by"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # User Status & Flags
        df_final["user_status"] = df_final["fcisactive"].astype(str).str.upper().replace({"N": "0", "Y": "1"})
        df_final["rec_actv_flag"] = np.nan # ตามต้นฉบับ

        # Convert Audit Datetime
        for c in ["crtd_dttm", "updt_dttm"]:
            df_final[c] = pd.to_datetime(safe_bud(df_final[c]), errors="coerce")

        # Combine logic for updt_by
        df_final["updt_by"] = df_final["updt_by"].replace("", np.nan).combine_first(df_final["crtd_by"])

        # 6. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "user_id", "usergroup_id", "user_cd", "user_nm", "user_status",
            "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_user Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final