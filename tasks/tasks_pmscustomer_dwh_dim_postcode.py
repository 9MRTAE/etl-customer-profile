"""
tasks/tasks_pmscustomer_dwh_dim_postcode.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_postcode
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_postcode")
def transform_dim_postcode(p_data: list[pd.DataFrame]) -> pd.DataFrame:
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
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")
        
        upd = df_final["ftupdatedate"].combine_first(df_final["ftcreatedate"])
        df_final["date_id"] = pd.to_datetime(
            upd.str[0:4] + upd.str[5:7].str.zfill(2) + upd.str[8:11].str.zfill(2)
        ).dt.date

        # 3. Rename Columns
        rename_map = {
            "fcpostcode": "zip_cd",
            "fcsubdistrictth": "sub_dstc_th",
            "fcsubdistricten": "sub_dstc_en",
            "fcdistrictth": "dstc_th",
            "fcdistricten": "dstc_en",
            "fcprovinceth": "prvn_th",
            "fcprovinceen": "prvn_en",
            "fcregionth": "region_th",
            "fcregionen": "region_en",
            "fcisactive": "rec_actv_flag",
            "ftcreatedate": "crtd_dttm",
            "fccreator": "crtd_by",
            "fcupdateby": "updt_by",
            "ftupdatedate": "updt_dttm",
            "fnid": "postcode_id"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # จัดการข้อมูลที่อยู่ที่เป็น String ไม่ให้ติดคำว่า 'nan'
        string_cols = [
            "zip_cd", "sub_dstc_th", "sub_dstc_en", "dstc_th", "dstc_en", 
            "prvn_th", "prvn_en", "region_th", "region_en"
        ]
        for col in string_cols:
            if col in df_final.columns:
                # ถ้าเป็น Null หรือ 'nan' ให้เปลี่ยนเป็นค่าว่าง และ strip space
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric & Date Casting
        df_final["postcode_id"] = pd.to_numeric(df_final["postcode_id"], errors="coerce").astype("Int64")
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")

        # 5. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "zip_cd",
            "sub_dstc_th", "sub_dstc_en", "dstc_th", "dstc_en", "prvn_th", "prvn_en",
            "region_th", "region_en", "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by",
            "postcode_id", "date_id"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_postcode Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final