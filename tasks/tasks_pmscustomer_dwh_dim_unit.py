"""
tasks/tasks_pmscustomer_dwh_dim_unit.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_unit
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_unit")
def transform_dim_unit(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Join
        df_R = p_data[0].copy().add_suffix("_R")
        df_B = p_data[1].copy().add_suffix("_B")
        del p_data
        gc.collect()

        # Join Unit (R) with Branch/Project (B)
        df_final = pd.merge(df_R, df_B, how="left", left_on="fcbrnid_R", right_on="fcid_B")
        
        # ใช้มาตรฐาน UTC+7
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")
        
        upd = df_final["ftupdatedate_R"].combine_first(df_final["ftcreatedate_R"])
        df_final["date_id"] = pd.to_datetime(
            upd.astype(str).str[0:4] + 
            upd.astype(str).str[5:7].str.zfill(2) + 
            upd.astype(str).str[8:11].str.zfill(2),
            errors="coerce"
        ).dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "fcid_R": "unit_id",
            "fccode_R": "unit_number",
            "fcaddressno_R": "room_number",
            "fcownerratio_R": "own_area_ratio",
            "fcleveingtype_R": "living_type",
            "fccomid_B": "project_id",
            "ftcreatedate_R": "crtd_dttm",
            "fccreateby_R": "crtd_by",
            "ftupdatedate_R": "updt_dttm",
            "fcupdateby_R": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # จัดการเลขที่ห้องและบ้านเลขที่
        str_cols = ["unit_number", "room_number"]
        for col in str_cols:
            df_final[col] = df_final[col].apply(
                lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
            )

        # Numeric Transformation
        df_final["own_area_ratio"] = pd.to_numeric(df_final["own_area_ratio"], errors="coerce").fillna(0.0).astype("float")
        
        # Mapping Values
        df_final["living_type"] = df_final["living_type"].astype(str).replace({
            "1": "อยู่เอง", 
            "2": "ปล่อยเช่า", 
            "3": "เช่าพื้นที่ส่วนกลาง"
        })
        
        # Flags & Audit
        df_final["addr_id"] = np.nan
        df_final["addr_id"] = df_final["addr_id"].astype("Int64")
        df_final["rec_actv_flag"] = df_final["fcisactive_R"].astype(str).str.upper().replace({
            "N": "0", "FALSE": "0", "Y": "1", "TRUE": "1"
        })
        df_final["updt_by"] = df_final["updt_by"].combine_first(df_final["crtd_by"])
        
        # Datetime Casting
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")

        # 5. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "addr_id",
            "unit_number", "room_number", "own_area_ratio", "living_type", "rec_actv_flag",
            "crtd_dttm", "crtd_by", "updt_dttm", "updt_by", "unit_id", "project_id", "date_id"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_unit Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final