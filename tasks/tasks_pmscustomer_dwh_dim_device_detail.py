"""
tasks/tasks_pmscustomer_dwh_dim_device_detail.py
Origin: etl-mobileregister@prefect-v1 — etl_mobileregister_dwh_dim_device_detail
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_device_detail")
def transform_dim_device_detail(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df_raw = p_data[0].copy()
        del p_data
        gc.collect()
        
        df_raw.columns = df_raw.columns.str.lower()
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Last Login Logic (Finding the latest login for disabled devices)
        df_lastlogin = df_raw.loc[df_raw["is_enable"] == "false"].copy()
        df_lastlogin["lastlogin_date"] = pd.to_datetime(
            df_lastlogin["updated_date"].combine_first(df_lastlogin["created_date"])
        )
        
        # Deduplicate to get the most recent date per device_id
        df_lastlogin = (
            df_lastlogin[["device_id", "lastlogin_date"]]
            .assign(rn=lambda x: x.sort_values("lastlogin_date", ascending=False)
                                  .groupby("device_id").cumcount() + 1)
            .query("rn == 1")
            .drop(columns="rn")
            .add_suffix("_cl")
        )

        # 3. Merge and Data Cleaning
        df_cd = df_raw.add_suffix("_cd")
        df_final = pd.merge(
            df_cd, 
            df_lastlogin, 
            how="left", 
            left_on="device_id_cd", 
            right_on="device_id_cl"
        )
        df_final = df_final.replace("", np.nan).copy()

        # 4. Add DWH Metadata
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        # 5. Column Mapping & Date Handling
        df_final["date_id"] = pd.to_datetime(
            df_final["updated_date_cd"].combine_first(df_final["created_date_cd"])
        ).dt.date

        rename_map = {
            "device_id_cd": "device_id",
            "lastlogin_date_cl": "last_login_dttm",
            "device_name_cd": "device_nm",
            "platform_cd": "platform",
            "os_version_cd": "version",
            "created_date_cd": "crtd_dttm",
            "customer_id_cd": "crtd_by",
            "updated_date_cd": "updt_dttm"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 6. Final Clean up & Feature Engineering
        df_final["last_login_dttm"] = pd.to_datetime(df_final["last_login_dttm"], errors="coerce")
        df_final["rec_actv_flag"] = np.where(df_final["is_enable_cd"].isin(["N", "false", "0"]), "0", "1")
        
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        
        df_final["crtd_by"] = df_final["crtd_by"].astype("object")
        df_final["updt_by"] = df_final["crtd_by"]

        # 7. Select Final Columns (Schema Consistency)
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "last_login_dttm",
            "device_nm", "platform", "version", "rec_actv_flag", "crtd_dttm", "crtd_by",
            "updt_by", "updt_dttm", "device_id", "date_id"
        ]
        df_final = df_final[target_columns]

        if not df_final.empty:
            logger.info(f"transform_dim_device_detail Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final