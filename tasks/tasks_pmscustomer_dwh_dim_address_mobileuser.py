"""
tasks/tasks_pmscustomer_dwh_dim_address_mobileuser.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_address_mobileuser
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task


@task(name="transform_dim_address_mobileuser")
def transform_dim_address_mobileuser(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        # หากข้อมูลต้นทางว่าง ให้ข้ามการทำ Transform ไปเลย
        if p_data[0].empty:
            return df_final

        # ==========================================
        # 1. Initialize and Standardize
        # ==========================================
        df = p_data[0].copy()
        df_pc = p_data[1].copy()
        
        df.columns = df.columns.str.lower()
        df_pc.columns = df_pc.columns.str.lower()
        
        df["fcmobileuserid"] = df["fcmobileuserid"].astype("object")
        
        # ==========================================
        # 2. Add Suffixes & Merge Data
        # ==========================================
        df_F = df.add_suffix("_F")
        df_P = df_pc.add_suffix("_P")
        
        df_final = pd.merge(df_F, df_P, how="left", left_on="fnpostcodeid_F", right_on="fnid_P")
        df_final.drop(columns=["fnpostcodeid_F", "fnid_P"], inplace=True)
        
        # ==========================================
        # 3. Create DWH Audit Columns
        # ==========================================
        # ปรับการดึงเวลาให้ Fix Timezone ตาม Asia/Bangkok
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)
        
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")
        
        # ==========================================
        # 4. Calculate Date ID
        # ==========================================
        upd = df_final["ftupdatedate_F"].combine_first(df_final["ftcreatedate_F"])
        date_str = upd.str[0:4] + upd.str[5:7].str.zfill(2) + upd.str[8:11].str.zfill(2)
        df_final["date_id"] = pd.to_datetime(date_str).dt.date
        
        # ==========================================
        # 5. Rename Columns
        # ==========================================
        rename_mapping = {
            "fcmobileuserid_F": "party_id",
            "fcaddrtype_F": "addr_type_id",
            "fcaddress_F": "addr_no",
            "fcsubdistrictth_P": "sub_dstc",
            "fcdistrictth_P": "dstc",
            "fcprovinceth_P": "prvn",
            "fcpostcode_P": "zip_cd",
            "fnid_F": "ref_addr_id",
            "ftcreatedate_F": "crtd_dttm",
            "fccreateby_F": "crtd_by",
            "ftupdatedate_F": "updt_dttm",
            "fcupdateby_F": "updt_by"
        }
        df_final.rename(columns=rename_mapping, inplace=True)
        
        # ==========================================
        # 6. Data Type Casting & Value Cleaning
        # ==========================================
        df_final["addr_type_id"] = df_final["addr_type_id"].astype("Int32")
        
        flag_mapping = {"N": "0", "false": "0", "Y": "1", "true": "1"}
        df_final["rec_actv_flag"] = df_final["fcisactive_F"].replace(flag_mapping)
        
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        df_final["updt_by"] = df_final["crtd_by"].combine_first(df_final["updt_by"])
        
        # ==========================================
        # 7. Select & Reorder Final Schema
        # ==========================================
        final_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note",
            "addr_no", "sub_dstc", "dstc", "prvn", "zip_cd", "rec_actv_flag",
            "crtd_dttm", "crtd_by", "updt_dttm", "updt_by",
            "ref_addr_id", "party_id", "addr_type_id", "date_id"
        ]
        df_final = df_final[final_columns]
        
        logger.info(f"transform_dim_address_mobileuser Rows: {len(df_final)}")
        
    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise
        
    return df_final