"""
tasks/tasks_pmscustomer_dwh_dim_userpermission.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_userpermission
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task
from tasks.main_components import ExtractSourceData

@task(name="transform_dim_userpermission")
def transform_dim_userpermission(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        from config import BUCKET_MSSQL as BUCKET_APPLICATION
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation (tmAUT)
        df_aut = p_data[0].copy()
        del p_data
        gc.collect()

        df_aut.columns = df_aut.columns.str.lower()
        df_aut = df_aut.add_suffix("_AUT")
        
        ex = ExtractSourceData()
        
        # 2. Sequential Joins from Data Lake
        # Join tmBRN (Branch/Project)
        cols_brn = ["fcid", "fccomid", "calendar_year", "month_no", "day_of_month"]
        df_brn = ex.fn_Gen_Source_Lake_Join("", "tmBRN", "fcid", BUCKET_APPLICATION, cols_brn)
        df_brn.columns = df_brn.columns.str.lower()
        df_brn = df_brn.add_suffix("_BRN")
        df_final = pd.merge(df_aut, df_brn, how="inner", left_on="fcbrnid_AUT", right_on="fcid_BRN").drop_duplicates()
        del df_brn; gc.collect()

        # Join tmUSG (User Group)
        cols_usg = ["fcid", "fcname", "fcisactive", "ftcreatedate", "fccreator", "ftupdatedate", "fcupdateby", "calendar_year", "month_no", "day_of_month"]
        df_usg = ex.fn_Gen_Source_Lake_Join("", "tmUSG", "fcid", BUCKET_APPLICATION, cols_usg)
        df_usg.columns = df_usg.columns.str.lower()
        df_usg = df_usg.add_suffix("_USG")
        df_final = pd.merge(df_final, df_usg, how="inner", left_on="fcusgid_AUT", right_on="fcid_USG").drop_duplicates()
        del df_usg; gc.collect()

        # Join tmUSR (User)
        cols_usr = ["fcid", "fcusgid", "fccode", "fcname", "fcisactive", "ftcreatedate", "fccreator", "ftupdatedate", "fcupdateby", "calendar_year", "month_no", "day_of_month"]
        df_usr = ex.fn_Gen_Source_Lake_Join("", "tmUSR", "fcid", BUCKET_APPLICATION, cols_usr)
        df_usr.columns = df_usr.columns.str.lower()
        df_usr = df_usr.add_suffix("_USR")
        # Join by fcusgid to match users in that group
        df_final = pd.merge(df_final, df_usr, how="inner", left_on="fcusgid_AUT", right_on="fcusgid_USR").drop_duplicates()
        del df_usr; gc.collect()

        # Join tmMNU (Menu)
        cols_mnu = ["fcid", "fccode", "fcname", "calendar_year", "month_no", "day_of_month"]
        df_mnu = ex.fn_Gen_Source_Lake_Join("", "tmMNU", "fcid", BUCKET_APPLICATION, cols_mnu)
        df_mnu.columns = df_mnu.columns.str.lower()
        df_mnu = df_mnu.add_suffix("_MNU")
        df_final = pd.merge(df_final, df_mnu, how="inner", left_on="fcmnuid_AUT", right_on="fcid_MNU").drop_duplicates()
        del df_mnu; gc.collect()

        # 3. Metadata & Date ID
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        upd = df_final["ftupdatedate_AUT"].combine_first(df_final["ftcreatedate_AUT"])
        df_final["date_id"] = pd.to_datetime(
            upd.astype(str).str[0:4] + upd.astype(str).str[5:7].str.zfill(2) + upd.astype(str).str[8:11].str.zfill(2),
            errors="coerce"
        ).dt.date

        # 4. Renaming & Transformation
        rename_map = {
            "fcid_USR": "user_id", "fccode_USR": "user_cd", "fcname_USR": "user_nm",
            "fcisactive_USR": "user_status", "ftcreatedate_USR": "user_crtddttm", "ftupdatedate_USR": "user_updtdttm",
            "fccomid_BRN": "project_id", "fcbrnid_AUT": "branch_id", "fcid_USG": "usergroup_id",
            "fcname_USG": "usergroup_nm", "fcisactive_USG": "usergroup_status", "fcmnuid_AUT": "mnu_id",
            "fccode_MNU": "mnu_nm_eng", "fcname_MNU": "mnu_nm_thai", "fcselect_AUT": "view",
            "fcinsert_AUT": "insert", "fcupdate_AUT": "update", "fcdelete_AUT": "delete",
            "fcprint_AUT": "print", "fccancel_AUT": "cancel", "fcapprove_AUT": "approve"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 5. Handling "nan" in Strings (ข้อ 4) & Cleaning
        str_cols = ["user_id", "user_cd", "user_nm", "project_id", "branch_id", "usergroup_id", 
                    "usergroup_nm", "mnu_id", "mnu_nm_eng", "mnu_nm_thai"]
        for col in str_cols:
            df_final[col] = df_final[col].apply(
                lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
            )

        # Datetime casting
        for c in ["user_crtddttm", "user_updtdttm"]:
            df_final[c] = pd.to_datetime(df_final[c], errors="coerce")
            
        # Audit Trail from User Group Table
        df_final["rec_actv_flag"] = df_final["user_status"].astype(str).str.upper().replace({"N": "0", "Y": "1", "FALSE": "0", "TRUE": "1"})
        df_final["crtd_dttm"] = pd.to_datetime(df_final.get("ftcreatedate_USG"), errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final.get("ftupdatedate_USG"), errors="coerce")
        df_final["crtd_by"] = df_final.get("fccreator_USG", pd.NA)
        df_final["updt_by"] = df_final.get("fcupdateby_USG", df_final["crtd_by"])

        # 6. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "user_id", "user_cd", "user_nm", "user_status", "user_crtddttm", "user_updtdttm",
            "project_id", "branch_id", "usergroup_id", "usergroup_nm", "usergroup_status",
            "mnu_id", "mnu_nm_eng", "mnu_nm_thai", "view", "insert", "update", "delete", "print", "cancel", "approve",
            "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_userpermission Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final