"""
tasks/tasks_pmscustomer_dwh_fact_userpermission.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_fact_userpermission
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task
from tasks.main_components import ExtractSourceData

@task(name="transform_fact_userpermission")
def transform_fact_userpermission(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        from config import BUCKET_MSSQL as BUCKET_APPLICATION
        
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation (Authorization Table)
        df_aut = p_data[0].copy()
        del p_data
        gc.collect()
        df_aut = df_aut.add_suffix("_AUT")
        
        ex = ExtractSourceData()

        # 2. Join Branch Data (tmBRN)
        cols_brn = ["fcid", "fccomid", "calendar_year", "month_no", "day_of_month"]
        df_brn = ex.fn_Gen_Source_Lake_Join("", "tmBRN", "fcid", BUCKET_APPLICATION, cols_brn)
        df_brn.columns = df_brn.columns.str.lower()
        df_brn = df_brn.add_suffix("_BRN")
        
        df_final = pd.merge(df_aut, df_brn, how="inner", left_on="fcbrnid_AUT", right_on="fcid_BRN").drop_duplicates()
        del df_brn; gc.collect()

        # 3. Join User Group Data (tmUSG)
        cols_usg = ["fcid", "fcname", "fcisactive", "calendar_year", "month_no", "day_of_month"]
        df_usg = ex.fn_Gen_Source_Lake_Join("", "tmUSG", "fcid", BUCKET_APPLICATION, cols_usg)
        df_usg.columns = df_usg.columns.str.lower()
        df_usg = df_usg.add_suffix("_USG")
        
        df_final = pd.merge(df_final, df_usg, how="inner", left_on="fcusgid_AUT", right_on="fcid_USG").drop_duplicates()
        del df_usg; gc.collect()

        # 4. Join Menu Data (tmMNU)
        cols_mnu = ["fcid", "fccode", "fcname", "calendar_year", "month_no", "day_of_month"]
        df_mnu = ex.fn_Gen_Source_Lake_Join("", "tmMNU", "fcid", BUCKET_APPLICATION, cols_mnu)
        df_mnu.columns = df_mnu.columns.str.lower()
        df_mnu = df_mnu.add_suffix("_MNU")
        
        df_final = pd.merge(df_final, df_mnu, how="inner", left_on="fcmnuid_AUT", right_on="fcid_MNU").drop_duplicates()
        del df_mnu; gc.collect()

        # 5. Add Metadata & Timezone
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        # 6. Buddhist Era (พ.ศ.) & Date ID Handling
        def safe_bud(s):
            s = s.astype(str)
            return pd.Series(np.where(
                (s.str[0:4] > "2400") & (s != "nan") & (s != "None"),
                (s.str[0:4].astype(float) - 543).fillna(0).astype(int).astype(str) + s.str[4:],
                s
            ), index=s.index)

        upd_raw = df_final["ftupdatedate_AUT"].combine_first(df_final["ftcreatedate_AUT"])
        upd_clean = safe_bud(upd_raw)
        df_final["date_id"] = pd.to_datetime(
            upd_clean.str[0:4] + upd_clean.str[5:7].str.zfill(2) + upd_clean.str[8:10].str.zfill(2),
            errors="coerce"
        ).dt.date

        # 7. Renaming Columns
        rename_map = {
            "fccomid_BRN": "project_id",
            "fcbrnid_AUT": "branch_id",
            "fcusgid_AUT": "usergroup_id",
            "fcname_USG": "usergroup_nm",
            "fcisactive_USG": "usergroup_status",
            "fcmnuid_AUT": "mnu_id",
            "fccode_MNU": "mnu_nm_eng",
            "fcname_MNU": "mnu_nm_thai",
            "fcselect_AUT": "view",
            "fcinsert_AUT": "insert",
            "fcupdate_AUT": "update",
            "fcdelete_AUT": "delete",
            "fcprint_AUT": "print",
            "fccancel_AUT": "cancel",
            "fcapprove_AUT": "approve"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 8. Data Transformation & Handling "nan" Strings (ข้อ 4)
        str_cols = [
            "project_id", "branch_id", "usergroup_id", "usergroup_nm", 
            "mnu_id", "mnu_nm_eng", "mnu_nm_thai"
        ]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Status & Audit Fields
        df_final["usergroup_status"] = df_final["usergroup_status"].astype(str).str.upper().replace({"N": "0", "Y": "1"})
        df_final["rec_actv_flag"] = np.nan
        df_final["crtd_dttm"] = pd.to_datetime(safe_bud(df_final["ftcreatedate_AUT"]), errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(safe_bud(df_final["ftupdatedate_AUT"]), errors="coerce")
        df_final["crtd_by"] = pd.NA
        df_final["updt_by"] = pd.NA

        # 9. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "project_id", "branch_id", "usergroup_id", "usergroup_nm", "usergroup_status",
            "mnu_id", "mnu_nm_eng", "mnu_nm_thai", "view", "insert", "update", "delete", "print", "cancel", "approve",
            "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_userpermission Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final