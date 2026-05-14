"""
tasks/tasks_pmscustomer_dwh_dim_project_bookbank.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_project_bookbank

Joins: tmBKB + tmBRN + tmBNK + tmACC + dim_project (DWH query)
Buddhist year conversion applied to all date columns.
"""

import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task
from tasks.main_components import ExtractSourceData, CommonSQL

@task(name="transform_dim_project_bookbank")
def transform_dim_project_bookbank(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()

    def safe_bud(s: pd.Series) -> pd.Series:
        """Convert Buddhist year to CE year where year > 2500."""
        # Ensure input is string and handle NA
        s_str = s.fillna("").astype(str)
        return pd.Series(np.where(
            s_str.str[0:4] > str(pd.Timestamp.max)[:4],
            (s_str.str[0:4].replace("", "0").astype("Int64") - 543).astype(str) + s_str.str[4:],
            s_str
        ), index=s.index)

    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df_bkb = p_data[0].copy()
        df_brn = p_data[1].copy()
        df_bnk = p_data[2].copy()
        df_acc = p_data[3].copy()
        del p_data
        gc.collect()

        for d in [df_bkb, df_brn, df_bnk, df_acc]:
            d.columns = d.columns.str.lower()

        df_acc_od = df_acc.copy().add_suffix("_AOD")
        
        df_bkb = df_bkb.add_suffix("_BKB")
        df_brn = df_brn.add_suffix("_BRN")
        df_bnk = df_bnk.add_suffix("_BNK")
        df_acc = df_acc.add_suffix("_ACC")

        # 2. Get project names from DWH
        ex = ExtractSourceData()
        dt_fcbrnid = df_bkb["fcbrnid_BKB"].drop_duplicates()
        df_brn_dist = pd.merge(
            dt_fcbrnid, df_brn, how="inner",
            left_on="fcbrnid_BKB", right_on="fcid_BRN"
        )[["fcbrnid_BKB", "fccomid_BRN"]]
        
        list_comid = ", ".join(repr(e) for e in df_brn_dist["fccomid_BRN"].drop_duplicates().tolist())
        df_proj = ex.fn_Get_DWH(CommonSQL().Get_DIM_PROJECT(list_comid)).add_suffix("_PRO")

        # 3. Joins
        df_final = pd.merge(df_bkb, df_brn, how="left", left_on="fcbrnid_BKB", right_on="fcid_BRN")
        df_final = pd.merge(df_final, df_proj, how="left", left_on="fccomid_BRN", right_on="project_id_PRO")
        df_final = pd.merge(df_final, df_bnk, how="left", left_on="fcbnkid_BKB", right_on="fcid_BNK")
        df_final = pd.merge(df_final, df_acc, how="left", left_on="fcaccid_BKB", right_on="fcid_ACC")
        df_final = pd.merge(df_final, df_acc_od, how="left", left_on="fcod_accid_BKB", right_on="fcid_AOD")
        
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 4. Metadata & Date ID Logic
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        # Handle Buddhist Year in Created/Updated Dates
        df_final["ftcreatedate_BKB"] = safe_bud(df_final["ftcreatedate_BKB"])
        df_final["ftupdatedate_BKB"] = safe_bud(df_final["ftupdatedate_BKB"])
        
        upd = df_final["ftupdatedate_BKB"].combine_first(df_final["ftcreatedate_BKB"])
        df_final["date_id"] = pd.to_datetime(
            upd.astype(str).str[0:4] + upd.astype(str).str[5:7].str.zfill(2) + upd.astype(str).str[8:11].str.zfill(2),
            errors="coerce"
        ).dt.date

        # 5. Column Renaming
        rename_map = {
            "fcid_BKB": "bookbank_id", 
            "fccode_BKB": "bookbank_cd", 
            "fcname_BKB": "bookbank_nm",
            "fcbookno_BKB": "bookbank_no",
            "project_id_PRO": "project_id",
            "project_code_PRO": "project_code", 
            "project_name_PRO": "project_name",
            "fcid_BNK": "bank_id", 
            "fccode_BNK": "bank_cd", 
            "fcname_BNK": "bank_nm",
            "fcid_ACC": "account_id", 
            "fccode_ACC": "account_cd", 
            "fcname_ACC": "account_nm",
            "fcid_AOD": "accountod_id", 
            "fccode_AOD": "accountod_cd", 
            "fcname_AOD": "accountod_nm",
            "fcbankbrname_BKB": "bookbank_branch", 
            "fcbankbooktype_BKB": "bookbank_type",
            "fcbankbookusetype_BKB": "bookbank_usetype", 
            "ftbegdate_BKB": "opening_dttm",
            "fnday_BKB": "numofdepositdays", 
            "ftduedate_BKB": "duedate_dttm",
            "fnbalamt_BKB": "balance_amt", 
            "fcremark1_BKB": "remark_nm",
            "fcremark2_BKB": "deposit_nm", 
            "fcremark3_BKB": "interest_rate",
            "fcremark4_BKB": "objective", 
            "fcremark5_BKB": "account_istransfer",
            "ftcreatedate_BKB": "crtd_dttm", 
            "fccreator_BKB": "crtd_by",
            "ftupdatedate_BKB": "updt_dttm", 
            "fcupdateby_BKB": "updt_by",
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 6. Data Transformation & Value Mapping
        df_final["bookbank_type"] = df_final["bookbank_type"].replace({"S": "สะสมทรัพย์", "C": "กระแสรายวัน", "F": "ฝากประจำ"})
        df_final["bookbank_usetype"] = df_final["bookbank_usetype"].replace({"P": "จ่ายชำระ", "R": "รับชำระ", "B": "รับและจ่ายชำระ"})
        df_final["numofdepositdays"] = pd.to_numeric(df_final["numofdepositdays"], errors="coerce").astype("Int64")
        df_final["balance_amt"] = pd.to_numeric(df_final["balance_amt"], errors="coerce").astype("float")
        df_final["rec_actv_flag"] = df_final["fcisactive_BKB"].astype(str).str.upper().replace({"N": "0", "FALSE": "0", "Y": "1", "TRUE": "1"})
        df_final["updt_by"] = df_final["updt_by"].combine_first(df_final["crtd_by"])

        # Datetime Handling with safe_bud for transaction dates
        for date_col in ["opening_dttm", "duedate_dttm", "crtd_dttm", "updt_dttm"]:
            if date_col in ["opening_dttm", "duedate_dttm"]:
                df_final[date_col] = safe_bud(df_final[date_col])
            df_final[date_col] = pd.to_datetime(df_final[date_col], errors="coerce")

        # 7. Handling "nan" in Strings (ข้อ 4)
        str_cols = [
            "bookbank_cd", "bookbank_no", "bookbank_nm", "project_code", "project_name",
            "bank_cd", "bank_nm", "account_cd", "account_nm", "accountod_cd", "accountod_nm", 
            "bookbank_branch", "remark_nm", "deposit_nm", "objective"
        ]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "bookbank_id", "bookbank_cd", "bookbank_nm", "bookbank_no",
            "project_id", "project_code", "project_name",
            "bank_id", "bank_cd", "bank_nm",
            "account_id", "account_cd", "account_nm",
            "accountod_id", "accountod_cd", "accountod_nm",
            "bookbank_branch", "bookbank_type", "bookbank_usetype",
            "opening_dttm", "numofdepositdays", "duedate_dttm", "balance_amt",
            "remark_nm", "deposit_nm", "interest_rate", "objective", "account_istransfer",
            "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_project_bookbank Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final