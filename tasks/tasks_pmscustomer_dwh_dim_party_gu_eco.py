"""
tasks/tasks_pmscustomer_dwh_dim_party_gu_eco.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_party_gu_eco
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_party_gu_eco")
def transform_dim_party_gu_eco(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Clean Column Names
        df_mu = p_data[0].copy()
        df_mg = p_data[1].copy()
        df_cus = p_data[2].copy()
        del p_data
        gc.collect()

        df_mu.columns = df_mu.columns.str.lower()
        df_mg.columns = df_mg.columns.str.lower()
        df_cus.columns = df_cus.columns.str.lower()

        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Pre-processing Source Data
        # Mapping Mobile User
        df_mu.rename(columns={
            "fcuserid": "fcid",
            "ftbirthdate": "ftbirthday",
            "fcemail": "fcshipemail",
            "fngender": "fcsex"
        }, inplace=True)
        
        df_mu["fcname"] = df_mu["fcfirstname"] + " " + df_mu["fclastname"]
        df_mu["fcengname"] = df_mu["fcname"]
        df_mu["fcshipmobile"] = "0" + df_mu["fcphoneno"].astype(str)
        df_mu["fcpassport"] = df_mu["fctaxid"]
        df_mu["fceducation"] = None
        df_mu["fcmarrystatus"] = None
        df_mu["fcsex"] = df_mu["fcsex"].str.strip()

        # Mapping Customer & Gender
        df_cus["phonenumber_new"] = "0" + df_cus["phonenumber"].astype(str)
        df_mg["fcid"] = df_mg["fcid"].astype("string").str.strip()

        # 3. Join Data
        df_mu = df_mu.add_suffix("_mu")
        df_mg = df_mg.add_suffix("_mg")
        df_cus = df_cus.add_suffix("_c")

        df_final = pd.merge(df_mu, df_cus, how="left", left_on="fcshipmobile_mu", right_on="phonenumber_new_c")
        df_final = pd.merge(df_final, df_mg, how="left", left_on="fcsex_mu", right_on="fcid_mg")

        # 4. DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        upd = df_final["ftupdatedate_mu"].combine_first(df_final["ftcreatedate_mu"])
        df_final["date_id"] = pd.to_datetime(
            upd.astype(str).str[0:4] + 
            upd.astype(str).str[5:7].str.zfill(2) + 
            upd.astype(str).str[8:11].str.zfill(2)
        ).dt.date

        # 5. Column Mapping (Rename to Final Schema)
        rename_map = {
            "fcname_mu": "full_nm_th",
            "fcfirstname_mu": "frst_nm_th",
            "fclastname_mu": "last_nm_th",
            "fcengname_mu": "full_nm_en",
            "ftbirthday_mu": "dob",
            "fctaxid_mu": "th_id",
            "fcpassport_mu": "passport_no",
            "fccountrycode_mu": "cnty_cd",
            "fcshipmobile_mu": "mob_no",
            "fcshipemail_mu": "email",
            "fceducation_mu": "edu",
            "fcmarrystatus_mu": "mrtl_sts",
            "fcnameth_mg": "gndr",
            "fcisactive_mu": "rec_actv_flag",
            "ftcreatedate_mu": "crtd_dttm",
            "fccreateby_mu": "crtd_by",
            "ftupdatedate_mu": "updt_dttm",
            "fcupdateby_mu": "updt_by",
            "fcid_mu": "party_id"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 6. Additional Columns & Feature Engineering
        for col in ["titl_th", "titl_en", "occu", "income_range", "rlgn", "fam_size", "mem_ref_code"]:
            df_final[col] = None

        df_final["frst_nm_en"] = df_final["frst_nm_th"]
        df_final["last_nm_en"] = df_final["last_nm_th"]

        # Handle Date of Birth (Buddhist to Christian Era if necessary)
        dob = df_final["dob"].astype(str)
        mask = dob.str.len() >= 10
        df_final.loc[mask, "dob"] = np.where(
            (dob[mask].str[0:4] > "2500") & (dob[mask].str[0:4] < "2600"),
            (dob[mask].str[0:4].astype("Int64") - 543).astype(str) + dob[mask].str[4:], 
            dob[mask]
        )
        df_final["dob"] = pd.to_datetime(df_final["dob"], errors="coerce")

        # Age Calculation
        df_final["age"] = np.where(
            df_final["dob"].notna(),
            df_final["dwh_crtd_dttm"].dt.year - df_final["dob"].dt.year, 
            np.nan
        )

        df_final["totl_kid"] = np.nan
        df_final["totl_eldr"] = np.nan
        df_final["rec_actv_flag"] = df_final["rec_actv_flag"].replace({"Y": "1", "N": "0"})
        
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce").combine_first(df_final["crtd_dttm"])
        df_final["updt_by"] = df_final["updt_by"].combine_first(df_final["crtd_by"])
        
        df_final["party_type_id"] = 2
        df_final["mem_sts"] = pd.array([pd.NA] * len(df_final), dtype="string")
        df_final["mem_crtd_dttm"] = pd.to_datetime(None)
        df_final["nationality_id"] = pd.array([pd.NA] * len(df_final), dtype="string")
        
        # Corporate Type Logic
        df_final["corporate_type"] = np.where(
            df_final["full_nm_th"].astype(str).str.contains("มหาชน|PCL", na=False), "PCL", "LOCAL"
        )

        # 7. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "full_nm_th", "titl_th",
            "frst_nm_th", "last_nm_th", "full_nm_en", "titl_en", "frst_nm_en", "last_nm_en",
            "dob", "age", "th_id", "passport_no", "cnty_cd", "mob_no", "email", "edu", "occu", "income_range",
            "mrtl_sts", "rlgn", "gndr", "fam_size", "totl_kid", "totl_eldr", "rec_actv_flag",
            "crtd_dttm", "crtd_by", "updt_dttm", "updt_by", "party_type_id", "party_id", "date_id",
            "mem_sts", "mem_crtd_dttm", "mem_ref_code", "corporate_type", "nationality_id"
        ]
        df_final = df_final[target_columns]

        if not df_final.empty:
            logger.info(f"transform_dim_party_gu_eco Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final