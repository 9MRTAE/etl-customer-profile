"""
tasks/tasks_pmscustomer_dwh_dim_party_update_memberstatus.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_party_update_memberstatus
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task
from tasks.main_components import ExtractSourceData

@task(name="transform_dim_party_update_memberstatus")
def transform_dim_party_update_memberstatus(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Updates dim_party with loyalty membership status.
    Reads dim_party from BigQuery, tmmobileuser from GCS lake, tmShareFriend from loyalty bucket.
    Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_party_update_memberstatus
    """
    from config import BUCKET_MSSQL as BUCKET_APPLICATION, BUCKET_LOYALTY as BUCKET_LOYALTY
    
    logger = get_run_logger()
    df_final = pd.DataFrame()
    ex = ExtractSourceData()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation (Member List)
        df_ml = p_data[0].drop_duplicates().add_suffix("_member")
        del p_data
        gc.collect()

        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Extract Supporting Data
        
        # --- DWH dim_party ---
        list_userid = df_ml["fcuserid_member"].drop_duplicates().tolist()
        list_userid_str = ", ".join(repr(e) for e in list_userid)
        query = f"""
            SELECT DISTINCT *
            FROM your-gcp-project-id.los.dim_party
            WHERE PARTY_TYPE_ID IN (1,2)
            AND LOWER(PARTY_ID) IN ({list_userid_str})
        """
        df_party = ex.fn_Get_DWH(query)

        # --- tmmobileuser from Lake ---
        cols_mu = ["fcphoneno", "fcuserid", "fnnatid", "calendar_year", "month_no", "day_of_month"]
        df_mu = ex.fn_Gen_Source_Lake_Join("", "tmmobileuser", "fcuserid", BUCKET_APPLICATION, cols_mu)
        df_mu = df_mu.add_suffix("_mobile")
        df_mu["fcphoneno_mobile"] = "0" + df_mu["fcphoneno_mobile"].astype(str)

        # --- tmShareFriend from Loyalty Bucket ---
        cols_sf = ["fcuserid", "fcfrienduserid", "calendar_year", "month_no", "day_of_month"]
        df_sf = ex.fn_Gen_Source_Lake_Join("", "tmShareFriend", ["fcuserid", "fcfrienduserid"], BUCKET_LOYALTY, cols_sf)
        df_sf = df_sf.add_suffix("_share")

        # 3. Join Logic
        # Join Member with Mobile Info
        df_merge = pd.merge(
            df_ml, df_mu, 
            how="inner", 
            left_on="fcuserid_member", 
            right_on="fcuserid_mobile"
        )
        del df_ml, df_mu
        gc.collect()

        # Join with DWH dim_party
        df_merge = pd.merge(
            df_merge, df_party, 
            how="inner",
            left_on=["fcphoneno_mobile", "fcuserid_mobile"], 
            right_on=["mob_no", "party_id"]
        )
        del df_party
        gc.collect()

        # Join with Share Friend (Referral)
        df_final = pd.merge(
            df_merge, df_sf, 
            how="left", 
            left_on="fcuserid_mobile", 
            right_on="fcfrienduserid_share"
        )
        del df_merge, df_sf
        gc.collect()

        # 4. Update Loyalty Columns
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = df_final["dwh_note"].astype("Int32")
        
        # Membership specific info
        df_final["mem_sts"] = "1"
        df_final["mem_crtd_dttm"] = pd.to_datetime(df_final['ftcreatedate_member'], unit='ns', errors='coerce')
        
        # Referral mapping
        df_final["mem_ref_code"] = np.where(
            df_final["fcuserid_share"].notna(), 
            df_final["fcuserid_share"], 
            "-"
        )
        df_final["mem_ref_code"] = df_final["mem_ref_code"].astype("string")

        # Casting for Schema Consistency
        df_final["totl_kid"] = df_final["totl_kid"].astype("float")
        df_final["totl_eldr"] = df_final["totl_eldr"].astype("float")

        # 5. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "full_nm_th", "titl_th", "frst_nm_th", "last_nm_th",
            "full_nm_en", "titl_en", "frst_nm_en", "last_nm_en", "dob", "age", "th_id", "passport_no", "cnty_cd",
            "mob_no", "email", "edu", "occu", "income_range", "mrtl_sts", "rlgn", "gndr", "fam_size", "totl_kid",
            "totl_eldr", "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by", "party_type_id",
            "party_id", "date_id", "mem_sts", "mem_crtd_dttm", "mem_ref_code", "corporate_type", "nationality_id"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_party_update_memberstatus Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final