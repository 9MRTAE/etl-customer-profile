import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_party_homeservice")
def transform_dim_party_homeservice(p_data: list[pd.DataFrame]) -> pd.DataFrame:
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

        # 2. Add DWH Metadata
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")

        # 3. Name & Identity Transformation
        df_final["full_nm_th"] = df_final["provider_name"].combine_first(df_final["registered_name"])
        df_final["full_nm_en"] = df_final["full_nm_th"]

        # Initialize Default Null Columns
        null_cols = [
            "titl_th", "frst_nm_th", "last_nm_th", "titl_en", "frst_nm_en", "last_nm_en",
            "th_id", "passport_no", "cnty_cd", "mob_no", "email", "edu", "occu",
            "income_range", "mrtl_sts", "rlgn", "gndr", "fam_size", "mem_ref_code"
        ]
        for col in null_cols:
            df_final[col] = None

        df_final["dob"] = pd.to_datetime(None)
        df_final["mem_crtd_dttm"] = pd.to_datetime(None)
        df_final["age"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        df_final["totl_kid"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        df_final["totl_eldr"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        df_final["mem_sts"] = pd.array([pd.NA] * len(df_final), dtype="string")
        df_final["nationality_id"] = pd.array([pd.NA] * len(df_final), dtype="string")

        # 4. Status & Flag Logic
        df_final["rec_actv_flag"] = np.where(
            df_final["is_active"].astype(str).str.lower().isin(["true", "1"]), "1",
            np.where(df_final["is_active"].astype(str).str.lower().isin(["false", "0"]), "0", np.nan)
        )

        # 5. Timestamp Handling (Unix to Datetime)
        # Convert to float first to handle potential string types
        create_ts = pd.to_numeric(df_final["create_date"], errors='coerce')
        update_ts = pd.to_numeric(df_final["update_date"], errors='coerce')

        df_final["crtd_dttm"] = pd.to_datetime(create_ts, unit="s", errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(
            update_ts.combine_first(create_ts), unit="s", errors="coerce"
        )
        
        df_final.rename(columns={"create_by": "crtd_by", "provider_id": "party_id"}, inplace=True)
        df_final["updt_by"] = df_final["update_by"].combine_first(df_final["crtd_by"])
        df_final["party_type_id"] = 4

        # 6. Corporate Type & Date ID
        df_final["corporate_type"] = np.where(
            df_final["full_nm_th"].astype(str).str.contains("มหาชน|PCL", na=False), "PCL", "LOCAL"
        )

        # Generate Date ID from the most recent timestamp
        updt_str = df_final["updt_dttm"].combine_first(df_final["crtd_dttm"]).astype(str)
        df_final["date_id"] = pd.to_datetime(
            updt_str.str[0:4] + updt_str.str[5:7].str.zfill(2) + updt_str.str[8:11].str.zfill(2)
        ).dt.date

        # 7. Final Column Selection
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
            logger.info(f"transform_dim_party_homeservice Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final