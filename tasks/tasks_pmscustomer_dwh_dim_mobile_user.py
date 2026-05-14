"""
tasks/tasks_pmscustomer_dwh_dim_mobile_user.py
Origin: etl-mobileregister@prefect-v1 — etl_mobileregister_dwh_dim_mobile_user
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task
from tasks.main_components import ExtractSourceData

@task(name="transform_dim_mobile_user")
def transform_dim_mobile_user(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Matches mobile customers to MSSQL RU/GU owners via phone number.
    Origin: etl-mobileregister@prefect-v1 — etl_mobileregister_dwh_dim_mobile_user
    """
    from config import BUCKET_MSSQL as BUCKET_APPLICATION_MSSQL, BUCKET_NOTIFICATION
    from config_flows import JOB_YEAR, JOB_MONTH, JOB_DAY, JOB_HISOFYER, JOB_NUMOFYER
    
    logger = get_run_logger()
    df_final = pd.DataFrame()
    ex = ExtractSourceData()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation (Source Customer Data)
        df_customer = p_data[0].copy()
        del p_data
        gc.collect()

        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Extract & Transform Supporting Data from Lake
        
        # --- tmRoomH_mobileuser (history) ---
        cols_room = ["fcid", "fcownerid", "fccurrentownerid", "fcownerid2", "fcownerid3", "ftcreatedate", "ftupdatedate"]
        df_roomh = ex.fn_Get_Source_Lake_Hist("", "tmRoomH_mobileuser", "fcid",
            p_hisofyear=JOB_HISOFYER, p_numofyear=JOB_NUMOFYER, p_columns=cols_room, p_bucket_app=BUCKET_APPLICATION_MSSQL)
        df_roomh_all = pd.melt(df_roomh, id_vars=["fcid", "ftcreatedate", "ftupdatedate"],
                            value_vars=["fcownerid", "fccurrentownerid", "fcownerid2", "fcownerid3"],
                            var_name="owner_type", 
                            value_name="fcownerid_temp") # 1. เปลี่ยนชื่อชั่วคราวไม่ให้ซ้ำ

        # 2. กรองข้อมูล และจัดการชื่อให้กลับมาเป็น fcownerid
        df_roomh_all = df_roomh_all[df_roomh_all["fcownerid_temp"].notna() & (df_roomh_all["fcownerid_temp"] != "")]
        df_roomh_all = df_roomh_all.rename(columns={"fcownerid_temp": "fcownerid"}).drop_duplicates()
        del df_roomh
        gc.collect()

        # --- ttCORHist_mobileuser (history) ---
        cols_cor = ["fcid", "fccornewrentalid", "fcroomid", "fctype", "ftcreatedate", "ftupdatedate"]
        df_corhist = ex.fn_Get_Source_Lake_Hist("", "ttCORHist_mobileuser", "fcid",
            p_hisofyear=JOB_HISOFYER, p_numofyear=JOB_NUMOFYER, p_columns=cols_cor, p_bucket_app=BUCKET_APPLICATION_MSSQL)
        
        df_corhist["fctype"] = df_corhist["fctype"].str.strip()
        df_corhist_all = df_corhist[df_corhist["fctype"].isin(["Rential", "Stay"])][["fccornewrentalid", "fcroomid"]].drop_duplicates()
        del df_corhist
        gc.collect()

        # --- tmCOR (phone number mapping) ---
        cols_tmcor = ["fcid", "fcshipmobile", "ftcreatedate", "ftupdatedate"]
        df_cor = ex.fn_Get_Source_Lake_Hist("", "tmCOR", "fcid",
            p_hisofyear=JOB_HISOFYER, p_numofyear=JOB_NUMOFYER, p_columns=cols_tmcor, p_bucket_app=BUCKET_APPLICATION_MSSQL)
        
        df_cor["fcshipmobile"] = df_cor["fcshipmobile"].str.strip()
        df_cor = df_cor[df_cor["fcshipmobile"].notna() & (df_cor["fcshipmobile"] != "")]
        df_cor["mobile_phone"] = np.where(df_cor["fcshipmobile"].str.len() == 9, "0" + df_cor["fcshipmobile"], df_cor["fcshipmobile"])
        df_cor_all = df_cor[["fcid", "mobile_phone"]].drop_duplicates()
        del df_cor
        gc.collect()

        # --- gu_device ---
        df_device = ex.fn_Get_Source_Lake("postgresql", "gu_device", p_bucket_app=BUCKET_NOTIFICATION,
            p_year=JOB_YEAR, p_month=JOB_MONTH, p_day=JOB_DAY,
            p_columns=["customer_id", "device_id"], p_full_data=1)
        df_device = df_device[["customer_id", "device_id"]]

        # 3. Customer Data Formatting
        df_customer["phonenumber"] = df_customer["phonenumber"].str.strip()
        df_customer["mobile_phone"] = np.where(
            df_customer["phonenumber"].str.len() == 9, 
            "0" + df_customer["phonenumber"], 
            df_customer["phonenumber"]
        )
        df_cus_all = df_customer[[
            "id", "username", "phonenumbercountrycode", "mobile_phone",
            "creationtime", "creatoruserid", "lastmodificationtime", "lastmodifieruserid"
        ]]
        del df_customer
        gc.collect()

        # 4. Join Logic (Identify Owner & Resident)
        # Suffixing for clarity before joins
        df_cor_all = df_cor_all.add_suffix("_cor")
        df_corhist_all = df_corhist_all.add_suffix("_hist")
        df_roomh_all = df_roomh_all.add_suffix("_room")
        df_cus_all = df_cus_all.add_suffix("_cus")
        df_device = df_device.add_suffix("_device")

        # Join to find direct owners
        df_owner = pd.merge(
            df_cor_all.assign(fcid_cor=df_cor_all["fcid_cor"].str.strip()), 
            df_roomh_all.assign(fcownerid_room=df_roomh_all["fcownerid_room"].str.strip()), 
            how="inner", left_on="fcid_cor", right_on="fcownerid_room"
        )
        df_owner_all = df_owner[["fcid_cor", "mobile_phone_cor", "fcid_room"]].copy()
        del df_owner

        # Join to find residents (via COR history)
        df_resident = pd.merge(
            df_corhist_all.assign(fccornewrentalid_hist=df_corhist_all["fccornewrentalid_hist"].str.strip()), 
            df_cor_all, 
            how="inner", left_on="fccornewrentalid_hist", right_on="fcid_cor"
        )
        df_resident_all = pd.merge(
            df_resident.assign(fcroomid_hist=df_resident["fcroomid_hist"].str.strip()), 
            df_roomh_all.assign(fcid_room=df_roomh_all["fcid_room"].str.strip()), 
            how="inner", left_on="fcroomid_hist", right_on="fcid_room"
        )
        df_resident_all = df_resident_all[["fcid_cor", "mobile_phone_cor", "fcid_room"]]
        
        # Combine and Final Merge
        df_corall = pd.concat([df_owner_all, df_resident_all]).drop_duplicates()
        del df_owner_all, df_resident_all, df_roomh_all, df_corhist_all, df_cor_all
        gc.collect()

        df_final = pd.merge(
            df_cus_all.assign(mobile_phone_cus=df_cus_all["mobile_phone_cus"].str.strip()), 
            df_corall.assign(mobile_phone_cor=df_corall["mobile_phone_cor"].str.strip()), 
            how="left", left_on="mobile_phone_cus", right_on="mobile_phone_cor"
        )
        df_final = pd.merge(df_final, df_device, how="left", left_on="id_cus", right_on="customer_id_device")
        
        del df_corall, df_device, df_cus_all
        gc.collect()

        # 5. Add DWH Metadata & Date Handling
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        upd = df_final["lastmodificationtime_cus"].combine_first(df_final["creationtime_cus"])
        df_final["date_id"] = pd.to_datetime(
            upd.str[0:4] + upd.str[5:7].str.zfill(2) + upd.str[8:11].str.zfill(2)
        ).dt.date

        # 6. Column Renaming & Clean up
        rename_map = {
            "id_cus": "customer_id",
            "device_id_device": "device_id",
            "username_cus": "user_name",
            "phonenumbercountrycode_cus": "country_code",
            "mobile_phone_cus": "mobile_phone",
            "fcid_room": "unit_id",
            "creationtime_cus": "crtd_dttm",
            "creatoruserid_cus": "crtd_by",
            "lastmodificationtime_cus": "updt_dttm",
            "lastmodifieruserid_cus": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # Transformation logic
        df_final["customer_id"] = df_final["customer_id"].astype("Int64")
        df_final["unit_id"] = df_final["unit_id"].fillna("0")
        df_final["user_type"] = np.where(df_final["unit_id"] == "0", "GU", "RU")
        
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        df_final["updt_dttm"] = df_final["updt_dttm"].combine_first(df_final["crtd_dttm"])
        df_final["updt_by"] = df_final["updt_by"].combine_first(df_final["crtd_by"])
        
        df_final["history_status"] = "N"
        df_final = df_final.drop_duplicates().sort_values(["date_id", "customer_id"])

        # 7. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "customer_id", "unit_id", "device_id", "user_name", "country_code", "mobile_phone",
            "user_type", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by", "history_status"
        ]
        df_final = df_final[target_columns]

        if not df_final.empty:
            logger.info(f"transform_dim_mobile_user Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final