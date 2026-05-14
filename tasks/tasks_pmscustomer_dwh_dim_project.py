"""
tasks/tasks_pmscustomer_dwh_dim_project.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_project
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_project")
def transform_dim_project(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Join
        df_com = p_data[0].copy().add_suffix("_com")
        df_ct = p_data[1].copy().add_suffix("_comtype")
        del p_data
        gc.collect()

        df_final = pd.merge(
            df_com, df_ct, 
            how="left", 
            left_on="fcremark33_com", 
            right_on="fcid_comtype"
        )
        
        # ใช้มาตรฐาน UTC+7
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")
        
        upd = df_final["ftupdatedate_com"].combine_first(df_final["ftcreatedate_com"])
        df_final["date_id"] = pd.to_datetime(
            upd.astype(str).str[0:4] + 
            upd.astype(str).str[5:7].str.zfill(2) + 
            upd.astype(str).str[8:11].str.zfill(2),
            errors="coerce"
        ).dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "fccode_com": "project_code",
            "fcname_com": "project_name",
            "fctaxid_com": "tax_id",
            "fcremark34_com": "latitude",
            "fcremark35_com": "longtitude",
            "fcremark32_com": "pmc_id",
            "fcisactive_com": "rec_actv_flag",
            "ftcreatedate_com": "crtd_dttm",
            "fccreator_com": "crtd_by",
            "ftupdatedate_com": "updt_dttm",
            "fcupdateby_com": "updt_by",
            "fcid_com": "project_id",
            "fccomshortcode_com": "project_shortcode",
            "fctel_com": "phone_no",
            "fntotalunit_com": "unit",
            "fntotalcarpark_com": "car_parking_slot",
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # Type Mapping
        type_map = {
            "1": "condominium", "2": "housing community", "3": "apartment", 
            "4": "housing community", "5": "housing community", "6": "domitory", 
            "7": "housing community", "8": "resort", "9": "company", "999": "other"
        }
        df_final["type"] = df_final["fnitemno_comtype"].astype(str).replace(type_map)

        # Address Construction with nan-handling
        addr_cols = ["fcaddr11_com", "fcaddr12_com", "fcaddr13_com"]
        for col in addr_cols:
            df_final[col] = df_final[col].apply(lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip())
        
        df_final["address"] = (df_final["fcaddr11_com"] + " " + df_final["fcaddr12_com"] + " " + df_final["fcaddr13_com"]).str.strip()

        # Handle total_unit with nan-handling and type conversion
        df_final['unit'] = df_final['unit'].fillna(0)
        df_final['unit'] = df_final['unit'].astype("float")

        # Handle car_parking_slot with nan-handling and type conversion
        df_final['commonfee_amount_mavg'] = np.NaN
        df_final['commonfee_amount_mavg'] = df_final['commonfee_amount_mavg'].astype("float")

        # Handle car_parking_slot with nan-handling and type conversion
        df_final['car_parking_slot'] = df_final['car_parking_slot'].fillna(0)
        df_final['car_parking_slot'] = df_final['car_parking_slot'].astype("float")
        
        # Initialize missing columns
        for col in ["subdistrict", "district", "province", "postal_code"]:
            df_final[col] = None
        df_final["IsContractActive"] = None

        # Project Profile ID & Corporate Type
        df_final["project_profile_id"] = df_final["project_id"]
        # จัดการ project_name ก่อนเช็ค PCL
        df_final["project_name"] = df_final["project_name"].apply(lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip())
        df_final["corporate_Type"] = np.where(df_final["project_name"].str.contains("มหาชน", na=False), "PCL", "LOCAL")

        # Flag & Datetime Casting
        df_final["rec_actv_flag"] = df_final["rec_actv_flag"].astype(str).str.upper().replace({"N": "0", "Y": "1", "FALSE": "0", "TRUE": "1"})
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        
        # 5. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "project_code", "project_name","unit","car_parking_slot","commonfee_amount_mavg",
            "tax_id", "type", "address", "subdistrict", "district", "province", "postal_code", "latitude",
            "longtitude", "pmc_id", "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by",
            "project_id", "project_profile_id", "date_id", "IsContractActive", "corporate_Type",
            "project_shortcode", "phone_no"
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_project Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final