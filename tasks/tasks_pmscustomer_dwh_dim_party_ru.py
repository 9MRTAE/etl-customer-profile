"""
tasks/tasks_pmscustomer_dwh_dim_party_ru.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_party_ru
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task
from datetime import datetime

@task(name="transform_dim_party_ru")
def transform_dim_party_ru(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        # ตรวจสอบ Input
        if not p_data or len(p_data) < 3 or p_data[0].empty:
            logger.warning("Input data is empty or incomplete.")
            return df_final

        # 1. Initial Data Preparation (จัดการ Memory)
        df_tmcor = p_data[0].copy()
        # df_mg = p_data[1].copy() # ถ้าไม่ได้ใช้ในการคำนวณในส่วนนี้สามารถข้ามได้
        df_r = p_data[2].copy()
        del p_data
        gc.collect()

        # Normalize Column Names
        df_tmcor.columns = df_tmcor.columns.str.lower()
        df_r.columns = df_r.columns.str.lower()
        
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Extract Unique Owners (Unpivot logic - หนึ่งห้องหลายเจ้าของ)
        owner_cols = ["fcownerid", "fcownerid2", "fcownerid3", "fccurrentownerid"]
        parts = []
        for col in owner_cols:
            if col in df_r.columns:
                p = df_r[["fcid", col]].rename(columns={col: "fcid_owner"})
                parts.append(p)

        df_owners = pd.concat(parts).drop_duplicates(subset=["fcid_owner"])
        df_owners = df_owners.loc[df_owners["fcid_owner"].fillna("") != ""].copy()
        df_owners = df_owners.add_suffix("_P")

        # 3. Pre-processing tmCOR (Logic จากโค้ดที่ 1)
        # Mapping Gender
        gender_map = {
            'WOMAN': 'หญิง', 'FEMALE': 'หญิง', 'FAMALE': 'หญิง',
            'MEN': 'ชาย', 'MALE': 'ชาย', 'MAN': 'ชาย',
            'FEMALE/MAN': 'PreferNotToRespond', 'MAN/FEMALE': 'PreferNotToRespond', 'COMPANY': 'PreferNotToRespond'
        }
        df_tmcor['fcsex_map'] = df_tmcor['fcsex'].astype(str).str.upper().replace(gender_map)
        df_tmcor['fcsex_map'] = np.where(df_tmcor['fcsex_map'].isin(['หญิง', 'ชาย', 'PreferNotToRespond']), 
                                        df_tmcor['fcsex_map'], 'ไม่ระบุเพศ')

        # Active Flag & Corporate Type
        df_tmcor["fcisactive"] = df_tmcor["fcisactive"].astype(str).replace({"N": "0", "false": "0", "Y": "1", "true": "1"})
        df_tmcor["corporate_type"] = np.where(
            df_tmcor["fcname"].astype(str).str.contains("มหาชน|PCL", na=False), "PCL", "LOCAL"
        )
        
        df_tmcor = df_tmcor.add_suffix("_C")

        # 4. Join Data
        df_final = pd.merge(
            df_owners, 
            df_tmcor, 
            how="inner", 
            left_on="fcid_owner_P", 
            right_on="fcid_C"
        )

        # 5. DWH Metadata & Date ID (ใช้วิธีที่แม่นยำขึ้น)
        df_final["dwh_crtd_dttm"] = datetime_now
        df_final["dwh_updt_dttm"] = datetime_now
        df_final["dwh_note"] = pd.Series([pd.NA] * len(df_final), dtype="Int64")
        
        # สร้าง date_id (YYYYMMDD)
        upd_series = df_final["ftupdatedate_C"].combine_first(df_final["ftcreatedate_C"])
        df_final["date_id"] = pd.to_datetime(upd_series, errors='coerce').dt.date

        # 6. Column Renaming
        rename_map = {
            "fcname_C": "full_nm_th",
            "fcengname_C": "full_nm_en",
            "ftbirthday_C": "dob",
            "fctaxid_C": "th_id",
            "fcpassport_C": "passport_no",
            "fccountrycode_C": "cnty_cd",
            "fcshipmobile_C": "mob_no",
            "fcshipemail_C": "email",
            "fceducation_C": "edu",
            "fcmarrystatus_C": "mrtl_sts",
            "ftcreatedate_C": "crtd_dttm",
            "fccreator_C": "crtd_by",
            "fcupdateby_C": "updt_by",
            "fcid_C": "party_id"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # Initialize Missing Columns
        null_cols = ["titl_th", "frst_nm_th", "last_nm_th", "titl_en", "frst_nm_en", "last_nm_en",
                     "occu", "income_range", "rlgn", "fam_size", "mem_ref_code"]
        for col in null_cols:
            df_final[col] = None

        # 7. Transformation (DOB & Age & Flag)
        # จัดการปี พ.ศ. ให้เป็น ค.ศ.
        def fix_thai_year(x):
            if pd.isna(x) or len(str(x)) < 4: return x
            y = int(str(x)[:4])
            return str(y-543) + str(x)[4:] if y > 2500 else x

        df_final['dob'] = df_final['dob'].apply(fix_thai_year)
        df_final['dob'] = pd.to_datetime(df_final['dob'], errors='coerce')
        
        # คำนวณอายุ
        df_final['age'] = np.where(
            df_final['dob'].notnull(), 
            datetime_now.year - df_final['dob'].dt.year,
            pd.to_numeric(df_final['fnage_C'], errors='coerce').fillna(0)
        )

        df_final["gndr"] = df_final["fcsex_map_C"]
        df_final["totl_kid"] = np.nan
        df_final["totl_eldr"] = np.nan
        df_final["rec_actv_flag"] = df_final["fcisactive_C"]
        
        # Update dates
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors='coerce')
        df_final["updt_dttm"] = pd.to_datetime(df_final["ftupdatedate_C"], errors='coerce').combine_first(df_final["crtd_dttm"])
        df_final["updt_by"] = df_final["updt_by"].combine_first(df_final["crtd_by"])
        
        # Constant fields
        df_final["party_type_id"] = 1
        df_final["mem_sts"] = pd.Series([pd.NA] * len(df_final), dtype="string")
        df_final["mem_crtd_dttm"] = pd.to_datetime(None)
        df_final["nationality_id"] = pd.Series([pd.NA] * len(df_final), dtype="string")
        df_final["corporate_type"] = df_final["corporate_type_C"]

        # 8. Select Final Columns
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
            logger.info(f"Success: {len(df_final)} rows transformed.")

    except Exception as e:
        logger.error(f"Transform Failed: {str(e)}")
        raise

    return df_final