"""
tasks/tasks_pmscustomer_dwh_fact_unitresident.py
Origin: etl-mobileregister@prefect-v1 — etl_mobileregister_dwh_fact_unitresident
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_unitresident")
def transform_fact_unitresident(p_data: list[pd.DataFrame]) -> pd.DataFrame:
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
        df_final = df_final.replace("", np.nan).copy()

        # 2. Add DWH Metadata & Date ID
        # มาตรฐานเวลา UTC+7 (Bangkok)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")
        
        # สำหรับตารางนี้ใช้เวลาปัจจุบันเป็น date_id ตามโค้ดต้นฉบับ
        df_final["date_id"] = datetime_now.date()

        # 3. Column Mapping & Renaming
        rename_map = {
            "fcid": "corhist_id",
            "fcroomid": "unit_id",
            "fccornewrentalid": "party_id",
            "fcisactive": "hisrory_status", # พิมพ์ผิดตามต้นฉบับ (history)
            "fctype": "resident_type",
            "ftdatein": "move_in_date",
            "ftdateout": "move_out_date",
            "ftcreatedate": "crtd_dttm",
            "fccreator": "crtd_by",
            "ftupdatedate": "updt_dttm",
            "fcupdateby": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # เคลียร์ค่าว่างในฟิลด์ String สำคัญ
        str_cols = ["corhist_id", "unit_id", "party_id", "resident_type", "hisrory_status", "crtd_by", "updt_by"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # 5. Buddhist Era (พ.ศ.) Handling & Datetime Casting
        # ฟังก์ชันแปลงปี พ.ศ. เป็น ค.ศ. แบบปลอดภัย
        def safe_convert_year(x):
            if not isinstance(x, str) or len(x) < 4:
                return x
            try:
                year_part = int(x[:4])
                # ถ้าปีเกิน 2400 สันนิษฐานว่าเป็น พ.ศ.
                if year_part > 2400:
                    return str(year_part - 543) + x[4:]
            except:
                pass
            return x

        date_cols = ["move_in_date", "move_out_date", "crtd_dttm", "updt_dttm"]
        for c in date_cols:
            if c in df_final.columns:
                df_final[c] = pd.to_datetime(df_final[c].apply(safe_convert_year), errors="coerce")

        # 6. Flags & Final Select
        df_final["rec_actv_flag"] = "1"
        
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id", "corhist_id",
            "unit_id", "party_id", "resident_type", "move_in_date", "move_out_date", "hisrory_status",
            "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_unitresident Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final