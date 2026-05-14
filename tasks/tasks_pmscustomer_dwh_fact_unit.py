"""
tasks/tasks_pmscustomer_dwh_fact_unit.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_fact_unit
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_unit")
def transform_fact_unit(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df_tmRoomH = p_data[0].copy() # ข้อมูลห้อง/ยูนิต
        df_tmBRN = p_data[1].copy()   # ข้อมูลสาขา/โครงการ
        del p_data
        gc.collect()

        df_tmRoomH.columns = df_tmRoomH.columns.str.lower()
        df_tmBRN.columns = df_tmBRN.columns.str.lower()

        # 2. Unpivot Owner IDs (การแตกแถวจาก 1 unit ที่มีหลายเจ้าของ เป็นหลายแถว)
        parts = []
        owner_cols = ["fcownerid", "fcownerid2", "fcownerid3", "fccurrentownerid"]
        
        subset_cols = [
            "fcbrnid", "fcid", "fttransferdate", "ftdatein", "ftdateout",
            "fcleveingtype", "fcisactive", "ftcreatedate", "fccreateby",
            "ftupdatedate", "fcupdateby"
        ]

        for col in owner_cols:
            if col in df_tmRoomH.columns:
                p = df_tmRoomH[subset_cols + [col]].copy()
                p.rename(columns={col: "ownerid"}, inplace=True)
                parts.append(p)
        
        df_all = pd.concat(parts, ignore_index=True)
        # กรองเฉพาะแถวที่มี Owner ID จริงๆ
        df_all = df_all[df_all["ownerid"].notna() & (df_all["ownerid"].astype(str).str.strip() != "")]
        
        df_all = df_all.add_suffix("_R")
        df_tmBRN = df_tmBRN.add_suffix("_B")

        # 3. Join & Metadata
        df_final = pd.merge(df_all, df_tmBRN, how="left", left_on="fcbrnid_R", right_on="fcid_B")
        del df_all; del df_tmBRN; gc.collect()

        # มาตรฐานเวลา UTC+7
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        # 4. Date & Buddhist Era Handling (จัดการปี พ.ศ. -> ค.ศ.)
        def safe_bud(s):
            s = s.astype(str)
            # ดึงปี 4 ตัวแรกออกมา
            years_str = s.str[0:4]
            # แปลงเป็นตัวเลข ถ้าไม่ใช่ตัวเลข (None, nan, string อื่นๆ) จะกลายเป็น NaN
            years_numeric = pd.to_numeric(years_str, errors='coerce')
            
            # สร้างหน้ากาก (Mask) สำหรับปีที่เป็น พ.ศ. (> 2500)
            is_buddhist = (years_numeric > 2500) & (years_numeric.notna())
            
            # คำนวณปี ค.ศ.
            fixed_years = (years_numeric[is_buddhist] - 543).astype(int).astype(str)
            
            # รวมปีที่แก้แล้วกับส่วนที่เหลือของ String (ตั้งแต่ตัวที่ 4 เป็นต้นไป)
            result = s.copy()
            result[is_buddhist] = fixed_years + s.str[4:]
            
            # กรองค่าที่เป็น string ขยะออกให้เป็นจริง
            result = result.replace(['nan', 'None', 'NAT', 'NaT'], np.nan)
            return result

        # ปรับปรุงส่วนการดึง date_id ให้ปลอดภัยขึ้นด้วย
        upd = df_final["ftupdatedate_R"].combine_first(df_final["ftcreatedate_R"]).astype(str)
        # ใช้ str.extract เพื่อความแม่นยำ หรือ pd.to_datetime ตรงๆ
        df_final["date_id"] = pd.to_datetime(upd, errors="coerce").dt.date

        date_map = {
            "fttransferdate_R": "transfer_date",
            "ftdatein_R": "move_in_date",
            "ftdateout_R": "move_out_date"
        }
        for src, tgt in date_map.items():
            df_final[tgt] = pd.to_datetime(safe_bud(df_final[src]), errors="coerce")

        # 5. Renaming & Formatting
        df_final.rename(columns={
            "fcid_R": "unit_id",
            "fccomid_B": "project_id",
            "ownerid_R": "party_id",
            "fcleveingtype_R": "resident_type",
            "ftcreatedate_R": "crtd_dttm",
            "fccreateby_R": "crtd_by",
            "ftupdatedate_R": "updt_dttm",
            "fcupdateby_R": "updt_by"
        }, inplace=True)

        # Mapping ประเภทผู้อยู่อาศัย
        df_final["resident_type"] = df_final["resident_type"].astype(str).str.strip().replace({
            "1": "อยู่เอง", "2": "ปล่อยเช่า", "3": "เช่าพื้นที่ส่วนกลาง"
        })

        # Handling "nan" Strings (ข้อ 4)
        str_cols = ["unit_id", "project_id", "party_id", "resident_type", "crtd_by", "updt_by"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Flags & Audit
        df_final["rec_actv_flag"] = df_final["fcisactive_R"].astype(str).str.lower().replace({
            "n": "0", "false": "0", "y": "1", "true": "1"
        })
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        df_final["updt_by"] = df_final["updt_by"].combine_first(df_final["crtd_by"])

        # 6. Final Select
        target_cols = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "transfer_date",
            "move_in_date", "move_out_date", "resident_type", "rec_actv_flag",
            "crtd_dttm", "crtd_by", "updt_dttm", "updt_by", "unit_id", "project_id", "party_id", "date_id"
        ]
        df_final = df_final[target_cols].drop_duplicates()

        logger.info(f"transform_fact_unit Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final