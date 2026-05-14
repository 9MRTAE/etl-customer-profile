"""
tasks/tasks_pmscustomer_dwh_fact_pmsinvitation_company.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_fact_pmsinvitation_company
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_pmsinvitation_company")
def transform_fact_pmsinvitation_company(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Join
        df_ic = p_data[0].copy() # Invitation Company
        df_inv = p_data[1].copy().reset_index().add_suffix("_invitations") # Invitations Details
        del p_data
        gc.collect()

        # Join Invitation Link with Invitation Details
        df_final = pd.merge(
            df_ic, df_inv, 
            how="left", 
            left_on="invitation_id", 
            right_on="id_invitations"
        )
        
        # ล้างค่าว่างเบื้องต้น
        df_final = df_final.replace("", np.nan).copy()
        
        # มาตรฐานเวลา UTC+7
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        # สร้าง date_id จากวันที่ตอบรับ (responded_at)
        df_final["date_id"] = pd.to_datetime(df_final["responded_at"], errors="coerce").dt.date

        # 3. Column Mapping & Renaming
        rename_map = {
            "id": "record_id",
            "invitation_id": "invite_id",
            "company_id": "pmscompany_id",
            "status": "invite_status",
            "invited_by_invitations": "invite_by",
            "email_invitations": "invite_email",
            "responded_at": "responded_dttm"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # จัดการข้อมูล String ไม่ให้ติดค่า 'nan' โดยเฉพาะ Email และ Status
        str_cols = ["invite_status", "invite_by", "invite_email"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric Casting
        df_final["pmscompany_id"] = pd.to_numeric(df_final["pmscompany_id"], errors="coerce").astype("Int64")
        
        # 5. Flags & Audit Columns
        df_final["responded_dttm"] = pd.to_datetime(df_final["responded_dttm"], errors="coerce")
        df_final["rec_actv_flag"] = "1"
        
        # สำหรับ Fact Table นี้ ใช้เวลาตอบรับเป็นตัวตั้งของ Audit Time
        df_final["crtd_dttm"] = df_final["responded_dttm"]
        df_final["updt_dttm"] = df_final["responded_dttm"]
        df_final["crtd_by"] = pd.NA
        df_final["updt_by"] = pd.NA

        # 6. Final Select Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "record_id", "invite_id", "pmscompany_id", "invite_status", "invite_by", "invite_email",
            "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_pmsinvitation_company Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final