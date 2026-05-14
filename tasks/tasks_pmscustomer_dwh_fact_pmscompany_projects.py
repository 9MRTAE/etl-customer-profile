"""
tasks/tasks_pmscustomer_dwh_fact_pmscompany_projects.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_fact_pmscompany_projects
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_fact_pmscompany_projects")
def transform_fact_pmscompany_projects(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Joins
        df_cp = p_data[0].copy()                  # Mapping Log (Main)
        df_comp = p_data[1].copy().add_suffix("_company") # Company Info
        df_proj = p_data[2].copy().add_suffix("_project") # Project Info
        del p_data
        gc.collect()

        # Join Log with Company and Project details
        df_final = pd.merge(df_cp, df_comp, how="left", left_on="company_id", right_on="id_company")
        df_final = pd.merge(df_final, df_proj, how="left", left_on="project_id", right_on="id_project")
        
        # Replace empty strings with NaN for consistent handling
        df_final = df_final.replace("", np.nan).copy()
        
        # มาตรฐานเวลา UTC+7
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        # Create Date ID from log timestamp
        df_final["date_id"] = pd.to_datetime(df_final["created_at"], errors="coerce").dt.date

        # 3. Column Renaming
        rename_map = {
            "id": "mapping_log_id",
            "company_project_id": "mapping_id",
            "code_company": "company_cd",
            "name_th_company": "company_nm_th",
            "name_th_project": "pmsproject_nm_th",
            "pms_project_id_project": "ref_pmsproject_id",
            "action_type": "status_nm",
            "created_at": "crtd_dttm",
            "created_by": "crtd_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 4. Data Transformation & Handling "nan" Strings (ข้อ 4)
        # ทำความสะอาดข้อมูล String สำคัญ
        str_cols = ["company_cd", "company_nm_th", "pmsproject_nm_th", "status_nm"]
        for col in str_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].apply(
                    lambda x: "" if pd.isna(x) or str(x).lower() == 'nan' else str(x).strip()
                )

        # Numeric Casting
        id_cols = ["mapping_log_id", "mapping_id", "company_id", "project_id"]
        for col in id_cols:
            df_final[col] = pd.to_numeric(df_final[col], errors="coerce").astype("Int64")

        # Flag & Audit Trail (Fact table มักมีแต่ insert)
        df_final["rec_actv_flag"] = "1"
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_by"] = None
        df_final["updt_dttm"] = pd.to_datetime(None)

        # 5. Final Column Selection
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "mapping_log_id", "mapping_id", "company_id", "company_cd", "company_nm_th",
            "project_id", "ref_pmsproject_id", "pmsproject_nm_th", "status_nm",
            "rec_actv_flag", "crtd_by", "crtd_dttm", "updt_by", "updt_dttm"
        ]
        
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_fact_pmscompany_projects Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final