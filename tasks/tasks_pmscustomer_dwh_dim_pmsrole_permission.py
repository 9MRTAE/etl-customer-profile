"""
tasks/tasks_pmscustomer_dwh_dim_pmsrole_permission.py
Origin: etl-pmsmanagement@prefect-v1 — etl_pmsmanagement_dwh_dim_pmsrole_permission
"""
import gc
import numpy as np
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_pmsrole_permission")
def transform_dim_pmsrole_permission(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()

    def _norm_bool(v):
        if pd.isna(v): return None
        if isinstance(v, bool): return v
        s = str(v).strip().lower()
        if s in {"true", "1", "t", "y", "yes", "active"}: return True
        if s in {"false", "0", "f", "n", "no", "inactive"}: return False
        return None

    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation & Join
        df_rp = p_data[0].copy()
        df_roles = p_data[1].copy().add_suffix("_roles")
        del p_data
        gc.collect()

        df_final = pd.merge(df_rp, df_roles, how="left", left_on="role_id", right_on="id_roles")
        df_final = df_final.replace("", np.nan).copy()
        
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Add DWH Metadata & Date ID
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int64")
        
        src_dt = pd.to_datetime(df_final["created_at"].combine_first(df_final["updated_at"]), errors="coerce")
        df_final["date_id"] = src_dt.dt.date

        # 3. Rename & Type Casting (Core IDs)
        df_final.rename(columns={"id": "record_id", "module_permission_id": "module_id"}, inplace=True)
        
        for c in ["record_id", "role_id", "module_id"]:
            df_final[c] = pd.to_numeric(df_final[c], errors="coerce").astype("Int64")

        # 4. Permission Flags Normalization
        perm_map = [
            ("can_all", "is_all"), ("can_view", "is_view"), ("can_create", "is_create"),
            ("can_edit", "is_edit"), ("can_print", "is_print"), ("can_approve", "is_approve"),
            ("can_cancel", "is_cancel"), ("allowed", "is_allowed")
        ]
        for old, new in perm_map:
            df_final.rename(columns={old: new}, inplace=True)
            df_final[new] = df_final[new].apply(_norm_bool)

        # 5. Roles Information Mapping
        role_rename_map = {
            "company_id_roles": "pmscompany_id",
            "project_id_roles": "pmsproject_id",
            "code_roles": "role_cd",
            "name_th_roles": "role_nm_th",
            "name_en_roles": "role_nm_en",
            "category_roles": "role_category",
            "description_roles": "role_desc",
            "is_system_roles": "role_issystem",
            "is_active_roles": "role_isactive",
            "created_by_roles": "role_crtd_by",
            "created_at_roles": "role_crtd_dttm",
            "updated_by_roles": "role_updt_by",
            "updated_at_roles": "role_updt_dttm"
        }
        df_final.rename(columns=role_rename_map, inplace=True)

        # Normalize Role Booleans & Datetime
        df_final["role_issystem"] = df_final["role_issystem"].apply(_norm_bool)
        df_final["role_isactive"] = df_final["role_isactive"].apply(_norm_bool)
        
        for c in ["pmscompany_id", "pmsproject_id"]:
            df_final[c] = pd.to_numeric(df_final[c], errors="coerce").astype("Int64")
            
        for c in ["role_crtd_dttm", "role_updt_dttm"]:
            df_final[c] = pd.to_datetime(df_final[c], errors="coerce")

        # 6. Final Clean up & Audit Columns
        df_final["rec_actv_flag"] = "1"
        df_final.rename(columns={
            "created_at": "crtd_dttm",
            "created_by": "crtd_by",
            "updated_at": "updt_dttm",
            "updated_by": "updt_by"
        }, inplace=True)

        for c in ["crtd_dttm", "updt_dttm"]:
            df_final[c] = pd.to_datetime(df_final[c], errors="coerce")

        # 7. Select Final Columns
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "date_id",
            "record_id", "role_id", "module_id", "is_all", "is_view", "is_create",
            "is_edit", "is_print", "is_approve", "is_cancel", "is_allowed",
            "pmscompany_id", "pmsproject_id", "role_cd", "role_nm_th", "role_nm_en",
            "role_category", "role_desc", "role_issystem", "role_isactive", "role_crtd_by",
            "role_crtd_dttm", "role_updt_by", "role_updt_dttm", "rec_actv_flag", "crtd_by", "crtd_dttm", "updt_by", "updt_dttm"
            
        ]
        df_final = df_final[target_columns].drop_duplicates()

        if not df_final.empty:
            logger.info(f"transform_dim_pmsrole_permission Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final