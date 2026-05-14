"""
tasks/tasks_pmscustomer_dwh_dim_address.py
Origin: etl-iprop@prefect-v1 — etl_iprop_dwh_dim_address
"""
import gc
import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_address")
def transform_dim_address(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = pd.DataFrame()
    
    try:
        if not p_data or p_data[0].empty:
            return df_final

        # 1. Initial Data Preparation
        df = p_data[0].copy()
        del p_data
        gc.collect()
        
        df.columns = df.columns.str.lower()
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)

        # 2. Define Internal Helper Function
        def make_addr(source_df, addr1, addr2, addr3, sub, dist, zip_code, type_id):
            cols = [
                "fcid", addr1, addr2, addr3, zip_code, sub, dist, 
                "fcisactive", "ftcreatedate", "fccreator", "ftupdatedate", "fcupdateby"
            ]
            d = source_df[cols].copy()
            
            # Create address string
            d["address"] = (
                d[addr1].str.strip() + " " + 
                d[sub].str.strip() + " " + 
                d[dist].str.strip() + " " + 
                d[addr3].str.strip()
            ).astype("object")
            
            d["addr_type_id"] = type_id
            
            # Rename to match original logic's intermediate state
            d.rename(columns={
                addr1: "addr_no", 
                zip_code: "zip_cd", 
                sub: "sub_dstc", 
                dist: "dstc", 
                addr3: "prvn"
            }, inplace=True)
            
            d.drop([addr2], axis=1, inplace=True)
            return d

        # 3. Transform each address type (Home, Shipping, Billing)
        df_h = make_addr(df, "fcaddress1", "fcaddress2", "fcaddress3", "fcaddresssubdistrict", "fcaddressdistrict", "fczipcode", 5)
        df_c = make_addr(df, "fcshipaddr1", "fcshipaddr2", "fcshipaddr3", "fcshipaddrsubdistrict", "fcshipaddrdistrict", "fcshipzipcode", 3)
        df_b = make_addr(df, "fcbilltoaddr1", "fcbilltoaddr2", "fcbilltoaddr3", "fcbilltoaddrsubdistrict", "fcbilltoaddrdistrict", "fcbilltozipcode", 4)

        # 4. Combine and Start Final Formatting
        df_final = pd.concat([df_h, df_c, df_b]).drop_duplicates().add_suffix("_F")

        # Add DWH Metadata
        df_final["dwh_crtd_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_updt_dttm"] = pd.to_datetime(datetime_now)
        df_final["dwh_note"] = pd.array([pd.NA] * len(df_final), dtype="Int32")

        # 5. Column Mapping & Date Handling
        upd = df_final["ftupdatedate_F"].combine_first(df_final["ftcreatedate_F"])
        df_final["date_id"] = pd.to_datetime(
            upd.str[0:4] + upd.str[5:7].str.zfill(2) + upd.str[8:11].str.zfill(2)
        ).dt.date

        rename_map = {
            "fcid_F": "party_id",
            "addr_type_id_F": "addr_type_id",
            "addr_no_F": "addr_no",
            "sub_dstc_F": "sub_dstc",
            "dstc_F": "dstc",
            "prvn_F": "prvn",
            "zip_cd_F": "zip_cd",
            "ftcreatedate_F": "crtd_dttm",
            "fccreator_F": "crtd_by",
            "ftupdatedate_F": "updt_dttm",
            "fcupdateby_F": "updt_by"
        }
        df_final.rename(columns=rename_map, inplace=True)

        # 6. Final Clean up & Feature Engineering
        df_final["addr_no"] = df_final["addr_no"].apply(lambda x: x.split(" ")[0] if isinstance(x, str) else None)
        df_final["ref_addr_id"] = pd.array([pd.NA] * len(df_final), dtype="string")
        df_final["rec_actv_flag"] = df_final["fcisactive_F"].replace({"N": "0", "Y": "1", "false": "0", "true": "1"})
        
        df_final["crtd_dttm"] = pd.to_datetime(df_final["crtd_dttm"], errors="coerce")
        df_final["updt_dttm"] = pd.to_datetime(df_final["updt_dttm"], errors="coerce")
        df_final["updt_by"] = df_final["crtd_by"].combine_first(df_final["updt_by"])
        
        # Primary Key generation
        df_final["addr_id"] = (
            df_final["date_id"].astype("string") + 
            df_final["party_id"].astype("string") + 
            df_final["addr_type_id"].astype("string")
        )

        # 7. Select Final Columns (Schema Consistency)
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note",
            "addr_no", "sub_dstc", "dstc", "prvn", "zip_cd",
            "rec_actv_flag", "crtd_dttm", "crtd_by", "updt_dttm", "updt_by",
            "ref_addr_id", "party_id", "addr_type_id", "date_id", "addr_id"
        ]
        df_final = df_final[target_columns]

        if not df_final.empty:
            logger.info(f"transform_dim_address Rows: {len(df_final)}")

    except Exception as e:
        logger.error(f"ERROR: {e}")
        raise

    return df_final