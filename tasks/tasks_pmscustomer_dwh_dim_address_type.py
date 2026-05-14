import pandas as pd
from prefect import get_run_logger, task

@task(name="transform_dim_address_type")
def transform_dim_address_type(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Transform source data into DWH dimension address type schema.
    """
    logger = get_run_logger()
    df_final = pd.DataFrame()

    if not p_data or p_data[0].empty:
        logger.warning("Input data is empty.")
        return df_final

    try:
        # 1. Setup Initial DataFrame
        df = p_data[0].copy()
        df.columns = df.columns.str.lower()

        # 2. Handle Timestamps (Using requested logic)
        datetime_now = pd.Timestamp.utcnow().tz_convert('Asia/Bangkok').tz_localize(None)
        df["dwh_crtd_dttm"] = datetime_now
        df["dwh_updt_dttm"] = datetime_now

        # 3. Handle Metadata and Derived Columns
        df["dwh_note"] = pd.array([pd.NA] * len(df), dtype="Int32")
        
        # Calculate date_id from updated or created date
        combined_date = df["ftupdatedate"].combine_first(df["ftcreatedate"])
        df["date_id"] = pd.to_datetime(
            combined_date.str[0:4] + 
            combined_date.str[5:7].str.zfill(2) + 
            combined_date.str[8:11].str.zfill(2)
        ).dt.date

        # 4. Column Renaming
        column_mapping = {
            "fcaddressnameth": "addr_type_name_th",
            "fcaddressnameen": "addr_type_name_en",
            "ftcreatedate": "crtd_dttm",
            "fccreateby": "crtd_by",
            "ftupdatedate": "updt_dttm",
            "fcupdateby": "updt_by",
            "fnid": "addr_type_id"
        }
        df.rename(columns=column_mapping, inplace=True)

        # 5. Data Type Formatting & Flags
        df["rec_actv_flag"] = df["fcisactive"].replace({"N": "0", "Y": "1"})
        df["addr_type_id"] = df["addr_type_id"].astype("Int64")
        df["crtd_dttm"] = pd.to_datetime(df["crtd_dttm"], errors="coerce")
        df["updt_dttm"] = pd.to_datetime(df["updt_dttm"], errors="coerce")
        
        # Logic: If updt_by is null, use crtd_by
        df["updt_by"] = df["updt_by"].combine_first(df["crtd_by"])

        # 6. Final Schema Selection (Exact original order)
        target_columns = [
            "dwh_crtd_dttm", "dwh_updt_dttm", "dwh_note", "addr_type_name_th",
            "addr_type_name_en", "rec_actv_flag", "crtd_dttm", "crtd_by", 
            "updt_dttm", "updt_by", "addr_type_id", "date_id"
        ]
        df_final = df[target_columns]

        logger.info(f"Successfully transformed {len(df_final)} rows.")

    except Exception as e:
        logger.error(f"Transformation Error: {str(e)}")
        raise

    return df_final