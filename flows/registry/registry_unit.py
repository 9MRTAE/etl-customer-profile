"""
flows/registry/registry_unit.py
=================================
Domain: unit  (Column I = 'unit' in ETL repo mapping)

Sources:
  etl-iprop → dim_unit, fact_unit
"""

from flows.flow_config import FlowConfig, SourceConfig
from config import BUCKET_MSSQL
from config_flows import CRON_04_40_ICT

from tasks.tasks_pmscustomer_dwh_dim_unit import transform_dim_unit
from tasks.tasks_pmscustomer_dwh_fact_unit import transform_fact_unit

FLOWS: tuple[FlowConfig, ...] = (

    # ── dim_unit ──────────────────────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_unit",
        pk           = ("date_id", "unit_id"),
        sources      = (
            SourceConfig(tablename="tmRoomH", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccode","fcbrnid","fcaddressno","fcownerratio",
                                  "fcleveingtype","fcownerid","fcownerid2","fcownerid3",
                                  "fccurrentownerid","fcisactive","ftcreatedate","fccreateby",
                                  "ftupdatedate","fcupdateby")),
            SourceConfig(tablename="tmBRN", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccomid","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
        ),
        transform_fn = transform_dim_unit,
        cron_override = CRON_04_40_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── fact_unit ─────────────────────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_unit",
        pk           = ("date_id", "unit_id"),
        sources      = (
            SourceConfig(tablename="tmRoomH", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcbrnid","fcid","fcownerid","fcownerid2","fcownerid3",
                                  "fccurrentownerid","fttransferdate","ftdatein","ftdateout",
                                  "fcleveingtype","fcisactive","ftcreatedate","fccreateby",
                                  "ftupdatedate","fcupdateby")),
            SourceConfig(tablename="tmBRN", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccomid","fccode","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
        ),
        transform_fn = transform_fact_unit,
        cron_override = CRON_04_40_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),
)
