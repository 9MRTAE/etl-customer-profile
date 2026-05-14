"""
flows/registry/registry_company.py
====================================
Domain: company  (Column I = 'company' in ETL repo mapping)

Sources:
  etl-pmsmanagement → dim_pmscompany, fact_pmsinvitation_company,
                       fact_pmsinvitationhist_company
"""

from flows.flow_config import FlowConfig, SourceConfig
from config import BUCKET_PMSMANAGEMENT
from config_flows import CRON_03_40_ICT

from tasks.tasks_pmscustomer_dwh_dim_pmscompany import transform_dim_pmscompany
from tasks.tasks_pmscustomer_dwh_fact_pmsinvitation_company import transform_fact_pmsinvitation_company
from tasks.tasks_pmscustomer_dwh_fact_pmsinvitationhist_company import transform_fact_pmsinvitationhist_company

FLOWS: tuple[FlowConfig, ...] = (

    # ── dim_pmscompany ────────────────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_pmscompany",
        pk           = ("date_id", "company_id"),
        sources      = (
            SourceConfig(tablename="companies", source_type="postgresql",
                         bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_dim_pmscompany,
        default_full_data = "1",
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── fact_pmsinvitation_company ────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_pmsinvitation_company",
        pk           = ("date_id", "record_id"),
        sources      = (
            SourceConfig(tablename="invitation_companies", source_type="postgresql",
                         bucket_app=BUCKET_PMSMANAGEMENT),
            SourceConfig(tablename="invitations", source_type="postgresql",
                         bucket_app=BUCKET_PMSMANAGEMENT, is_join=True, groupby=("id")),
        ),
        transform_fn = transform_fact_pmsinvitation_company,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── fact_pmsinvitationhist_company ────────────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_pmsinvitationhist_company",
        pk           = ("date_id", "record_id"),
        sources      = (
            SourceConfig(tablename="company_audit_logs", source_type="postgresql",
                         bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_fact_pmsinvitationhist_company,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),
)
