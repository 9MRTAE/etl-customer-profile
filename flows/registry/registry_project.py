"""
flows/registry/registry_project.py
=====================================
Domain: project  (Column I = 'project' in ETL repo mapping)

Sources:
  etl-iprop          → dim_address, dim_address_mobileuser, dim_address_type,
                        dim_postcode, dim_project, dim_project_profile,
                        dim_project_update_data, dim_project_bookbank
  etl-pmsmanagement  → dim_pmsproject, dim_pmsrole_permission,
                        fact_pmscompany_projects, fact_pmsinvitation_project,
                        fact_pmsinvitationhist_project, fact_pmsproject_features
"""

from flows.flow_config import FlowConfig, SourceConfig
from config import BUCKET_MSSQL, BUCKET_PMSMANAGEMENT, BUCKET_MOBILEREGISTER
from config_flows import CRON_03_40_ICT, CRON_04_00_ICT, CRON_04_40_ICT

from tasks.tasks_pmscustomer_dwh_dim_address import transform_dim_address
from tasks.tasks_pmscustomer_dwh_dim_address_mobileuser import transform_dim_address_mobileuser
from tasks.tasks_pmscustomer_dwh_dim_address_type import transform_dim_address_type
from tasks.tasks_pmscustomer_dwh_dim_postcode import transform_dim_postcode
from tasks.tasks_pmscustomer_dwh_dim_project import transform_dim_project
from tasks.tasks_pmscustomer_dwh_dim_project_profile import transform_dim_project_profile
from tasks.tasks_pmscustomer_dwh_dim_project_bookbank import transform_dim_project_bookbank
from tasks.tasks_pmscustomer_dwh_dim_pmsproject import transform_dim_pmsproject
from tasks.tasks_pmscustomer_dwh_dim_pmsrole_permission import transform_dim_pmsrole_permission
from tasks.tasks_pmscustomer_dwh_fact_pmscompany_projects import transform_fact_pmscompany_projects
from tasks.tasks_pmscustomer_dwh_fact_pmsinvitation_project import transform_fact_pmsinvitation_project
from tasks.tasks_pmscustomer_dwh_fact_pmsinvitationhist_project import transform_fact_pmsinvitationhist_project
from tasks.tasks_pmscustomer_dwh_fact_pmsproject_features import transform_fact_pmsproject_features

FLOWS: tuple[FlowConfig, ...] = (

    # ── etl-iprop: dim_address (tmCOR → 3 address types) ─────────────────────
    FlowConfig(
        dwh_table    = "dim_address",
        pk           = ("date_id","addr_id"),
        sources      = (
            SourceConfig(tablename="tmCOR", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fcaddress1","fcaddress2","fcaddress3",
                                  "fcaddresssubdistrict","fcaddressdistrict","fczipcode",
                                  "fcshipaddr1","fcshipaddr2","fcshipaddr3",
                                  "fcshipaddrsubdistrict","fcshipaddrdistrict","fcshipzipcode",
                                  "fcbilltoaddr1","fcbilltoaddr2","fcbilltoaddr3",
                                  "fcbilltoaddrsubdistrict","fcbilltoaddrdistrict","fcbilltozipcode",
                                  "fcisactive","ftcreatedate","fccreator","ftupdatedate","fcupdateby")),
        ),
        transform_fn = transform_dim_address,
        p_insert     = 1,
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_address (mobileuser / tmMobileUser_Address) ────────────
    FlowConfig(
        dwh_table    = "dim_address",
        pk           = ("date_id","addr_id"),
        sources      = (
            SourceConfig(tablename="tmMobileUser_Address", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fnid","fcmobileuserid","fcaddrtype","fcaddress","fnpostcodeid",
                                  "fcisactive","fccreateby","ftcreatedate","fcupdateby","ftupdatedate")),
            SourceConfig(tablename="tmPostCode", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fnid","fcprovinceth","fcdistrictth","fcsubdistrictth","fcpostcode"),
                         is_join=True, groupby=("fnid")),
        ),
        transform_fn = transform_dim_address_mobileuser,
        p_insert     = 1,
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_address_type ───────────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_address_type",
        pk           = ("date_id", "addr_type_id"),
        sources      = (
            SourceConfig(tablename="tmMobileAddressType", source_type="", bucket_app=BUCKET_MSSQL),
        ),
        transform_fn = transform_dim_address_type,
        default_full_data = "1",
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_postcode ───────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_postcode",
        pk           = ("date_id", "postcode_id"),
        sources      = (
            SourceConfig(tablename="tmPostCode", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fnid","fcpostcode","fcsubdistrictth","fcsubdistricten",
                                  "fcdistrictth","fcdistricten","fcprovinceth","fcprovinceen",
                                  "fcregionth","fcregionen","fcisactive","ftcreatedate",
                                  "fccreator","ftupdatedate","fcupdateby")),
        ),
        transform_fn = transform_dim_postcode,
        default_full_data = "1",
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_project (tmCOM + tmCOM_Type) ───────────────────────────
    FlowConfig(
        dwh_table    = "dim_project",
        pk           = ("date_id", "project_id", "project_profile_id"),
        sources      = (
            SourceConfig(tablename="tmCOM", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccode","fcname","fctaxid","fcremark32","fcremark33",
                                  "fcremark34","fcremark35","fcaddr11","fcaddr12","fcaddr13",
                                  "fntotalunit","fntotalcarpark","fcisactive","ftcreatedate",
                                  "fccreator","ftupdatedate","fcupdateby","fccomshortcode","fctel")),
            SourceConfig(tablename="tmCOM_Type", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fnitemno","fccode","fcname","fcisactive",
                                  "calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
        ),
        transform_fn = transform_dim_project,
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_project_profile (tmCOM) ────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_project_profile",
        pk           = ("date_id", "project_profile_id"),
        sources      = (
            SourceConfig(tablename="tmCOM", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fcname","fcisactive","ftcreatedate","fccreator","ftupdatedate","fcupdateby")),
        ),
        transform_fn = transform_dim_project_profile,
        default_full_data = "1",
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_project_bookbank ───────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_project_bookbank",
        pk           = ("date_id", "bookbank_id"),
        sources      = (
            SourceConfig(tablename="tmBKB", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccode","fcname","fcbrnid","fcbnkid","fcaccid","fcod_accid",
                                  "fcbookno","fcbankbrname","fcbankbooktype","fcbankbookusetype",
                                  "ftbegdate","fnday","ftduedate","fnbalamt","fcremark1","fcremark2",
                                  "fcremark3","fcremark4","fcremark5","fcisactive","fccreator",
                                  "ftcreatedate","fcupdateby","ftupdatedate")),
            SourceConfig(tablename="tmBRN", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccomid","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
            SourceConfig(tablename="tmBNK", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccode","fcname","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
            SourceConfig(tablename="tmACC", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fccode","fcname","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
        ),
        transform_fn = transform_dim_project_bookbank,
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-pmsmanagement: dim_pmsproject ─────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_pmsproject",
        pk           = ("date_id", "project_id"),
        sources      = (
            SourceConfig(tablename="projects", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_dim_pmsproject,
        default_full_data = "1",
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── etl-pmsmanagement: dim_pmsrole_permission ─────────────────────────────
    FlowConfig(
        dwh_table    = "dim_pmsrole_permission",
        pk           = ("date_id", "record_id"),
        sources      = (
            SourceConfig(tablename="role_permissions", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
            SourceConfig(tablename="roles", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_dim_pmsrole_permission,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── etl-pmsmanagement: fact_pmscompany_projects ───────────────────────────
    FlowConfig(
        dwh_table    = "fact_pmscompany_projects",
        pk           = ("date_id", "mapping_log_id"),
        sources      = (
            SourceConfig(tablename="companies_projects_logs", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
            SourceConfig(tablename="companies", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT,
                         is_join=True, groupby=("id")),
            SourceConfig(tablename="projects", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT,
                         is_join=True, groupby=("id")),
        ),
        transform_fn = transform_fact_pmscompany_projects,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── etl-pmsmanagement: fact_pmsinvitation_project ─────────────────────────
    FlowConfig(
        dwh_table    = "fact_pmsinvitation_project",
        pk           = ("date_id", "record_id"),
        sources      = (
            SourceConfig(tablename="invitation_projects", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
            SourceConfig(tablename="invitations", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT,
                         is_join=True, groupby=("id")),
        ),
        transform_fn = transform_fact_pmsinvitation_project,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── etl-pmsmanagement: fact_pmsinvitationhist_project ─────────────────────
    FlowConfig(
        dwh_table    = "fact_pmsinvitationhist_project",
        pk           = ("date_id", "record_id"),
        sources      = (
            SourceConfig(tablename="project_audit_logs", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_fact_pmsinvitationhist_project,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── etl-pmsmanagement: fact_pmsproject_features ───────────────────────────
    FlowConfig(
        dwh_table    = "fact_pmsproject_features",
        pk           = ("date_id", "project_id", "feature_id"),
        sources      = (
            SourceConfig(tablename="project_features", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
            SourceConfig(tablename="companies_projects", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT,
                         is_join=True, groupby=("company_id")),
        ),
        transform_fn = transform_fact_pmsproject_features,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),
)
