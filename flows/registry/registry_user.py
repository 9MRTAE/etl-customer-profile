"""
flows/registry/registry_user.py
================================
Domain: user  (Column I = 'user' in ETL repo mapping)

Sources:
  etl-authentication    → fact_authuser
  etl-homeservice       → dim_party (provider/homeservice)
  etl-iprop             → dim_party_gu_eco, dim_party_ru, dim_party_update_memberstatus,
                          dim_userpermission, fact_user, fact_userpermission
  etl-mobileregister    → dim_device_detail, dim_mobile_user, fact_unitresident
  etl-pdpa              → fact_userconsent
  etl-pmsmanagement     → dim_pmsuserprofile, fact_pmsinvitations, fact_pmsuser_role
"""

from flows.flow_config import FlowConfig, SourceConfig
from config import (
    BUCKET_AUTHENTICATION, BUCKET_HOMESERVICE, BUCKET_MSSQL, BUCKET_MOBILEREGISTER,
    BUCKET_NOTIFICATION, BUCKET_PDPA, BUCKET_PMSMANAGEMENT, BUCKET_LOYALTY,
)
from config_flows import CRON_01_40_ICT, CRON_03_40_ICT, CRON_04_00_ICT, CRON_04_40_ICT, CRON_05_00_ICT

from tasks.tasks_pmscustomer_dwh_fact_authuser import transform_fact_authuser
from tasks.tasks_pmscustomer_dwh_dim_party_homeservice import transform_dim_party_homeservice
from tasks.tasks_pmscustomer_dwh_dim_party_gu_eco import transform_dim_party_gu_eco
from tasks.tasks_pmscustomer_dwh_dim_party_ru import transform_dim_party_ru
from tasks.tasks_pmscustomer_dwh_dim_party_update_memberstatus import transform_dim_party_update_memberstatus
from tasks.tasks_pmscustomer_dwh_dim_userpermission import transform_dim_userpermission
from tasks.tasks_pmscustomer_dwh_fact_user import transform_fact_user
from tasks.tasks_pmscustomer_dwh_fact_userpermission import transform_fact_userpermission
from tasks.tasks_pmscustomer_dwh_dim_device_detail import transform_dim_device_detail
from tasks.tasks_pmscustomer_dwh_dim_mobile_user import transform_dim_mobile_user
from tasks.tasks_pmscustomer_dwh_fact_unitresident import transform_fact_unitresident
from tasks.tasks_pmscustomer_dwh_fact_userconsent import transform_fact_userconsent
from tasks.tasks_pmscustomer_dwh_dim_pmsuserprofile import transform_dim_pmsuserprofile
from tasks.tasks_pmscustomer_dwh_fact_pmsinvitations import transform_fact_pmsinvitations
from tasks.tasks_pmscustomer_dwh_fact_pmsuser_role import transform_fact_pmsuser_role

FLOWS: tuple[FlowConfig, ...] = (

    # ── etl-authentication ────────────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_authuser",
        pk           = ("date_id", "auth_id"),
        sources      = (
            SourceConfig(tablename="users", source_type="postgresql",
                         bucket_app=BUCKET_AUTHENTICATION),
        ),
        transform_fn = transform_fact_authuser,
        cron_override = CRON_01_40_ICT,
        origin       = "etl-authentication@prefect-v1",
    ),

    # ── etl-homeservice [dim_party]───────────────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_party",
        pk           = ("date_id", "party_id", "party_type_id"),
        sources      = (
            SourceConfig(tablename="seekster_provider", source_type="",
                         bucket_app=BUCKET_HOMESERVICE,
                         columns=("provider_id","provider_name","registered_name","is_active",
                                  "create_date","create_by","update_date","update_by")),
        ),
        transform_fn = transform_dim_party_homeservice,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-homeservice@prefect-v1",
    ),

    # ── etl-iprop: dim_party GU (ecoapp/mobileuser) [dim_party_1]──────────────────────────
    FlowConfig(
        dwh_table    = "dim_party",
        pk           = ("date_id", "party_id", "party_type_id"),
        sources      = (
            SourceConfig(tablename="tmmobileuser", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcfirstname","fclastname","fcuserid","ftbirthdate","fctaxid",
                                  "fccountrycode","fcphoneno","fcemail","fngender","fcisactive",
                                  "ftcreatedate","fccreateby","ftupdatedate","fcupdateby")),
            SourceConfig(tablename="tmMobileGender", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fcnameth","fcnameen","fcisactive","fcisdefualt",
                                  "calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
            SourceConfig(tablename="customer", source_type="", bucket_app=BUCKET_MOBILEREGISTER,
                         columns=("id","phonenumber","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("id")),
        ),
        transform_fn = transform_dim_party_gu_eco,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_party RU (tmCOR/MSSQL) [dim_party_2]────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_party",
        pk           = ("date_id", "party_id", "party_type_id"),
        sources      = (
            SourceConfig(tablename="tmCOR", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fcname","fcengname","ftbirthday","fnage","fctaxid",
                                  "fcpassport","fccountrycode","fcshipmobile","fcshipemail",
                                  "fceducation","fcmarrystatus","fcsex","fcisactive",
                                  "ftcreatedate","fccreator","fcupdateby","ftupdatedate","fccode","fccomid")),
            SourceConfig(tablename="tmMobileGender", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fcnameen","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
            SourceConfig(tablename="tmRoomH", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcid","fcownerid","fcownerid2","fcownerid3","fccurrentownerid",
                                  "fcownerratio","calendar_year","month_no","day_of_month"),
                         is_join=True, groupby=("fcid")),
        ),
        transform_fn = transform_dim_party_ru,
        cron_override = CRON_04_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_party memberstatus update [dim_party_3]──────────────────────────────
    FlowConfig(
        dwh_table    = "dim_party",
        pk           = ("date_id", "party_id", "party_type_id"),
        sources      = (
            SourceConfig(tablename="tmMemberLoyalty", source_type="", bucket_app=BUCKET_LOYALTY,
                         columns=("fcuserid","ftcreatedate","ftupdatedate")),
        ),
        transform_fn = transform_dim_party_update_memberstatus,
        cron_override = CRON_05_00_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: dim_userpermission ─────────────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_userpermission",
        pk           = ("date_id", "project_id", "branch_id", "usergroup_id", "user_id", "mnu_id"),
        sources      = (
            SourceConfig(tablename="tmAUT", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcusgid","fcbrnid","fcmnuid","fcselect","fcinsert","fcupdate",
                                  "fcdelete","fcprint","fccancel","fcapprove","ftcreatedate","ftupdatedate")),
        ),
        transform_fn = transform_dim_userpermission,
        default_full_data = "1",
        cron_override = CRON_04_40_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: fact_user ──────────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_user",
        pk           = ("date_id", "user_id", "usergroup_id"),
        sources      = (
            SourceConfig(tablename="tmUSR", source_type="", bucket_app=BUCKET_MSSQL),
        ),
        transform_fn = transform_fact_user,
        default_full_data = "1",
        cron_override = CRON_04_40_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-iprop: fact_userpermission ────────────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_userpermission",
        pk           = ("date_id", "project_id", "branch_id", "usergroup_id", "mnu_id"),
        sources      = (
            SourceConfig(tablename="tmAUT", source_type="", bucket_app=BUCKET_MSSQL,
                         columns=("fcusgid","fcbrnid","fcmnuid","fcselect","fcinsert","fcupdate",
                                  "fcdelete","fcprint","fccancel","fcapprove","ftcreatedate","ftupdatedate")),
        ),
        transform_fn = transform_fact_userpermission,
        default_full_data = "1",
        cron_override = CRON_04_40_ICT,
        origin       = "etl-iprop@prefect-v1",
    ),

    # ── etl-mobileregister: dim_device_detail ────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_device_detail",
        pk           = ("date_id", "device_id"),
        sources      = (
            SourceConfig(tablename="gu_device", source_type="postgresql",
                         bucket_app=BUCKET_NOTIFICATION),
        ),
        transform_fn = transform_dim_device_detail,
        cron_override = CRON_01_40_ICT,
        origin       = "etl-mobileregister@prefect-v1",
    ),

    # ── etl-mobileregister: dim_mobile_user ──────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_mobile_user",
        pk           = ("date_id", "customer_id", "unit_id"),
        sources      = (
            SourceConfig(tablename="customer", source_type="", bucket_app=BUCKET_MOBILEREGISTER,
                         columns=("id","username","phonenumbercountrycode","phonenumber",
                                  "creationtime","creatoruserid","lastmodificationtime","lastmodifieruserid"),
                         is_join=False),
        ),
        transform_fn = transform_dim_mobile_user,
        default_full_data = "1",
        cron_override = CRON_01_40_ICT,
        origin       = "etl-mobileregister@prefect-v1",
    ),

    # ── etl-mobileregister: fact_unitresident ────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_unitresident",
        pk           = ("date_id", "corhist_id"),
        sources      = (
            SourceConfig(tablename="ttCORHist", source_type="", bucket_app=BUCKET_MSSQL),
        ),
        transform_fn = transform_fact_unitresident,
        default_full_data = "1",
        cron_override = CRON_01_40_ICT,
        origin       = "etl-mobileregister@prefect-v1",
    ),

    # ── etl-pdpa: fact_userconsent ────────────────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_userconsent",
        pk           = ("date_id", "uconsent_id"),
        sources      = (
            SourceConfig(tablename="users_consent", source_type="postgresql", bucket_app=BUCKET_PDPA),
            SourceConfig(tablename="consents", source_type="postgresql", bucket_app=BUCKET_PDPA,
                         is_join=True, groupby=("id")),
            SourceConfig(tablename="consent_versions", source_type="postgresql", bucket_app=BUCKET_PDPA,
                         is_join=True, groupby=("id")),
        ),
        transform_fn = transform_fact_userconsent,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pdpa@prefect-v1",
    ),

    # ── etl-pmsmanagement: dim_pmsuserprofile ────────────────────────────────
    FlowConfig(
        dwh_table    = "dim_pmsuserprofile",
        pk           = ("date_id", "user_id"),
        sources      = (
            SourceConfig(tablename="users", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
            SourceConfig(tablename="profiles", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT,
                         is_join=True, groupby=("id")),
        ),
        transform_fn = transform_dim_pmsuserprofile,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── etl-pmsmanagement: fact_pmsinvitations ────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_pmsinvitations",
        pk           = ("date_id", "invite_id"),
        sources      = (
            SourceConfig(tablename="invitations", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_fact_pmsinvitations,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),

    # ── etl-pmsmanagement: fact_pmsuser_role ──────────────────────────────────
    FlowConfig(
        dwh_table    = "fact_pmsuser_role",
        pk           = ("date_id", "record_id"),
        sources      = (
            SourceConfig(tablename="user_roles", source_type="postgresql", bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_fact_pmsuser_role,
        cron_override = CRON_03_40_ICT,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),
)
