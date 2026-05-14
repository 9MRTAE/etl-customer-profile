# etl-customer-profile

ETL pipeline — **GCS Data Lake → BigQuery DWH · PMS Customer domain**

Reads Parquet from GCS Data Lake (output of ingest repos), transforms, and upserts into BigQuery.
Covers Customer, Party, User, Company, Project, and Unit for the PMS (Property Management System).

**Stack:** Python · pandas · BigQuery Client · pandas-gbq · gcsfs · GCP Secret Manager · Prefect v3 · Docker · Jenkins

---

## TL;DR

| | |
|---|---|
| **Source** | GCS Data Lake — Parquet from 8+ ingest repos (`pmsmanagement`, `gcp-ingest-mssql`, `authentication`, `pdpa`, `homeservice`, `notification`, `mobileregister`, `loyalty`) |
| **Sink** | BigQuery schema `los` — 33 flows → dim/fact tables |
| **Pattern** | Factory + Registry — `FlowConfig` dataclass is the single source of truth; `make_flow()` generates Prefect flows from config |
| **Load mode** | Upsert by PK (temp-table pattern) or append — configured per table in `FlowConfig` |
| **Auth** | GCP Secret Manager (ADC on worker) — no credentials hardcoded in the repo |

---

## Architecture Overview

```
GCS Data Lake
  gs://{bucket}/gcp-storage-parquet/{application}/{source_type}/{table}/
      calendar_year=YYYY/month_no=M/day_of_month=D/
  │
  │  gcsfs + pyarrow · read_table()
  ▼
[Extract Task]  — 3 modes (see Extract Modes below)
  extract_lake()      — incremental: reads a single date partition
  extract_lake_join() — full dedup: reads all partitions → latest row per key
  extract_dwh()       — queries BigQuery directly (cross-flow reference data)
  │
  ▼
[Transform Task]
  transform_fn() per table — table-specific business logic
  output: pd.DataFrame ready for upsert
  │
  ▼
[Load Task]  — temp-table upsert (see Load Pattern below)
  upsert: write temp → DELETE matching PKs from target → INSERT SELECT with CAST → TRUNCATE temp
  append: pandas_gbq.to_gbq(..., if_exists="append")
  target: your-gcp-project-id.los.{dwh_table}
```

**Credentials flow:**
- Worker Node uses **ADC (Workload Identity)** to call GCP Secret Manager
- Secret Manager returns SA key JSON → separate credentials for GCS (Storage) and BigQuery
- `PREFECT_DEPLOY_MODE=1` → skips Secret Manager during the deploy step

---

## Design Decisions

### 1. FlowConfig + flow_factory — why not write flow functions directly

The previous repo (Prefect v1) wrote a separate flow function per table — that pattern caused:
- repeated boilerplate: extract → transform → load duplicated in every flow
- adding a new table required creating a full new file and editing the deploy script

**Decision:** separate _config_ from _execution_ using a dataclass + factory

```python
# FlowConfig — describes what the flow does
FlowConfig(
    dwh_table    = "fact_authuser",
    pk           = ("date_id", "auth_id"),
    sources      = (
        SourceConfig(tablename="users", source_type="postgresql",
                     bucket_app=BUCKET_AUTHENTICATION),
    ),
    transform_fn = transform_fact_authuser,
    cron_override = CRON_01_40_ICT,
)

# make_flow() — automatically generates a @flow from config
# loop: extract sources → transform → load → upsert BQ
```

Adding a new table = adding a `FlowConfig` entry to the registry → the factory generates the Prefect flow automatically without touching flow logic.

---

### 2. Domain-split registry — 4 files instead of one

`FLOW_REGISTRY` is split into 4 domain registry files:

```
flows/registry/
├── registry_user.py     ← 13 flows · dim/fact tables sourced from authentication, homeservice,
│                          iprop/MSSQL, mobileregister, pdpa, pmsmanagement
├── registry_company.py  ←  3 flows · dim/fact tables for PMS company
├── registry_project.py  ← 10 flows · dim/fact tables for project, address, postcode
└── registry_unit.py     ←  2 flows · dim/fact unit
```

`flow_registry.py` merges all four into a single `FLOW_REGISTRY: tuple[FlowConfig, ...]`.

**Why not consolidate into one file:**
- a single registry would have 33 entries in one file → hard to diff when two people edit simultaneously
- the domain boundary aligns with feature ownership: editing the user domain doesn't require opening the project registry

---

### 3. Extract modes — 3 functions for 3 use cases

`main_components.py` exposes three extract functions, each designed for a specific read pattern:

```
fn_Get_Source_Lake()        → incremental
fn_Gen_Source_Lake_Join()   → full dedup to latest row per key
fn_Get_Source_Lake_Hist()   → multi-year history scan
```

**Incremental (`fn_Get_Source_Lake`)** — default mode. Reads a single hive partition
(`calendar_year=YYYY/month_no=M/day_of_month=D`) matching the job date. If `p_full_data=1`,
the date predicate is omitted and all partitions are read.

**Full dedup (`fn_Gen_Source_Lake_Join`)** — used for lookup/reference tables (e.g. gender codes,
branch master) where every record is needed, not just yesterday's. Reads all partitions then
deduplicates by keeping the most recent row per `groupby` key:

```python
idx = (
    df.sort_values(["calendar_year", "month_no", "day_of_month"], ascending=False)
    .groupby(p_groupby)
    .head(1)
    .index
)
```

This dedup happens in the extract layer so the transform function always receives a clean,
non-duplicate DataFrame without handling it itself.

**Multi-year history (`fn_Get_Source_Lake_Hist`)** — iterates year by year from the current year
back `p_numofyear` steps. Used by `mobileregister` full loads that require the full historical
register, not just a rolling window.

`SourceConfig.is_join=True` routes a source through `fn_Gen_Source_Lake_Join`; the default
(`is_join=False`) routes through `fn_Get_Source_Lake`.

---

### 4. Load pattern — temp-table upsert instead of MERGE

BigQuery's `MERGE` statement would be the natural choice for upsert, but it was not used here
because `CREATE TABLE ... LIKE` copies the partition and clustering spec from the target table
onto the temp dataset, which BigQuery rejects when the temp dataset has no default partitioning.

**Decision:** a 4-step temp-table pattern that avoids this constraint:

```
Step 1 — pandas_gbq.to_gbq(..., if_exists="replace")
         writes DataFrame → temp_table.<temp>
         pandas_gbq infers schema without partition/cluster spec → no BadRequest

Step 2 — DELETE FROM target WHERE EXISTS (SELECT 1 FROM temp WHERE PK match)
         removes all rows whose PK exists in the temp table

Step 3 — INSERT INTO target SELECT ... FROM temp
         with explicit CAST per column (see below)

Step 4 — TRUNCATE TABLE temp
         clears the temp table for the next run
```

**Why explicit CAST in the INSERT SELECT:**
`pandas_gbq.to_gbq` infers `pd.Timestamp` columns as `DATETIME` in the temp table,
but the target table declares those columns as `TIMESTAMP`. BigQuery rejects a bare
`SELECT *` across this type mismatch. The fix is `_build_insert_select()`, which reads
the target table's schema via the BigQuery client API and wraps every column with its
correct `CAST(col AS <type>)`:

```python
def _build_insert_select(self, client, p_schema, p_table, temp_table):
    target_schema = client.get_table(client.dataset(p_schema).table(p_table)).schema
    cols = []
    for field in target_schema:
        fname = f"`{field.name}`"
        if field.field_type == "TIMESTAMP":
            cols.append(f"CAST({fname} AS TIMESTAMP) AS {fname}")
        elif field.field_type == "DATE":
            cols.append(f"CAST({fname} AS DATE) AS {fname}")
        elif field.field_type == "INT64":
            cols.append(f"CAST({fname} AS INT64) AS {fname}")
        elif field.field_type == "STRING":
            cols.append(f"CAST({fname} AS STRING) AS {fname}")
        else:
            cols.append(fname)
    select_clause = ", ".join(cols)
    return f"INSERT INTO `{GCP_PROJECT}.{p_schema}.{p_table}` SELECT {select_clause} FROM `{GCP_PROJECT}.temp_table.{temp_table}`"
```

---

### 5. SourceConfig.is_join — separates lookup tables from incremental sources

```python
# is_join=False (default) — incremental: filter by job_year/month/day
SourceConfig(tablename="tmCOR", source_type="", bucket_app=BUCKET_MSSQL)

# is_join=True — full dedup: reads all partitions + keeps the latest row per groupby key
SourceConfig(tablename="tmMobileGender", source_type="", bucket_app=BUCKET_MSSQL,
             columns=("fcid","fcnameen",...),
             is_join=True, groupby=("fcid"))
```

---

### 6. dim_party has 4 FlowConfigs for a single target table

`dim_party` in BigQuery stores customer party data from 4 different sources; each source
has a completely different schema and business logic:

| FlowConfig key | Source | Party type |
|---|---|---|
| `dim_party` | homeservice seekster_provider | Provider (GU/homeservice) |
| `dim_party_1` | MSSQL tmmobileuser + mobileregister customer | GU (eco app user) |
| `dim_party_2` | MSSQL tmCOR | RU (real estate buyer) |
| `dim_party_3` | loyalty tmMemberLoyalty | Member status update |

Rather than writing a single transform function to handle all cases, each case has its own `FlowConfig` and
its own `transform_fn` — keeping logic clearly separated and independently testable.

`flow_factory.py` handles duplicate `dwh_table` names by appending a `_<n>` suffix as the dict key.

---

### 7. cron_override per flow — each flow schedules independently

Each ETL table depends on the ingest schedule of its source repo.
`FlowConfig.cron_override` allows per-flow scheduling without hardcoding schedules in the factory:

```python
CRON_01_40_ICT = "40 18 * * *"   # 01:40 ICT — must run after ingest_gcp_mssql_popcorn
CRON_03_40_ICT = "40 20 * * *"   # 03:40 ICT — must run after ingest_gcp_postgresql_ecoapp
CRON_04_00_ICT = "0  21 * * *"   # 04:00 ICT
CRON_05_00_ICT = "0  22 * * *"   # 05:00 ICT — dim_party memberstatus (must run after dim_party RU)
```

Priority: `FlowConfig.cron_override` → `CRON_SCHEDULE` (default) → `None` (develop)

---

## Project Structure

```
etl-customer-profile/
├── flows/
│   ├── flow_config.py          # FlowConfig + SourceConfig dataclasses
│   ├── flow_factory.py         # make_flow() + ALL_FLOWS dict
│   ├── flow_registry.py        # merges 4 domain registries → FLOW_REGISTRY
│   └── registry/
│       ├── registry_user.py    # 13 FlowConfigs · user domain
│       ├── registry_company.py #  3 FlowConfigs · company domain
│       ├── registry_project.py # 10 FlowConfigs · project domain
│       └── registry_unit.py    #  2 FlowConfigs · unit domain
│
├── tasks/
│   ├── tasks.py                # Prefect @task wrappers: extract_lake, extract_lake_join, extract_dwh, load
│   ├── main_components.py      # ETL core: ExtractSourceData (3 modes), LoadSourceData (temp-table upsert)
│   └── tasks_pmscustomer_dwh_{table}.py   # per-table transform function (33 files)
│
├── config/
│   ├── __init__.py             # env dispatcher + Secret Manager + PREFECT_DEPLOY_MODE guard
│   ├── production.py           # production constants
│   └── development.py          # development constants
│
├── config_flows/
│   └── __init__.py             # APPLICATION_TYPE, cron constants, job date params
│
├── scripts/
│   ├── build.sh                # Build Docker image
│   ├── register.sh             # Register Prefect deployments
│   └── generate_manifest.py    # Generate deployment manifest
│
├── deploy.py                   # Registers all flows as Prefect deployments
├── run_local.py                # CLI: run a single flow or all flows locally
├── Dockerfile
└── requirements.txt
```

---

## Registered Flows (33 flows)

### Domain: user — `registry_user.py`

| BQ Table | PK | Sources | Schedule |
|---|---|---|---|
| `fact_authuser` | date_id, auth_id | authentication/users | 01:40 |
| `dim_party` | date_id, party_id, party_type_id | homeservice/seekster_provider | 03:40 |
| `dim_party_1` | date_id, party_id, party_type_id | MSSQL/tmmobileuser + mobileregister/customer | 03:40 |
| `dim_party_2` | date_id, party_id, party_type_id | MSSQL/tmCOR + tmMobileGender + tmRoomH | 04:00 |
| `dim_party_3` | date_id, party_id, party_type_id | loyalty/tmMemberLoyalty | 05:00 |
| `dim_userpermission` | date_id, project_id, branch_id, usergroup_id, user_id, mnu_id | MSSQL/tmAUT | 04:40 |
| `fact_user` | date_id, user_id, usergroup_id | MSSQL/tmUSR | 04:40 |
| `fact_userpermission` | date_id, project_id, branch_id, usergroup_id, mnu_id | MSSQL/tmAUT | 04:40 |
| `dim_device_detail` | date_id, device_id | notification/gu_device | 01:40 |
| `dim_mobile_user` | date_id, customer_id, unit_id | mobileregister/customer | 01:40 |
| `fact_unitresident` | date_id, corhist_id | MSSQL/ttCORHist | 01:40 |
| `fact_userconsent` | date_id, uconsent_id | pdpa/users_consent + consents + consent_versions | 03:40 |
| `dim_pmsuserprofile` | date_id, user_id | pmsmanagement/users + profiles | 03:40 |
| `fact_pmsinvitations` | date_id, invite_id | pmsmanagement/invitations | 03:40 |
| `fact_pmsuser_role` | date_id, record_id | pmsmanagement/user_roles | 03:40 |

### Domain: company — `registry_company.py`

| BQ Table | PK | Sources | Schedule |
|---|---|---|---|
| `dim_pmscompany` | date_id, company_id | pmsmanagement/companies | 03:40 |
| `fact_pmsinvitation_company` | date_id, record_id | pmsmanagement/invitation_companies + invitations | 03:40 |
| `fact_pmsinvitationhist_company` | date_id, record_id | pmsmanagement/company_audit_logs | 03:40 |

### Domain: project — `registry_project.py`

| BQ Table | PK | Sources | Schedule |
|---|---|---|---|
| `dim_address` | date_id, addr_id | MSSQL/tmADR (×2 sub-flows) | — |
| `dim_address_type` | date_id, addr_type_id | MSSQL/tmADT | — |
| `dim_postcode` | date_id, postcode_id | MSSQL/tmPCODE | — |
| `dim_project` | date_id, project_id, project_profile_id | MSSQL/tmBRN + tmCOM + tmPRJ | — |
| `dim_project_profile` | date_id, project_profile_id | MSSQL/tmPRJ | — |
| `dim_project_bookbank` | date_id, bookbank_id | MSSQL/tmBKB | — |
| `dim_pmsproject` | date_id, project_id | pmsmanagement/companies_projects | 03:40 |
| `dim_pmsrole_permission` | date_id, record_id | pmsmanagement/features + feature_groups | 03:40 |
| `fact_pmscompany_projects` | date_id, mapping_log_id | pmsmanagement/companies_projects_logs | 03:40 |
| `fact_pmsinvitation_project` | date_id, record_id | pmsmanagement/invitation_projects | 03:40 |
| `fact_pmsinvitationhist_project` | date_id, record_id | pmsmanagement/invitation_projects + audit | 03:40 |
| `fact_pmsproject_features` | date_id, project_id, feature_id | pmsmanagement/companies_projects + features | 03:40 |

### Domain: unit — `registry_unit.py`

| BQ Table | PK | Sources | Schedule |
|---|---|---|---|
| `dim_unit` | date_id, unit_id | MSSQL/tmRoomH + tmBRN + ... | — |
| `fact_unit` | date_id, unit_id | MSSQL/tmRoomH | — |

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CI_COMMIT_BRANCH` | `develop` | `main` → production config |
| `IMAGE_TAG` | `etl-customer-profile:local` | Docker image tag |
| `PREFECT_DEPLOY_MODE` | *(unset)* | `1` → skips Secret Manager during deploy |
| `PREFECT_WORK_POOL` | `kubernetes-pool` | Prefect work pool |
| `JOB_YEAR` / `JOB_MONTH` / `JOB_DAY` | `None` | Run date (None = yesterday ICT) |
| `JOB_FULL_DATA` | `0` | `1` = full scan, `0` = incremental |
| `JOB_NUMOFYER` | `10` | years of history for mobileregister full load |

### GCP Secret Manager Keys

| Secret key | Used for |
|---|---|
| `your-secret-sa-key` | GCS + BigQuery credentials (service account JSON) |

---

## FlowConfig Reference

```python
@dataclass(frozen=True)
class FlowConfig:
    dwh_table:        str                       # BQ target table name
    pk:               tuple[str, ...]           # PK columns for upsert
    sources:          tuple[SourceConfig, ...]  # ordered list of sources
    transform_fn:     Callable                  # transform function (@task)
    schema:           str = "los"               # BQ dataset/schema
    default_full_data: str = "0"                # "0"=incremental, "1"=full
    p_insert:         int = 0                   # 0=upsert (temp-table), 1=append
    cron_override:    str | None = None         # per-flow cron override
    origin:           str = ""                  # reference to the original v1 repo

@dataclass(frozen=True)
class SourceConfig:
    tablename:   str                            # GCS table name
    source_type: str                            # sub-folder: 'postgresql' or '' (MSSQL)
    bucket_app:  str                            # GCS application prefix
    columns:     tuple[str, ...] | None = None  # None = all columns
    groupby:     tuple[str, ...] | None = None  # dedup key for is_join
    is_join:     bool = False                   # True → extract_lake_join (full dedup)
    is_dwh:      bool = False                   # True → extract_dwh (BQ query)
    query:       str = ""                       # SQL for is_dwh=True sources
```

---

## Local Development

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your environment values

export CI_COMMIT_BRANCH=develop

# run a single flow
python run_local.py --flow fact_authuser

# full data scan
python run_local.py --flow dim_pmscompany --full_data 1

# specific date
python run_local.py --flow fact_userconsent --year 2024 --month 3 --day 15
```

---

## Adding a New Flow

1. Create a transform function in `tasks/tasks_pmscustomer_dwh_{table}.py`
2. Add a `FlowConfig` entry to the appropriate domain registry (`registry_user.py`, `registry_project.py`, etc.)
3. Import the transform function at the top of the registry file
4. `flow_factory.py` generates the Prefect flow automatically — no changes to `deploy.py` or `prefect.yaml` needed