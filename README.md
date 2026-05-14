# etl-pms-customer

ETL pipeline for **PMS Customer domain** → BigQuery DWH (`los` schema)

Merged from 6 legacy Prefect v1 repos:

| Legacy repo | Flows | Registry domain |
|---|---|---|
| `etl-authentication` | 1 | user |
| `etl-homeservice` | 1 | user |
| `etl-iprop` | 15 | user, project, unit |
| `etl-mobileregister` | 3 | user |
| `etl-pdpa` | 1 | user |
| `etl-pmsmanagement` | 12 | user, company, project |

**Runtime:** Prefect v3 · Kubernetes worker pool · SA key via GCP Secret Manager

---

## Table of Contents

1. [Repository Structure](#1-repository-structure)
2. [Flows & Tables](#2-flows--tables)
3. [Architecture](#3-architecture)
4. [Authentication](#4-authentication)
5. [Configuration](#5-configuration)
6. [Local Development](#6-local-development)
7. [Repair & Backfill](#7-repair--backfill)
8. [Deployment](#8-deployment)
9. [Adding a New Flow](#9-adding-a-new-flow)
10. [Manifest & Overview](#10-manifest--overview)
11. [Key Changes from v1](#11-key-changes-from-v1)

---

## 1. Repository Structure

```
etl-pms-customer/
├── config/
│   ├── __init__.py              # env-based import + SA key from Secret Manager
│   ├── development.py           # nonprd bucket, secret name
│   └── production.py            # prd bucket, secret name
│
├── config_flows/
│   └── __init__.py              # APPLICATION_TYPE, JOB_* env vars, CRON_SCHEDULE constants
│
├── flows/
│   ├── __init__.py
│   ├── flow_registry.py         # FlowConfig / SourceConfig dataclasses + FLOW_REGISTRY
│   ├── flow_factory.py          # make_flow(cfg) engine + ALL_FLOWS dict
│   └── registry/
│       ├── __init__.py
│       ├── registry_user.py     # 13 flows — user domain
│       ├── registry_company.py  #  3 flows — company domain
│       ├── registry_project.py  # 13 flows — project domain
│       └── registry_unit.py     #  2 flows — unit domain
│
├── tasks/
│   ├── main_components.py       # ConnectorDB, ExtractSourceData, LoadSourceData, CommonSQL
│   ├── tasks.py                 # Generic @task: extract_lake, extract_lake_join, extract_dwh, load
│   └── tasks_pmscustomer_dwh_*.py   # Per-table Transform @task (31 files)
│
├── scripts/
│   ├── build.sh                 # Docker build + push → Artifact Registry
│   ├── register.sh              # run deploy.py inside container
│   └── generate_manifest.py     # Generate manifest.yaml from FLOW_REGISTRY
│
├── deploy.py                    # Register flows via flow.deploy() Python API
├── run_local.py                 # Local repair runner (pre-platform / backfill)
├── prefect.yaml                 # Prefect v3 project metadata
├── Dockerfile
├── Jenkinsfile
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## 2. Flows & Tables

### Domain: user (13 flows)

| Flow (dwh_table) | BQ Target | Origin repo | PK |
|---|---|---|---|
| `fact_authuser` | `los.fact_authuser` | etl-authentication | `date_id, auth_id` |
| `dim_party` (homeservice) | `los.dim_party` | etl-homeservice | `date_id, party_id, party_type_id` |
| `dim_party` (GU/ecoapp) | `los.dim_party` | etl-iprop | `date_id, party_id, party_type_id` |
| `dim_party` (RU/MSSQL) | `los.dim_party` | etl-iprop | `date_id, party_id, party_type_id` |
| `dim_party` (memberstatus) | `los.dim_party` | etl-iprop | `date_id, party_id, party_type_id` |
| `dim_userpermission` | `los.dim_userpermission` | etl-iprop | `date_id, project_id, branch_id, usergroup_id, user_id, mnu_id` |
| `fact_user` | `los.fact_user` | etl-iprop | `date_id, user_id, usergroup_id` |
| `fact_userpermission` | `los.fact_userpermission` | etl-iprop | `date_id, project_id, branch_id, usergroup_id, mnu_id` |
| `dim_device_detail` | `los.dim_device_detail` | etl-mobileregister | `date_id, device_id` |
| `dim_mobile_user` | `los.dim_mobile_user` | etl-mobileregister | `date_id, customer_id, unit_id` |
| `fact_unitresident` | `los.fact_unitresident` | etl-mobileregister | `date_id, corhist_id` |
| `fact_userconsent` | `los.fact_userconsent` | etl-pdpa | `date_id, uconsent_id` |
| `dim_pmsuserprofile` | `los.dim_pmsuserprofile` | etl-pmsmanagement | `date_id, user_id` |
| `fact_pmsinvitations` | `los.fact_pmsinvitations` | etl-pmsmanagement | `date_id, invite_id` |
| `fact_pmsuser_role` | `los.fact_pmsuser_role` | etl-pmsmanagement | `date_id, record_id` |

### Domain: company (3 flows)

| Flow (dwh_table) | BQ Target | Origin repo | PK |
|---|---|---|---|
| `dim_pmscompany` | `los.dim_pmscompany` | etl-pmsmanagement | `date_id, company_id` |
| `fact_pmsinvitation_company` | `los.fact_pmsinvitation_company` | etl-pmsmanagement | `date_id, record_id` |
| `fact_pmsinvitationhist_company` | `los.fact_pmsinvitationhist_company` | etl-pmsmanagement | `date_id, record_id` |

### Domain: project (13 flows)

| Flow (dwh_table) | BQ Target | Origin repo | PK |
|---|---|---|---|
| `dim_address` (tmCOR) | `los.dim_address` | etl-iprop | `date_id` |
| `dim_address` (mobileuser) | `los.dim_address` | etl-iprop | `date_id` |
| `dim_address_type` | `los.dim_address_type` | etl-iprop | `date_id, addr_type_id` |
| `dim_postcode` | `los.dim_postcode` | etl-iprop | `date_id, postcode_id` |
| `dim_project` | `los.dim_project` | etl-iprop | `date_id, project_id, project_profile_id` |
| `dim_project_profile` | `los.dim_project_profile` | etl-iprop | `date_id, project_profile_id` |
| `dim_project_bookbank` | `los.dim_project_bookbank` | etl-iprop | `date_id, bookbank_id` |
| `dim_pmsproject` | `los.dim_pmsproject` | etl-pmsmanagement | `date_id, project_id` |
| `dim_pmsrole_permission` | `los.dim_pmsrole_permission` | etl-pmsmanagement | `date_id, record_id` |
| `fact_pmscompany_projects` | `los.fact_pmscompany_projects` | etl-pmsmanagement | `date_id, mapping_log_id` |
| `fact_pmsinvitation_project` | `los.fact_pmsinvitation_project` | etl-pmsmanagement | `date_id, record_id` |
| `fact_pmsinvitationhist_project` | `los.fact_pmsinvitationhist_project` | etl-pmsmanagement | `date_id, record_id` |
| `fact_pmsproject_features` | `los.fact_pmsproject_features` | etl-pmsmanagement | `date_id, project_id, feature_id` |

### Domain: unit (2 flows)

| Flow (dwh_table) | BQ Target | Origin repo | PK |
|---|---|---|---|
| `dim_unit` | `los.dim_unit` | etl-iprop | `date_id, unit_id` |
| `fact_unit` | `los.fact_unit` | etl-iprop | `date_id, unit_id` |

**Total: 33 flows**

---

## 3. Architecture

### Data Flow

```
GCS Data Lake (Parquet/Hive)
        │
        │  extract_lake / extract_lake_join @task
        │  (1 call per SourceConfig)
        ▼
  pd.DataFrame
        │
        │  transform_<table> @task
        │  (business logic — 1 file per BQ table)
        ▼
  pd.DataFrame (cleaned)
        │
        │  load @task
        │  (upsert via temp table or append)
        ▼
BigQuery DWH — los.<table>
```

### Registry Pattern

```
FLOW_REGISTRY  (flow_registry.py)
      ├── FLOWS from registry_user.py     (15 FlowConfig rows)
      ├── FLOWS from registry_company.py  ( 3 FlowConfig rows)
      ├── FLOWS from registry_project.py  (13 FlowConfig rows)
      └── FLOWS from registry_unit.py     ( 2 FlowConfig rows)
               │
               ▼
      flow_factory.py → make_flow(cfg)
               │
               ▼
      ALL_FLOWS dict  {dwh_table: @flow_fn}
               │
     ┌─────────┴─────────┐
     ▼                   ▼
deploy.py           run_local.py
(register flows)    (repair/backfill)
```

---

## 4. Authentication

| Resource | Method |
|---|---|
| GCS (read Parquet) | SA key JSON → `service_account.Credentials` (STORAGE_SCOPES) |
| BigQuery (write DWH) | SA key JSON → `service_account.Credentials` (BIGQUERY_SCOPES) |
| `pandas_gbq.to_gbq()` | SA key JSON credentials on every call |
| SA key source | **GCP Secret Manager** — secret: `gcp-dwh-service-account` |
| Secret Manager access | **ADC / Workload Identity** on worker node — no key file in container |

SA key fetched via `config._load_sa_key_json()` with `@lru_cache(maxsize=1)` — fetched once per process.

```bash
# Local dev: authenticate with ADC
gcloud auth application-default login
```

---

## 5. Configuration

### `config_flows/__init__.py` — Schedule constants

```python
CRON_SCHEDULE   = "40 20 * * *"   # 03:40 ICT — repo default
CRON_01_40_ICT  = "40 18 * * *"   # 01:40 ICT
CRON_03_40_ICT  = "40 20 * * *"   # 03:40 ICT
CRON_04_00_ICT  = "0  21 * * *"   # 04:00 ICT
CRON_04_40_ICT  = "40 21 * * *"   # 04:40 ICT
CRON_05_00_ICT  = "0  22 * * *"   # 05:00 ICT
```

**Schedule priority in `deploy.py`:**
```
FlowConfig.cron_override   ← per-flow override
        ↓ fallback
CRON_SCHEDULE              ← repo-level default
        ↓ develop branch
None (paused)              ← always paused on develop
```

---

## 6. Local Development

```bash
# 1. Create virtualenv
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure env
cp .env.example .env  # edit as needed
export $(cat .env | xargs)

# 4. GCP auth (ADC)
gcloud auth application-default login

# 5. List all flows
python -c "from flows.flow_factory import ALL_FLOWS; print(list(ALL_FLOWS))"

# 6. Generate manifest
python scripts/generate_manifest.py
```

---

## 7. Repair & Backfill

```bash
# Single date
python run_local.py --flow dim_pmscompany --start 2025-06-15

# Date range
python run_local.py --flow fact_authuser --start 2025-06-01 --end 2025-06-30

# Full scan
python run_local.py --flow dim_pmscompany --full

# All flows (yesterday)
python run_local.py --flow all

# dim_party sub-flows (suffix _n for duplicates)
python run_local.py --flow dim_party_0   # homeservice sub-flow
python run_local.py --flow dim_party_1   # GU/ecoapp
python run_local.py --flow dim_party_2   # RU/MSSQL
python run_local.py --flow dim_party_3   # memberstatus update
```

---

## 8. Deployment

### CI/CD Flow (Jenkins)

```
git push → Jenkins trigger
    ├── Build Docker image
    ├── Push → Google Artifact Registry
    └── Register flows → Prefect Server (python deploy.py)
```

### Manual Deploy

```bash
export CI_COMMIT_BRANCH=main
export IMAGE_TAG=asia-southeast1-docker.pkg.dev/your-gcp-project-id/docker-registry/etl-pms-customer:main-abc123
export PREFECT_API_URL=http://<PREFECT_SERVER_IP>:4200/api

bash scripts/build.sh
bash scripts/register.sh
```

---

## 9. Adding a New Flow

### Step 1 — Create transform task

```python
# tasks/tasks_pmscustomer_dwh_<table>.py

from prefect import get_run_logger, task
import pandas as pd

@task(name="transform_<table>")
def transform_<table>(p_data: list[pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    df_final = p_data[0].copy()
    # ... business logic ...
    return df_final
```

### Step 2 — Add FlowConfig to the appropriate registry

```python
# flows/registry/registry_<domain>.py

from tasks.tasks_pmscustomer_dwh_<table> import transform_<table>

FLOWS: tuple[FlowConfig, ...] = (
    *existing_flows,
    FlowConfig(
        dwh_table    = "<table>",
        pk           = ("date_id", "<pk_col>"),
        sources      = (
            SourceConfig(tablename="<gcs_table>", source_type="postgresql",
                         bucket_app=BUCKET_PMSMANAGEMENT),
        ),
        transform_fn = transform_<table>,
        origin       = "etl-pmsmanagement@prefect-v1",
    ),
)
```

> **No changes needed** to `deploy.py`, `run_local.py`, `flow_registry.py`, or `flow_factory.py` — everything updates automatically through `FLOW_REGISTRY`.

---

## 10. Manifest & Overview

```bash
# Generate stdout manifest (develop env)
python scripts/generate_manifest.py

# Write YAML for main env
python scripts/generate_manifest.py --env main --out manifest.yaml

# JSON for Google Sheet sync
python scripts/generate_manifest.py --env main --format json --out manifest.json
```

---

## 11. Key Changes from v1

| Area | v1 (Prefect v1) | v3 (Prefect v3 — this repo) |
|---|---|---|
| Flow declaration | `with Flow(...) as flow:` | `@flow` via `make_flow()` factory |
| Task declaration | `class T(Task): def run(self,...)` | `@task` function |
| Logger | `prefect.context.get("logger")` | `get_run_logger()` |
| Run config | `KubernetesRun(...)` | `flow.deploy(work_pool_name=...)` |
| Schedule | `CronClock` in `config_flows/__init__.py` | `cron=` param in `deploy.py` |
| Auth | SA key file path from env var | SA key JSON from **Secret Manager** via ADC |
| `GCSFS` | `GCSFileSystem(token=file_path)` | `GCSFileSystem(token=credentials_object)` |
| Flow structure | 6 separate repos, boilerplate duplication | 1 repo, registry pattern |
| Scheduler repo | Separate repo | Not needed — schedule in `config_flows` + `deploy.py` |
| Overview | Not available | `scripts/generate_manifest.py` |
