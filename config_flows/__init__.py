import os

APP_ENV  = os.getenv("CI_COMMIT_BRANCH", "develop")
IMAGE_TAG = os.getenv("IMAGE_TAG", "etl-pms-customer:local")

PREFECT_WORK_POOL  = os.getenv("PREFECT_WORK_POOL", "kubernetes-pool")
PREFECT_WORK_QUEUE = os.getenv("PREFECT_WORK_QUEUE", "default")
PREFECT_IMAGE      = IMAGE_TAG

APPLICATION_TYPE = "etl_pmscustomer_dwh"

# --- Job date parameters (resolved at deploy time; overridable via Prefect UI) ---
JOB_YEAR      = os.getenv("JOB_YEAR",      "None")
JOB_MONTH     = os.getenv("JOB_MONTH",     "None")
JOB_DAY       = os.getenv("JOB_DAY",       "None")
JOB_FULL_DATA = os.getenv("JOB_FULL_DATA", "0")

JOB_NUMOFYER  = os.getenv("JOB_NUMOFYER",  "10") #Default = 10 , meaning 10 years of history (including current year) will be fetched from source tables. Adjust as needed.
JOB_HISOFYER  = os.getenv("JOB_HISOFYER",  "2026") # Default = current year (e.g., 2026), meaning history will be fetched up to this year. Adjust as needed, especially if you want to fetch future-dated records or limit to past years.

# --- Named cron schedule constants (ICT = UTC+7) ---
# Priority in deploy.py: FlowConfig.cron_override → CRON_SCHEDULE → None (develop)
CRON_SCHEDULE   = "40 20 * * *"   # 03:40 ICT  — repo-level default

CRON_01_00_ICT  = "0  18 * * *"   # 01:00 ICT
CRON_01_30_ICT  = "30 18 * * *"   # 01:30 ICT
CRON_01_40_ICT  = "40 18 * * *"   # 01:40 ICT
CRON_03_20_ICT  = "20 20 * * *"   # 03:20 ICT
CRON_03_30_ICT  = "30 20 * * *"   # 03:30 ICT
CRON_03_40_ICT  = "40 20 * * *"   # 03:40 ICT  (= CRON_SCHEDULE)
CRON_03_50_ICT  = "50 20 * * *"   # 03:50 ICT
CRON_04_00_ICT  = "0  21 * * *"   # 04:00 ICT
CRON_04_40_ICT  = "40 21 * * *"   # 04:40 ICT
CRON_05_00_ICT  = "0  22 * * *"   # 05:00 ICT
