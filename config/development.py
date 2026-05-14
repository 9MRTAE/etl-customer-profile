# GCP
SECRET_PROJECT_ID   = "your-gcp-project-id"          # [SCRUBBED] replace with your GCP project ID
GCP_SA_SECRET_NAME  = "your-secret-sa-key"            # [SCRUBBED] replace with your Secret Manager key name for service account JSON
GCP_PROJECT         = "your-gcp-project-id"          # [SCRUBBED] replace with your GCP project ID

STORAGE_SCOPES  = ["https://www.googleapis.com/auth/devstorage.full_control"]
BIGQUERY_SCOPES = ["https://www.googleapis.com/auth/bigquery",
                   "https://www.googleapis.com/auth/bigquery.insertdata"]

BUCKET_LAKE    = "your-datalake-bucket"               # [SCRUBBED] replace with your GCS data lake bucket name
BUCKET_PARQUET = "gcp-storage-parquet"

# GCS application bucket constants (source lake paths) — match ingest repo GCS_APPLICATION values
BUCKET_AUTHENTICATION   = "authentication"
BUCKET_HOMESERVICE      = "homeservice"
BUCKET_MOBILEREGISTER   = "mobileregister"
BUCKET_MSSQL            = "gcp-ingest-mssql"
BUCKET_NOTIFICATION     = "notification"
BUCKET_PDPA             = "pdpa"
BUCKET_PMSMANAGEMENT    = "pmsmanagement"
BUCKET_LOYALTY          = "loyalty"
BUCKET_LIVINGMART       = "livingmart"
BUCKET_PAYMENT          = "payment"
BUCKET_STOCKKEYCARD     = "stockkeycard"
BUCKET_TIMEATTENDANCE   = "timeattendance"
BUCKET_VISITOR          = "visitor"
BUCKET_SMARTAUDIT       = "smartaudit"
