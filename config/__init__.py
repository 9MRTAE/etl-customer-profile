import os
import json
from functools import lru_cache

APP_ENV = os.getenv("CI_COMMIT_BRANCH", "develop")
DEPLOY_MODE = os.getenv("PREFECT_DEPLOY_MODE", "") == "1"

if APP_ENV == "main":
    from .production import *
else:
    from .development import *


@lru_cache(maxsize=1)
def _load_sa_key_json() -> dict:
    """Fetch SA key JSON from Secret Manager using ADC (Workload Identity on worker node)."""
    from google.auth import default as google_auth_default
    from google.cloud import secretmanager
    adc_credentials, _ = google_auth_default()
    client = secretmanager.SecretManagerServiceClient(credentials=adc_credentials)
    name = f"projects/{SECRET_PROJECT_ID}/secrets/{GCP_SA_SECRET_NAME}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode("utf-8"))


def get_sa_credentials(scopes: list[str]):
    from google.oauth2 import service_account
    sa_info = _load_sa_key_json()
    return service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)


# Only materialise credentials at runtime (not at deploy/import time).
# PREFECT_DEPLOY_MODE=1 skips this block, matching ingest repo pattern.
if not DEPLOY_MODE:
    import gcsfs
    STORAGE_CREDENTIAL = get_sa_credentials(STORAGE_SCOPES)
    BIGQUERY_CREDENTIAL = get_sa_credentials(BIGQUERY_SCOPES)
    GCSFS = gcsfs.GCSFileSystem(project=GCP_PROJECT, token=STORAGE_CREDENTIAL)
else:
    STORAGE_CREDENTIAL = None
    BIGQUERY_CREDENTIAL = None
    GCSFS = None
