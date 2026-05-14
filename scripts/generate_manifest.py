"""
scripts/generate_manifest.py
=============================
Generate manifest.yaml (or JSON) from FLOW_REGISTRY — no Prefect Server required.

Usage:
    python scripts/generate_manifest.py
    python scripts/generate_manifest.py --env main --out manifest.yaml
    python scripts/generate_manifest.py --env main --format json --out manifest.json
"""

import argparse
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("CI_COMMIT_BRANCH", "develop")

import yaml

from config_flows import APPLICATION_TYPE, CRON_SCHEDULE
from flows.flow_registry import FLOW_REGISTRY

parser = argparse.ArgumentParser()
parser.add_argument("--env",    default="develop", choices=["develop","main"])
parser.add_argument("--format", default="yaml",   choices=["yaml","json"])
parser.add_argument("--out",    default=None,      help="Output file path")
args = parser.parse_args()

IS_PRODUCTION = args.env == "main"


def get_cron(cfg) -> str | None:
    if not IS_PRODUCTION:
        return None
    return cfg.cron_override or CRON_SCHEDULE


flows_out = []
for cfg in FLOW_REGISTRY:
    cron = get_cron(cfg)
    sources = []
    for src in cfg.sources:
        sources.append({
            "bucket_app":   src.bucket_app,
            "source_type":  src.source_type,
            "tablename":    src.tablename,
            "columns":      list(src.columns) if src.columns else None,
            "is_join":      src.is_join,
        })
    flows_out.append({
        "repo":           "etl-pms-customer",
        "flow_name":      f"{APPLICATION_TYPE}_{cfg.dwh_table}",
        "dwh_table":      cfg.dwh_table,
        "bq_target": {
            "project": "your-gcp-project-id",
            "schema":  cfg.schema,
            "table":   cfg.dwh_table,
        },
        "pk":              list(cfg.pk),
        "sources":         sources,
        "schedule":        cron,
        "schedule_source": "cron_override" if cfg.cron_override else "global_default",
        "default_full_data": cfg.default_full_data,
        "origin":          cfg.origin,
        "active":          IS_PRODUCTION,
    })

manifest = {
    "meta": {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repo":         "etl-pms-customer",
        "env":          args.env,
        "total_flows":  len(flows_out),
    },
    "flows": flows_out,
}

if args.format == "json":
    output = json.dumps(manifest, indent=2, ensure_ascii=False)
else:
    output = yaml.dump(manifest, allow_unicode=True, sort_keys=False, default_flow_style=False)

if args.out:
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(output)
    print(f"Manifest written to {args.out}  ({len(flows_out)} flows)")
else:
    print(output)
