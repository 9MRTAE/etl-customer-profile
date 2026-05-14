"""
flows/flow_registry.py
======================
Single source of truth for all flows in etl-pms-customer.

Registry domains (Column I from ETL repo mapping spreadsheet):
  user     → registry_user.py
  company  → registry_company.py
  project  → registry_project.py
  unit     → registry_unit.py

NOTE: FlowConfig and SourceConfig live in flows/flow_config.py to avoid
circular imports — registry_*.py also import those dataclasses.
"""

# Re-export so existing "from flows.flow_registry import FlowConfig, SourceConfig" works.
from flows.flow_config import FlowConfig, SourceConfig  # noqa: F401

from flows.registry.registry_user import FLOWS as FLOWS_USER
from flows.registry.registry_company import FLOWS as FLOWS_COMPANY
from flows.registry.registry_project import FLOWS as FLOWS_PROJECT
from flows.registry.registry_unit import FLOWS as FLOWS_UNIT

FLOW_REGISTRY: tuple[FlowConfig, ...] = (
    *FLOWS_USER,
    *FLOWS_COMPANY,
    *FLOWS_PROJECT,
    *FLOWS_UNIT,
)
