# orchestration/assets/batch.py
"""
dbt models as Dagster assets via dagster-dbt integration.
The manifest.json must exist before Dagster starts (run `dbt parse` first).
"""
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "analytics"
DBT_MANIFEST = DBT_PROJECT_DIR / "target" / "manifest.json"

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROJECT_DIR),
)


@dbt_assets(manifest=DBT_MANIFEST)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


assets = [dbt_models]
