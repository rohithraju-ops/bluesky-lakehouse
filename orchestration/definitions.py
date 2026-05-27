# orchestration/definitions.py
"""
Dagster Definitions for the bluesky-lakehouse.
Assets span all pipeline layers: ingestion → streaming → iceberg → dbt models.
"""
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource

from orchestration.assets import batch, ingestion, streaming
from orchestration.assets.batch import DBT_PROJECT_DIR

all_assets = [*ingestion.assets, *streaming.assets, *batch.assets]

dbt_job = define_asset_job(
    name="dbt_refresh",
    selection=AssetSelection.groups("dbt_models"),
)

hourly_dbt = ScheduleDefinition(
    job=dbt_job,
    cron_schedule="0 * * * *",
)

defs = Definitions(
    assets=all_assets,
    jobs=[dbt_job],
    schedules=[hourly_dbt],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        )
    },
)
