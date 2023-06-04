import os
from prefect_gcp.credentials import GcpCredentials
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile


BASE_NAME = "world-earthquake-pipeline"
PROJECT_ID = os.environ.get("WORLD_EARTHQUAKE_PROJECT_ID")
ENV = os.environ.get("ENV")
BLOCK_NAME = f"{BASE_NAME}-{ENV}"
SCHEMA_NAME = f"earthquake_{ENV}"
DBT_PROFILE_NAME = "world_earthquake_pipeline"

credentials = GcpCredentials.load(BLOCK_NAME)
target_configs = BigQueryTargetConfigs(
    schema=SCHEMA_NAME,
    project=PROJECT_ID,
    location="EU",
    credentials=credentials,
)
target_configs.save(BLOCK_NAME, overwrite=True)

dbt_cli_profile = DbtCliProfile(
    name=DBT_PROFILE_NAME,
    target=ENV,
    target_configs=target_configs,
)
dbt_cli_profile.save(BLOCK_NAME, overwrite=True)
