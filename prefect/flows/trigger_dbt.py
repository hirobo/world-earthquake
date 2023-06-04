import os
from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation, DbtCliProfile

BASE_NAME = "world-earthquake-pipeline"
PROJECT_ID = os.environ.get("WORLD_EARTHQUAKE_PROJECT_ID")
ENV = os.environ.get("ENV")
BLOCK_NAME = f"{BASE_NAME}-{ENV}"
DBT_DIR = "../dbt"
DBT_PROFILES_DIR = "../dbt" # we don't use ~/.dbt


@flow(name="world-earthquake-pipeline: trigger_dbt")
def trigger_dbt() -> str:
    command = f"dbt build --target {ENV} --vars 'is_test_run: false'"
    result = DbtCoreOperation(
        commands=[command],
        project_dir=DBT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        dbt_cli_profile = DbtCliProfile.load(BLOCK_NAME),
        overwrite_profiles=True
    ).run()

    return result


if __name__ == "__main__":

    trigger_dbt()