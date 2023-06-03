import os
from prefect.deployments import Deployment
from flows.web_to_gcs_to_bq_with_params import web_to_gcs_to_bq_with_params
from flows.web_to_gcs_to_bq_all import web_to_gcs_to_bq_all
from flows.web_to_gcs_to_bq_daily import web_to_gcs_to_bq_daily
from flows.trigger_dbt import trigger_dbt
from prefect.infrastructure.docker import DockerContainer
from prefect.server.schemas.schedules import CronSchedule


BASE_NAME = "world-earthquake-pipeline"
PROJECT_ID = os.environ.get("WORLD_EARTHQUAKE_PROJECT_ID")
ENV = os.environ.get("ENV")
BLOCK_NAME = f"{BASE_NAME}-{ENV}"


docker_block = DockerContainer.load(BLOCK_NAME)

docker_dep_web_to_gcs_to_bq_with_params = Deployment.build_from_flow(
    flow=web_to_gcs_to_bq_with_params,
    name="deploy",
    infrastructure=docker_block,
    infra_overrides={"env.WORLD_EARTHQUAKE_PROJECT_ID": PROJECT_ID, "env.ENV": ENV},
    tags=[BASE_NAME, ENV],
)

docker_dep_web_to_gcs_to_bq_all = Deployment.build_from_flow(
    flow=web_to_gcs_to_bq_all,
    name="deploy",
    infrastructure=docker_block,
    infra_overrides={"env.WORLD_EARTHQUAKE_PROJECT_ID": PROJECT_ID, "env.ENV": ENV},
    tags=[BASE_NAME, ENV],
)

docker_dep_web_to_gcs_to_bq_daily = Deployment.build_from_flow(
    flow=web_to_gcs_to_bq_daily,
    name="deploy",
    infrastructure=docker_block,
    infra_overrides={"env.WORLD_EARTHQUAKE_PROJECT_ID": PROJECT_ID, "env.ENV": ENV},
    tags=[BASE_NAME, ENV],
    schedule=(CronSchedule(cron="0 5 * * *", timezone="UTC"))
)


docker_dep_trigger_dbt = Deployment.build_from_flow(
    flow=trigger_dbt,
    name="deploy",
    infrastructure=docker_block,
    infra_overrides={"env.WORLD_EARTHQUAKE_PROJECT_ID": PROJECT_ID, "env.ENV": ENV},
    tags=[BASE_NAME, ENV],
    schedule=(CronSchedule(cron="5 5 * * *", timezone="UTC"))
)


if __name__ == "__main__":
    docker_dep_web_to_gcs_to_bq_with_params.apply()
    docker_dep_web_to_gcs_to_bq_all.apply()
    docker_dep_web_to_gcs_to_bq_daily.apply()
    docker_dep_trigger_dbt.apply()
