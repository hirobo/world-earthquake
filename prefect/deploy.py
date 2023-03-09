import os
from prefect.deployments import Deployment
from flows.web_to_gcs import web_to_gcs
from flows.gcs_to_bq import gcs_to_bq
from prefect.infrastructure.docker import DockerContainer


BLOCK_NAME = "world-earthquake"
PROJECT_ID = os.environ.get("WORLD_EARTHQUAKE_PROJECT_ID")


docker_block = DockerContainer.load("world-earthquake")

docker_dep_web_to_gcs = Deployment.build_from_flow(
    flow=web_to_gcs,
    name="web_to_gcs",
    infrastructure=docker_block,
    tags=[BLOCK_NAME],
)

docker_dep_gcs_to_bq = Deployment.build_from_flow(
    flow=gcs_to_bq,
    name="gcs_to_bq",
    infrastructure=docker_block,
    infra_overrides={"env.WORLD_EARTHQUAKE_PROJECT_ID": PROJECT_ID},
    tags=[BLOCK_NAME],
)


if __name__ == "__main__":
    docker_dep_web_to_gcs.apply()
    docker_dep_gcs_to_bq.apply()    