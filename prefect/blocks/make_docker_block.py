import os
from prefect.infrastructure.docker import DockerContainer

WORLD_EARTHQUAKE_FLOWS_DOCKER_IMAGE = os.environ.get("WORLD_EARTHQUAKE_FLOWS_DOCKER_IMAGE")
BASE_NAME = "world-earthquake-pipeline"
ENV = os.environ.get("ENV")
BLOCK_NAME = f"{BASE_NAME}-{ENV}"

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image=WORLD_EARTHQUAKE_FLOWS_DOCKER_IMAGE,
    image_pull_policy="ALWAYS",
    auto_remove=True
)

docker_block.save(BLOCK_NAME, overwrite=True)
