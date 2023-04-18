import os
from prefect.infrastructure.docker import DockerContainer

WORLD_EARTHQUAKE_DOCKER_IMAGE = os.environ.get("WORLD_EARTHQUAKE_DOCKER_IMAGE")
# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image=WORLD_EARTHQUAKE_DOCKER_IMAGE,
    image_pull_policy="ALWAYS",
    auto_remove=True
)

docker_block.save("world-earthquake", overwrite=True)
