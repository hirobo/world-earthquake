import os
from prefect.infrastructure.docker import DockerContainer

WORLD_EARTHQUAKE_DOCKER_IMAGE_NAME = os.environ.get("WORLD_EARTHQUAKE_DOCKER_IMAGE_NAME")
# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image=WORLD_EARTHQUAKE_DOCKER_IMAGE_NAME,
    image_pull_policy="ALWAYS",
    auto_remove=True
)

docker_block.save("world-earthquake", overwrite=True)
