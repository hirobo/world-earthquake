FROM prefecthq/prefect:2.8.4-python3.9

RUN apt-get update && apt-get install -y curl docker.io && rm -rf /var/lib/apt/lists/*
RUN pip install google-cloud-secret-manager

RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

ARG PREFECT_API_URL
ENV PREFECT_API_URL $PREFECT_API_URL
ARG ARTIFACT_REGISTRY_PROJECT_ID
ENV ARTIFACT_REGISTRY_PROJECT_ID $ARTIFACT_REGISTRY_PROJECT_ID
ARG WORK_POOL_NAME
ENV WORK_POOL_NAME $WORK_POOL_NAME

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
