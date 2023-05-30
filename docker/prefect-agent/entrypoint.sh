#!/bin/sh

# check project id (for Artifact Registry)
echo "ARTIFACT_REGISTRY_PROJECT_ID: $ARTIFACT_REGISTRY_PROJECT_ID"

PREFECT_API_URL_SECRET_ID="prefect-api-url-world-earthquake-pipeline"
PREFECT_API_KEY_SECRET_ID="prefect-api-key-world-earthquake-pipeline"

# check the current service account
gcloud auth list

export PREFECT_API_URL=$(python -c 'from google.cloud import secretmanager; client = secretmanager.SecretManagerServiceClient(); name = client.secret_version_path("'$ARTIFACT_REGISTRY_PROJECT_ID'", "'$PREFECT_API_URL_SECRET_ID'", "latest"); response = client.access_secret_version(name=name); print(response.payload.data.decode("UTF-8"))')
export PREFECT_API_KEY=$(python -c 'from google.cloud import secretmanager; client = secretmanager.SecretManagerServiceClient(); name = client.secret_version_path("'$ARTIFACT_REGISTRY_PROJECT_ID'", "'$PREFECT_API_KEY_SECRET_ID'", "latest"); response = client.access_secret_version(name=name); print(response.payload.data.decode("UTF-8"))')
echo "PREFECT_API_URL: $PREFECT_API_URL"

# configure gcloud for the artifact registry host (need for docker pull)
gcloud auth configure-docker europe-west3-docker.pkg.dev --quiet

# start prefect agent
exec prefect agent start -p default-agent-pool
