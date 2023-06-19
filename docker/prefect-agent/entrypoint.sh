#!/bin/sh

# check environments
echo "ARTIFACT_REGISTRY_PROJECT_ID: $ARTIFACT_REGISTRY_PROJECT_ID"
echo "PREFECT_API_URL: $PREFECT_API_URL"
echo "WORK_POOL_NAME: $WORK_POOL_NAME"

# check the current service account
gcloud auth list

# load prefect api key from secret manager
PREFECT_API_KEY_SECRET_ID="PREFECT_API_KEY"
export PREFECT_API_KEY=$(python -c 'from google.cloud import secretmanager; client = secretmanager.SecretManagerServiceClient(); name = client.secret_version_path("'$ARTIFACT_REGISTRY_PROJECT_ID'", "'$PREFECT_API_KEY_SECRET_ID'", "latest"); response = client.access_secret_version(name=name); print(response.payload.data.decode("UTF-8"))')

# configure gcloud for the artifact registry host (need for docker pull)
gcloud auth configure-docker europe-west3-docker.pkg.dev --quiet

# start prefect agent
exec prefect agent start -p $WORK_POOL_NAME
