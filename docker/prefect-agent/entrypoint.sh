#!/bin/sh

gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
gcloud auth configure-docker

exec prefect agent start -p default-agent-pool
