import os
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse


BASE_NAME = "world-earthquake-pipeline"
ENV = os.environ.get("ENV")
DL_BUCKET = os.environ.get("WORLD_EARTHQUAKE_DL_BUCKET")
BLOCK_NAME = f"{BASE_NAME}-{ENV}"


with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], 'r') as file:
    service_account_data = file.read()

credentials_block = GcpCredentials(
    service_account_info = service_account_data
)
credentials_block.save(BLOCK_NAME, overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load(BLOCK_NAME),
    bucket=DL_BUCKET,
)

bucket_block.save(BLOCK_NAME, overwrite=True)

bigquery_block = BigQueryWarehouse(
    gcp_credentials=GcpCredentials.load(BLOCK_NAME),
    fetch_size=100
)

bigquery_block.save(BLOCK_NAME, overwrite=True)
