import os
from prefect_gcp import GcpCredentials, GcpSecret
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse

BLOCK_NAME = "world-earthquake"
DL_BUCKET = os.environ.get("WORLD_EARTHQUAKE_DL_BUCKET")

with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], 'r') as file:
    service_account_data = file.read()

credentials_block = GcpCredentials(
    service_account_info = service_account_data
)
credentials_block.save(BLOCK_NAME, overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load(BLOCK_NAME),
    bucket=f"{DL_BUCKET}",
)

bucket_block.save(BLOCK_NAME, overwrite=True)

bigquery_block = BigQueryWarehouse(
    gcp_credentials=GcpCredentials.load(BLOCK_NAME),
    fetch_size=100
)

bigquery_block.save(BLOCK_NAME, overwrite=True)

secret_block = GcpSecret(
    gcp_credentials=GcpCredentials.load(BLOCK_NAME),
    secret_name = "kaggle-json"
)

secret_block.save(f"kaggle-json", overwrite=True)
