import os
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse


project_id = os.environ.get("WORLD_EARTHQUAKE_PROJECT_ID")
raw_dataset = "world_earthquake_raw"

@task(retries=3, log_prints=True)
def update_external_table():

    gcs_block = GcsBucket.load("world-earthquake")
    bucket_name = gcs_block.bucket
 
    with BigQueryWarehouse.load("world-earthquake") as warehouse:
        operation = f'''
        CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{raw_dataset}.ext_kaggle_data` 
        OPTIONS (
            format = 'PARQUET',
            uris = ["gs://{bucket_name}/data/garrickhague/world-earthquake-data-from-1906-2022/Global_Earthquake_Data.parquet"]
        );
        '''
        warehouse.execute(operation)

    return


@task(retries=3, log_prints=True)
def update_bigquery_table():

    with BigQueryWarehouse.load("world-earthquake") as warehouse:
        operation = f'''
        CREATE OR REPLACE TABLE `{project_id}.{raw_dataset}.kaggle_data`
        AS 
        SELECT * FROM `{project_id}.{raw_dataset}.ext_kaggle_data`
        '''
        warehouse.execute(operation)

    return


@flow(name="world-earthquake: update BigQUery table")
def gcs_to_bq() -> None:
    """save to BigQuery table"""
    
    update_external_table()
    update_bigquery_table()


if __name__ == "__main__":

    gcs_to_bq()
