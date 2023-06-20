import os
import json
import gcsfs
import re
import pandas as pd
from datetime import datetime, timezone
from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

BASE_NAME = "world-earthquake-pipeline"
PROJECT_ID = os.environ.get("WORLD_EARTHQUAKE_PROJECT_ID")
ENV = os.environ.get("ENV")
BLOCK_NAME = f"{BASE_NAME}-{ENV}"
RAW_DATASET = "earthquake_raw"
USGS_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.usgs_data"


@task
def get_last_datetime() -> datetime:
    query = f"SELECT MAX(properties_time) as last_datetime FROM `{USGS_TABLE}`"

    with BigQueryWarehouse.load(BLOCK_NAME) as warehouse:
        result = warehouse.fetch_one(query)
        if result:
            last_datetime = datetime.fromtimestamp(result['last_datetime'] / 1000, tz=timezone.utc)
        else:
            return None

    return last_datetime


def get_schema():
    return [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("properties_mag", "FLOAT"),
        bigquery.SchemaField("properties_place", "STRING"),
        bigquery.SchemaField("properties_time", "INTEGER"),
        bigquery.SchemaField("properties_updated", "INTEGER"),
        bigquery.SchemaField("properties_tz", "STRING"),
        bigquery.SchemaField("properties_url", "STRING"),
        bigquery.SchemaField("properties_detail", "STRING"),
        bigquery.SchemaField("properties_felt", "INTEGER"),
        bigquery.SchemaField("properties_cdi", "FLOAT"),
        bigquery.SchemaField("properties_mmi", "FLOAT"),
        bigquery.SchemaField("properties_alert", "STRING"),
        bigquery.SchemaField("properties_status", "STRING"),
        bigquery.SchemaField("properties_tsunami", "INTEGER"),
        bigquery.SchemaField("properties_sig", "INTEGER"),
        bigquery.SchemaField("properties_net", "STRING"),
        bigquery.SchemaField("properties_code", "STRING"),
        bigquery.SchemaField("properties_ids", "STRING"),
        bigquery.SchemaField("properties_sources", "STRING"),
        bigquery.SchemaField("properties_types", "STRING"),
        bigquery.SchemaField("properties_nst", "INTEGER"),
        bigquery.SchemaField("properties_dmin", "FLOAT"),
        bigquery.SchemaField("properties_rms", "FLOAT"),
        bigquery.SchemaField("properties_gap", "FLOAT"),
        bigquery.SchemaField("properties_magType", "STRING"),
        bigquery.SchemaField("properties_type", "STRING"),
        bigquery.SchemaField("properties_title", "STRING"),
        bigquery.SchemaField("geometry_type", "STRING"),
        bigquery.SchemaField("geometry_longitude", "FLOAT"),
        bigquery.SchemaField("geometry_latitude", "FLOAT"),
        bigquery.SchemaField("geometry_altitude", "FLOAT")
    ]


def get_dataframe_schema():
    bq_schema = get_schema()
    schema = {}

    for field in bq_schema:
        field_type = field.field_type
        field_name = field.name

        dtype = None
        if field_type == "STRING":
            dtype = "str"
        elif field_type == "FLOAT":
            dtype = "float"
        elif field_type == "INTEGER":
            dtype = "Int64"
        else:
            dtype = "object"

        schema[field_name] = dtype

    return schema


def get_schema_field_names():
    schema = get_schema()
    return [field.name for field in schema]


def if_table_exists(client, table_ref):
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def get_temp_table_ref(file_path) -> str:
    file_name = os.path.basename(file_path)
    pattern = r"\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}"
    match = re.search(pattern, file_name)

    if match:
        date_part = match.group()
        table_ref = f"{PROJECT_ID}.{RAW_DATASET}.usgs_temp_{date_part}"
        return table_ref
    else:
        raise ValueError("file name is invalid!")


@task(retries=1, log_prints=True)
def load_data_from_gcs_to_temp_table(file_path) -> str:
    logger = get_run_logger()
    logger.info(f"load_data_from_gcs_to_temp_table: {file_path}")

    gcp_credentials = GcpCredentials.load(BLOCK_NAME)
    client = gcp_credentials.get_bigquery_client()

    fs = gcsfs.GCSFileSystem(
        project=PROJECT_ID, token=gcp_credentials.get_credentials_from_service_account())

    gcs_block = GcsBucket.load(BLOCK_NAME)
    bucket_name = gcs_block.bucket
    gsc_file_path = f"gs://{bucket_name}/{file_path}"

    with fs.open(gsc_file_path) as file:
        data = [json.loads(line) for line in file]
        if len(data) == 0:
            return None

        df = pd.json_normalize(data, sep="_")

        df[["geometry_longitude", "geometry_latitude", "geometry_altitude"]] = pd.DataFrame(
            df["geometry_coordinates"].to_list())
        df.drop(columns=["geometry_coordinates"], inplace=True)
        df = df.reindex(columns=get_schema_field_names())
        schema_data_types = get_dataframe_schema()
        df = df.astype(schema_data_types)

    # delete table if exists
    table_ref = get_temp_table_ref(file_path)
    if if_table_exists(client, table_ref):
        delete_temp_table(table_ref)

    schema = get_schema()
    table = bigquery.Table(table_ref, schema=schema)
    table.temporary = True

    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()

    return table_ref


@task(retries=1, log_prints=True)
def update_bigquery_table(temp_ref):
    logger = get_run_logger()
    logger.info("update_bigquery_table")

    gcp_credentials = GcpCredentials.load(BLOCK_NAME)
    client = gcp_credentials.get_bigquery_client()

    # create table if not exists
    if not if_table_exists(client, USGS_TABLE):
        logger = get_run_logger()
        logger.info(f"create table: {USGS_TABLE}")

        schema = get_schema()
        schema.extend([
            bigquery.SchemaField("is_valid", "BOOL", mode="REQUIRED",
                                 description="Indicates whether the record is currently valid."),
            bigquery.SchemaField("valid_from", "TIMESTAMP", mode="REQUIRED",
                                 description="Timestamp when the record became valid."),
            bigquery.SchemaField("valid_to", "TIMESTAMP",
                                 description="Timestamp when the record became invalid. NULL if the record is valid."),
            bigquery.SchemaField("hash_value", "INT64", mode="REQUIRED",
                                 description="Hash value of the record calculated using FARM_FINGERPRINT.")
        ])

        table = bigquery.Table(USGS_TABLE, schema=schema)
        client.create_table(table, exists_ok=True)

    # update table
    tmp_schema = client.get_table(temp_ref).schema
    schema = client.get_table(USGS_TABLE).schema

    tmp_columns = [f.name for f in tmp_schema if f.name != "id"]
    columns = [f.name for f in schema if f.name not in [
        "id", "is_valid", "valid_from", "valid_to", "hash_value"]]

    tmp_columns_str = ", ".join(tmp_columns)
    columns_str = ", ".join(columns)

    farm_fingerprint_arg = ', '.join(
        [f"IFNULL(CAST({field} AS STRING), '')" for field in columns])
    farm_fingerprint_expr = f"FARM_FINGERPRINT(CONCAT({farm_fingerprint_arg}))"

    query = f"""

    -- invalidate current data if there are some updates
    UPDATE `{USGS_TABLE}` AS t2
    SET
    is_valid = FALSE,
    valid_to = CURRENT_TIMESTAMP()
    WHERE
    is_valid = TRUE
    AND EXISTS (
        SELECT 1
        FROM `{temp_ref}` AS t1
        WHERE t1.id = t2.id
        AND NOT {farm_fingerprint_expr} = t2.hash_value
    );

    -- add new data or update data from t1
    INSERT INTO `{USGS_TABLE}` (id, {columns_str}, is_valid, valid_from, valid_to, hash_value)
    SELECT
    id,
    {tmp_columns_str},
    TRUE AS is_valid,
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    {farm_fingerprint_expr} AS hash_value
    FROM
    `{temp_ref}` AS t1
    WHERE
    NOT EXISTS (
        SELECT 1
        FROM `{USGS_TABLE}` AS t2
        WHERE t2.id = t1.id
        AND t2.is_valid = TRUE
        AND {farm_fingerprint_expr} = t2.hash_value
    );
    """

    with BigQueryWarehouse.load(BLOCK_NAME) as warehouse:
        warehouse.execute(query)

    delete_temp_table(temp_ref)


def delete_temp_table(table_ref):
    logger = get_run_logger()
    logger.info(f"delete_temp_table: {table_ref}")

    gcp_credentials = GcpCredentials.load(BLOCK_NAME)
    client = gcp_credentials.get_bigquery_client()
    client.delete_table(table_ref, not_found_ok=True)


@flow(name="world-earthquake-pipeline: gcs_to_bq")
def gcs_to_bq(file_path) -> None:
    """update data BigQuery table"""

    logger = get_run_logger()
    logger.info(f"gcs_to_bq: {file_path}")

    temp_ref = load_data_from_gcs_to_temp_table(file_path)
    if temp_ref:
        update_bigquery_table(temp_ref)
