import os
import requests
import json
from datetime import date
from io import StringIO
from prefect import task, flow, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket
from requests.exceptions import RequestException

from flows.utils.gcs_to_bq import gcs_to_bq


BASE_NAME = "world-earthquake-pipeline"
ENV = os.environ.get("ENV")
BLOCK_NAME = f"{BASE_NAME}-{ENV}"


def get_file_path(start_date: date, end_date: date, split_time=True) -> str:
    if split_time:
        year = start_date.year
        month = start_date.month
        return f"usgs/{year}/{month:02d}/earthquake_{start_date}_{end_date}.ndjson"
    else:
        return f"usgs/earthquake_{start_date}_{end_date}.ndjson"


def process_data(start_date: date,
                 end_date: date,
                 replace: bool,
                 split_time: bool) -> None:
    file_path = get_file_path(start_date, end_date, split_time)
    web_to_gcs(start_date, end_date, replace, file_path)
    gcs_to_bq(file_path)


@task(retries=1, log_prints=True)
def if_file_exists(file_path) -> bool:
    gcs_block = GcsBucket.load(BLOCK_NAME)
    blobs = gcs_block.list_blobs(file_path)
    return any([blob.name == file_path for blob in blobs])


@task(retries=1, log_prints=True)
def fetch_earthquake_data(start_date, end_date) -> None:
    """
    Fetch earthquake data from USGS (https://earthquake.usgs.gov/)
    e.g. https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=1568-01-01&endtime=1949-12-31
    limit = 20000
    """
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    params = {
        "format": "geojson",
        "starttime": start_date,
        "endtime": end_date
    }

    try:
        response = requests.get(url, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            Exception(f"Error fetching data: {response.status_code}")
    except RequestException as e:
        Exception(f"Error sending request: {e}")
    except json.JSONDecodeError as e:
        Exception(f"Error decoding JSON: {e}")


@task(retries=1, log_prints=True)
def convert_to_ndjson(earthquake_data) -> str:
    ndjson_data = ""
    for feature in earthquake_data["features"]:
        ndjson_data += json.dumps(feature) + "\n"
    return ndjson_data


@task(retries=1, log_prints=True)
def upload_to_gcs(ndjson_data, file_path) -> None:
    """Upload data to GCS"""
    gcs_block = GcsBucket.load(BLOCK_NAME)
    with StringIO(ndjson_data) as file_obj:
        gcs_block.upload_from_file_object(
            file_obj,
            file_path, timeout=120)
    return


@flow(name="world-earthquake-pipeline: web_to_gcs")
def web_to_gcs(start_date: date,
               end_date: date,
               replace: bool,
               file_path: str
               ) -> None:

    logger = get_run_logger()
    file_exists = if_file_exists(file_path)
    if file_exists and not replace:
        logger.info(f"file already exists, nothing to do: {file_path}")
    else:
        logger.info(file_path)
        earthquake_data = fetch_earthquake_data(start_date, end_date)
        if earthquake_data:
            ndjson_data = convert_to_ndjson(earthquake_data)
            upload_to_gcs(ndjson_data, file_path)
