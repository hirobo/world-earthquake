import os, stat
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpSecret
from prefect_gcp.cloud_storage import GcsBucket


dataset = "garrickhague/world-earthquake-data-from-1906-2022"
local_dir = f"data/{dataset}"
csv_file_path = f"{local_dir}/Global_Earthquake_Data.csv"
parquet_file_path = f"{local_dir}/Global_Earthquake_Data.parquet"


@task(retries=3, log_prints=True)
def write_gcs() -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("world-earthquake")
    gcs_block.upload_from_path(
        from_path=parquet_file_path, to_path=parquet_file_path, timeout=120)
    return


@task(retries=3, log_prints=True)
def clean_up() -> None:
    """delete local files"""

    try:
        os.remove(csv_file_path)
        os.remove(parquet_file_path)
    except FileNotFoundError:
        pass

    return


@task(retries=3, log_prints=True)
def convert_to_parquet() -> None:
    df = pd.read_csv(csv_file_path, parse_dates=["time", "updated"])
    df.to_parquet(parquet_file_path, engine="pyarrow", compression="gzip")
    return


@task(retries=3, log_prints=True)
def download() -> None:
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    if os.path.exists(csv_file_path):
        pass
    else:
        api.dataset_download_files(dataset, path=local_dir, unzip=True)

    return


@task(retries=3, log_prints=True)
def prepare_kaggle_credentials() -> None:
    """
    read kaggle credentials data from GCP Secret Manager and create kaggle.json file if not exists
    """
    kaggle_dir = Path.home()/".kaggle"
    kaggle_json_path = kaggle_dir/"kaggle.json"

    if kaggle_json_path.exists():
        print("kaggle.json already exists. Nothing to do")
        return

    gcpsecret_block = GcpSecret.load("kaggle-json")
    kaggle_json = gcpsecret_block.read_secret()

    kaggle_dir.mkdir(parents=True, exist_ok=True)
    with open(kaggle_json_path, 'w') as f:
        f.write(kaggle_json.decode("utf-8"))

    os.chmod(kaggle_json_path, stat.S_IREAD);
    return


@flow(name="world-earthquake: load data from Kaggle and upload to GCS")
def web_to_gcs() -> None:
    """
    kaggle dataset url: "https://www.kaggle.com/datasets/garrickhague/world-earthquake-data-from-1906-2022"
    """
    prepare_kaggle_credentials()
    download()
    convert_to_parquet()
    write_gcs()
    clean_up()


if __name__ == "__main__":

    web_to_gcs()
