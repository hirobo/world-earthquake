from datetime import datetime
from prefect import flow

from flows.utils.web_to_gcs_to_bq import web_to_gcs_to_bq

# Define constants for the special years
YEAR_START = 1568
YEAR_SPLIT = 1950


@flow(name="world-earthquake-pipeline: web_to_gcs_to_bq_all")
def web_to_gcs_to_bq_all(replace=False) -> None:
    """
    Fetch earthquake data from 1568-01-01 till yesterday
    and save ndjson files to GCS and then update the BigQuery table.
    The time period for fetching data is like this because of the limitation (up to 20000) of the request:
    - from 1568-01-01 till 1949-12-31: one time
    - from 1950-01-01: monthly (or weekly)
    """

    # Fetch data from YEAR_START till YEAR_SPLIT in one go
    start = datetime(YEAR_START, 1, 1).date()
    end = datetime(YEAR_SPLIT, 1, 1).date()
    web_to_gcs_to_bq(start, end, replace, split_time=False)

    # Fetch data from YEAR_SPLIT till now in monthly (or weekly) splits
    start = datetime(YEAR_SPLIT, 1, 1).date()
    end = datetime.now().date()
    web_to_gcs_to_bq(start, end, replace, split_time=True)


if __name__ == "__main__":

    web_to_gcs_to_bq_all(False)
