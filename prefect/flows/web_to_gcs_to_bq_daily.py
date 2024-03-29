from datetime import datetime
from prefect import flow

from flows.utils.web_to_gcs_to_bq import web_to_gcs_to_bq
from flows.utils.gcs_to_bq import get_last_datetime


@flow(name="world-earthquake-pipeline: web_to_gcs_to_bq_daily")
def web_to_gcs_to_bq_daily() -> None:

    # Get the datetime of the last earthquake in the BigQuery table
    last_datetime = get_last_datetime()
    if last_datetime:
        # Fetch and save earthquake data from the last earthquake till now
        start_date = last_datetime.date()
        end_date = datetime.now().date()
        web_to_gcs_to_bq(start_date, end_date, replace=False, split_time=True)

    else:
        raise Exception("There is no data in the BigQuery table. Please run web_to_gcs_to_bq_all.")


if __name__ == "__main__":
    web_to_gcs_to_bq_daily()
