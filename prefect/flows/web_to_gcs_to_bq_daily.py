from datetime import datetime, timedelta
from prefect import flow

from flows.utils.web_to_gcs_to_bq import web_to_gcs_to_bq


@flow(name="world-earthquake-pipeline: web_to_gcs_to_bq_daily")
def web_to_gcs_to_bq_daily() -> None:
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.date()
    end_date = today
    web_to_gcs_to_bq(start_date,
                     end_date,
                     replace=False,
                     split_time=True)

if __name__ == "__main__":
    web_to_gcs_to_bq_daily()
