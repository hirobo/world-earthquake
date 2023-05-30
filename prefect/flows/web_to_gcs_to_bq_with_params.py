from datetime import datetime
from prefect import flow

from flows.utils.web_to_gcs_to_bq import web_to_gcs_to_bq


@flow(name="world-earthquake-pipeline: web_to_gcs_to_bq_with_params")
def web_to_gcs_to_bq_with_params(start_date: str, end_date: str, replace: bool=False) -> None:

    web_to_gcs_to_bq(datetime.strptime(start_date, '%Y-%m-%d').date(),
                     datetime.strptime(end_date, '%Y-%m-%d').date(),
                     replace,
                     split_time=True)
