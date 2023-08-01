from datetime import datetime
from prefect import flow

from flows.utils.web_to_gcs_to_bq import web_to_gcs_to_bq


@flow(name="world-earthquake-pipeline: web_to_gcs_to_bq_with_params")
def web_to_gcs_to_bq_with_params(start_date: str, end_date: str, replace: bool = False) -> None:

    try:
        # Parse the start and end dates
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        # Fetch and save earthquake data from start_date till end_date
        web_to_gcs_to_bq(start_date, end_date, replace, split_time=True)
    except ValueError as e:
        raise ValueError(f"Invalid date format. Please provide dates in 'YYYY-MM-DD' format. {e}")
