import requests
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from prefect import task, flow, get_run_logger

from flows.utils.gcs_to_bq import gcs_to_bq
from flows.utils.web_to_gcs import web_to_gcs, get_file_path


USGS_LIMIT = 20000


def process_data(start_date: date, end_date: date, replace: bool, split_time: bool) -> None:
    file_path = get_file_path(start_date, end_date, split_time)
    web_to_gcs(start_date, end_date, replace, file_path)
    gcs_to_bq(file_path)


@task(retries=1, log_prints=True)
def check_count(start_date, end_date) -> int:
    logger = get_run_logger()

    count_url = "https://earthquake.usgs.gov/fdsnws/event/1/count"
    params = {"starttime": start_date,
              "endtime": end_date}

    response = requests.get(count_url, params=params)
    count = response.json()

    logger.info(f"check count: {count}")

    return count


@flow(name="world-earthquake-pipeline: web_to_gcs_to_bq")
def web_to_gcs_to_bq(start_date: date,
                     end_date: date,
                     replace: bool=False,
                     split_time: bool=True,
                     ) -> None:

    logger = get_run_logger()
    logger.info(
        f"web_to_gcs_to_bq: start={start_date}, end={end_date}, replace={replace}")

    if not split_time:
        # don't split request
        process_data(start_date, end_date, replace, False)

    else:
        # basically we will split request monthly
        current_date = start_date

        while (current_date + timedelta(days=1)) <= end_date:
            year = current_date.year
            month = current_date.month

            if current_date == start_date:
                it_start_date = start_date
            else:
                it_start_date = current_date.replace(day=1)

            it_end_date = min((current_date + relativedelta(months=1)).replace(day=1), end_date)

            # check count
            count = check_count(it_start_date, it_end_date)

            if count <= USGS_LIMIT:
                process_data(it_start_date, it_end_date, replace, True)
            else:
                # split weekly
                week_start_date = it_start_date                
                while (week_start_date + timedelta(days=1)) <= it_end_date:
                    week_end_date = min(week_start_date + timedelta(days=7), it_end_date)
                    
                    process_data(week_start_date, week_end_date, replace, True)
                    week_start_date += timedelta(days=7)

            # next month
            current_date = (current_date + relativedelta(months=1)).replace(day=1)
