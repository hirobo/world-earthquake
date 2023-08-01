from unittest.mock import patch
from datetime import datetime, date
from flows.web_to_gcs_to_bq_daily import web_to_gcs_to_bq_daily


@patch("flows.web_to_gcs_to_bq_daily.get_last_datetime", return_value=datetime(2023, 6, 1, 10, 30))
@patch("flows.web_to_gcs_to_bq_daily.web_to_gcs_to_bq")
def test_web_to_gcs_to_bq_daily(mock_web_to_gcs_to_bq, mock_get_last_datetime):
    web_to_gcs_to_bq_daily()
    mock_web_to_gcs_to_bq.assert_called_once_with(
        date(2023, 6, 1),
        datetime.now().date(),
        replace=False,
        split_time=True
    )
