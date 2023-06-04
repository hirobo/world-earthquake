import unittest
from unittest.mock import patch
from datetime import datetime, timedelta
from flows.web_to_gcs_to_bq_daily import web_to_gcs_to_bq_daily


class TestWebToGcsToBqDaily(unittest.TestCase):
    @patch("flows.web_to_gcs_to_bq_daily.web_to_gcs_to_bq")
    def test_web_to_gcs_to_bq_daily(self, mock_web_to_gcs_to_bq):

        web_to_gcs_to_bq_daily()

        today = datetime.now()
        yesterday = today - timedelta(days=1)

        mock_web_to_gcs_to_bq.assert_called_once_with(
            yesterday.date(),
            today.date(),
            replace=False,
            split_time=True
        )


if __name__ == "__main__":
    unittest.main()
