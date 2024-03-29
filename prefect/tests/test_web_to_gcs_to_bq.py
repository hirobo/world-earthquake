from unittest.mock import patch, call, MagicMock
import pytest
import requests
import logging
from datetime import date
from flows.utils.web_to_gcs_to_bq import web_to_gcs_to_bq, check_count


@patch("flows.utils.web_to_gcs_to_bq.check_count", return_value=10000)
@patch("flows.utils.web_to_gcs_to_bq.process_data")
def test_web_to_gcs_to_bq_over_month_with_split(mock_process_data, mock_check_count):
    start = date(2023, 1, 15)
    end = date(2023, 2, 15)
    web_to_gcs_to_bq(start, end, False, True)

    assert mock_check_count.call_args_list == [
        call(date(2023, 1, 15), date(2023, 2, 1)), call(date(2023, 2, 1), date(2023, 2, 15))]

    assert mock_process_data.call_args_list == [call(date(2023, 1, 15), date(
        2023, 2, 1), False, True), call(date(2023, 2, 1), date(2023, 2, 15), False, True)]


@patch("flows.utils.web_to_gcs_to_bq.check_count", return_value=10000)
@patch("flows.utils.web_to_gcs_to_bq.process_data")
def test_web_to_gcs_to_bq_split_time_false(mock_process_data, mock_check_count):
    start = date(2023, 1, 15)
    end = date(2023, 2, 15)
    web_to_gcs_to_bq(start, end, False, False)

    assert mock_process_data.call_args_list == [call(date(2023, 1, 15), date(2023, 2, 15), False, False)]


@patch("flows.utils.web_to_gcs_to_bq.check_count", return_value=20001)
@patch("flows.utils.web_to_gcs_to_bq.process_data")
def test_web_to_gcs_to_bq_over_limit(mock_process_data, mock_check_count):
    start = date(2023, 1, 1)
    end = date(2023, 2, 1)

    web_to_gcs_to_bq(start, end, False, True)

    calls = [call(date(2023, 1, 1), date(2023, 1, 8), False, True),
             call(date(2023, 1, 8), date(2023, 1, 15), False, True),
             call(date(2023, 1, 15), date(2023, 1, 22), False, True),
             call(date(2023, 1, 22), date(2023, 1, 29), False, True),
             call(date(2023, 1, 29), date(2023, 2, 1), False, True)]

    assert mock_process_data.call_args_list == calls


@patch("flows.utils.web_to_gcs_to_bq.check_count", return_value=20000)
@patch("flows.utils.web_to_gcs_to_bq.process_data")
def test_web_to_gcs_to_bq_on_limit(mock_process_data, mock_check_count):
    start = date(2023, 1, 1)
    end = date(2023, 2, 1)

    web_to_gcs_to_bq(start, end, False, True)

    assert mock_process_data.call_args_list == [call(date(2023, 1, 1), date(2023, 2, 1), False, True)]


@patch('requests.get')
@patch('flows.utils.web_to_gcs_to_bq.get_run_logger')
def test_check_count_url(mock_logger, mock_get):
    mock_logger.return_value = logging.getLogger()
    mock_get.return_value.json.return_value = 12345

    start_date = date(2023, 1, 1)
    end_date = date(2023, 2, 1)
    count = check_count.fn(start_date, end_date)
    assert count == 12345

    mock_get.assert_called_with(
        'https://earthquake.usgs.gov/fdsnws/event/1/count',
        params={'starttime': start_date, 'endtime': end_date}
    )


@patch("requests.get")
@patch('flows.utils.web_to_gcs_to_bq.get_run_logger')
def test_check_count_success(mock_logger, mock_get):
    mock_logger.return_value = logging.getLogger()
    mock_get.return_value.json.return_value = 12345
    start_date = date(2023, 1, 1)
    end_date = date(2023, 2, 1)

    count = check_count.fn(start_date, end_date)
    assert count == 12345


@patch("requests.get")
@patch('flows.utils.web_to_gcs_to_bq.get_run_logger')
def test_check_count_error_status(mock_logger, mock_get):
    mock_logger.return_value = logging.getLogger()
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
    mock_get.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 2, 1)
        check_count.fn(start_date, end_date)


@patch("requests.get")
@patch('flows.utils.web_to_gcs_to_bq.get_run_logger')
def test_check_count_invalid_json(mock_logger, mock_get):
    mock_logger.return_value = logging.getLogger()
    mock_response = MagicMock()
    mock_response.json.side_effect = ValueError()
    mock_get.return_value = mock_response

    with pytest.raises(ValueError):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 2, 1)
        check_count.fn(start_date, end_date)
