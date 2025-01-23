from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from novonordisk import (
    download_and_create_delta_table,
    fetch_data_availability,
    get_last_modified_date_from_delta,
    get_last_modified_date_from_headers,
    process_last_five_days,
    process_total_cases,
    save_to_zip,
)

# Sample data
sample_response = MagicMock()
sample_response.status_code = 200
sample_response.headers = {"Last-Modified": "Wed, 22 Jan 2025 15:00:00 GMT"}


# Test for `fetch_data_availability`
@patch("requests.head")
def test_fetch_data_availability(mock_head):
    # Test successful fetch
    mock_head.return_value = sample_response
    response = fetch_data_availability()
    assert response is not None
    assert response.status_code == 200

    # Test fetch failure
    mock_head.side_effect = requests.exceptions.RequestException("Request failed")
    response = fetch_data_availability()
    assert response is None


# Test for `get_last_modified_date_from_headers`
def test_get_last_modified_date_from_headers():
    # Test getting last modified date from headers
    expected_date = datetime(2025, 1, 22, 15, 0, 0)
    actual_date = get_last_modified_date_from_headers(sample_response)
    assert actual_date == expected_date


# Test for `get_last_modified_date_from_delta`
@patch("mymodule.DeltaTable.forName")
def test_get_last_modified_date_from_delta(mock_delta):
    # Mock the DeltaTable history method
    mock_history = MagicMock()
    mock_history.orderBy.return_value.first.return_value = {"timestamp": "2025-01-22 14:00:00"}
    mock_delta.return_value.history.return_value = mock_history

    expected_date = datetime(2025, 1, 22, 14, 0, 0)
    actual_date = get_last_modified_date_from_delta()
    assert actual_date == expected_date


# Test for `download_and_create_delta_table`
@patch("mymodule.dbutils.fs.cp")
@patch("mymodule.spark.read.csv")
@patch("mymodule.sparknl.write.mode")
def test_download_and_create_delta_table(mock_write, mock_csv, mock_cp):
    # Mocking the behavior of reading CSV and writing to Delta table
    mock_cp.return_value = None
    mock_csv.return_value = MagicMock()
    df_mock = MagicMock()
    mock_write.return_value = df_mock

    result_df = download_and_create_delta_table()
    mock_cp.assert_called_once_with(sample_response.url, "path_to_csv")
    mock_csv.assert_called_once_with("path_to_csv", header=True, inferSchema=True)
    assert result_df is df_mock


# Test for `process_last_five_days`
@patch("mymodule.sparknl.read.csv")
@patch("mymodule.sparknl.write.mode")
def test_process_last_five_days(mock_write, mock_csv):
    # Mock dataframe and its methods
    df_mock = MagicMock()
    mock_csv.return_value = df_mock
    df_mock.agg.return_value.collect.return_value = [MagicMock()[0, 0]]
    df_mock.filter.return_value = df_mock
    df_mock.coalesce.return_value.write.option.return_value = mock_write

    process_last_five_days(df_mock)
    df_mock.coalesce.return_value.write.option.assert_called_once_with("header", "true")


# Test for `process_total_cases`
@patch("mymodule.sparknl.read.csv")
@patch("mymodule.sparknl.write.mode")
def test_process_total_cases(mock_write, mock_csv):
    # Mock dataframe and its methods
    df_mock = MagicMock()
    mock_csv.return_value = df_mock
    df_mock.groupBy.return_value.agg.return_value = df_mock
    df_mock.coalesce.return_value.write.option.return_value = mock_write

    process_total_cases(df_mock)
    df_mock.coalesce.return_value.write.option.assert_called_once_with("header", "true")


# Test for `save_to_zip`
@patch("zipfile.ZipFile")
def test_save_to_zip(mock_zip):
    # Mock zip file writing behavior
    mock_zip.return_value.__enter__.return_value = MagicMock()
    save_to_zip()
    mock_zip.return_value.__enter__.return_value.write.assert_called()


# Running the tests
if __name__ == "__main__":
    pytest.main()
