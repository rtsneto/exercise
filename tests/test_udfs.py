"""
Unit tests for User Defined Functions (UDFs)
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from exercise.plugins.udfs import generate_nse_id


def test_generate_nse_id_with_mocked_api():
    """Test the generate_nse_id UDF with mocked API responses"""
    # Prepare test data
    input_series = pd.Series(["test1", "test2", None])

    # Mock responses for requests.get
    def mock_requests_get(url, timeout):
        mock_resp = MagicMock()
        if "test1" in url:
            mock_resp.json.return_value = {"Digest": "aaa"}
        elif "test2" in url:
            mock_resp.json.return_value = {"Digest": "bbb"}

        mock_resp.raise_for_status.return_value = None
        return mock_resp

    with patch("exercise.plugins.udfs.requests.get", side_effect=mock_requests_get):
        result_series = generate_nse_id.func(input_series)

    assert list(result_series) == ["aaa", "bbb", None]
