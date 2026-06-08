import pytest
from unittest.mock import MagicMock, patch

from etl import transform


def make_ti(input_path):
    ti = MagicMock()
    ti.xcom_pull.return_value = input_path
    return ti


class TestTransform:
    @patch("etl.pd")
    def test_reads_from_xcom_path(self, mock_pd):
        transform(ti=make_ti("/data/extracted_run.csv"), run_id="run")
        mock_pd.read_csv.assert_called_once_with("/data/extracted_run.csv")

    @patch("etl.pd")
    def test_xcom_pull_targets_extract_task(self, mock_pd):
        ti = make_ti("/data/extracted_run.csv")
        transform(ti=ti, run_id="run")
        ti.xcom_pull.assert_called_once_with(task_ids="extract")

    @patch("etl.pd")
    def test_data_written_to_csv(self, mock_pd):
        mock_df = MagicMock()
        mock_pd.read_csv.return_value = mock_df
        transform(ti=make_ti("/data/extracted_run.csv"), run_id="run")
        mock_df.to_csv.assert_called_once()

    @patch("etl.pd")
    def test_return_value_matches_to_csv_path(self, mock_pd):
        mock_df = MagicMock()
        mock_pd.read_csv.return_value = mock_df
        result = transform(ti=make_ti("/data/extracted_run.csv"), run_id="run")
        written_path = mock_df.to_csv.call_args[0][0]
        assert result == written_path

    @patch("etl.pd")
    def test_run_id_colons_replaced(self, mock_pd):
        result = transform(
            ti=make_ti("/data/input.csv"),
            run_id="scheduled__2026-06-01T00:00:00+00:00",
        )
        assert ":" not in result
        assert "+" not in result

    @patch("etl.pd")
    def test_output_path_contains_sanitized_run_id(self, mock_pd):
        result = transform(
            ti=make_ti("/data/input.csv"),
            run_id="scheduled__2026-06-01T00:00:00+00:00",
        )
        assert "scheduled__2026-06-01T00-00-00-00-00" in result
