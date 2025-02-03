from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from utils.finance.config import ConfigHandler
from utils.finance.source import (
    DataReader,
    DataSourceStandardizationPipeline,
    DataStandardizer,
    SchemaTransformer,
)


# ---- FIXTURES ----
@pytest.fixture
def mock_config_handler():
    """Creates a mock configuration handler with dummy mappings."""
    mock = MagicMock(spec=ConfigHandler)
    mock.dtype_dict = {
        "DONOR": "str",
        "RECIPID": "Int32",
        "TRANSDATE": "str",
        "DONORCOMPNAME": "str",
        "amount": "Float32",
        "EOFFICE": "str",
        "USELESS": "str",
    }
    mock.read_csv_params = {"sep": ","}
    mock.raw_data_file_paths = [Path("contributions.csv")]
    mock.column_mapper = {
        "DONOR": "donor--name",
        "RECIPID": "recipient_id",
        "TRANSDATE": "date",
        "DONORCOMPNAME": "donor--employer--organization--full_name",
        "EOFFICE": "donor--election_result--election--office_sought",
    }
    mock.relevant_columns = [
        "donor--name",
        "recipient_id",
        "date",
        "amount",
        "donor--employer--organization--full_name",
        "donor--election_result--election--office_sought",
    ]
    mock.duplicate_columns = {"amount": ["duplicate_amount"]}
    mock.enum_mapper = {
        "donor--election_result--election--office_sought": {
            "SEN": "State Senator",
            "REP": "State Representative",
        }
    }
    mock.column_to_date_format = {"date": "%m/%d/%Y"}
    return mock


@pytest.fixture
def raw_data():
    """Sample raw data to use in tests"""
    return pd.DataFrame(
        {
            "DONOR": ["person 1", "person 2", "person 3"],
            "RECIPID": [1234, 4567, 910],
            "TRANSDATE": ["1/20/2025", "12/31/2024", "3/4/2024"],
            "DONORCOMPNAME": ["big company", "medium company", "small comp"],
            "amount": [120.34, 1200, 3400.23],
            "EOFFICE": ["SEN", "SEN", "REP"],
            "USELESS": ["non", "sense", "data"],
        }
    )


@pytest.fixture
def renamed_columns_data():
    """Sample raw data with standardized column names"""
    return pd.DataFrame(
        {
            "donor--name": ["person 1", "person 2", "person 3"],
            "recipient_id": [1234, 4567, 910],
            "date": ["1/20/2025", "12/31/2024", "3/4/2024"],
            "donor--employer--organization--full_name": [
                "big company",
                "medium company",
                "small comp",
            ],
            "amount": [120.34, 1200, 3400.23],
            "donor--election_result--election--office_sought": ["SEN", "SEN", "REP"],
            "USELESS": ["non", "sense", "data"],
        }
    )


@pytest.fixture
def standardized_schema():
    """Sample raw data with standardized column names"""
    return pd.DataFrame(
        {
            "donor--name": ["person 1", "person 2", "person 3"],
            "recipient_id": [1234, 4567, 910],
            "date": ["1/20/2025", "12/31/2024", "3/4/2024"],
            "donor--employer--organization--full_name": [
                "big company",
                "medium company",
                "small comp",
            ],
            "amount": [120.34, 1200, 3400.23],
            "duplicate_amount": [120.34, 1200, 3400.23],
            "donor--election_result--election--office_sought": ["SEN", "SEN", "REP"],
        }
    )


@pytest.fixture
def standardized_data():
    """Sample raw data with standardized column names"""
    return pd.DataFrame(
        {
            "donor--name": ["person 1", "person 2", "person 3"],
            "recipient_id": [1234, 4567, 910],
            "date": ["2025-01-20", "2024-12-31", "2024-03-04"],
            "donor--employer--organization--full_name": [
                "big company",
                "medium company",
                "small comp",
            ],
            "amount": [120.34, 1200, 3400.23],
            "duplicate_amount": [120.34, 1200, 3400.23],
            "donor--election_result--election--office_sought": [
                "State Senator",
                "State Senator",
                "State Representative",
            ],
        }
    )


# ---- UNIT TESTS ----
class TestDataReader:
    """Tests for DataReader class."""

    @patch("pandas.read_csv")
    def test_read_tabular_data(self, mock_read_csv, mock_config_handler, raw_data):
        """Ensure read_tabular_data reads CSV and applies dtype mapping."""
        mock_read_csv.return_value = pd.DataFrame(raw_data)
        reader = DataReader(mock_config_handler)

        raw_data = reader.read_tabular_data()

        mock_read_csv.assert_called_once_with(
            "contributions.csv", dtype=mock_config_handler.dtype_dict, sep=","
        )
        assert not raw_data.empty
        assert list(raw_data.columns) == [
            "DONOR",
            "RECIPID",
            "TRANSDATE",
            "DONORCOMPNAME",
            "amount",
            "EOFFICE",
            "USELESS",
        ]

    def test_missing_file_error(self, mock_config_handler):
        """Ensure an error is raised if the file is missing."""
        reader = DataReader(mock_config_handler)
        with pytest.raises(FileNotFoundError):
            reader.read_tabular_data("non_existent_file.csv")


class TestSchemaTransformer:
    """Tests for SchemaTransformer class."""

    def test_rename_columns(self, mock_config_handler, raw_data):
        """Test that column names are correctly renamed."""
        transformer = SchemaTransformer(mock_config_handler)
        renamed_column_data = transformer._rename_columns(raw_data)

        assert "donor--name" in renamed_column_data.columns
        assert "DONOR" not in renamed_column_data.columns

    def test_drop_unused_columns(self, mock_config_handler, renamed_columns_data):
        """Ensure only relevant columns remain."""
        transformer = SchemaTransformer(mock_config_handler)
        relevant_columns_data = transformer._drop_unused_columns(renamed_columns_data)

        assert set(relevant_columns_data.columns) == {
            "donor--name",
            "recipient_id",
            "date",
            "amount",
            "donor--employer--organization--full_name",
            "donor--election_result--election--office_sought",
        }

    def test_add_duplicate_columns(self, mock_config_handler, renamed_columns_data):
        """Ensure additional empty columns are added."""
        transformer = SchemaTransformer(mock_config_handler)
        duplicate_columns_data = transformer._add_duplicate_columns(
            renamed_columns_data
        )

        assert "duplicate_amount" in duplicate_columns_data.columns
        assert (
            duplicate_columns_data["duplicate_amount"]
            .eq(duplicate_columns_data["amount"])
            .all()
        )


class TestDataStandardizer:
    """Tests for DataStandardizer class."""

    def test_standardize_enums(self, mock_config_handler, renamed_columns_data):
        """Ensure enum values are mapped correctly."""
        standardizer = DataStandardizer(mock_config_handler)
        standardized_enum_data = standardizer._standardize_enums(renamed_columns_data)

        assert standardized_enum_data[
            "donor--election_result--election--office_sought"
        ].tolist() == [
            "State Senator",
            "State Senator",
            "State Representative",
        ]  # Enum mapping check

    def test_standardize_column_to_date_format(
        self, mock_config_handler, renamed_columns_data
    ):
        """Ensure date formats are correctly converted."""
        standardizer = DataStandardizer(mock_config_handler)
        standardized_date_data = standardizer._standardize_column_to_date_format(
            renamed_columns_data
        )
        assert [str(date) for date in standardized_date_data["date"].tolist()] == [
            "2025-01-20",
            "2024-12-31",
            "2024-03-04",
        ]


# ---- INTEGRATION TEST ----
@pytest.fixture
def mock_pipeline(
    mock_config_handler, raw_data, standardized_schema, standardized_data
):
    """Creates a fully mocked DataSourceStandardizationPipeline instance to prevent real initialization."""

    # Mock DataReader
    mock_data_reader = MagicMock()
    mock_data_reader.default_raw_data_paths = mock_config_handler.raw_data_file_paths
    mock_data_reader.read_tabular_data.return_value = raw_data

    # Mock SchemaTransformer
    mock_schema_transformer = MagicMock()
    mock_schema_transformer.standardize_schema.return_value = standardized_schema

    # Mock DataStandardizer
    mock_data_standardizer = MagicMock()
    mock_data_standardizer.standardize_data.return_value = standardized_data

    # Patch the pipeline's __init__ method to prevent real initialization
    with patch.object(
        DataSourceStandardizationPipeline,
        "__init__",
        lambda self, *args, **kwargs: None,
    ):
        pipeline = DataSourceStandardizationPipeline()
        pipeline.state_code = "NY"
        pipeline.form_code = "FORM1"
        pipeline.data_reader = mock_data_reader
        pipeline.schema_transformer = mock_schema_transformer
        pipeline.data_standardizer = mock_data_standardizer

    return pipeline


def test_standardize_data_source(mock_pipeline, standardized_data):
    """Test standardize_data_source to ensure it processes data correctly without real I/O."""

    # Run the pipeline's standardization process
    result_df = mock_pipeline.standardize_data_source()

    # Validate the output
    pd.testing.assert_frame_equal(result_df, standardized_data)

    # Ensure each step was called once
    mock_pipeline.data_reader.read_tabular_data.assert_called_once_with(
        Path("contributions.csv")
    )
    mock_pipeline.schema_transformer.standardize_schema.assert_called_once()
    mock_pipeline.data_standardizer.standardize_data.assert_called_once()
