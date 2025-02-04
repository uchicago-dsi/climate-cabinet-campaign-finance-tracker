from pathlib import Path

import pytest
import utils.constants
import utils.finance
import utils.finance.config
from utils.finance.config import ConfigHandler
from yaml import safe_dump


@pytest.fixture
def sample_config(tmp_path):
    config_data = {
        "contributions": {
            "columns": [
                {"raw_name": "FILERID", "type": "str", "standard_name": "recipient_id"},
                {"raw_name": "CYCLE", "type": "str"},
                {
                    "raw_name": "CONTDATE1",
                    "type": "Int32",
                    "date_format": "%Y%m%d",
                    "standard_name": "date-1",
                },
            ],
            "enum_mapper": {"transactor_type": {"1": "Candidate", "2": "Committee"}},
            "read_csv_params": {"sep": ",", "encoding": "latin-1"},
            "duplicate_columns": {"CYCLE": "REPORT_CYCLE"},
            "new_empty_columns": ["additional_column"],
            "state_code": "NY",
            "table_type": "Transaction",
            "path_pattern": "(?i)^(19[0-9][0-9]|20[0-1][0-9]|2020|2021)/contrib.*\\.txt$",
        }
    }
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        safe_dump(config_data, f)
    return config_path


@pytest.fixture
def mock_raw_data_directory(tmp_path, monkeypatch):
    """Override RAW_DATA_DIRECTORY with a temporary path."""
    monkeypatch.setattr(utils.finance.config, "RAW_DATA_DIRECTORY", tmp_path)
    return tmp_path


def test_config_handler_init(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler._columns, "Columns should be loaded from config"
    assert handler._enum_mapper, "Enum mapper should be loaded from config"
    assert handler._read_csv_params, "Read CSV parameters should be loaded from config"
    assert handler._table_type, "Table type must be loaded from config"
    assert handler._raw_data_path_pattern, "Path pattern must be loaded from config"


def test_dtype_dict(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.dtype_dict == {
        "FILERID": "str",
        "CYCLE": "str",
        "CONTDATE1": "Int32",
    }


def test_column_mapper(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.column_mapper == {"FILERID": "recipient_id", "CONTDATE1": "date-1"}


def test_relevant_columns(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert set(handler.relevant_columns) == {"recipient_id", "date-1"}


def test_enum_mapper(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.enum_mapper == {
        "transactor_type": {"1": "Candidate", "2": "Committee"}
    }


def test_read_csv_params(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.read_csv_params == {"sep": ",", "encoding": "latin-1"}


def test_column_to_date_format(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.column_to_date_format == {"CONTDATE1": "%Y%m%d"}


def test_duplicate_columns(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.duplicate_columns == {"CYCLE": "REPORT_CYCLE"}


def test_new_empty_columns(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.new_empty_columns == ["additional_column"]


def test_invalid_form_code(sample_config):
    with pytest.raises(KeyError):
        ConfigHandler("invalid_form", config_file_path=sample_config)


def test_missing_file():
    with pytest.raises(FileNotFoundError):
        ConfigHandler("contributions", config_file_path=Path("non_existent.yaml"))


def test_raw_data_path_pattern(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler.raw_data_path_pattern.fullmatch("2021/contrib_test.txt")
    assert not handler.raw_data_path_pattern.fullmatch("2018/noncontrib.csv")


def test_raw_data_file_paths(sample_config, mock_raw_data_directory):
    state_data_directory = mock_raw_data_directory / "NY"
    state_data_directory.mkdir()
    valid_file = state_data_directory / "2021/contrib_test.txt"
    invalid_file = state_data_directory / "2018/noncontrib.csv"
    valid_file.parent.mkdir(parents=True, exist_ok=True)
    invalid_file.parent.mkdir(parents=True, exist_ok=True)
    valid_file.touch()
    invalid_file.touch()

    handler = ConfigHandler("contributions", config_file_path=sample_config)

    matching_files = handler.raw_data_file_paths
    assert valid_file in matching_files
    assert invalid_file not in matching_files


@pytest.fixture
def inheritance_config(tmp_path):
    config_data = {
        "base_form": {
            "columns": [
                {
                    "raw_name": "PARENT_COLUMN",
                    "type": "str",
                    "standard_name": "base_col",
                }
            ],
            "enum_mapper": {"category": {"A": "Alpha", "B": "Beta"}},
            "read_csv_params": {"sep": "|"},
            "state_code": "CA",
            "table_type": "BaseTable",
            "path_pattern": "(?i)^base/.*\\.txt$",
        },
        "derived_form": {
            "inherits": "base_form",
            "columns": [
                {
                    "raw_name": "CHILD_COLUMN",
                    "type": "int",
                    "standard_name": "derived_col",
                }
            ],
        },
    }
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        safe_dump(config_data, f)
    return config_path


def test_inherits(inheritance_config):
    handler = ConfigHandler("derived_form", config_file_path=inheritance_config)
    assert "PARENT_COLUMN" not in [
        col["raw_name"] for col in handler._columns
    ], "PARENT_COLUMN should not be present as the provided config has a 'columns' key"
    assert "CHILD_COLUMN" in [
        col["raw_name"] for col in handler._columns
    ], "CHILD_COLUMN should be present as it takes precedence over inherited column"
    assert (
        handler.table_type == "BaseTable"
    ), "Table type should be inherited from base_form"
    assert handler.read_csv_params["sep"] == "|", "Read CSV params should be inherited"
    assert handler.enum_mapper == {
        "category": {"A": "Alpha", "B": "Beta"}
    }, "Enum mapper should be inherited"
    assert handler.raw_data_path_pattern.fullmatch(
        "base/data.txt"
    ), "Inherited path pattern should match"
