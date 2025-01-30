from pathlib import Path

import pytest
from yaml import safe_dump

from utils.finance.config import ConfigHandler


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
        }
    }
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        safe_dump(config_data, f)
    return config_path


def test_config_handler_init(sample_config):
    handler = ConfigHandler("contributions", config_file_path=sample_config)
    assert handler._columns, "Columns should be loaded from config"
    assert handler._enum_mapper, "Enum mapper should be loaded from config"
    assert handler._read_csv_params, "Read CSV parameters should be loaded from config"


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
