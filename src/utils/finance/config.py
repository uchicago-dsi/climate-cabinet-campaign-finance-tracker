"""Configuration hanlder class for state campaign finance data"""

import re
from pathlib import Path

from yaml import safe_load

from utils.constants import BASE_FILEPATH, RAW_DATA_DIRECTORY


class ConfigHandler:
    """Handles and validates metadata"""

    @property
    def default_config_folder(self) -> Path:
        """Default path to state campaign finance configuration files"""
        return BASE_FILEPATH / "src" / "utils" / "config" / "finance" / "states"

    @property
    def state_code(self) -> str:
        """Two letter state abbreviation"""
        return self._state_code

    @property
    def table_type(self) -> str:
        """Type of table: 'Transaction', 'Transactor', etc."""
        return self._table_type

    @property
    def raw_data_path_pattern(self) -> str:
        """Regex matching names of raw files in data/raw folder"""
        return re.compile(self._raw_data_path_pattern)

    @property
    def raw_data_file_paths(self) -> list[Path]:
        """All files matching raw data pattern at compile time"""
        state_data_directory = RAW_DATA_DIRECTORY / self.state_code
        print(state_data_directory)
        matching_files = [
            path
            for path in state_data_directory.rglob("*")
            if path.is_file()
            and self.raw_data_path_pattern.fullmatch(
                str(path.relative_to(state_data_directory)).replace("\\", "/")
            )
        ]
        return matching_files

    @property
    def dtype_dict(self) -> dict[str, str]:
        """Maps raw column names to pandas dtypes"""
        return {col["raw_name"]: col["type"] for col in self._columns}

    @property
    def read_csv_params(self) -> dict:
        """Keyword arguments used in pandas read_csv for reading in tabular file"""
        return self._read_csv_params

    @property
    def column_mapper(self) -> dict[str, str]:
        """Maps raw column names to standardized column names"""
        return {
            col["raw_name"]: col["standard_name"]
            for col in self._columns
            if "standard_name" in col
        }

    @property
    def relevant_columns(self) -> list[str]:
        """List of standardized column names to keep"""
        return list(self.column_mapper.values())

    @property
    def duplicate_columns(self) -> dict[str, str]:
        """Maps existing columns to additional columns that should have equal values"""
        return self._duplicate_columns

    @property
    def new_empty_columns(self) -> list[str]:
        """Names of new columns that should be added empty"""
        return self._new_empty_columns

    @property
    def enum_mapper(self) -> dict[dict[str, str]]:
        """Maps enum column names to mappings of raw values to standard values"""
        return self._enum_mapper

    @property
    def column_to_date_format(self) -> dict[str, str]:
        """Maps date columns to default raw date format"""
        return {
            col["raw_name"]: col["date_format"]
            for col in self._columns
            if "date_format" in col
        }

    def __init__(
        self,
        form_code: str,
        config_file_path: Path | None = None,
        state_code: str | None = None,
    ) -> None:
        """Manage configuration from config file

        Notes: One and only one of config_file_path or state_code must be provided.

        Args:
            form_code: Name of form, must be key in configuration file
            config_file_path: Path to configuration file
            state_code: two letter abbreviation of state with config
                file in default config folder
        """
        if config_file_path is None:
            if state_code is not None:
                config_file_path = (
                    self.default_config_folder / f"{state_code.lower()}.yaml"
                )
            else:
                raise RuntimeError(
                    "Either config_file_path or state_code must be provided"
                )
        else:
            if state_code is not None:
                raise RuntimeError(
                    "Only one of state_code and config_file_path should be provided"
                )

        with config_file_path.open("r") as f:
            config = safe_load(f)

        if form_code not in config:
            raise KeyError(f"Form code '{form_code}' not found in configuration file.")

        form_config = config[form_code]

        self._columns = form_config.get("columns", [])
        self._enum_mapper = form_config.get("enum_mapper", {})
        self._read_csv_params = form_config.get("read_csv_params", {})
        self._duplicate_columns = form_config.get("duplicate_columns", {})
        self._new_empty_columns = form_config.get("new_empty_columns", [])
        self._state_code = form_config.get("state_code", state_code)
        self._table_type = form_config.get("table_type")
        self._raw_data_path_pattern = form_config.get("path_pattern")
