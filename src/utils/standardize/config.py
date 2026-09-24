"""Configuration hanlder class for state campaign finance data"""

import re
from dataclasses import dataclass
from pathlib import Path

from yaml import safe_load

from utils.constants import STATE_FINANCE_CONFIG_DIRECTORY
from utils.sources import SourceRegistry, get_default_registry


@dataclass(frozen=True)
class IdSourceRule:
    """How to assign a source to the values of an id column

    Attributes:
        source: name of a source in sources.yaml
        when: regex a raw id must fully match for this rule to apply. None
            matches every id.
        source_id_format: optional python format string that builds the
            source_id from the raw id ('{value}') and other standard columns
            of the same row, e.g. '{reported_election_year}-{value}'.
    """

    source: str
    when: re.Pattern | None = None
    source_id_format: str | None = None


def parse_id_source(id_source: str | list[dict]) -> list[IdSourceRule]:
    """Convert a column's 'id_source' config value to a list of rules

    Args:
        id_source: either a source name, or a list of mappings with keys
            'source' and optionally 'when' and 'source_id_format'. Rules are
            tried in order and the first whose 'when' matches is used.

    Raises:
        ValueError: if a rule without 'when' is followed by other rules, since
            those rules could never apply.
    """
    if isinstance(id_source, str):
        return [IdSourceRule(source=id_source)]
    rules = [
        IdSourceRule(
            source=rule["source"],
            when=re.compile(rule["when"]) if rule.get("when") else None,
            source_id_format=rule.get("source_id_format"),
        )
        for rule in id_source
    ]
    for rule in rules[:-1]:
        if rule.when is None:
            raise ValueError(
                f"id_source rule for '{rule.source}' has no 'when' but is not the "
                "last rule, so the rules after it can never apply."
            )
    return rules


def declared_id_sources(
    state_code: str, config_directory: Path = STATE_FINANCE_CONFIG_DIRECTORY
) -> list[IdSourceRule]:
    """All id source rules declared anywhere in a state's config

    Args:
        state_code: two letter state abbreviation
        config_directory: directory containing <state_code>.yaml

    Returns:
        Distinct rules across every form's column_details
    """
    with (config_directory / f"{state_code.lower()}.yaml").open() as f:
        config = safe_load(f)
    rules = []
    for form_config in config.values():
        if not isinstance(form_config, dict):
            continue
        for column_detail in form_config.get("column_details", []) or []:
            if "id_source" in column_detail:
                for rule in parse_id_source(column_detail["id_source"]):
                    if rule not in rules:
                        rules.append(rule)
    return rules


def resolve_inheritance(config: dict, form_code: str) -> dict:
    """Recursively resolves inheritance from parent form_configs

    If the a key exists in both a child and parent config:
    - For most cases, the child value will override the parent value
    - If the value is 'column_details', the child value will 'update' the parent
      value. (i.e. the child value will be used in duplicate 'raw_name' keys, all
      keys not present in the child will be used from the parent).
    - If there is no column_order in parent or child, the column order will be the
      order of appearance in column_details in the parent, followed by those that only
      appear in the child.

    Args:
        config: dictionary contents of a state config
        form_code: key in config
    Raises:
        KeyError: if the value of config[form_code]['inherits'] is
            not a key in config
        RecursionError: if a form in config inherits from itself.
            Note that more complex cases of recusrion are not caught by
            this.
    """
    child_config = config.get(form_code, {}).copy()
    parent_form_code = child_config.get("inherits")
    if not parent_form_code:
        return child_config
    if parent_form_code not in config:
        raise KeyError(
            "{inherits} not in config file. Avaialbe options are:"
            f" {', '.join(child_config.keys())}"
        )
    if parent_form_code == form_code:
        raise RecursionError(f"Configuration {form_code} inherits from itself")
    parent_config = resolve_inheritance(config, parent_form_code)
    for key, value in child_config.items():
        if key in parent_config and key == "column_details":
            parent_column_details_dict = {
                column_detail["raw_name"]: column_detail
                for column_detail in parent_config["column_details"]
            }
            child_column_details_dict = {
                column_detail["raw_name"]: column_detail for column_detail in value
            }
            parent_config[key] = {
                **parent_column_details_dict,
                **child_column_details_dict,
            }.values()
        elif value is not None:
            parent_config[key] = value
    return parent_config


class ConfigHandler:
    """Handles and validates metadata"""

    @property
    def default_config_folder(self) -> Path:
        """Default path to state campaign finance configuration files"""
        return STATE_FINANCE_CONFIG_DIRECTORY

    @property
    def state_code(self) -> str:
        """Two letter state abbreviation"""
        return self._state_code

    @property
    def table_name(self) -> str:
        """Type of table: 'Transaction', 'Transactor', etc."""
        return self._table_name

    @property
    def raw_data_path_pattern(self) -> str:
        """Regex matching names of raw files in data/raw folder"""
        return re.compile(self._raw_data_path_pattern)

    @property
    def dtype_dict(self) -> dict[str, str]:
        """Maps raw column names to pandas dtypes"""
        return {col["raw_name"]: col["type"] for col in self._column_details}

    @property
    def read_csv_params(self) -> dict:
        """Keyword arguments used in pandas read_csv for reading in tabular file"""
        if self._include_column_order:
            self._read_csv_params["names"] = self.raw_column_order
        elif "header" not in self._read_csv_params:
            self._read_csv_params["header"] = 0
        return self._read_csv_params

    @property
    def column_mapper(self) -> dict[str, str]:
        """Maps raw column names to standardized column names"""
        return {
            col["raw_name"]: col["standard_name"]
            for col in self._column_details
            if "standard_name" in col
        }

    @property
    def relevant_columns(self) -> list[str]:
        """List of raw column names to keep"""
        return list(self.column_mapper.keys())

    @property
    def duplicate_columns(self) -> dict[str, str]:
        """Maps existing columns to additional columns that should have equal values"""
        return self._duplicate_columns

    @property
    def new_empty_columns(self) -> list[str]:
        """Names of new columns that should be added empty"""
        return self._new_empty_columns

    @property
    def state_code_columns(self) -> list[str]:
        """Names of columns that should be added with all rows containing state code"""
        return self._state_code_columns

    @property
    def enum_mapper(self) -> dict[dict[str, str]]:
        """Maps enum column names to mappings of raw values to standard values"""
        return self._enum_mapper

    @property
    def column_to_date_format(self) -> dict[str, str]:
        """Maps date columns to default raw date format"""
        return {
            col["standard_name"]: col["date_format"]
            for col in self._column_details
            if "date_format" in col
        }

    @property
    def post_load_float_columns(self) -> list[str]:
        """List of columns that should be converted to floats after loading

        These are columns that have floats that pandas cannot accurately convert
        to floats on read (they contain , or $).
        """
        return [
            col["standard_name"]
            for col in self._column_details
            if col.get("post_load_float", False)
        ]

    @property
    def raw_column_order(self) -> list[str]:
        """List of columns in order in raw file"""
        return self._column_order

    @property
    def year_filter_filepath_regex(self) -> str | None:
        """Regex pattern to extract year from filepath for filtering"""
        return self._year_filter_filepath_regex

    @property
    def year_column(self) -> str | None:
        """Raw column name containing year data for filtering"""
        return self._year_column

    @property
    def source_registry(self) -> SourceRegistry:
        """Registry of valid id sources that id_source_rules were validated against"""
        return self._source_registry

    @property
    def id_source_rules(self) -> dict[str, list[IdSourceRule]]:
        """Maps standard id column names to rules assigning their values a source"""
        return self._id_source_rules

    @property
    def overloaded_columns(self) -> dict[str, dict[str, str]]:
        """Maps column names to information about how to split them into multiple columns"""
        return self._overloaded_columns

    def __init__(
        self,
        form_code: str,
        config_file_path: Path | None = None,
        state_code: str | None = None,
        source_registry: SourceRegistry | None = None,
    ) -> None:
        """Manage configuration from config file

        Notes: One and only one of config_file_path or state_code must be provided.

        Args:
            form_code: Name of form, must be key in configuration file
            config_file_path: Path to configuration file
            state_code: two letter abbreviation of state with config
                file in default config folder
            source_registry: registry that id sources are validated against.
                Defaults to sources.yaml.

        Raises:
            KeyError: if a column's id_source names a source not in the registry
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

        form_config = resolve_inheritance(config, form_code)

        # default column order is the order in column_details

        self._column_details = form_config.get("column_details", [])
        self._column_order = form_config.get(
            "column_order", [col["raw_name"] for col in self._column_details]
        )

        self._include_column_order = form_config.get("include_column_order", True)
        self._enum_mapper = form_config.get("enum_mapper", {})
        self._read_csv_params = form_config.get("read_csv_params", {})
        self._duplicate_columns = form_config.get("duplicate_columns", {})
        self._new_empty_columns = form_config.get("new_empty_columns", [])
        self._state_code_columns = form_config.get("state_code_columns", [])
        self._state_code = form_config.get("state_code", state_code)
        self._table_name = form_config.get("table_name")
        self._raw_data_path_pattern = form_config.get("path_pattern")
        self._year_filter_filepath_regex = form_config.get("year_filter_filepath_regex")
        self._year_column = form_config.get("year_column")
        self._overloaded_columns = form_config.get("overloaded_columns", {})
        self._null_values = form_config.get("null_values", {})
        self._filter = form_config.get("filter", {})
        self._source_registry = source_registry or get_default_registry()
        self._id_source_rules = self._load_id_source_rules(self._source_registry)

    def _load_id_source_rules(
        self, source_registry: SourceRegistry
    ) -> dict[str, list[IdSourceRule]]:
        """Parse and validate the id_source of each column in column_details"""
        id_source_rules = {}
        for column_detail in self._column_details:
            if "id_source" not in column_detail:
                continue
            if "standard_name" not in column_detail:
                raise ValueError(
                    f"Column '{column_detail['raw_name']}' has an id_source but no "
                    "standard_name, so it would be dropped during standardization."
                )
            standard_name = column_detail["standard_name"]
            if standard_name in id_source_rules:
                raise ValueError(
                    f"More than one column with an id_source maps to "
                    f"'{standard_name}'. Override the unused column without a "
                    "standard_name."
                )
            rules = parse_id_source(column_detail["id_source"])
            for rule in rules:
                source_registry[rule.source]  # raises KeyError if unregistered
            id_source_rules[standard_name] = rules
        return id_source_rules
