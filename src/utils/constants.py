"""Constants to be used in various parts of the project."""

from pathlib import Path

BASE_FILEPATH = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_FILEPATH / "data"
RAW_DATA_DIRECTORY = BASE_FILEPATH / "data" / "raw"
STATE_FINANCE_CONFIG_DIRECTORY = (
    BASE_FILEPATH / "src" / "utils" / "config" / "finance" / "states"
)
# returns the base_path to the directory

source_metadata_directory = BASE_FILEPATH / "src" / "utils" / "static"

SPLIT = "--"
ID_SUFFIX = "_id"
DEFAULT_SCHEMA_PATH = BASE_FILEPATH / "src" / "utils" / "table.yaml"
