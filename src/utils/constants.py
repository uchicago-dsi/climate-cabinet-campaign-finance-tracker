"""Constants to be used in various parts of the project."""

import os
from pathlib import Path

import dotenv

dotenv.load_dotenv()
BASE_FILEPATH = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_FILEPATH / "data")).resolve()
RAW_DATA_DIRECTORY = DATA_DIR / "raw"
STATE_FINANCE_CONFIG_DIRECTORY = (
    BASE_FILEPATH / "src" / "utils" / "standardize" / "finance" / "config"
)
# returns the base_path to the directory

source_metadata_directory = BASE_FILEPATH / "src" / "utils" / "static"

SPLIT = "--"
ID_SUFFIX = "_id"
DEFAULT_SCHEMA_PATH = BASE_FILEPATH / "src" / "utils" / "table.yaml"
