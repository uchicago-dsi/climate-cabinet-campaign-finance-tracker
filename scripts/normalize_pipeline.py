"""Script for running cleaning pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.io import load_database_from_csv, save_database_to_csv
from utils.normalize import Normalizer

parser = argparse.ArgumentParser()

parser.add_argument(
    "-i",
    "--input-directory",
    default=None,
    help="Path to standardized input data. Default is 'output/standardized' in repo root",
)
parser.add_argument(
    "-o",
    "--output-directory",
    default=None,
    help="Path to directory to save output. Default is 'output/normalized'",
)
parser.add_argument(
    "-s",
    "--schema",
    default=None,
    help="Path to data schema, defaulting to src/utils/table.yaml",
)
args = parser.parse_args()

if args.output_directory is None:
    output_directory = BASE_FILEPATH / "output" / "normalized"
else:
    output_directory = args.output_directory
if args.input_directory is None:
    input_directory = BASE_FILEPATH / "output" / "standardized"
else:
    input_directory = args.input_directory
input_directory.mkdir(parents=True, exist_ok=True)
output_directory.mkdir(parents=True, exist_ok=True)
if args.schema is None:
    schema_path = BASE_FILEPATH / "src" / "utils" / "table.yaml"
else:
    schema_path = args.schema

standardized_database = load_database_from_csv(input_directory)
normalizer = Normalizer(standardized_database, schema_path)
normalized_database = normalizer.normalize_database()
save_database_to_csv(normalized_database, output_directory)
