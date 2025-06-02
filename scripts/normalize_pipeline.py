"""Script for running cleaning pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.io import load_database, save_database
from utils.normalize import Normalizer

parser = argparse.ArgumentParser()

parser.add_argument(
    "-i",
    "--input-directory",
    default=None,
    help="Path to standardized input data. Default is 'data/standardized' in repo root",
)
parser.add_argument(
    "-o",
    "--output-directory",
    default=None,
    help="Path to directory to save data. Default is 'data/normalized'",
)
parser.add_argument(
    "-s",
    "--schema",
    default=None,
    help="Path to data schema, defaulting to src/utils/table.yaml",
)
parser.add_argument(
    "--input-format",
    choices=["csv", "parquet"],
    default="csv",
    help="Input file format (csv or parquet). Default is csv",
)
parser.add_argument(
    "--output-format",
    choices=["csv", "parquet"],
    default="csv",
    help="Output file format (csv or parquet). Default is csv",
)
args = parser.parse_args()

if args.output_directory is None:
    output_directory = BASE_FILEPATH / "data" / "normalized"
else:
    output_directory = args.output_directory
if args.input_directory is None:
    input_directory = BASE_FILEPATH / "data" / "standardized"
else:
    input_directory = args.input_directory
input_directory.mkdir(parents=True, exist_ok=True)
output_directory.mkdir(parents=True, exist_ok=True)
if args.schema is None:
    schema_path = BASE_FILEPATH / "src" / "utils" / "table.yaml"
else:
    schema_path = args.schema

standardized_database = load_database(input_directory, format=args.input_format)
normalizer = Normalizer(standardized_database, schema_path)
normalized_database = normalizer.normalize_database()
save_database(normalized_database, output_directory, format=args.output_format)
