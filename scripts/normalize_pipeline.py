"""Script for running cleaning pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.normalize import normalize_data_pipeline

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

individuals_output_path = output_directory / "individuals_table-*.csv"
organizations_output_path = output_directory / "organizations_table-*.csv"
transactions_output_path = output_directory / "transactions_table-*.csv"
id_table_output_path = output_directory / "id_map-*.csv"

# iterate through input directory and read tables
normalize_data_pipeline(input_directory, output_directory)
