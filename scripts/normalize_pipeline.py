"""Script for running cleaning pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.transform.pipeline import transform_and_merge

parser = argparse.ArgumentParser()

parser.add_argument(
    "-i",
    "--input-directory",
    default=None,
    help="Path to raw data directory. Default is 'data/raw' in repo root",
)
parser.add_argument(
    "-o",
    "--output-directory",
    default=None,
    help="Path to directory to save output. Default is 'output/transformed'",
)
args = parser.parse_args()

if args.output_directory is None:
    output_directory = BASE_FILEPATH / "output" / "transformed"
else:
    output_directory = args.output_directory
if args.input_directory is None:
    input_directory = BASE_FILEPATH / "data" / "raw"
else:
    input_directory = args.input_directory
input_directory.mkdir(parents=True, exist_ok=True)
output_directory.mkdir(parents=True, exist_ok=True)

individuals_output_path = output_directory / "individuals_table-*.csv"
organizations_output_path = output_directory / "organizations_table-*.csv"
transactions_output_path = output_directory / "transactions_table-*.csv"
id_table_output_path = output_directory / "id_map-*.csv"
database = transform_and_merge(["PA"])
for table_type in database:
    database[table_type].to_csv(output_directory / f"{table_type}.csv")
print("pipeline finished and save data to csv.")
