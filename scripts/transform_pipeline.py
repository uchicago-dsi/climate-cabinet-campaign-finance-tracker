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

individuals_output_path = output_directory / "individuals_table_TX.csv"
organizations_output_path = output_directory / "organizations_table_TX.csv"
transactions_output_path = output_directory / "transactions_table_TX.csv"
id_table_output_path = output_directory / "id_map.csv"
(
    complete_individuals_table,
    complete_organizations_table,
    complete_transactions_table,
    complete_id_table,
) = transform_and_merge()
complete_individuals_table.to_csv(individuals_output_path)
complete_organizations_table.to_csv(organizations_output_path)
complete_transactions_table.to_csv(transactions_output_path)
complete_id_table.to_csv(id_table_output_path)
print("pipeline finished and save data to csv.")
