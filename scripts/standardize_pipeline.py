"""Script for running cleaning pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.finance.pipeline import standardize_states
from utils.io import save_database_to_csv

parser = argparse.ArgumentParser()

parser.add_argument(
    "-s",
    "--states",
    default=None,
    help="State abbreviations of all states to run pipeline on",
)
parser.add_argument(
    "-o",
    "--output-directory",
    default=None,
    help="Path to directory to save output. Default is 'output/standardized'",
)
args = parser.parse_args()

if args.output_directory is None:
    output_directory = BASE_FILEPATH / "output" / "standardized"
else:
    output_directory = args.output_directory
if args.states is None:
    states = None
else:
    states = args.states
output_directory.mkdir(parents=True, exist_ok=True)

individuals_output_path = output_directory / "individuals_table-*.csv"
organizations_output_path = output_directory / "organizations_table-*.csv"
transactions_output_path = output_directory / "transactions_table-*.csv"
id_table_output_path = output_directory / "id_map-*.csv"
database = standardize_states(states=states)
save_database_to_csv(database, output_directory)
