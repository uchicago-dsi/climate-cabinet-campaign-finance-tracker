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
    "--start_year",
    type=int,
    default=None,
    help="Start year of elections to run pipeline on",
)
parser.add_argument(
    "--end_year",
    type=int,
    default=None,
    help="End year of elections to run pipeline on",
)
parser.add_argument(
    "-o",
    "--output-directory",
    default=None,
    help="Path to directory to save data. Default is 'data/standardized'",
)
args = parser.parse_args()

if args.output_directory is None:
    output_directory = BASE_FILEPATH / "data" / "standardized"
else:
    output_directory = args.output_directory
states = args.states
output_directory.mkdir(parents=True, exist_ok=True)

database = standardize_states(
    states=states, start_year=args.start_year, end_year=args.end_year
)
save_database_to_csv(database, output_directory)
