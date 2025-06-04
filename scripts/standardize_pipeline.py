"""Script for running cleaning pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.finance.pipeline import standardize_states
from utils.io import save_database

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
parser.add_argument(
    "-f",
    "--format",
    choices=["csv", "parquet"],
    default="csv",
    help="Output file format (csv or parquet). Default is csv",
)
parser.add_argument(
    "-d",
    "--data-directory",
    default=None,
    help="Path to directory containing raw data. Default is 'data/raw'",
)
args = parser.parse_args()

if args.output_directory is None:
    output_directory = BASE_FILEPATH / "data" / "standardized"
else:
    output_directory = args.output_directory
states = args.states
output_directory.mkdir(parents=True, exist_ok=True)

database = standardize_states(
    states=states,
    start_year=args.start_year,
    end_year=args.end_year,
    data_directory=args.data_directory,
)
save_database(database, output_directory, format=args.format)
