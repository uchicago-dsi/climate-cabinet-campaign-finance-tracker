"""Script for running cleaning pipeline for election results, should be run after running the transform_pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.transform.election_pipeline import transform_and_merge

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

election_results_output_path = output_directory / "elction_results_table.csv"

complete_election_resutls_table = transform_and_merge()

complete_election_resutls_table.to_csv(election_results_output_path)

