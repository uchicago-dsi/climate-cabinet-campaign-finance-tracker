"""Script to clean data from normalized database"""

import argparse
from pathlib import Path

import pandas as pd
from submitit import AutoExecutor
from tqdm import tqdm
from utils.clean.address import clean_address
from utils.clean.columns import clean_database_columns
from utils.clean.names import clean_transactors
from utils.constants import BASE_FILEPATH
from utils.io import load_database, save_database

parser = argparse.ArgumentParser()

parser.add_argument(
    "-i",
    "--input-directory",
    default=None,
    help="Path to standardized input data. Default is 'data/normalized' in repo root",
)
parser.add_argument(
    "-o",
    "--output-directory",
    default=None,
    help="Path to directory to save data. Default is 'data/cleaned'",
)
parser.add_argument(
    "--chunk-size",
    type=int,
    default=None,
    help="Maximum number of rows to process at once. If not specified, processes entire dataset in memory.",
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
parser.add_argument(
    "--config-file",
    default=None,
    help="Path to a yaml file with details about the database schema. Default is 'utils/schema.yaml'",
)
parser.add_argument(
    "--cluster",
    action="store_true",
    help="Run pipeline on cluster",
)
args = parser.parse_args()

# Set up directory paths
if args.output_directory is None:
    output_directory = BASE_FILEPATH / "data" / "cleaned"
else:
    output_directory = Path(args.output_directory)
if args.input_directory is None:
    input_directory = BASE_FILEPATH / "data" / "normalized"
else:
    input_directory = Path(args.input_directory)
if args.config_file is None:
    config_file = BASE_FILEPATH / "src" / "utils" / "table.yaml"
else:
    config_file = Path(args.config_file)
input_directory.mkdir(parents=True, exist_ok=True)
output_directory.mkdir(parents=True, exist_ok=True)


def clean_data(
    database: dict[str, pd.DataFrame], config_file: Path
) -> dict[str, pd.DataFrame]:
    """Clean data from normalized database

    Args:
        database: dictionary of pandas DataFrames
        config_file: Path to a yaml file with details about the database schema.

    Returns:
        dictionary of pandas DataFrames
    """
    database = clean_database_columns(database, config_file)
    if "Transactor" in database:
        database["Transactor"] = clean_transactors(database["Transactor"])
    if "Address" in database:
        database["Address"] = clean_address(database["Address"])
    return database


def cleaning_pipeline(args: argparse.Namespace) -> None:
    """Run cleaning pipeline"""
    if args.chunk_size is None:
        normalized_database = load_database(input_directory, args.input_format)
        cleaned_database = clean_data(normalized_database, config_file)
        save_database(cleaned_database, output_directory, args.output_format)
    else:
        database_chunks = load_database(
            input_directory, args.input_format, chunk_size=args.chunk_size
        )
        first_chunk = True
        for chunk_database in tqdm(database_chunks, desc="Processing chunks"):
            cleaned_database = clean_data(chunk_database, config_file)
            save_mode = "overwrite" if first_chunk else "append"
            save_database(
                cleaned_database, output_directory, args.output_format, mode=save_mode
            )
            first_chunk = False


if __name__ == "__main__":
    if args.cluster is None:
        cleaning_pipeline(args)
    else:
        executor = AutoExecutor(folder=args.cluster)
        executor.update_parameters(
            slurm_time=600,
            slurm_cpus_per_task=1,
            slurm_mem_per_cpu=256000,
            slurm_array_parallelism=3,
            slurm_partition="general",
        )
        for state_dir in input_directory.iterdir():
            if state_dir.is_dir():
                args.input_directory = state_dir
                args.output_directory = output_directory / state_dir.name
                executor.submit(cleaning_pipeline, args)
