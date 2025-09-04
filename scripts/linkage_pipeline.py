"""Script to perform record linkage on cleaned database"""

import argparse

import submitit
from utils.database import (
    connect_duckdb,
    create_database_from_nested_parquet_directories,
)
from utils.link.predict import run_linkage_pipeline
from utils.link.train import train_splink

parser = argparse.ArgumentParser()

parser.add_argument(
    "-i",
    "--input-directory",
    default=None,
    help=(
        "Path to cleaned input data with subdirectories for each state. "
        "If provided, this data will be loaded into the database at "
        "database_path and clear existing data from the database."
    ),
)
parser.add_argument(
    "--database-path",
    default=None,
    required=True,
    help=(
        "Path to database to load data (if no input directory is provided) and save data"
    ),
)
parser.add_argument(
    "--model-path",
    default=None,
    required=True,
    help="Path to model to load",
)
parser.add_argument(
    "--train",
    action="store_true",
    default=False,
    help="Run training pipeline",
)
parser.add_argument(
    "--cluster",
    action="store_true",
    default=False,
    help="Run pipeline on cluster",
)
parser.add_argument(
    "--threshold",
    type=float,
    default=0.95,
    help="Threshold for linkage",
)
parser.add_argument(
    "--table-name",
    type=str,
    default="transactor_detailed_view",
    help="Table to perform record linkage on",
)
args = parser.parse_args()

# Set up directory paths
if args.input_directory is not None:
    con = create_database_from_nested_parquet_directories(
        args.database_path, args.input_directory, overwrite=True
    )
else:
    con = connect_duckdb(args.database_path)
    if (
        con.execute("SELECT COUNT(*) FROM information_schema.tables")
        .fetch_df()
        .iloc[0][0]
        == 0
    ):
        raise ValueError("Database is empty. Please provide an input directory.")


if __name__ == "__main__":
    if not args.cluster:
        print("Running locally")
        if args.train:
            print("Training")
            train_splink(con, args.table_name, args.model_path)
        print("Starting linkage pipeline")
        run_linkage_pipeline(
            duckdb_path=args.database_path,
            model_path=args.model_path,
            parquet_dir=args.input_directory,
            threshold=args.threshold,
            table_name=args.table_name,
        )
    else:
        print("Submitting to cluster compute nodes with submitit")
        executor = submitit.AutoExecutor(folder="logs")
        executor.update_parameters(
            slurm_time=600,
            slurm_cpus_per_task=1,
            slurm_mem_per_cpu="128G",
            slurm_array_parallelism=3,
            slurm_partition="general",
        )
        executor.submit(
            run_linkage_pipeline,
            duckdb_path=args.database_path,
            model_path=args.model_path,
            parquet_dir=args.input_directory,
            threshold=args.threshold,
            table_name=args.table_name,
        )
