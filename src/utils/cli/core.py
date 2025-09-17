"""Command-line interface for campaign finance pipelines."""

from __future__ import annotations

import argparse
from pathlib import Path

import submitit
from tqdm import tqdm

from utils.clean import clean_data
from utils.cli.utils import add_common_args, validate_args
from utils.database import (
    connect_duckdb,
    create_database_from_nested_parquet_directories,
)
from utils.ids import load_id_mapping, save_id_mapping
from utils.io import load_database, save_database
from utils.link.predict import run_linkage_pipeline
from utils.link.train import train_splink
from utils.normalize import Normalizer
from utils.standardize import standardize_state


def run_scrape(args: argparse.Namespace) -> int:
    """Command entry point for scraping raw data"""
    return 0


def run_standardize(args: argparse.Namespace) -> int:
    """Command entry point for standardizing raw data"""
    db = standardize_state(
        args.state, args.start_year, args.end_year, args.input_directory
    )
    save_database(db, Path(args.output_directory) / args.state, format=args.format)
    return 0


def run_normalize(args: argparse.Namespace) -> int:
    """Command entry point for normalizing standardized data"""
    # Process database in chunks (a chunk may be all data)
    database_chunks = load_database(
        args.input_directory, format=args.input_format, chunk_size=args.chunk_size
    )
    # Load existing ID mappings or start with empty dict
    accumulated_id_mapping = load_id_mapping(args.id_mapping_file)

    first_chunk = True
    for chunk_database in tqdm(database_chunks, desc="Processing chunks"):
        # Create normalizer with accumulated ID mappings from previous chunks
        normalizer = Normalizer(chunk_database, args.schema, accumulated_id_mapping)
        normalized_database = normalizer.normalize_database()

        # Update accumulated ID mappings with new mappings from this chunk
        accumulated_id_mapping.update(normalizer.id_mapping)

        # Save first chunk with overwrite mode, subsequent chunks with append mode
        save_mode = "overwrite" if first_chunk else "append"
        save_database(
            normalized_database,
            args.output_directory,
            format=args.output_format,
            mode=save_mode,
        )

        # Save updated ID mappings after each chunk
        save_id_mapping(accumulated_id_mapping, args.id_mapping_file)

        first_chunk = False
    return 0


def run_clean(args: argparse.Namespace) -> int:
    """Command entry point for cleaning normalized data"""
    database_chunks = load_database(
        args.input_directory, format=args.input_format, chunk_size=args.chunk_size
    )
    first_chunk = True
    for chunk_database in tqdm(database_chunks, desc="Processing chunks"):
        cleaned_database = clean_data(chunk_database, args.config_file)
        save_mode = "overwrite" if first_chunk else "append"
        save_database(
            cleaned_database,
            args.output_directory,
            format=args.output_format,
            mode=save_mode,
        )
        first_chunk = False
    return 0


def run_link(args: argparse.Namespace) -> int:
    """Command entry point for performing record linkage on cleaned data"""
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
    if args.train:
        train_splink(con, args.table_name, args.model_path)
    run_linkage_pipeline(
        duckdb_path=args.database_path,
        model_path=args.model_path,
        parquet_dir=args.input_directory,
        threshold=args.threshold,
        table_name=args.table_name,
    )
    return 0


def run_classify(args: argparse.Namespace) -> int:
    """Command entry point for classifying data by relation to climate"""
    return 0


def build_complete_parser() -> argparse.ArgumentParser:
    """Build a complete parser for CLI options for all pipeline steps"""
    parser = argparse.ArgumentParser(prog="ccf", description="Campaign finance CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # Standardize
    p_standardize = sub.add_parser(
        "standardize",
        help="Standardize raw data with deterministic rules while maintaining shape.",
    )
    add_common_args(
        p_standardize, input_directory_name="raw", output_directory_name="standardized"
    )
    p_standardize.add_argument("--input-format", help=argparse.SUPPRESS)
    p_standardize.add_argument("--chunk-size", help=argparse.SUPPRESS)
    p_standardize.set_defaults(
        func=route_pipeline_step, pipeline_step_func=run_standardize
    )

    # Normalize
    p_norm = sub.add_parser(
        "normalize",
        help="Reshape standardized data to match normalized database schema.",
    )
    add_common_args(
        p_norm, input_directory_name="standardized", output_directory_name="normalized"
    )
    p_norm.set_defaults(func=route_pipeline_step, pipeline_step_func=run_normalize)

    # Clean
    p_clean = sub.add_parser(
        "clean", help="Clean normalized data with heuristic transformations."
    )
    add_common_args(
        p_clean, input_directory_name="normalized", output_directory_name="cleaned"
    )
    p_clean.set_defaults(func=route_pipeline_step, pipeline_step_func=run_clean)

    # Link
    p_link = sub.add_parser(
        "link",
        help="Perform probabilistic record linkage on cleaned data to identify duplicate records.",
    )
    add_common_args(
        p_link, input_directory_name="cleaned", output_directory_name="linked"
    )
    p_link.add_argument(
        "--database-path",
        type=Path,
        required=True,
        help="Path to DuckDB database to load/save data",
    )
    p_link.add_argument(
        "--model-path",
        type=Path,
        required=True,
        help="Path to record linkage model. If training, this will be the path to save the model.",
    )
    p_link.add_argument(
        "--train",
        action="store_true",
        default=False,
        help="Run training pipeline before linkage, overwriting any model at model-path if it exists.",
    )
    p_link.add_argument(
        "--threshold",
        type=float,
        default=0.95,
        help="Match probability threshold for to consider two records as a match.",
    )
    p_link.add_argument(
        "--table-name",
        type=str,
        default="transactor_detailed_view",
        help="Table to perform record linkage on",
    )
    p_link.set_defaults(func=route_pipeline_step, pipeline_step_func=run_link)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for the `ccf` console script."""
    parser = build_complete_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())


def route_pipeline_step(
    args: argparse.Namespace,
    input_directory_name: str,
    output_directory_name: str,
) -> int:
    """Route pipeline step to either local execution or SLURM submission.

    Args:
        args: Arguments to validate. Must contain a pipeline_step_func attribute
            that should handle saving its outputs.
        input_directory_name: Name of the input directory
        output_directory_name: Name of the output directory

    Returns:
        int: 0 if successful
    """
    args = validate_args(
        args,
        input_directory_name=input_directory_name,
        output_directory_name=output_directory_name,
    )

    if args.slurm:
        print("Submitting to cluster compute nodes with submitit")
        executor = submitit.AutoExecutor(folder="logs")
        executor.update_parameters(
            slurm_time=600,
            slurm_cpus_per_task=1,
            slurm_mem_per_cpu=256000,
            slurm_array_parallelism=3,
            slurm_partition="general",
        )
        with executor.batch():
            for state in args.states:
                executor.submit(args.pipeline_step_func, state=state, **args)
    else:
        for state in args.states:
            print(f"Running pipeline step for {state}")
            args.pipeline_step_func(state=state, **args)
    return 0
