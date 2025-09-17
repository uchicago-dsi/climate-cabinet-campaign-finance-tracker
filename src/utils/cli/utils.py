"""Utility functions for the CLI"""

from __future__ import annotations

import argparse
from pathlib import Path

from utils.constants import DATA_DIR


def add_common_args(
    parser: argparse.ArgumentParser,
    input_directory_name: str,
    output_directory_name: str,
) -> None:
    """Add common arguments to parser.

    Args:
        parser: ArgumentParser to add arguments to
        input_directory_name: Name of the input directory
        output_directory_name: Name of the output directory
    """
    parser.add_argument(
        "-s",
        "--states",
        nargs="+",
        default=None,
        help="State codes on which to run the pipeline",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Earliest year to run the pipeline on",
    )
    parser.add_argument(
        "--end-year", type=int, default=None, help="Latest year to run the pipeline on"
    )
    parser.add_argument(
        "-i",
        "--input-directory",
        type=Path,
        default=None,
        help=f"Path to directory containing raw data. Default is the {input_directory_name} directory in DATA_DIR",
    )
    parser.add_argument(
        "-o",
        "--output-directory",
        type=Path,
        default=None,
        help=f"Path to directory to save data. Default is the {output_directory_name} directory in DATA_DIR",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help=(
            "Desired file format (csv or parquet). If a separate input and output"
            " format is desired, use --input-format and --output-format instead. "
            "Default is parquet"
        ),
    )
    parser.add_argument(
        "--input-format",
        choices=["csv", "parquet"],
        default="csv",
        help="Input file format (csv or parquet). Default is parquet",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output file format (csv or parquet). Default is parquet",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=None,
        help="Path to data schema, defaulting to src/utils/table.yaml",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Maximum number of rows to process at once. If not specified, processes entire dataset in memory.",
    )
    parser.add_argument(
        "--slurm",
        action="store_true",
        default=False,
        help="Run pipeline on an HPC cluster using SLURM",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)


def validate_args(
    args: argparse.Namespace, input_directory_name: str, output_directory_name: str
) -> argparse.Namespace:
    """Validate arguments.

    Args:
        args: Arguments to validate
        input_directory_name: Name of the input directory
        output_directory_name: Name of the output directory
    Returns:
        args: Validated arguments
    """
    if args.chunk_size is not None and args.chunk_size <= 0:
        raise ValueError("Chunk size must be greater than 0")
    # handle format
    if (
        args.input_format is not None or args.output_format is not None
    ) and args.format is not None:
        raise ValueError(
            "Cannot specify both --format and --input-format or --output-format"
        )
    if args.format is not None:
        args.input_format = args.format
        args.output_format = args.format
    # validate directories
    if args.input_directory is not None:
        if not args.input_directory.exists():
            raise ValueError(f"Input directory {args.input_directory} does not exist")
    else:
        args.input_directory = DATA_DIR / input_directory_name
    # its okay if the output directory doesn't exist
    if args.output_directory is None:
        args.output_directory = DATA_DIR / output_directory_name
    args.output_directory.mkdir(parents=True, exist_ok=True)

    return args
