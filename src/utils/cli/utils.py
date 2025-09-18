"""Utility functions for the CLI"""

from __future__ import annotations

import argparse
from pathlib import Path

from utils.constants import DATA_DIR, DEFAULT_SCHEMA_PATH

pipeline_step_details = {
    "collect": {
        "arguments": [
            "states",
            "start-year",
            "end-year",
            "data-directory",
            "output-directory",
            "slurm",
            "verbose",
        ],
        "help": "Collect raw data from sources.",
        "input_directory_name": None,
        "output_directory_name": "raw",
    },
    "standardize": {
        "arguments": [
            "states",
            "start-year",
            "end-year",
            "data-directory",
            "input-directory",
            "output-directory",
            "slurm",
            "verbose",
            "output-format",
            "format",
        ],
        "help": "Standardize raw data with deterministic rules while maintaining shape.",
        "input_directory_name": "raw",
        "output_directory_name": "standardized",
    },
    "normalize": {
        "arguments": [
            "states",
            "start-year",
            "end-year",
            "data-directory",
            "input-directory",
            "output-directory",
            "slurm",
            "verbose",
            "chunk-size",
            "format",
            "output-format",
            "input-format",
            "schema",
        ],
        "help": "Reshape standardized data to match normalized database schema.",
        "input_directory_name": "standardized",
        "output_directory_name": "normalized",
    },
    "clean": {
        "arguments": [
            "states",
            "start-year",
            "end-year",
            "data-directory",
            "input-directory",
            "output-directory",
            "chunk-size",
            "slurm",
            "verbose",
            "format",
            "output-format",
            "input-format",
            "schema",
        ],
        "help": "Clean normalized data with heuristic transformations.",
        "input_directory_name": "normalized",
        "output_directory_name": "cleaned",
    },
    "link": {
        "arguments": [
            "states",
            "start-year",
            "end-year",
            "data-directory",
            "input-directory",
            "output-directory",
            "slurm",
            "verbose",
            "format",
            "output-format",
            "input-format",
            "database-path",
            "model-path",
            "threshold",
            "table-name",
        ],
        "help": "Perform probabilistic record linkage on cleaned data to identify duplicate records.",
        "input_directory_name": "cleaned",
        "output_directory_name": "linked",
    },
    "classify": {
        "arguments": [
            "states",
            "start-year",
            "end-year",
            "data-directory",
            "input-directory",
            "output-directory",
            "slurm",
            "verbose",
        ],
        "help": "Classify data by relation to climate.",
        "input_directory_name": "linked",
        "output_directory_name": "classified",
    },
}


def create_parser_for_step(
    parser: argparse.ArgumentParser, step: str
) -> argparse.ArgumentParser:
    """Create a parser for a given step"""
    step_details = pipeline_step_details[step]
    step_parser = parser.add_parser(step, help=step_details["help"])
    for argument in step_details["arguments"]:
        step_parser = add_argument_to_parser(step_parser, step, argument)
    step_parser.add_argument(
        "--input-directory-name",
        type=str,
        default=step_details["input_directory_name"],
        help=argparse.SUPPRESS,
    )
    step_parser.add_argument(
        "--output-directory-name",
        type=str,
        default=step_details["output_directory_name"],
        help=argparse.SUPPRESS,
    )

    return step_parser


def add_argument_to_parser(
    parser: argparse.ArgumentParser,
    step_name: str,
    argument_name: str,
) -> argparse.ArgumentParser:
    """Add arguments to a parser for a given step.

    Args:
        parser: ArgumentParser to add arguments to
        step_name: Name of the step
        argument_name: Name of the argument
    """
    if argument_name == "states":
        parser.add_argument(
            "-s",
            "--states",
            nargs="+",
            default=None,
            help="State codes on which to run the pipeline",
        )
    elif argument_name == "start-year":
        parser.add_argument(
            "--start-year",
            type=int,
            default=None,
            help="Earliest year to run the pipeline on",
        )
    elif argument_name == "end-year":
        parser.add_argument(
            "--end-year",
            type=int,
            default=None,
            help="Latest year to run the pipeline on",
        )
    elif argument_name == "data-directory":
        parser.add_argument(
            "-d",
            "--data-directory",
            type=Path,
            default=DATA_DIR,
            help=(
                "Path to main data directory. If --input-directory or "
                "--ouptut-directory are not set, pipeline steps will look for "
                "data in specified subdirectories of --data-directory. "
                "For this step, the default input directory is "
                f"{pipeline_step_details[step_name]['input_directory_name']} "
                "and the default ouptut directory is "
                f"{pipeline_step_details[step_name]['output_directory_name']}. "
                "If this argument is not set, the default data directory is taken "
                "from the DATA_DIR environment variable, which is currently "
                f"{DATA_DIR}."
            ),
        )
    elif argument_name == "input-directory":
        parser.add_argument(
            "-i",
            "--input-directory",
            type=Path,
            default=None,
            help=(
                f"Path to directory containing raw data. Default is the "
                f"{pipeline_step_details[step_name]['input_directory_name']} "
                "directory in DATA_DIR. Setting this ignore --data-directory."
            ),
        )
    elif argument_name == "output-directory":
        parser.add_argument(
            "-o",
            "--output-directory",
            type=Path,
            default=None,
            help=(
                f"Path to directory to save data. Default is the "
                f"{pipeline_step_details[step_name]['output_directory_name']} "
                "directory in DATA_DIR. Setting this ignores --data-directory."
            ),
        )
    elif argument_name == "slurm":
        parser.add_argument(
            "--slurm",
            action="store_true",
            default=False,
            help="Run pipeline on an HPC cluster using SLURM",
        )
    elif argument_name == "database-path":
        parser.add_argument(
            "--database-path",
            type=Path,
            default=None,
            help="Path to DuckDB database to load/save data",
        )
    elif argument_name == "model-path":
        parser.add_argument(
            "--model-path",
            type=Path,
            default=None,
            help="Path to record linkage model. If training, this will be the path to save the model.",
        )
    elif argument_name == "threshold":
        parser.add_argument(
            "--threshold",
            type=float,
            default=0.95,
            help="Match probability threshold for to consider two records as a match.",
        )
    elif argument_name == "table-name":
        parser.add_argument(
            "--table-name",
            type=str,
            default="transactor_detailed_view",
            help="Table to perform record linkage on",
        )
    elif argument_name == "format":
        parser.add_argument(
            "-f",
            "--format",
            choices=["csv", "parquet"],
            default="parquet",
            help=(
                "Desired file format (csv or parquet). If a separate input and output"
                " format is desired, use --input-format and --output-format instead. "
                "Default is parquet"
            ),
        )
    elif argument_name == "input-format":
        parser.add_argument(
            "--input-format",
            choices=["csv", "parquet"],
            default="parquet",
            help="Input file format (csv or parquet). Default is parquet",
        )
    elif argument_name == "output-format":
        parser.add_argument(
            "--output-format",
            choices=["csv", "parquet"],
            default="parquet",
            help="Output file format (csv or parquet). Default is parquet",
        )
    elif argument_name == "schema":
        parser.add_argument(
            "--schema",
            type=Path,
            default=DEFAULT_SCHEMA_PATH,
            help="Path to data schema, defaulting to src/utils/table.yaml",
        )
    elif argument_name == "chunk-size":
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=None,
            help="Maximum number of rows to process at once. If not specified, processes entire dataset in memory.",
        )
    elif argument_name == "verbose":
        parser.add_argument("-v", "--verbose", action="count", default=0)
    else:
        raise ValueError(f"Argument {argument_name} not found")
    return parser


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
    print(args.states)
    if "chunk-size" in args and args.chunk_size is not None and args.chunk_size <= 0:
        raise ValueError("Chunk size must be greater than 0")
    # handle format
    if (
        ("input-format" in args and args.input_format is not None)
        or ("output-format" in args and args.output_format is not None)
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
        args.input_directory = args.data_directory / input_directory_name
    # its okay if the output directory doesn't exist
    if args.output_directory is None:
        args.output_directory = args.data_directory / output_directory_name
    args.output_directory.mkdir(parents=True, exist_ok=True)

    return args
