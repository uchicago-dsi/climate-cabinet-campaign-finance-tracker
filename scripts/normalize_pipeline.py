"""Script for running cleaning pipeline"""

import argparse

from utils.constants import BASE_FILEPATH
from utils.ids import load_id_mapping, save_id_mapping
from utils.io import load_database, save_database
from utils.normalize import Normalizer

parser = argparse.ArgumentParser()

parser.add_argument(
    "-i",
    "--input-directory",
    default=None,
    help="Path to standardized input data. Default is 'data/standardized' in repo root",
)
parser.add_argument(
    "-o",
    "--output-directory",
    default=None,
    help="Path to directory to save data. Default is 'data/normalized'",
)
parser.add_argument(
    "-s",
    "--schema",
    default=None,
    help="Path to data schema, defaulting to src/utils/table.yaml",
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
    "--chunk-size",
    type=int,
    default=None,
    help="Maximum number of rows to process at once. If not specified, processes entire dataset in memory.",
)
args = parser.parse_args()

# Set up directory paths
if args.output_directory is None:
    output_directory = BASE_FILEPATH / "data" / "normalized"
else:
    output_directory = args.output_directory
if args.input_directory is None:
    input_directory = BASE_FILEPATH / "data" / "standardized"
else:
    input_directory = args.input_directory
input_directory.mkdir(parents=True, exist_ok=True)
output_directory.mkdir(parents=True, exist_ok=True)

# Set up schema path
if args.schema is None:
    schema_path = BASE_FILEPATH / "src" / "utils" / "table.yaml"
else:
    schema_path = args.schema

# Set up ID mapping file path
id_mapping_file = output_directory / "id_mapping.json"

# Process database in chunks or all at once
if args.chunk_size is None:
    # Process entire database in memory (original behavior)
    standardized_database = load_database(input_directory, format=args.input_format)
    normalizer = Normalizer(standardized_database, schema_path)
    normalized_database = normalizer.normalize_database()
    save_database(normalized_database, output_directory, format=args.output_format)

    # Save ID mappings
    save_id_mapping(normalizer.id_mapping, id_mapping_file)
else:
    # Process database in chunks
    database_chunks = load_database(
        input_directory, format=args.input_format, chunk_size=args.chunk_size
    )

    # Load existing ID mappings or start with empty dict
    accumulated_id_mapping = load_id_mapping(id_mapping_file)

    first_chunk = True
    for chunk_database in database_chunks:
        # Create normalizer with accumulated ID mappings from previous chunks
        normalizer = Normalizer(chunk_database, schema_path, accumulated_id_mapping)
        normalized_chunk = normalizer.normalize_database()

        # Update accumulated ID mappings with new mappings from this chunk
        accumulated_id_mapping.update(normalizer.id_mapping)

        # Save first chunk with overwrite mode, subsequent chunks with append mode
        save_mode = "overwrite" if first_chunk else "append"
        save_database(
            normalized_chunk,
            output_directory,
            format=args.output_format,
            mode=save_mode,
        )

        # Save updated ID mappings after each chunk
        save_id_mapping(accumulated_id_mapping, id_mapping_file)

        first_chunk = False
