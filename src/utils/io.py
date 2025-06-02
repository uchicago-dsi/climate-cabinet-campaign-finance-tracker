"""Module handling file input/output"""

from pathlib import Path
from typing import Literal

import pandas as pd

FileFormat = Literal["csv", "parquet"]


def save_database(
    database: dict[str, pd.DataFrame], output_path: Path, format: FileFormat = "csv"
) -> None:
    """Save each table in database with specified format.

    Args:
        database: Dictionary mapping table names to DataFrames
        output_path: Directory path to store database files
        format: File format to use ('csv' or 'parquet')
    """
    for table_name, table in database.items():
        file_path = output_path / f"{table_name}.{format}"

        if format == "csv":
            save_index = bool(table.index.name)
            table.to_csv(file_path, index=save_index)
        elif format == "parquet":
            table.to_parquet(file_path, index=bool(table.index.name))


def load_database(
    input_path: Path, format: FileFormat = "csv"
) -> dict[str, pd.DataFrame]:
    """Load database from directory containing files of specified format.

    Args:
        input_path: Directory path containing data files
        format: File format to read ('csv' or 'parquet')

    Returns:
        Dictionary mapping table names to DataFrames
    """
    database = {}
    file_extension = f".{format}"

    for file_path in input_path.iterdir():
        if file_path.suffix == file_extension:
            table_name = file_path.stem

            if format == "csv":
                database[table_name] = pd.read_csv(file_path)
            elif format == "parquet":
                database[table_name] = pd.read_parquet(file_path)

    return database
