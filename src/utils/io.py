"""Module handling file input/output"""

from pathlib import Path

import pandas as pd


def save_database_to_csv(database: dict[str, pd.DataFrame], output_path: Path) -> None:
    """Save each value in database as a csv with name key.csv

    Args:
        database: TODO
        output_path: path to directory to store database in
    """
    for table_name, table in database.items():
        if table.index.name:
            save_index = True
        else:
            save_index = False
        table.to_csv(output_path / f"{table_name}.csv", index=save_index)


def load_database_from_csv(input_path: Path) -> dict[str, pd.DataFrame]:
    """Load a database from a directory containing csv file for each table type

    Args:
        input_path: path to directory with csv files
    """
    database = {}
    for file in input_path.iterdir():
        database[file.stem] = pd.read_csv(
            file,
        ).loc[:1000]
    return database
