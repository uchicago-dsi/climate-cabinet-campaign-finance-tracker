"""Code for management and creation of unique identifies"""

import uuid

import pandas as pd


def add_uuids_to_table(table: pd.DataFrame, id_column: str) -> pd.DataFrame:
    """Add id_column to table where each row has a uuid

    Args:
        table: arbitrary dataframe
        id_column: name of column to be added to table
    """
    table.loc[:, id_column] = table.apply(lambda _: str(uuid.uuid4()), axis=1)
    return table


def replace_ids_in_table(table: pd.DataFrame, id_column: str) -> pd.DataFrame:
    """WIP Replace values of id_column with uuids and return a mapping

    Args:
        table: dataframe with existing id_column
        id_column: name of column to be replaced with uuids
    """
    existing_ids_mask = table[id_column].notna()
    table_with_new_ids = add_uuids_to_table(table.loc[existing_ids_mask], id_column)
    return table_with_new_ids
