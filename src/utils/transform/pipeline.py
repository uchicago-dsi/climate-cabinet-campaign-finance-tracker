"""Merge raw state campaign finance into standardized schema"""

import pandas as pd

from utils.constants import BASE_FILEPATH
from utils.finance.source import DataSource
from utils.finance.states.pennsylvania import (
    PennsylvaniaContributionForm,
    PennsylvaniaExpenseForm,
    PennsylvaniaFilerForm,
)
from utils.finance.states.texas import (
    TexasContributionForm,
    TexasExpenseForm,
    TexasFilerForm,
)
from utils.yamltable import DataSchema, normalize_database

ALL_STATE_SOURCES = {
    "TX": [TexasContributionForm(), TexasFilerForm(), TexasExpenseForm()],
    "PA": [
        PennsylvaniaFilerForm(),
        PennsylvaniaExpenseForm(),
        PennsylvaniaContributionForm(),
    ],
}
ALL_STATE_SOURCES: dict[str, list[DataSource]]


def transform_and_merge(
    states: list[str] = None,
) -> dict[str, pd.DataFrame]:
    """From raw datafiles, clean, merge, and reformat data from specified states.

    Args:
        states: List of states to merge data from. If None,
            will default to all states that have been implemented

    Returns:
        dictionary mapping table type to table
    """
    if states is None:
        states = ALL_STATE_SOURCES.keys()

    schema = DataSchema(BASE_FILEPATH / "src" / "utils" / "table.yaml")

    database = schema.empty_database()
    for state in states:
        for source in ALL_STATE_SOURCES[state]:
            standardized_database = source.read_and_standardize_table()
            for table_type in standardized_database:
                database[table_type].extend(standardized_database[table_type])
    normalized_database = normalize_database(database, schema)

    return normalized_database
