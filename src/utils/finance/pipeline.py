"""Merge raw state campaign finance into standardized schema"""

import pandas as pd

from utils.finance.data_source_registry import (
    get_registered_sources,
    load_state_modules,
)

ALL_STATE_SOURCES = get_registered_sources()


def standardize_states(
    states: list[str] = None,
) -> dict[str, pd.DataFrame]:
    """From raw datafiles, standardize data from specified states.

    Args:
        states: List of states to merge data from. If None,
            will default to all states that have been implemented

    Returns:
        dictionary mapping table name to tables of that type
    """
    load_state_modules()
    if states is None:
        states = ALL_STATE_SOURCES.keys()

    database = {}
    for state in states:
        for source in ALL_STATE_SOURCES[state]:
            standardized_source_table = source.load_and_standardize_data_source()
            if source.table_name not in database:
                database[source.table_name] = pd.DataFrame()

            database[source.table_name] = pd.concat(
                [database[source.table_name], standardized_source_table]
            )

    return database
