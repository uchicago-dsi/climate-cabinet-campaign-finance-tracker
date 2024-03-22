"""Merge raw state campaign finance into standardized schema"""

import pandas as pd

from utils.clean.arizona import ArizonaCleaner
from utils.clean.clean import StateCleaner
from utils.clean.michigan import MichiganCleaner
from utils.clean.minnesota import MinnesotaCleaner
from utils.clean.pennsylvania import PennsylvaniaCleaner

ALL_STATE_CLEANERS = [
    ArizonaCleaner(),
    MichiganCleaner(),
    MinnesotaCleaner(),
    PennsylvaniaCleaner(),
]


def clean_and_merge_state_data(
    state_cleaners: list[StateCleaner] = None,
) -> list[pd.DataFrame]:
    """From raw datafiles, clean, merge, and reformat data from specified states.

    Args:
        state_cleaners: List of state cleaners to merge data from. If None,
            will default to all state_cleaners

    Returns:
        list of individuals, organizations, and transactions tables
    """
    if state_cleaners is None:
        state_cleaners = ALL_STATE_CLEANERS
    single_state_individuals_tables = []
    single_state_organizations_tables = []
    single_state_transactions_tables = []
    for state_cleaner in state_cleaners:
        print("Cleaning...")
        (
            individuals_table,
            organizations_table,
            transactions_table,
        ) = state_cleaner.clean_state()
        single_state_individuals_tables.append(individuals_table)
        single_state_organizations_tables.append(organizations_table)
        single_state_transactions_tables.append(transactions_table)

    complete_individuals_table = pd.concat(single_state_individuals_tables)
    complete_organizations_table = pd.concat(single_state_organizations_tables)
    complete_transactions_table = pd.concat(single_state_transactions_tables)
    return (
        complete_individuals_table,
        complete_organizations_table,
        complete_transactions_table,
    )
