"""Merge raw state campaign finance into standardized schema"""

import pandas as pd

from utils.transform.arizona import ArizonaTransformer
from utils.transform.clean import StateTransformer
from utils.transform.michigan import MichiganTransformer
from utils.transform.minnesota import MinnesotaTransformer
from utils.transform.pennsylvania import PennsylvaniaTransformer

ALL_STATE_CLEANERS = [
    ArizonaTransformer(),
    MichiganTransformer(),
    MinnesotaTransformer(),
    PennsylvaniaTransformer(),
]


def transform_and_merge(
    state_cleaners: list[StateTransformer] = None,
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
    # harvard_individuals_table = harvard_cleaners.clean_state()
    # TODO: #96 harvard cleaner should be its own pipeline, not related to campaign finance
    complete_individuals_table = pd.concat(single_state_individuals_tables)
    complete_organizations_table = pd.concat(single_state_organizations_tables)
    complete_transactions_table = pd.concat(single_state_transactions_tables)
    return (
        complete_individuals_table,
        complete_organizations_table,
        complete_transactions_table,
    )
