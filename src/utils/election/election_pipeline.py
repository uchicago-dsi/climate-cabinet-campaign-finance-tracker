"""Merge raw election data into standardized schema"""

import pandas as pd
from utils.election.clean import ElectionResultTransformer
from utils.election.harvard import HarvardTransformer

ALL_ELECTION_CLEANERS = [
    HarvardTransformer(),
]


def transform_and_merge(
    election_cleaners: list[ElectionResultTransformer] = None,
) -> pd.DataFrame:
    """From raw datafiles, clean, merge, and reformat election result data .

    Args:
        election_cleaners: List of election cleaners to merge data from. If None,
            will default to all election_cleaners

    Returns:
        election result table
    """
    if election_cleaners is None:
        election_cleaners = ALL_ELECTION_CLEANERS

    single_source_election_tables = []
    for election_cleaner in election_cleaners:
        print("Cleaning...")
        (election_result_table) = election_cleaner.clean_state()
        single_source_election_tables.append(election_result_table)
    complete_election_result_table = pd.concat(single_source_election_tables)

    return complete_election_result_table
