"""Module for running election linkage pipeline"""

import pandas as pd
from utils.constants import BASE_FILEPATH
from utils.election.constants import election_blocking, election_settings
from utils.election.election_linkage import (
    create_single_last_name,
    decide_foreign_key,
    extract_first_name,
)


def preprocess_election_results(election_df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess and clean a dataframe of election results

    Args:
        election_df: dataframe of election results

    Returns:
        cleaned dataframe of election_results ready for building network
    """
    election_df = election_df.apply(create_single_last_name, axis = 1)
    election_df.loc[
        election_df["first_name"] == "", "first_name"
        ] = election_df["full_name"].apply(extract_first_name)
    
    return election_df

def preprocess_cleaned_individuals(ind_df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess and clean a dataframe of individuals table

    Inputs: 
        ind__df: dataframe of election results

    Returns:
        cleaned dataframe of election_results ready for building network
    """
    ind_df = ind_df.apply(create_single_last_name, axis = 1)

    return ind_df


def preprocess_data_and_create_table(election_df: pd.DataFrame, ind_df: pd.DataFrame) -> None:
    """Clean data, link duplicates, classify nodes and create a network

    Args:
    election_df: election data
    ind_df: individual table
    """
    election_df = preprocess_election_results(election_df)
    ind_df = preprocess_cleaned_individuals(ind_df)

    final_df, duplicated_id = decide_foreign_key(election_df,ind_df)

    output_path = BASE_FILEPATH / "output" / "cleaned"
    output_path.mkdir(exist_ok=True)
    cleaned_election_output_path = (
        BASE_FILEPATH / "output" / "cleaned" / "election_table.csv"
    )
    duplicated_output_path = (
        BASE_FILEPATH / "output" / "cleaned" / "duplicated_ind_table.csv"
    )

    final_df.to_csv(cleaned_election_output_path, index=False)
    duplicated_id.to_csv(duplicated_output_path, index=False)





