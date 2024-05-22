"""Module for running TX election linkage pipeline"""

import pandas as pd
from utils.constants import BASE_FILEPATH
from utils.election.election_linkage import manual_dedupe
from utils.election.linkage_pipeline import (
    preprocess_election_results,
)


def decide_foreign_key(
    election_data: pd.DataFrame, ind_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Include only the individual names with existed data and find id

    Inputs:
    election_data: election result cleaned data
    ind_df: cleaned individual table

    Returns:
    a table of election result with a new column of individual uuid
    and another table with duplicated information susceptible for inacuracy
    """
    transform_ind_df = ind_df.copy()[["first_name", "single_last_name", "id", "state"]]
    merged_data = election_data.merge(
        transform_ind_df, on=["first_name", "single_last_name", "state"], how="inner"
    ).rename(columns={"id": "candidate_uuid", "unique_id": "case_id"})
    merged_data = merged_data.drop(["single_last_name"], axis=1)
    transform_ind_df["is_duplicate"] = transform_ind_df.duplicated(
        subset=["first_name", "single_last_name", "state"], keep=False
    )
    duplicated_id = transform_ind_df[transform_ind_df["is_duplicate"]]

    duplicated_id = duplicated_id.merge(
        merged_data[["candidate_uuid"]],
        left_on="id",
        right_on="candidate_uuid",
        how="left",
    )
    duplicated_id["in_merged_data"] = duplicated_id["candidate_uuid"].notna()

    # Optionally drop the temporary 'candidate_uuid' column from duplicated_id DataFrame
    duplicated_id.drop(columns=["candidate_uuid"])
    return merged_data, duplicated_id


def preprocess_data_and_create_table(
    election_df: pd.DataFrame, ind_df: pd.DataFrame
) -> None:
    """Clean data, link duplicates, classify nodes and create a network

    Args:
    election_df: election data
    ind_df: individual table
    """
    ind_df, duplicated_tx_record = manual_dedupe(ind_df)

    election_df = preprocess_election_results(election_df)
    # ind_df should be preprocessed here, but since there is no middle names in the last_name column, I just copied it.
    # ind_df = preprocess_cleaned_individuals(ind_df)
    ind_df["single_last_name"] = ind_df["last_name"]

    final_df, duplicated_id = decide_foreign_key(election_df, ind_df)

    output_path = BASE_FILEPATH / "output" / "cleaned"
    output_path.mkdir(exist_ok=True)
    cleaned_election_output_path = (
        BASE_FILEPATH / "output" / "cleaned" / "tx_election_table.csv"
    )
    duplicated_output_path = (
        BASE_FILEPATH / "output" / "cleaned" / "tx_duplicated_ind_table.csv"
    )

    duplicated_election_record_path = (
        BASE_FILEPATH / "output" / "cleaned" / "tx_duplicated_election_table.csv"
    )

    final_df.to_csv(cleaned_election_output_path, index=False)
    duplicated_id.to_csv(duplicated_output_path, index=False)
    duplicated_tx_record.to_csv(duplicated_election_record_path, index=False)
