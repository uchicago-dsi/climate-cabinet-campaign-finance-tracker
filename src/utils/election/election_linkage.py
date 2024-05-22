"""Module for performing record linakge on individual record and election results"""

import warnings

import pandas as pd
from splink.duckdb.linker import DuckDBLinker


def contains_custom_special_characters(s: str) -> bool:
    """Check if a string contains special characters

    Inputs: Any input, expected to handle any type.

    Returns: bool
    """
    if pd.isna(s):
        return False
    special_chars = "!@#$%^&*()"
    s = str(s)
    return any(char in special_chars for char in s)


def create_single_last_name(row: pd.Series) -> pd.Series:
    """Create single_last_name column based on last name or full name columns

    Datasources present full names and last names in different ways.
    Some contains middle names and some last names may have several words that they may contain words like "van" or "Mr."
    The get_likely_name function also fails to identify last name from full name columns
    For more efficient and accurate matching, we create a column that shows the last word of full name / last name columns

    Inputs: Row

    Returns: Row with single_last_name column
    """
    last_name = str(row["last_name"])
    if contains_custom_special_characters(last_name):
        full_name = str(row["full_name"])
        row["single_last_name"] = full_name.lower().strip().split()[-1]
    else:
        row["single_last_name"] = last_name.lower().strip().split()[-1]
    return row


def extract_first_name(full_name: str) -> str:
    """Extracts and standardizes the first name from a full name string.

    Assumes format: "LastName, FirstName (Nickname)" or "LastName, FirstName".
    The result is returned in lower case.
    The function is designed for the harvard election result data

    Args:
    full_name (str): A string containing the full name.

    Returns:
    str: The standardized first name in lower case.
    """
    full_name = str(full_name)
    parts = full_name.split(",")
    if len(parts) > 1:
        first_name_part = parts[1].strip()
        first_name = first_name_part.split("(")[0].strip()
        return first_name.lower()
    return ""


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


def manual_dedupe(tx_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Delete potentially duplicated columns.

    This function identifies and removes duplicated records based on the
    combination of 'first_name', 'last_name', and 'city' columns.

    Inputs:
    tx_df: pd.DataFrame - The input dataframe containing tax records.

    Returns:
    (pd.DataFrame, pd.DataFrame) - Two dataframes, one with deduplicated records
    and one with the duplicated records.
    """
    duplicates = tx_df.duplicated(
        subset=["first_name", "single_last_name"], keep="first"
    )

    deduped_df = tx_df[~duplicates]

    duplicated_records_df = tx_df[duplicates].copy()

    original_ids = tx_df[~duplicates][["first_name", "single_last_name", "id"]]

    duplicated_records_df = duplicated_records_df.merge(
        original_ids, on=["first_name", "single_last_name"], suffixes=("", "_original")
    )

    duplicated_records_df.rename(columns={"ID_uniform": "original_unique_id"})

    return deduped_df, duplicated_records_df


# I didn't use this code since it took too long time and there are not a lot of duplications in the existing data
def splink_dedupe(df: pd.DataFrame, settings: dict, blocking: list) -> pd.DataFrame:
    """Use splink to deduplicate dataframe based on settings

    Configuration settings and blocking can be found in constants.py as
    individuals_settings, organizations_settings

    Uses the splink library which employs probabilistic matching for
    record linkage
    https://moj-analytical-services.github.io/splink/index.html

    Args:
        df: dataframe
        settings: configuration settings
            (based on splink documentation and dataframe columns)
        blocking: list of columns to block on for the table
            (cuts dataframe into parts based on columns labeled blocks)

    Returns:
        deduplicated version of initial dataframe with column 'matching_id'
        that holds list of matching unique_ids
    """
    # Initialize the linker object
    linker = DuckDBLinker(df, settings)

    # Use warnings to manage any potential issues during processing
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")  # Capture all warnings

        # Estimate probability that two random records match
        linker.estimate_probability_two_random_records_match(blocking, recall=0.6)

        # Check for warnings and stop if any are raised
        if len(w) > 0:
            print(
                "Warning detected, stopping the process and returning original dataframe."
            )
            return df

        # Estimate the parameter u using random sampling
        linker.estimate_u_using_random_sampling(max_pairs=5e6)

        # Run expectation maximisation on each block
        for block in blocking:
            linker.estimate_parameters_using_expectation_maximisation(block)

        # Predict matches
        df_predict = linker.predict()

        # Cluster predictions and threshold
        clusters = linker.cluster_pairwise_predictions_at_threshold(
            df_predict, threshold_match_probability=0.7
        )
        clusters_df = clusters.as_pandas_dataframe()

        match_list_df = (
            clusters_df.groupby("cluster_id")["unique_id"].agg(list).reset_index()
        )
        match_list_df = match_list_df.rename(columns={"unique_id": "duplicated"})

        deduped_df = df.merge(
            match_list_df, left_on="unique_id", right_on="duplicated", how="left"
        )

        deduped_df["matching_id"] = deduped_df["cluster_id"]

        deduped_df = deduped_df.drop(columns=["duplicated", "cluster_id"])

    return deduped_df
