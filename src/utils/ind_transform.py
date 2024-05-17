"""Module for standardize transform_pipeline result"""

import pandas as pd
from nameparser import HumanName

from utils.linkage import get_likely_name


# this function needs to be changed -- it takes too long time
def standardize_individual_names(row: pd.Series) -> pd.Series:
    """Standardizes the name-related columns for individuals

    Create/strandardize name-reatled columns including first, last and full names.

    Inputs: Row

    Returns: Row (the function is used to "apply" to each row)
    """
    if pd.isna(row["first_name"]) and pd.notna(row["full_name"]):
        name = HumanName(row["full_name"])
        row["first_name"] = name.first.strip().upper()
    if pd.isna(row["last_name"]) and pd.notna(row["full_name"]):
        name = HumanName(row["full_name"])
        row["last_name"] = name.last.strip().upper()

    # Update full name based on first and last name
    row["full_name"] = get_likely_name(
        row["first_name"], row["last_name"], row["full_name"]
    )
    return row


def standardize_last_name(row: pd.Series) -> pd.Series:
    """Standardizes the last_name col for individuals based on name-related columns

    Inputs: Row

    Returns: Row (the function is used to "apply" to each row)
    """
    # there is no row with both last name and full name columns empty
    if pd.isna(row["last_name"]):
        name = HumanName(row["full_name"])
        row["last_name"] = name.last.strip().lower()
    else:
        last_name = row["last_name"].strip().lower()
        row["last_name"] = last_name
    return row


def contains_custom_special_characters(s: str) -> bool:
    """Check if a string contains special characters

    Inputs: String

    Returns: bool
    """
    special_chars = "!@#$%^&*()"
    return any(char in special_chars for char in s)


def ind_table_create_single_last_name(row: pd.Series) -> pd.Series:
    """Create single_last_name column for individual table

    For more efficient matching, create the a column that shows the last word of full name

    Inputs: Row

    Returns: Row with single_last_name column
    """
    last_name = row["last_name"]
    if contains_custom_special_characters(last_name):
        full_name = row["full_name"]
        row["single_last_name"] = full_name.lower().strip().split()[-1]
    else:
        row["single_last_name"] = last_name.lower().strip().split()[-1]
    return row


def election_table_create_single_last_name(row: pd.Series) -> pd.Series:
    """Create single_last_name column for election result table

    For more efficient matching, create the a column that shows the last word of full name

    Inputs: Row

    Returns: Row with single_last_name column
    """
    last_name = row["last"]
    row["single_last_name"] = last_name.lower().strip().split()[-1]
    return row


# create a function to select columns with single_last_name is unique
def add_unique_lastname_column(
    df_with_names: pd.DataFrame, lastname_col: str
) -> pd.DataFrame:
    """Adds a column to the DataFrame indicating if the last name is unique.

    Inputs:
    dataframe (pd.DataFrame): The DataFrame containing the personal information.
    lastname_col (str): The column name where the last names are stored.

    Returns:
    pd.DataFrame: The original DataFrame with an additional 'is_unique_lastname' column.
    """
    # Check the uniqueness of each last name
    lastname_counts = df_with_names[lastname_col].value_counts()
    # Map the counts back to the original dataframe to check if each lastname appears only once
    df_with_names["is_unique_lastname"] = (
        df_with_names[lastname_col].map(lastname_counts) == 1
    )
    return df_with_names


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
    parts = full_name.split(",")
    if len(parts) > 1:
        first_name_part = parts[1].strip()
        first_name = first_name_part.split("(")[0].strip()
        return first_name.lower()
    return ""


# if unique, compare directly
# if not unique, compare the first name and state


# last name should be lower letters
# if there is no last name,
# if there is last name, turn it into lower and strip
