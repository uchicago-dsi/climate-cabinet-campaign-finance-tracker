"""Module for standardize transform_pipeline result"""


import pandas as pd
from nameparser import HumanName

from utils.linkage import get_likely_name


# I've copied this function into the clean.py file. Not sure where to put it
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
    row["full_name"] = get_likely_name(row["first_name"], row["last_name"], row["full_name"])
    return row