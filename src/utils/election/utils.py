"""Utilities for cleaning election results data"""

import pandas as pd


def create_single_last_name(row: pd.Series) -> pd.Series:
    """Create single_last_name column for election result table

    For more efficient matching, create the a column that shows the last word of full name

    Inputs: Row

    Returns: Row with single_last_name column
    """
    last_name = row["last"]
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
    parts = full_name.split(",")
    if len(parts) > 1:
        first_name_part = parts[1].strip()  
        first_name = first_name_part.split("(")[0].strip()  
        return first_name.lower() 
    return ""