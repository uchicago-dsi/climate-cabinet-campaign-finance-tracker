"""Utilities for cleaning state campaign finance data"""

import re
from datetime import datetime

import pandas as pd


def convert_date(date_str: str) -> datetime.utcfromtimestamp:
    """Reformat UNIX timestamp

    args: UNIX-formatted date string

    returns: reformatted date string

    """
    timestamp_match = re.match(r"/Date\((\d+)\)/", date_str)
    if timestamp_match:
        timestamp = int(timestamp_match.group(1))
        return datetime.utcfromtimestamp(timestamp / 1000)
    else:
        return None  # Return None for invalid date formats


def remove_nonstandard(col: pd.Series) -> pd.Series:
    """Remove nonstandard characters from columns

    Using regex, we remove html tags and turn inconsistent
    whitespace into single spaces

    args: column of a pandas dataframe

    returns: modified column of a pandas dataframe
    """
    col = col.str.replace(r"<[^<>]*>", " ", regex=True)
    # removes html tags

    col = (
        col.str.replace("/\\s\\s+/g", " ", regex=True)
        .replace(" ", "_", regex=True)
        .replace("\\W", "", regex=True)
    )
    # turns oversized whitespace to single space

    return col


def get_full_name(first_name: str, last_name: str, full_name: str) -> str:
    """Returns potential full name based on first_name and last_name column

    Input: First_name, last_name: strings of first name and last name

    Return: String of full name
    """
    if pd.isna(full_name) or full_name.strip() == "":
        return f"{first_name} {last_name}"
    return full_name
