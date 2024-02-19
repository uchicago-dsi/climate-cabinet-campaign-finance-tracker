from typing import Tuple

import pandas as pd
from nameparser import HumanName

from utils.linkage import (
    cleaning_company_column,
    get_address_line_1_from_full_address,
    get_address_number_from_address_line_1,
    get_street_from_address_line_1,
    standardize_corp_names,
)


def preprocess_pipeline(
    individuals: pd.DataFrame,
    organizations: pd.DataFrame,
    transactions: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Preprocesses data for record linkage

    Args:
        Individuals: dataframe of individual contributions
        Organizations: dataframe of organization contributions
        Transactions: dataframe of transactions
    Returns:
        preprocessed tuple of dataframes
        first element is the individuals dataframe,
        second element is the organizations dataframe,
        third element is the transactions dataframe
    """
    # Preprocess organizations dataframe
    organizations["name"] = (
        organizations["name"].astype(str, skipna=True).apply(standardize_corp_names)
    )

    # Preprocess individuals dataframe
    if "Unnamed: 0" in individuals.columns:
        individuals.drop(columns="Unnamed: 0", inplace=True)

    individuals = individuals.astype(
        {"first_name": str, "last_name": str, "full_name": str, "company": str}
    )

    # Standardize company names in individuals dataframe
    individuals["company"] = individuals["company"].apply(standardize_corp_names)
    individuals["company"] = individuals["company"].apply(cleaning_company_column)

    # Address functions, assuming address column is named 'address'
    individuals["Address Line 1"] = individuals["Address"].apply(
        get_address_line_1_from_full_address
    )
    individuals["Street Name"] = individuals["Address Line 1"].apply(
        get_street_from_address_line_1
    )
    individuals["Address Number"] = individuals["Address Line 1"].apply(
        get_address_number_from_address_line_1
    )

    # Check if first name or last names are empty, if so, extract from full name column
    individuals["full_name"] = individuals["full_name"].astype(str)[
        individuals["full_name"].notnull()
    ]
    if individuals["first_name"].isnull().any():
        name = individuals["full_name"].apply(HumanName).apply(lambda x: x.as_dict())
        first_name = name.apply(lambda x: x["first"])
        individuals["first_name"] = first_name

    if individuals["last_name"].isnull().any():
        name = individuals["full_name"].apply(HumanName).apply(lambda x: x.as_dict())
        last_name = name.apply(lambda x: x["last"])
        individuals["last_name"] = last_name

    # Transactions
    if "Unnamed: 0" in transactions.columns:
        transactions.drop(columns="Unnamed: 0", inplace=True)

    transactions["purpose"] = transactions["purpose"].str.upper()

    return individuals, organizations, transactions
