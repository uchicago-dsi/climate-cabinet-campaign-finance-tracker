from typing import Tuple

import pandas as pd
from nameparser import HumanName

from utils.constants import BASE_FILEPATH
from utils.linkage import (
    cleaning_company_column,
    deduplicate_perfect_matches,
    get_address_line_1_from_full_address,
    get_address_number_from_address_line_1,
    get_likely_name,
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
        organizations["name"].astype(str).apply(standardize_corp_names)
    )
    if "Unnamed: 0" in organizations.columns:
        organizations.drop(columns="Unnamed: 0", inplace=True)

    # Preprocess individuals dataframe
    if "Unnamed: 0" in individuals.columns:
        individuals.drop(columns="Unnamed: 0", inplace=True)

    individuals = individuals.astype(
        {
            "first_name": str,
            "last_name": str,
            "full_name": str,
            "company": "string",
        }
    )

    # Standardize company names in individuals dataframe
    individuals["company"] = (
        individuals["company"]
        .loc[individuals["company"].notnull()]
        .apply(standardize_corp_names)
    )
    individuals["company"] = (
        individuals["company"]
        .loc[individuals["company"].notnull()]
        .apply(cleaning_company_column)
    )

    # Address functions, assuming address column is named 'Address'
    # If there is an "Address" column in the first place
    if "Address" in individuals.columns:
        individuals["Address"] = individuals["Address"].astype(str)
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
        name = (
            individuals["full_name"]
            .apply(HumanName)
            .apply(lambda x: x.as_dict())
        )
        first_name = name.apply(lambda x: x["first"])
        individuals["first_name"] = first_name

    if individuals["last_name"].isnull().any():
        name = (
            individuals["full_name"]
            .apply(HumanName)
            .apply(lambda x: x.as_dict())
        )
        last_name = name.apply(lambda x: x["last"])
        individuals["last_name"] = last_name

    individuals["full_name"] = individuals.apply(
        lambda row: get_likely_name(
            row["first_name"], row["last_name"], row["full_name"]
        ),
        axis=1,
    )

    if "Unnamed: 0" in transactions.columns:
        transactions.drop(columns="Unnamed: 0", inplace=True)

    transactions["purpose"] = transactions["purpose"].str.upper()

    return individuals, organizations, transactions


organizations = pd.read_csv(
    BASE_FILEPATH / "output" / "complete_organizations_table.csv"
)

individuals = pd.read_csv(
    BASE_FILEPATH / "output" / "complete_individuals_table.csv"
)

transactions = pd.read_csv(
    BASE_FILEPATH / "output" / "complete_transactions_table.csv"
)

individuals, organizations, transactions = preprocess_pipeline(
    individuals, organizations, transactions
)

individuals = deduplicate_perfect_matches(individuals)
organizations = deduplicate_perfect_matches(organizations)

processed_individuals_output_path = (
    BASE_FILEPATH / "output" / "processed_individuals_table.csv"
)

processed_organizations_output_path = (
    BASE_FILEPATH / "output" / "processed_organizations_table.csv"
)

processed_transactions_output_path = (
    BASE_FILEPATH / "output" / "processed_transactions_table.csv"
)

individuals.to_csv(processed_individuals_output_path)
organizations.to_csv(processed_organizations_output_path)
transactions.to_csv(processed_transactions_output_path)
