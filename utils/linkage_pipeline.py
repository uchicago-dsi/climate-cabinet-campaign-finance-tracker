import pandas as pd
from classify import classify_wrapper
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


def preprocess_individuals(individuals: pd.DataFrame) -> pd.DataFrame:
    """
    Given a dataframe of individual donors, preprocesses the data,
    and return a cleaned dataframe.

    Args:
        individuals: dataframe of individual contributions

    Returns:
        cleaned dataframe of individuals
    """
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
    if "Address" in individuals.columns:
        individuals["Address"] = individuals["Address"].astype(str)[
            individuals["Address"].notnull()
        ]
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

    return individuals


def preprocess_organizations(organizations: pd.DataFrame) -> pd.DataFrame:
    """
    Given a dataframe of organization donors, preprocesses the data,
    and return a cleaned dataframe.
    """
    if "Unnamed: 0" in organizations.columns:
        organizations.drop(columns="Unnamed: 0", inplace=True)

    organizations["name"] = (
        organizations["name"]
        .loc[organizations["name"].notnull()]
        .apply(standardize_corp_names)
    )

    return organizations


def preprocess_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Given a dataframe of transactions, preprocesses the data,
    and return a cleaned dataframe.

    Args:
        transactions: dataframe of transactions

    Returns:
        cleaned dataframe of transactions
    """
    if "Unnamed: 0" in transactions.columns:
        transactions.drop(columns="Unnamed: 0", inplace=True)

    transactions["purpose"] = transactions["purpose"].str.upper()

    return transactions


def main():
    organizations = pd.read_csv(
        BASE_FILEPATH / "output" / "complete_organizations_table.csv"
    )

    individuals = pd.read_csv(
        BASE_FILEPATH / "output" / "complete_individuals_table.csv"
    )

    transactions = pd.read_csv(
        BASE_FILEPATH / "output" / "complete_transactions_table.csv"
    )

    individuals = preprocess_individuals(individuals)
    organizations = preprocess_organizations(organizations)
    transactions = preprocess_transactions(transactions)

    # Deduplicates perfect matches and creates a new csv file
    # in output titled "deduplicated_UUIDs.csv"
    individuals = deduplicate_perfect_matches(individuals)
    organizations = deduplicate_perfect_matches(organizations)

    cleaned_individuals_output_path = (
        BASE_FILEPATH / "output" / "cleaned_individuals_table.csv"
    )

    cleaned_organizations_output_path = (
        BASE_FILEPATH / "output" / "cleaned_organizations_table.csv"
    )

    cleaned_transactions_output_path = (
        BASE_FILEPATH / "output" / "cleaned_transactions_table.csv"
    )

    deduped = pd.read_csv(BASE_FILEPATH / "output" / "deduplicated_UUIDs.csv")

    transactions[["donor_id", "recipient_id"]] = transactions[
        ["donor_id", "recipient_id"]
    ].replace(deduped)

    individuals, organizations = classify_wrapper(individuals, organizations)

    individuals.to_csv(cleaned_individuals_output_path, index=False)
    organizations.to_csv(cleaned_organizations_output_path, index=False)
    transactions.to_csv(cleaned_transactions_output_path, index=False)


if __name__ == "__main__":
    main()
