import networkx as nx
import pandas as pd
from nameparser import HumanName

from utils.classify import classify_wrapper
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
from utils.network import combine_datasets_for_network_graph, create_network_graph


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
            "first_name": "string",
            "last_name": "string",
            "full_name": "string",
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
        first_name = individuals["full_name"].apply(
            lambda x: HumanName(x).first if pd.notnull(x) else x
        )
        individuals["first_name"] = first_name

    if individuals["last_name"].isnull().any():
        last_name = individuals["full_name"].apply(
            lambda x: HumanName(x).last if pd.notnull(x) else x
        )
        individuals["last_name"] = last_name

    individuals["full_name"] = individuals.apply(
        lambda row: get_likely_name(
            row["first_name"] if pd.notnull(row["first_name"]) else "",
            row["last_name"] if pd.notnull(row["last_name"]) else "",
            row["full_name"] if pd.notnull(row["full_name"]) else "",
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
    organizations = pd.read_csv(BASE_FILEPATH / "data" / "orgs_mini.csv")

    individuals = pd.read_csv(BASE_FILEPATH / "data" / "inds_mini.csv")

    transactions = pd.read_csv(BASE_FILEPATH / "data" / "trans_mini.csv")

    # organizations = pd.read_csv(
    #     BASE_FILEPATH / "data" / "complete_organizations.csv"
    # )

    # individuals = pd.read_csv(
    #     BASE_FILEPATH / "data" / "complete_individuals.csv"
    # )

    # transactions = pd.read_csv(
    #     BASE_FILEPATH / "data" / "complete_transactions.csv"
    # )

    individuals = preprocess_individuals(individuals)
    organizations = preprocess_organizations(organizations)
    transactions = preprocess_transactions(transactions)

    individuals, organizations = classify_wrapper(individuals, organizations)

    individuals = deduplicate_perfect_matches(individuals)
    organizations = deduplicate_perfect_matches(organizations)

    deduped = pd.read_csv(BASE_FILEPATH / "output" / "deduplicated_UUIDs.csv")

    individuals["unique_id"] = individuals["id"]
    organizations["unique_id"] = organizations["id"]

    individuals = individuals.drop(columns=[])

    # organizations = splink_dedupe(
    #     organizations, organizations_settings, organizations_blocking
    # )

    # individuals = splink_dedupe(
    #     individuals, individuals_settings, individuals_blocking
    # )

    transactions[["donor_id", "recipient_id"]] = transactions[
        ["donor_id", "recipient_id"]
    ].replace(deduped)

    cleaned_individuals_output_path = (
        BASE_FILEPATH / "output" / "cleaned_individuals_table.csv"
    )

    cleaned_organizations_output_path = (
        BASE_FILEPATH / "output" / "cleaned_organizations_table.csv"
    )

    cleaned_transactions_output_path = (
        BASE_FILEPATH / "output" / "cleaned_transactions_table.csv"
    )

    individuals.to_csv(cleaned_individuals_output_path, index=False)
    organizations.to_csv(cleaned_organizations_output_path, index=False)
    transactions.to_csv(cleaned_transactions_output_path, index=False)

    print("deduplication complete, initiating aggregation")

    aggreg_df = combine_datasets_for_network_graph(
        [individuals, organizations, transactions]
    )

    print("aggregation complete, initiating graphing")

    g = create_network_graph(aggreg_df)

    g_output_path = BASE_FILEPATH / "output" / "g.gml"

    nx.write_graphml(g, g_output_path)

    # construct_network_graph(2000, 2024, [individuals, organizations, transactions])


if __name__ == "__main__":
    main()
