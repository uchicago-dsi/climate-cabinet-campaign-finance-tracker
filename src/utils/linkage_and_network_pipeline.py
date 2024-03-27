"""Module for running linkage pipeline"""

import networkx as nx
import pandas as pd
from nameparser import HumanName

from utils.classify import classify_wrapper
from utils.constants import (
    BASE_FILEPATH,
    individuals_blocking,
    individuals_settings,
    organizations_blocking,
    organizations_settings,
)
from utils.linkage import (
    cleaning_company_column,
    deduplicate_perfect_matches,
    get_address_line_1_from_full_address,
    get_address_number_from_address_line_1,
    get_likely_name,
    get_street_from_address_line_1,
    splink_dedupe,
    standardize_corp_names,
)
from utils.network import (
    combine_datasets_for_network_graph,
    construct_network_graph,
    create_network_graph,
)


def preprocess_individuals(individuals: pd.DataFrame) -> pd.DataFrame:
    """Preprocess and clean a dataframe of individuals

    Args:
        individuals: dataframe of individual contributions

    Returns:
        cleaned dataframe of individuals
    """
    if "Unnamed: 0" in individuals.columns:
        individuals = individuals.drop(columns="Unnamed: 0")

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
        .loc[individuals["company"].notna()]
        .apply(standardize_corp_names)
    )
    individuals["company"] = (
        individuals["company"]
        .loc[individuals["company"].notna()]
        .apply(cleaning_company_column)
    )

    # Address functions, assuming address column is named 'Address'
    if "Address" in individuals.columns:
        individuals["Address"] = individuals["Address"].astype(str)[
            individuals["Address"].notna()
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
        individuals["full_name"].notna()
    ]
    if individuals["first_name"].isna().any():
        first_name = individuals["full_name"].apply(
            lambda x: HumanName(x).first if pd.notna(x) else x
        )
        individuals["first_name"] = first_name

    if individuals["last_name"].isna().any():
        last_name = individuals["full_name"].apply(
            lambda x: HumanName(x).last if pd.notna(x) else x
        )
        individuals["last_name"] = last_name

    individuals["full_name"] = individuals.apply(
        lambda row: get_likely_name(
            row["first_name"], row["last_name"], row["full_name"]
        ),
        axis=1,
    )

    # Ensure that columns with values are prioritized and appear first
    # important for splink implementation
    individuals["sort_priority"] = (
        ~individuals["first_name"].isna()
        & ~individuals["last_name"].isna()
        & ~individuals["company"].isna()
    ) * 2 + (~individuals["party"].isna() * 2)

    individuals = individuals.sort_values(by="sort_priority", ascending=False).drop(
        columns=["sort_priority"]
    )

    individuals["unique_id"] = individuals["id"]

    return individuals


def preprocess_organizations(organizations: pd.DataFrame) -> pd.DataFrame:
    """Preprocess and clean an organizations dataframe

    Args:
        organizations: dataframe with organization details
    """
    if "Unnamed: 0" in organizations.columns:
        organizations = organizations.drop(columns="Unnamed: 0")

    organizations["name"] = (
        organizations["name"]
        .loc[organizations["name"].notna()]
        .apply(standardize_corp_names)
    )

    organizations["unique_id"] = organizations["id"]

    return organizations


def preprocess_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """Preprocess and clean an transactions dataframe

    Args:
        transactions: dataframe of transactions

    Returns:
        cleaned dataframe of transactions
    """
    if "Unnamed: 0" in transactions.columns:
        transactions = transactions.drop(columns="Unnamed: 0")

    transactions["purpose"] = transactions["purpose"].str.upper()

    deduped = pd.read_csv(BASE_FILEPATH / "output" / "deduplicated_UUIDs.csv")
    transactions[["donor_id", "recipient_id"]] = transactions[
        ["donor_id", "recipient_id"]
    ].replace(deduped)

    return transactions


def clean_data_and_build_network(
    individuals_table: pd.DataFrame,
    organizations_table: pd.DataFrame,
    transactions_table: pd.DataFrame,
) -> None:
    """Clean data, link duplicates, classify nodes and create a network

    Args:
        individuals_table: standardized individuals table
        organizations_table: standardized organizations table
        transactions_table: standardized transactions table
    """
    individuals_table = preprocess_individuals(individuals_table)
    organizations_table = preprocess_organizations(organizations_table)
    transactions_table = preprocess_transactions(transactions_table)

    individuals_table, organizations_table = classify_wrapper(
        individuals_table, organizations_table
    )

    individuals_table = deduplicate_perfect_matches(individuals_table)
    organizations_table = deduplicate_perfect_matches(organizations_table)

    organizations = splink_dedupe(
        organizations_table, organizations_settings, organizations_blocking
    )

    individuals = splink_dedupe(
        individuals_table, individuals_settings, individuals_blocking
    )

    transactions = preprocess_transactions(transactions_table)

    output_path = BASE_FILEPATH / "output" / "cleaned"
    output_path.mkdir(exist_ok=True)
    cleaned_individuals_output_path = (
        BASE_FILEPATH / "output" / "cleaned" / "individuals_table.csv"
    )

    cleaned_organizations_output_path = (
        BASE_FILEPATH / "output" / "cleaned" / "organizations_table.csv"
    )

    cleaned_transactions_output_path = (
        BASE_FILEPATH / "output" / "cleaned" / "transactions_table.csv"
    )

    individuals_table.to_csv(cleaned_individuals_output_path, index=False)
    organizations_table.to_csv(cleaned_organizations_output_path, index=False)
    transactions_table.to_csv(cleaned_transactions_output_path, index=False)

    aggreg_df = combine_datasets_for_network_graph(
        [individuals_table, organizations_table, transactions_table]
    )
    g = create_network_graph(aggreg_df)
    g_output_path = BASE_FILEPATH / "output" / "g.gml"
    nx.write_graphml(g, g_output_path)

    construct_network_graph(2018, 2023, [individuals, organizations, transactions])
