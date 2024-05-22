"""Performs record linkage to join with campaign finance data"""

import pandas as pd
from utils.constants import (
    BASE_FILEPATH,
    ORGANIZATIONS_BLOCKING,
    ORGANIZATIONS_SETTINGS,
)
from utils.linkage import splink_link

company_classification_csv = (
    BASE_FILEPATH
    / "data"
    / "classification"
    / "merged_cleaned_company_classification.csv"
)

texas_organizations_csv = (
    BASE_FILEPATH / "data" / "classification" / "organizations_table_TX_14-16.csv"
)

company_classification_df = pd.read_csv(company_classification_csv)
texas_organizations_df = pd.read_csv(texas_organizations_csv)

print("standardizing data sources...")

# processing and standardizing texas_organizations_df and company_classification_df
column_mapper = {
    "ID": "unique_id",
    "FULL_NAME": "company_name",
    "CITY": "city",
    "STATE": "state",
    "ADDRESS_LINE_1": "address",
    "ZIP_CODE": "zipcode",
}
texas_organizations_df = texas_organizations_df.rename(columns=column_mapper)

relevant_columns = list(column_mapper.values())
texas_organizations_link_df = texas_organizations_df[relevant_columns]
company_classification_df = company_classification_df[relevant_columns]
classified_subset = company_classification_df[
    company_classification_df["zipcode"].isin([43215, 75080, 75223])
]

print("performing record linkage...")
linked_texas_df = splink_link(
    campaign_finance_df=texas_organizations_df,
    linkage_dfs=[texas_organizations_link_df, classified_subset],
    settings=ORGANIZATIONS_SETTINGS,
    blocking=ORGANIZATIONS_BLOCKING,
    output_file_path="texas_data_linked.csv",
)

print(linked_texas_df)
