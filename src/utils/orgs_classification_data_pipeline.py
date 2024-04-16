"""Script to combine raw datasets of companies from CSVs into one output CSV for use in classification"""

import re

# TODO: #92 Make orgs classification script into more well-defined pipeline
import numpy as np
import pandas as pd
import requests

from utils.constants import BASE_FILEPATH
from utils.linkage import *

# FILE PATHS

FFF_coal_company_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "ff_companies"
    / "FFF_coal_companies.csv"
)

FFF_oil_company_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "ff_companies"
    / "FFF_oil_companies.csv"
)

infogroup_data_2023 = (
    BASE_FILEPATH / "data" / "raw_classification" / "2023_Business_Academic_QCQ.txt"
)

SIC6_codes_csv = BASE_FILEPATH / "data" / "raw_classification" / "SIC6_codes.csv"

organization_classification_csv = (
    BASE_FILEPATH / "data" / "raw_classification" / "organization_classifications.csv"
)

SIC6_codes_csv = BASE_FILEPATH / "data" / "raw_classification" / "SIC6_codes.csv"

relevant_InfoGroup_2023_csv = (
    BASE_FILEPATH / "data" / "raw_classification" / "relevant_InfoGroup_2023.csv"
)

aggregated_classification_csv = (
    BASE_FILEPATH / "data" / "raw_classification" / "aggregated_org_classification.csv"
)

organizations_cleaned_csv = (
    BASE_FILEPATH / "output" / "cleaned" / "organizations_table.csv"
)


def merge_FFF_data(FFF_data_classification_dict: dict) -> pd.DataFrame:
    """Merge FFF CSV files into one DataFrame

    Args:
        FFF_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv where
        'f' is fossil fuel, 'c' is clean energy, 'n' is neutral

    Returns:
        a DataFrame with the merged CSV files w/ new column
    """
    FFF_dfs = []
    # reading in each CSV to df and adding a classification column
    for csv in FFF_data_classification_dict:
        FFF_df = pd.read_csv(csv)
        # add the classification column
        classification = FFF_data_classification_dict[csv]
        FFF_df["classification"] = [classification] * len(FFF_df)
        FFF_dfs.append(FFF_df)

    # combining the CSVs bc they have the same structure
    merged_FFF_dfs = pd.concat(FFF_dfs)

    # rename the columns for consistency with output CSV
    merged_FFF_dfs = merged_FFF_dfs.rename(
        mapper={
            "Company": "company_name",
            "Symbol": "stock_symbol",
            "Legal name": "legal_name",
        },
        axis=1,
    )
    return merged_FFF_dfs


def prepare_FFF_data(merged_FFF_df: pd.DataFrame) -> pd.DataFrame:
    """Merge FFF CSV files into one DataFrame

    Args:
        merged_FFF_df: a DataFrame with the data from all FFF CSVs merged

    Returns:
        a cleaned DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline.
    """
    # establishing schema of outuput DataFrame
    df_schema = {
        "company_name": str,
        "stock_symbol": str,
        "legal_name": str,
        "address": str,
        "city": str,
        "state": str,
        "zipcode": str,
        "area_code": str,
        "primary_SIC_code": int,
        "SIC6_description": str,
        "primary_NAICS_code": str,
        "NAICS8_description": str,
        "classification": str,
    }
    cleaned_aggregated_df = pd.DataFrame(df_schema, index=[])
    merged_FFF_df = merged_FFF_df.drop_duplicates()
    cleaned_aggregated_df = pd.concat([cleaned_aggregated_df, merged_FFF_df])
    return cleaned_aggregated_df


def convert_SIC_codes_regex(SIC6_codes_csv: str) -> pd.DataFrame:
    """Creates a column in SIC6 code DataFrame with regex codes for matching

    Args:
      SIC6_codes_csv: a csv of relevant SIC6 codes and NCAIS codes and their descriptions

    Returns:
    A DataFrame of the SIC6 code csv w/ an added column to convert the codes to regular expressions
    for matching purposes. the n digits of the SIC codes should correspond to the first n digits
    # of the SIC codes in the InfoGroup dataset

    Examples:
    SIC Code 123 should match SIC Code 12345
    SIC Code 672 should not match SIC code 345

    """
    SIC6_codes_df = pd.read_csv(SIC6_codes_csv)

    SIC6_codes_df["SIC_regex_code"] = SIC6_codes_df["SIC_code"].apply(
        lambda x: r"^" + str(x) + "\d{0,}$"
    )
    return SIC6_codes_df


def SIC_matcher(IF_SIC_code: float, relevant_SIC_code_df: pd.DataFrame) -> str:
    """Returns classification of a SIC6 code from InfoGroup if it matches a relevant SIC code

    Args:
      IF_SIC_code: an SIC6 code from the InfoGroup DataFrame (implement with apply function)
      relevant_SIC_code_df: a df of the relevant SIC codes w/ corresponding regex patterns

    Returns:
    String of the classification of that SIC code if there's a match, None otherwise
    """
    # goes through each relevant SIC code and identifies if it matches the input code
    # from the InfoGroup dataset.
    for i in np.arange(0, len(relevant_SIC_code_df)):
        regex_code = relevant_SIC_code_df.iloc[i].SIC_regex_code
        # if it matches, return the classification associated with that SIC6 code
        if re.match(regex_code, str(IF_SIC_code)):
            return relevant_SIC_code_df.iloc[i].classification
    # none of the relevant SIC codes matched, so return None
    return None


def get_symbol_from_company(company_name: str) -> str:
    """Gets the stock symbol based on the name of the company for use
    in record linkage.

    Function should be used with .apply() on a company name column. Function taken from
    stack overflow article: https://stackoverflow.com/questions/38967533/retrieve-company-name-with-ticker-symbol-input-yahoo-or-google-api
    Companies that return 'None' are either not found in the Yahoo Finance API or they
    are not publicly traded companies
    Args:
        company_name: a str that of the company name

    Returns:
        the stock symbol of the company name. None if the symbol cannot be found
    """
    url = "https://query2.finance.yahoo.com/v1/finance/search"
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    params = {"q": company_name, "quotes_count": 1, "country": "United States"}

    res = requests.get(url=url, params=params, headers={"User-Agent": user_agent})
    data = res.json()

    try:
        company_code = data["quotes"][0]["symbol"]
    except:
        return None
    return company_code


def prepare_infogroup_data(
    infogroup_csv: str, SIC6_codes_df: pd.DataFrame
) -> pd.DataFrame:
    """Subsets InfoGroup company data into only relevant CE, FF, and energy companies

    Args:
        infogroup_csv: the InfoGroup csv file
        SIC6_codes_df: DataFrame of the relevant SIC6 codes w/ corresponding regex codes and descriptions
    Returns:
        a DataFrame with information for only the relevant companies from the InfoGroup
        dataset that is formatted in the same schema as the aggregated company df for downstream
        use in the pipeline.
        also writes the subsetted DataFrame to an output data file that can be found in
        the Google Drive folder.
    """
    # columns of interest in the InfoGroup DataFrame
    subset_columns = [
        "COMPANY",
        "ADDRESS LINE 1",
        "CITY",
        "STATE",
        "ZIPCODE",
        "AREA CODE",
        "PRIMARY SIC CODE",
        "SIC6_DESCRIPTIONS (SIC)",
        "PRIMARY NAICS CODE",
        "NAICS8 DESCRIPTIONS",
        "classification",
        "stock_symbol",
    ]

    # establishing schema of outuput DataFrame
    df_schema = {
        "company_name": str,
        "stock_symbol": str,
        "legal_name": str,
        "address": str,
        "city": str,
        "state": str,
        "zipcode": str,
        "area_code": str,
        "primary_SIC_code": int,
        "SIC6_description": str,
        "primary_NAICS_code": str,
        "NAICS8_description": str,
        "classification": str,
    }
    cleaned_aggregated_df = pd.DataFrame(df_schema, index=[])

    counter = 0  # only looking at around 20 chunks for now since file is quite large
    business_data_df = pd.read_csv(infogroup_csv, sep=",", header=0, chunksize=1000)
    for chunk in business_data_df:
        print("processing chunk", counter, "...")
        chunk = chunk.dropna(subset=["PRIMARY SIC CODE"])
        # applies SIC matcher to each company in the chunk
        chunk["classification"] = chunk["PRIMARY SIC CODE"].apply(
            lambda x: SIC_matcher(x, SIC6_codes_df)
        )

        # drops any rows in chunk that do not have classification
        # (companies that are not classified are not relevant)
        chunk = chunk.dropna(subset=["classification"])

        # gets stock symbol for each relevant company if found
        chunk["stock_symbol"] = chunk["COMPANY"].apply(
            lambda x: get_symbol_from_company(x)
        )

        # subsetting the chunk by only relevant columns
        chunk = chunk[subset_columns]

        # renaming the columns to match the aggregated df output column names
        column_name_mapper = {
            "COMPANY": "company_name",
            "ADDRESS LINE 1": "address",
            "CITY": "city",
            "STATE": "state",
            "ZIPCODE": "zipcode",
            "AREA CODE": "area_code",
            "PRIMARY SIC CODE": "primary_SIC_code",
            "SIC6_DESCRIPTIONS (SIC)": "SIC6_description",
            "PRIMARY NAICS CODE": "primary_NAICS_code",
            "NAICS8 DESCRIPTIONS": "NAICS8_description",
            "classification": "classification",
        }
        cleaned_chunk = chunk.rename(mapper=column_name_mapper, axis=1)

        # appending chunk to the aggregated output DataFrame
        cleaned_aggregated_df = pd.concat([cleaned_chunk, cleaned_aggregated_df])
        counter += 1

    # write cleaned DF to output file in data/raw_classification
    cleaned_aggregated_df.to_csv(relevant_InfoGroup_2023_csv, mode="w", index=False)

    return cleaned_aggregated_df


def merge_company_dfs(
    FFF_data_classification_dict: dict, InfoGroup_csv: str
) -> pd.DataFrame:
    """Merges all company DataFrames from FFF and InfoGroup into one DataFrame

    Args:
        FFF_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv
        InfoGroup_csv: the InfoGroup csv

    Returns:
        a merged DataFrame from FFF and InfoGroup data. also writes this to a CSV
        in data/raw_classification
    """
    # preparing FF data
    print("preparing FFF data...")
    merged_FFF_data = merge_FFF_data(FFF_dict)
    cleaned_FFF_df = prepare_FFF_data(merged_FFF_data)

    # preparing InfoGroup data
    print("preparing InfoGroup data...")
    SIC6_codes_df = convert_SIC_codes_regex(SIC6_codes_csv)
    cleaned_InfoGroup_df = prepare_infogroup_data(infogroup_data_2023, SIC6_codes_df)

    # merge the data into one DataFrame
    print("merging the FFF and InfoGroup data...")
    merged_dfs = pd.concat([cleaned_FFF_df, cleaned_InfoGroup_df])
    merged_dfs.to_csv(aggregated_classification_csv, mode="w", index=False)
    return merged_dfs


def clean_aggregated_company_df(company_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the aggregated company DataFrame to prepare for record linkage

    Args:
        company_df: a DataFrame of companies from the FFF datasets and
        InfoGroup dataset

    Returns:
        a cleaned aggregated company DataFrame w/ standardized company names, ...

    """
    company_df["company_name"] = company_df["company_name"].apply(
        lambda x: standardize_corp_names(x)
    )
    print("Cleaning the aggregated company df...")
    # removes any exact duplicates that may have occurred due to overlapping SIC Codes
    company_df = company_df.drop_duplicates()
    return company_df


# IN PROGRESS
def company_record_linkage(company_df: pd.DataFrame) -> pd.DataFrame:
    """Performs record linkage on aggregated company DataFrame

    Args:
        company_df: a DataFrame of companies from the FFF datasets and
        InfoGroup dataset

    Returns:
        a cleaned aggregated company DataFrame w/ combined rows where
        possible and duplicates removed.
    """


# IN PROGRESS
def transform_organizations_df(organizations_df: pd.DataFrame) -> pd.DataFrame:
    """Performs record linkage on aggregated company DataFrame

    Args:
        organizations_df: a DataFrame of organizations from previous pipeline

    Returns:
        the organizations_df with a new column for stock_symbol
    """
    # would like to get the stock symbols of all the organizations listed in the
    # organization table where possible but it is taking too long to run...

    organizations_df["stock_symbol"] = organizations_df["name"].apply(
        lambda x: get_symbol_from_company(x)
    )
    return organizations_df


def classify_stock_symbol_match(
    org_stock_symbol: str, classifed_companies_df: pd.DataFrame
) -> pd.DataFrame:
    """Returns the classification of the company with the input stock symbol

    Locates the index of the company with the input stock symbol within the classified
    aggregated company DataFrame. If the stock symbol is found in the DF, returns its
    associated classification. Otherwise, returns None. To be used w/ apply function to
    stock_symbol column in the organizations DataFrame in company matching.

    Args:
        org_stock_symbol: a stock symbol from the organizations table
        classifed_companies_df: df of classified companies (output from this company_record_linkage)

    Returns:
        a classification "f" or "c" if the stock symbol is found, None otherwise
    """
    stock_matches = classifed_companies_df[
        classifed_companies_df["stock_symbol"] == org_stock_symbol
    ]
    if len(stock_matches > 0):
        return stock_matches.iloc[0].classification
    else:
        return None


# IN PROGRESS, not tested yet and pseudocode for now
def match_organizations(
    org_company: str, classified_companies_df: pd.DataFrame
) -> pd.DataFrame:
    """Attempts to match a company from the organizations_df to already classified companies

    attempts to match the input company to a company that has a known classification "f" or "c"
    from this pipeline. If match is found, replaces the classification of the match's classification
    Else, returns None. This function should be implemented with .apply() row-wise

    Args:
        org_company: an organization from the organizations DataFrame
        classified_companies_df: a DataFrame of companies with known classifications
        (from this pipeline)

    Returns:
        str classification "f" or "c" if there is a company that matches, None otherwise
    """
    # will implement this when the transformation of organizations_df w/ the stock data works:
    # if stock symbol matches, then we know they are the same company and can return the classification

    org_stock_symbol = org_company["stock_symbol"]
    stock_matches = classified_companies_df[
        classified_companies_df["stock_symbol"] == org_stock_symbol
    ]
    if len(stock_matches > 0):
        return stock_matches.iloc[0].classification

    # if no symbol match, then we rely on company names and states
    # if there's enough of a match on company name and state, then we can classify

    org_name = org_company["name"]

    # will test out different similarity score metrics to match company names

    # if string similarity w/ companies is above a certain threshold, classify

    # if string similarity is above a certain threshold but below the threshold needed
    # to classify, then we look at the state. if state matches, classify?
    return None


# executing the pipeline
FFF_dict = {FFF_oil_company_csv: "f", FFF_coal_company_csv: "f"}
company_classification_df = merge_company_dfs(FFF_dict, infogroup_data_2023)
cleaned_company_classification_df = clean_aggregated_company_df(
    company_classification_df
)

# TESTING
# organizations_df = pd.read_csv(organizations_cleaned_csv)
# print(transform_organizations_df(organizations_df).head())

# trying to find best similarity score...
print(calculate_string_similarity("EXXON", "Exxon Mobil"))


def jaccard_similarity(str1, str2):
    # Convert the strings to sets of characters
    set1 = set(str1.lower())
    set2 = set(str2.lower())

    # Calculate the intersection and union of the sets
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))

    # Calculate Jaccard Similarity
    similarity = intersection / union if union != 0 else 0

    return similarity


# Test the function
str1 = "EXXON"
str2 = "Exxon Mobil"
similarity = jaccard_similarity(str1, str2)
print(f"Jaccard Similarity between '{str1}' and '{str2}': {similarity:.2f}")


def jaccard_similarity_ngrams(str1, str2, n=3):
    # Generate n-grams for each string
    set1 = set([str1[i : i + n].lower() for i in range(len(str1) - n + 1)])
    set2 = set([str2[i : i + n].lower() for i in range(len(str2) - n + 1)])

    # Calculate the intersection and union of the sets
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))

    # Calculate Jaccard Similarity
    similarity = intersection / union if union != 0 else 0

    return similarity


# Test the function
str1 = "EXXON"
str2 = "Exxon Mobil"
similarity = jaccard_similarity_ngrams(str1, str2, n=3)
print(f"Jaccard Similarity between '{str1}' and '{str2}': {similarity:.2f}")
