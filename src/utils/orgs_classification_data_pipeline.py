"""Script to combine raw datasets of companies from CSVs into one output CSV for use in classification"""

import re

import numpy as np
import pandas as pd

from utils.constants import BASE_FILEPATH

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
        chunk = chunk.dropna(subset=["PRIMARY SIC CODE"])
        # applies SIC matcher to each company in the chunk
        chunk["classification"] = chunk["PRIMARY SIC CODE"].apply(
            lambda x: SIC_matcher(x, SIC6_codes_df)
        )

        # drops any rows in chunk that do not have classification
        # (companies that are not classified are not relevant)
        chunk = chunk.dropna(subset=["classification"])

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
        if counter >= 20:
            break

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
    merged_FFF_data = merge_FFF_data(FFF_dict)
    cleaned_FFF_df = prepare_FFF_data(merged_FFF_data)

    # preparing InfoGroup data

    SIC6_codes_df = convert_SIC_codes_regex(SIC6_codes_csv)
    cleaned_InfoGroup_df = prepare_infogroup_data(infogroup_data_2023, SIC6_codes_df)

    # merge the data into one DataFrame
    merged_dfs = pd.concat([cleaned_FFF_df, cleaned_InfoGroup_df])
    merged_dfs.to_csv(aggregated_classification_csv, mode="w", index=False)
    return merged_dfs


def company_record_linkage(company_df: pd.DataFrame) -> pd.DataFrame:
    """Performs record linkage on aggregated company DataFrame

    Args:
        company_df: a DataFrame of companies from the FFF datasets and
        InfoGroup dataset

    Returns:
        a cleaned aggregated company DataFrame w/ combined rows where
        possible and duplicates removed.
    """


FFF_dict = {FFF_oil_company_csv: "f", FFF_coal_company_csv: "f"}
print(merge_company_dfs(FFF_dict, infogroup_data_2023).head())
