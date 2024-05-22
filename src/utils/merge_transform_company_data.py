"""Script to merge and transform company data from FFF and InfoGroup"""

import numpy as np
import pandas as pd
import requests

from utils.linkage import standardize_corp_names


def set_parent_company(row: pd.Series, company_df: pd.DataFrame) -> None:
    """Sets the parent company name for a row of the InfoGroup DataFrame

        If the parent company ABI of the InfoGroup data row is found in the
        parent company dict, sets the parent company's IG UUID to the parent company column.
        This function should be used with .apply() on each row of the
        subsetted InfoGroup dataframe.

    Args:
        row: each row of the DataFrame
        company_df: the InfoGroup DataFrame (to search for the parent company from ABI)

    Returns:
        Returns the parent company's InfoGroup UUID. If there is no corresponding parent
        company, returns None
    """
    parent_company_abi = row["parent_company_ABI"]
    if parent_company_abi is None:
        return None
    parent_abis = company_df[company_df["ABI"] == parent_company_abi]
    if len(parent_abis) > 0:
        # find UUID that corresponds to this ABI
        parent = parent_abis.iloc[0]
        parent_uuid = parent["unique_id"]
        return parent_uuid
    return None


def get_stock_symbol_from_company(company_name: str) -> str:
    """Gets the stock symbol based on the name of the company for use in record linkage.

    Function should be used with .apply() on a company name column. Function taken from
    stack overflow article:
    https://stackoverflow.com/questions/38967533/retrieve-company-name-with-ticker-symbol-input-yahoo-or-google-api
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

    res = requests.get(
        url=url, params=params, headers={"User-Agent": user_agent}, timeout=10
    )
    data = res.json()

    try:
        company_code = data["quotes"][0]["symbol"]
    except requests.RequestException as e:
        print("Request Exception:", e)
        return None
    except IndexError:
        print("IndexError: Unable to retrieve symbol for", company_name)
        return None
    except KeyError:
        print("KeyError: Unable to retrieve symbol for", company_name)
        return None
    return company_code


def clean_aggregated_company_df(company_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the aggregated company DataFrame to prepare for record linkage

    Standardizes company names and drops duplicate rows


    Args:
    company_df: a DataFrame of companies from the FFF datasets and
    InfoGroup dataset

    Returns:
    a cleaned aggregated company DataFrame w/ standardized company names, and None for
    Null values

    """
    # ensure that company_name is a str and standardize company names
    company_df["company_name"] = company_df["company_name"].apply(
        lambda company: standardize_corp_names(str(company))
    )

    # change NaNs to None
    company_df = company_df.replace(np.nan, None)

    # removes any exact duplicates that may have occurred due to overlapping SIC Codes
    company_df = company_df.drop_duplicates()
    return company_df


def transform_aggregated_company_df(
    company_df: pd.DataFrame,
) -> pd.DataFrame:
    """Transforms the aggregated company DataFrame to prepare for record linkage

        Creates UUID for each row of the df.
        Sets reference to parent company's UUID if applicable


    Args:
        company_df: a DataFrame of companies from the FFF datasets and
        InfoGroup dataset

    Returns:
        a cleaned aggregated company DataFrame w/ standardized company names, ...

    """
    print("transforming the aggregated company df...")

    # set parent company to parent company's UUID if applicable
    company_df["parent_company_unique_id"] = company_df.apply(
        lambda row: set_parent_company(row, company_df), axis=1
    )

    return company_df


def merge_company_dfs(
    output_file_path: str,
    cleaned_fff_df: pd.DataFrame = None,
    cleaned_infogroup_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """Merges, trasnforms, and cleans all company DataFrames from FFF and InfoGroup into one df

    Args:
        output_file_path: the file location where the resulting df will be written to as a CSV
        cleaned_fff_df: a prepared, cleaned and merged FFF dataframe
        cleaned_infogroup_df: a prepared and cleaned InfoGroup dataframe with only relevant
            companies

    Returns:
        a merged DataFrame from FFF and InfoGroup data. also writes this to a CSV
        in data/raw_classification
    """
    # merge the data into one DataFrame
    print("merging the FFF and InfoGroup data...")
    # ensure that indexes are diff for concatenation
    cleaned_fff_df = cleaned_fff_df.reset_index(drop=True)
    cleaned_infogroup_df = cleaned_infogroup_df.reset_index(drop=True)

    merged_dfs = pd.concat([cleaned_fff_df, cleaned_infogroup_df])

    # transform the merged DataFrame
    transformed_merged_dfs = transform_aggregated_company_df(merged_dfs)
    cleaned_merged_dfs = clean_aggregated_company_df(transformed_merged_dfs)
    cleaned_merged_dfs.to_csv(output_file_path, mode="w", index=False)
    return cleaned_merged_dfs
