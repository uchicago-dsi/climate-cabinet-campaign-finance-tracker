"""Script to combine raw datasets of companies from CSVs into one output CSV for use in classification"""

import re
import uuid

# TODO: #92 Make orgs classification script into more well-defined pipeline
import numpy as np
import pandas as pd
import requests
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer

from utils.constants import BASE_FILEPATH
from utils.linkage import standardize_corp_names

nlp = spacy.load("en_core_web_sm")

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
    BASE_FILEPATH / "data" / "raw_classification" / "2023_InfoGroup.txt"
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

TESTING_relevant_InfoGroup_2023_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "TESTING_relevant_InfoGroup_2023.csv"
)

parent_company_validation_csv = (
    BASE_FILEPATH / "data" / "raw_classification" / "parent_company_validation.csv"
)

transformed_IG_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "transformed_relevant_InfoGroup_2023.csv"
)

output_df_schema = {
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
    "SIC_code": float,
    "primary_NAICS_code": str,
    "NAICS8_description": str,
    "parent_company_name": str,
    "parent_company_ABI": int,
    "classification": str,
}


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
    """Clean the merged FFF DataFrame and apply consistent data schema

    Args:
        merged_FFF_df: a DataFrame with the data from all FFF CSVs merged

    Returns:
        a cleaned DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline.
    """
    # establishing schema of outuput DataFrame
    df_schema = output_df_schema
    cleaned_aggregated_df = pd.DataFrame(df_schema, index=[])
    merged_FFF_df = merged_FFF_df.drop_duplicates()
    cleaned_aggregated_df = pd.concat([cleaned_aggregated_df, merged_FFF_df])
    return cleaned_aggregated_df


def get_FFF_df(FFF_data_classification_dict: dict) -> pd.DataFrame:
    """Returns a DataFrame of all FFF data for use in pipeline

    Args:
        FFF_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv where
        'f' is fossil fuel, 'c' is clean energy, 'n' is neutral

    Returns:
        a cleaned FFF DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline.
    """
    print("preparing FFF data...")
    merged_FFF_data = merge_FFF_data(FFF_dict)
    cleaned_FFF_df = prepare_FFF_data(merged_FFF_data)
    return cleaned_FFF_df


def convert_SIC_codes_regex(SIC6_codes_csv: str) -> pd.DataFrame:
    """Creates a column in SIC6 code DataFrame with regex codes for matching

    Args:
      SIC6_codes_csv: a csv of relevant SIC6 codes and NCAIS codes and their descriptions

    Returns:
    A DataFrame of the SIC6 code csv w/ an added column to convert the codes to regular expressions
    for matching purposes. the n digits of the SIC codes should correspond to the first n digits
    of the SIC codes in the InfoGroup dataset

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


# STILL IN PROGRESS, determining best NLP method to accomplish this
def extract_company(company_name: str) -> str:
    """Extracts the significant company name from a company name string

    Uses Named Entity Recognition (NER) with the spacy package to identify
    the main company name from a company name string
    Args:
        company_name: a str of the company name

    Returns:
        the relevant part of the company name. if no word of the str is deemed
        particularly relevant, returns the og str

    Sample Usage:
    >>> extract_company('Exxon Rest Stop')
    'Exxon'
    >>> extract_company('Chevron Food Mart')
    'Chevron'
    >>> extract_company('Electric Charging Station')
    'Electric Charging Station'
    """
    # Create a TF-IDF vectorizer
    vectorizer = TfidfVectorizer()

    # Fit the vectorizer on the input string
    vectorizer.fit([company_name])

    # Get the feature names (terms)
    feature_names = vectorizer.get_feature_names_out()

    # Return the first significant term (highest TF-IDF score)
    return feature_names
    # process company name w/ spacy
    spacy_str = nlp(company_name)
    print(spacy_str)
    # Extract named entities
    named_entities = [entity.text.lower() for entity in spacy_str.ents]
    print(named_entities)
    org_entities = [
        entity.text.lower() for entity in spacy_str.ents if entity.label_ == "ORG"
    ]
    print(org_entities)
    proper_nouns = [
        entity.text.lower() for entity in spacy_str.ents if entity.label_ == "PROPN"
    ]
    print(proper_nouns)
    # If there are named entities, return the first one
    if named_entities:
        return named_entities[0]

    # If no named entities are found, return the original string
    return company_name


def get_symbol_from_company(company_name: str) -> str:
    """Gets the stock symbol based on the name of the company for use in record linkage.

    Function should be used with .apply() on a company name column. Function taken from
    stack overflow article: https://stackoverflow.com/questions/38967533/retrieve-company-name-with-ticker-symbol-input-yahoo-or-google-api
    Companies that return 'None' are either not found in the Yahoo Finance API or they
    are not publicly traded companies
    Args:
        company_name: a str that of the company name

    Returns:
        the stock symbol of the company name. None if the symbol cannot be found
    """
    # company_name = extract_company(company_name)

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


def get_classification(row: pd.Series) -> str:
    """Gets the final classification of the company

    Returns the final classification of the company based on the Primary SIC Code and the SIC Code Column.
    Theoretically these should be the same (both fossil fuel, both clean energy, or neither)
    Function should be used with .apply() on a row of the DataFrame.

    Args:
        row: a row of the InfoGroup DataFrame

    Returns:
        the str classification of the company (f or c). if neither SIC code is relevant, returns None.
        if they return different classifications, returns "ambiguous"
    """
    # if the same classification, just return one of them
    if row["classification_primary_code"] == row["classification_code"]:
        return row["classification_primary_code"]
    elif (row["classification_primary_code"] is not None) & (
        row["classification_code"] is None
    ):
        return row["classification_primary_code"]
    elif (row["classification_primary_code"] is None) & (
        row["classification_code"] is not None
    ):
        return row["classification_code"]
    # if different classifications, return ambiguous
    else:
        return "ambiguous"


# TODO: make a pipeline that skips the infogroup processing
def prepare_infogroup_data(
    infogroup_csv: str, SIC6_codes_df: pd.DataFrame, testing: bool = False
) -> pd.DataFrame:
    """Subsets InfoGroup company data into only relevant CE, FF, and energy companies

    Args:
        infogroup_csv: the InfoGroup csv file
        SIC6_codes_df: DataFrame of the relevant SIC6 codes w/ corresponding regex codes and descriptions
        testing: Boolean - True if code is being tested on only several chunks, False if whole InfoGroup csv should be used
    Returns:
        a DataFrame with information for only the relevant companies from the InfoGroup
        dataset that is formatted in the same schema as the aggregated company df for downstream
        use in the pipeline.
        also returns a dictionary of parent companies and their associated IDs for record linkage.
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
        "ABI",
        "PARENT NUMBER",
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
        "ABI": float,
        "primary_SIC_code": int,
        "SIC6_description": str,
        "primary_NAICS_code": str,
        "NAICS8_description": str,
        "parent_company_name": str,
        "parent_company_ABI": float,
        "classification": str,
    }

    parent_company_ids = set()

    # df of actual parent companies
    confirmed_parent_companies_df = pd.DataFrame(
        columns=[
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
            "ABI",
            "PARENT NUMBER",
            "stock_symbol",
            "legal_name",
            "parent_company_name",
            "SIC CODE",
            "classification_primary_code",
            "classification_code",
        ],
        index=[],
    )

    # df of potential parent companies throughout chunks
    potential_parent_companies_df = pd.DataFrame()

    cleaned_aggregated_df = pd.DataFrame(df_schema, index=[])

    counter = 0  # only looking at around 20 chunks for now since file is quite large
    business_data_df = pd.read_csv(infogroup_csv, sep=",", header=0, chunksize=10000)
    for chunk in business_data_df:
        print("processing chunk", counter, "...")

        # applies SIC matcher to each company in the chunk based on both
        # primary SIC code as well as SIC Code (which can be different)
        chunk["classification_primary_code"] = chunk["PRIMARY SIC CODE"].apply(
            lambda x: SIC_matcher(x, SIC6_codes_df)
        )

        chunk["classification_code"] = chunk["SIC CODE"].apply(
            lambda x: SIC_matcher(x, SIC6_codes_df)
        )

        # drops any rows in chunk that do not have classification
        # (companies that are not classified are not relevant)
        no_classification_rows = chunk[
            (chunk["classification_primary_code"].isna())
            & (chunk["classification_code"].isna())
        ]

        relevant_rows = chunk[
            (chunk["classification_primary_code"].notna())
            | (chunk["classification_code"].notna())
        ]

        # get the potential parent companies from the chunk (companies that do not have a parent company)

        potential_parent_companies = chunk[chunk["PARENT NUMBER"].isna()]

        # and add them to the potential parent company df from prev chunks
        potential_parent_companies_df = pd.concat(
            [potential_parent_companies, potential_parent_companies_df]
        )

        # get all the unique values from parent numbers
        parent_companies_in_chunk = set(relevant_rows["PARENT NUMBER"].unique())
        # print(parent_companies_in_chunk)
        # after the first chunk, want to see if each parent company is found
        # in any of the chunks
        if counter != 0:
            parent_company_ids = parent_companies_in_chunk.union(
                parent_companies_in_chunk, parent_company_ids
            )
        # print("parent company ids: ", parent_company_ids)

        # creating a set of the parent company ids that have been confirmed
        # these ids will be removed for the next chunk iteration
        confirmed_parent_company_ids = set()

        # look at each parent company id and see if it is in the chunk.
        # if so, add the row to the confirmed_parent_company df
        for parent_company_id in parent_company_ids:
            # searching if parent company is in chunk
            parent_company_df = chunk[chunk["ABI"] == parent_company_id]

            # searching if parent company is in potential parent companies
            potential_company_match_df = potential_parent_companies_df[
                potential_parent_companies_df["ABI"] == parent_company_id
            ]

            # searching if parent company is in confirmed parent companies
            confirmed_parent_companies = confirmed_parent_companies_df[
                confirmed_parent_companies_df["ABI"] == parent_company_id
            ]

            # if the parent company id is already confirmed, remove from parent number ABI
            # set and skip it
            if len(confirmed_parent_companies) > 0:
                confirmed_parent_company_ids.add(parent_company_id)
                pass
            # if the parent company id is not a number, skip as well
            elif np.isnan(parent_company_id):
                pass
            # if the parent company is found within the chunk, add it to the
            # confirmed parent company df

            elif len(parent_company_df) > 0:
                parent_company = parent_company_df.iloc[0]
                # formatting into row properly to add to confirmed dataframe
                new_parent_company_row = parent_company.to_frame().transpose()
                confirmed_parent_companies_df = pd.concat(
                    [
                        confirmed_parent_companies_df,
                        new_parent_company_row,
                    ],
                    ignore_index=True,
                )
                # also remove from parent company id set since it's been matched
                confirmed_parent_company_ids.add(parent_company_id)
                # print("parent_company_id: ", parent_company_id)
                # print("parent company found within chunk")
                # print(new_parent_company_row[["PARENT NUMBER", "ABI", "COMPANY"]])
                # print(parent_company_df)
            # if the parent company id is NOT found within the chunk,
            # see if it's in the potential company list
            # and add to confirmed parent company df if so
            elif len(potential_company_match_df) > 0:
                parent_company = potential_company_match_df.iloc[0]
                # formatting into row properly to add to confirmed dataframe
                new_parent_company_row = parent_company.to_frame().transpose()
                # append parent company to confirmed parent company DataFrame
                confirmed_parent_companies_df = pd.concat(
                    [
                        confirmed_parent_companies_df,
                        new_parent_company_row,
                    ],
                    ignore_index=True,
                )
                # also remove from parent company id set since it's been matched
                confirmed_parent_company_ids.add(parent_company_id)
                # print("parent_company_id: ", parent_company_id)
                # print("parent company found in potential parent company list")
                # print(new_parent_company_row[["PARENT NUMBER", "ABI", "COMPANY"]])

            # if the parent company id does not have an associated name in the df and it is NOT
            # found within the chunk, add it to the set of parent company IDs to match against
            # other chunks
            else:
                parent_company_ids.add(parent_company_id)

        # removing the ids that have been confirmed in this chunk iteration
        parent_company_ids -= confirmed_parent_company_ids

        chunk = chunk.drop(no_classification_rows.index)
        print(confirmed_parent_companies_df)

        # get the final classification of the chunk (based on classification of SIC code
        # and SIC6 code)
        chunk["classification"] = chunk.apply(
            lambda row: get_classification(row), axis=1
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
            "PARENT NUMBER": "parent_company_ABI",
            "parent_company_name": "parent_company_name",
            "ABI": "ABI",
        }
        cleaned_chunk = chunk.rename(mapper=column_name_mapper, axis=1)

        # appending chunk to the aggregated output DataFrame
        cleaned_aggregated_df = pd.concat([cleaned_chunk, cleaned_aggregated_df])
        counter += 1
        testing_max_chunks = 4
        if testing & (counter > testing_max_chunks):
            # adding confirmed parent company columns back into DataFrame (ensures that
            # parent companies that are not classified based on sic codes are still in the
            # output DataFrame)
            # print(
            #     "after chunking confirmed parent company: ",
            #     confirmed_parent_companies_df,
            # )
            confirmed_parent_companies_df["classification"] = (
                confirmed_parent_companies_df.apply(
                    lambda row: get_classification(row), axis=1
                )
            )
            confirmed_parent_companies_df = confirmed_parent_companies_df[
                subset_columns
            ]
            cleaned_parent_companies_df = confirmed_parent_companies_df.rename(
                mapper=column_name_mapper, axis=1
            )

            cleaned_aggregated_df = pd.concat(
                [cleaned_aggregated_df, cleaned_parent_companies_df]
            )
            cleaned_aggregated_df = cleaned_aggregated_df.drop_duplicates()
            cleaned_aggregated_df.to_csv(
                TESTING_relevant_InfoGroup_2023_csv, mode="w", index=False
            )
            return cleaned_aggregated_df

            # adding confirmed parent company columns back into DataFrame (ensures that
            # parent companies that are not classified based on sic codes are still in the
            # output DataFrame)
    confirmed_parent_companies_df["classification"] = (
        confirmed_parent_companies_df.apply(lambda row: get_classification(row), axis=1)
    )
    confirmed_parent_companies_df = confirmed_parent_companies_df[subset_columns]
    cleaned_parent_companies_df = confirmed_parent_companies_df.rename(
        mapper=column_name_mapper, axis=1
    )

    cleaned_aggregated_df = pd.concat(
        [cleaned_aggregated_df, cleaned_parent_companies_df]
    )
    cleaned_aggregated_df = cleaned_aggregated_df.drop_duplicates()
    cleaned_aggregated_df.to_csv(
        TESTING_relevant_InfoGroup_2023_csv, mode="w", index=False
    )
    # write cleaned DF to output file in data/raw_classification
    cleaned_aggregated_df.to_csv(relevant_InfoGroup_2023_csv, mode="w", index=False)

    return cleaned_aggregated_df  # parent_companies


def get_InfoGroup_df(
    SIC6_codes_csv: str, infogroup_csv: str, testing: bool = False
) -> pd.DataFrame:
    """Returns a DataFrame of all relevant InfoGroup data for use in pipeline

    Args:
      SIC6_codes_csv: a csv of relevant SIC6 codes and NCAIS codes and their descriptions
      infogroup_csv: the InfoGroup csv file
      testing: Boolean - True if code is being tested on only several chunks, False if whole InfoGroup csv should be used
    Returns:
        a cleaned InfoGroup DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline and the dictionary of ABI codes that
        correspond to parent companies
    """
    # preparing InfoGroup data
    print("preparing InfoGroup data...")
    SIC6_codes_df = convert_SIC_codes_regex(SIC6_codes_csv)
    InfoGroup_df = prepare_infogroup_data(infogroup_csv, SIC6_codes_df, testing=testing)

    return InfoGroup_df


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
    parent_company_ABI = row["parent_company_ABI"]
    if parent_company_ABI is None:
        return None
    parent_ABIs = company_df[company_df["ABI"] == parent_company_ABI]
    if len(parent_ABIs) > 0:
        # find UUID that corresponds to this ABI
        parent = parent_ABIs.iloc[0]
        parent_UUID = parent["UUID"]
        return parent_UUID
    else:
        return None


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

    # creating UUIDs for table
    company_df["UUID"] = [uuid.uuid4() for i in range(len(company_df))]

    # set parent company to parent company's UUID if applicable
    company_df["parent_company_UUID"] = company_df.apply(
        lambda row: set_parent_company(row, company_df), axis=1
    )

    # getting the stock symbols for companies that are parent companies
    # and are not from the FFF dataset
    company_df["stock_symbol"] = company_df.apply(
        lambda row: get_symbol_from_company(row["company_name"])
        if (row["parent_company_UUID"] is None) & (pd.isna(row["stock_symbol"]))
        else row["stock_symbol"],
        axis=1,
    )
    return company_df


def clean_aggregated_company_df(company_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the aggregated company DataFrame to prepare for record linkage

    Standardizes company names and drops duplicate rows


    Args:
    company_df: a DataFrame of companies from the FFF datasets and
    InfoGroup dataset

    Returns:
    a cleaned aggregated company DataFrame w/ standardized company names, ...

    """
    # standardize company names
    company_df["company_name"] = company_df["company_name"].apply(
        lambda company: standardize_corp_names(company)
    )

    # change NaNs to None
    company_df = company_df.replace(np.nan, None)

    # removes any exact duplicates that may have occurred due to overlapping SIC Codes
    company_df = company_df.drop_duplicates()
    return company_df


def merge_company_dfs(
    cleaned_FFF_df: pd.DataFrame = None,
    cleaned_InfoGroup_df: pd.DataFrame = None,
    FFF_data_classification_dict: dict = None,
    InfoGroup_csv: str = None,
    SIC6_codes_csv: str = None,
    testing: bool = False,
) -> pd.DataFrame:
    """Merges all company DataFrames from FFF and InfoGroup into one DataFrame

        If you provide the FFF_df and the InfoGroup_df, it will merge the company dataframes
        If FFF_df and InfoGroup_df are left as None, will perform the necessary transforamtions
        to the original dataset to get the cleaned FFF and InfoGroup dataframes (this is useful
        if you dont' want to run the InfoGroup data cleaning function)

    Args:
        cleaned_FFF_df: a prepared, cleaned and merged FFF dataframe
        cleaned_InfoGroup_df: a prepared and cleaned InfoGroup dataframe with only relevant companies
        FFF_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv (for original FFF data)
        InfoGroup_csv: the original InfoGroup csv
        SIC6_codes_csv: a csv of relevant SIC6 codes and NCAIS codes and their descriptions
        testing: Boolean, True if you don't want to use the whole IG dataset, False if not

    Returns:
        a merged DataFrame from FFF and InfoGroup data. also writes this to a CSV
        in data/raw_classification
    """
    if cleaned_FFF_df is None:
        cleaned_FFF_df = get_FFF_df(FFF_data_classification_dict)

    if cleaned_InfoGroup_df is None:
        cleaned_InfoGroup_df = get_InfoGroup_df(
            SIC6_codes_csv, InfoGroup_csv, testing=testing
        )

    # merge the data into one DataFrame
    print("merging the FFF and InfoGroup data...")
    merged_dfs = pd.concat([cleaned_FFF_df, cleaned_InfoGroup_df])

    # transform the merged DataFrame
    transformed_merged_dfs = transform_aggregated_company_df(merged_dfs)
    cleaned_merged_dfs = clean_aggregated_company_df(transformed_merged_dfs)
    cleaned_merged_dfs.to_csv(aggregated_classification_csv, mode="w", index=False)
    return cleaned_merged_dfs


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

    # record linkage on parent company within InfoGroup DataFrame and also between
    # FFF and InfoGroup Data.
    # how granular do we want the organization data? do we want to keep
    # organizations if they are the same but have different addresses?


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

    # org_name = org_company["name"]

    # will test out different similarity score metrics to match company names

    # if string similarity w/ companies is above a certain threshold, classify

    # if string similarity is above a certain threshold but below the threshold needed
    # to classify, then we look at the state. if state matches, classify?
    return None


# TODO: #105 move company info pipeline to a script
# executing a test of the pipeline
FFF_dict = {FFF_oil_company_csv: "f", FFF_coal_company_csv: "f"}
company_classification_df = merge_company_dfs(
    FFF_data_classification_dict=FFF_dict,
    InfoGroup_csv=infogroup_data_2023,
    SIC6_codes_csv=SIC6_codes_csv,
    testing=True,
)


# TESTING

# parent_testing_df = get_InfoGroup_df(
#     SIC6_codes_csv=SIC6_codes_csv,
#     infogroup_csv=parent_company_validation_csv,
#     testing=True,
# )

# print(parent_testing_df.head())
# print(parent_testing_df.columns)

# FFF_dict = {FFF_oil_company_csv: "f", FFF_coal_company_csv: "f"}
# company_classification_df = merge_company_dfs(
#     FFF_data_classification_dict=FFF_dict,
#     InfoGroup_csv=parent_company_validation_csv,
#     SIC6_codes_csv=SIC6_codes_csv,
# )
# print(
#     company_classification_df[
#         ["parent_company_ABI", "ABI", "UUID", "parent_company_UUID"]
#     ]
# )

# SIC6_codes_df = convert_SIC_codes_regex(SIC6_codes_csv)

# IG_data = prepare_infogroup_data(
#     infogroup_csv=parent_company_validation_csv,
#     SIC6_codes_df=SIC6_codes_df,
#     testing=True,
# )
# print(IG_data)

# FFF_dict = {FFF_oil_company_csv: "f", FFF_coal_company_csv: "f"}
# company_classification_df = merge_company_dfs(
#     FFF_data_classification_dict=FFF_dict,
#     InfoGroup_csv=parent_company_validation_csv,
#     SIC6_codes_csv=SIC6_codes_csv,
#     testing=True,
# )

# testing_companies = [
#     "shell food mart",
#     "chevron food mart",
#     "shell gas",
#     "ctr inc",
#     "freeport-mcmoran oil & gas",
#     "SUNOCO GAS STATION",
#     "exxon gas station",
# ]


# def find_most_different_word(input_string):
#     # Process the input string with spaCy
#     doc = nlp(input_string)

#     # Get word vectors for each token in the document
#     word_vectors = [
#         token.vector for token in doc if not token.is_punct and not token.is_space
#     ]

#     # Compute the mean vector of all word vectors in the document
#     mean_vector = np.mean(word_vectors, axis=0)

#     # Calculate cosine similarity between each word vector and the mean vector
#     similarity_scores = [
#         cosine_similarity([mean_vector], [token.vector])[0][0]
#         for token in doc
#         if not token.is_punct and not token.is_space
#     ]

#     # Find the index of the word with the lowest cosine similarity (most different)
#     most_different_index = np.argmin(similarity_scores)

#     # Return the word corresponding to the index
#     return doc[most_different_index].text


# for company in testing_companies:
#     print(find_most_different_word(company))
# IF_testing_df, IF_testing_company_dict = get_InfoGroup_df(
#     SIC6_codes_csv=SIC6_codes_csv, infogroup_csv=infogroup_data_2023, testing=True
# )
# print(IF_testing_df.head())
# print(IF_testing_df.columns)
# print(IF_testing_company_dict)

# transform_IG_df(IF_testing_df, IF_testing_company_dict)

# FFF_dict = {FFF_oil_company_csv: "f", FFF_coal_company_csv: "f"}
# relevant_InfoGroup_df = pd.read_csv(relevant_InfoGroup_2023_csv)
# company_classification_df = merge_company_dfs(
#     FFF_data_classification_dict=FFF_dict, cleaned_InfoGroup_df=relevant_InfoGroup_df
# )


# organizations_df = pd.read_csv(organizations_cleaned_csv)
# print(transform_organizations_df(organizations_df).head())

# # trying to find best similarity score...
# print(calculate_string_similarity("EXXON", "Exxon Mobil"))


# def jaccard_similarity(str1, str2):
#     # Convert the strings to sets of characters
#     set1 = set(str1.lower())
#     set2 = set(str2.lower())

#     # Calculate the intersection and union of the sets
#     intersection = len(set1.intersection(set2))
#     union = len(set1.union(set2))

#     # Calculate Jaccard Similarity
#     similarity = intersection / union if union != 0 else 0

#     return similarity


# # Test the function
# str1 = "EXXON"
# str2 = "Exxon Mobil"
# similarity = jaccard_similarity(str1, str2)
# print(f"Jaccard Similarity between '{str1}' and '{str2}': {similarity:.2f}")


# def jaccard_similarity_ngrams(str1, str2, n=3):
#     # Generate n-grams for each string
#     set1 = set([str1[i : i + n].lower() for i in range(len(str1) - n + 1)])
#     set2 = set([str2[i : i + n].lower() for i in range(len(str2) - n + 1)])

#     # Calculate the intersection and union of the sets
#     intersection = len(set1.intersection(set2))
#     union = len(set1.union(set2))

#     # Calculate Jaccard Similarity
#     similarity = intersection / union if union != 0 else 0

#     return similarity


# # Test the function
# str1 = "EXXON"
# str2 = "Exxon Mobil"
# similarity = jaccard_similarity_ngrams(str1, str2, n=3)
# print(f"Jaccard Similarity between '{str1}' and '{str2}': {similarity:.2f}")
