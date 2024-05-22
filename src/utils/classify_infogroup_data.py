"""Script to get relevant InfoGroup data for classification in the pipeline"""

import re
import uuid

import numpy as np
import pandas as pd

from utils.constants import (
    COMPANY_CLASSIFICATION_OUTPUT_SCHEMA,
    RELEVANT_INFOGROUP_COLUMNS,
)


def convert_sic_codes_regex(sic6_codes_csv: str) -> pd.DataFrame:
    """Creates a column in SIC6 code DataFrame with regex codes for matching

    Args:
      sic6_codes_csv: a csv of relevant SIC6 codes and NCAIS codes and their descriptions

    Returns:
    A DataFrame of the SIC6 code csv w/ an added column to convert the codes to regular expressions
    for matching purposes. the n digits of the SIC codes should correspond to the first n digits
    of the SIC codes in the InfoGroup dataset

    Examples:
    SIC Code 123 should match SIC Code 12345
    SIC Code 672 should not match SIC code 345

    """
    sic6_codes_df = pd.read_csv(sic6_codes_csv)

    sic6_codes_df["SIC_regex_code"] = sic6_codes_df["SIC_code"].apply(
        lambda x: r"^" + str(x) + r"\d{0,}$"
    )
    return sic6_codes_df


def sic_matcher(if_sic_code: float, relevant_sic_code_df: pd.DataFrame) -> str:
    """Returns classification of a SIC6 code from InfoGroup if it matches a relevant SIC code

    Args:
      if_sic_code: an SIC6 code from the InfoGroup DataFrame (implement with apply function)
      relevant_sic_code_df: a df of the relevant SIC codes w/ corresponding regex patterns

    Returns:
    String of the classification of that SIC code if there's a match, None otherwise
    """
    # goes through each relevant SIC code and identifies if it matches the input code
    # from the InfoGroup dataset.
    for i in np.arange(0, len(relevant_sic_code_df)):
        regex_code = relevant_sic_code_df.iloc[i].SIC_regex_code
        # if it matches, return the classification associated with that SIC6 code
        if re.match(regex_code, str(if_sic_code)):
            return relevant_sic_code_df.iloc[i].classification
    # none of the relevant SIC codes matched, so return None
    return None


def get_classification(row: pd.Series) -> str:
    """Gets the final classification of the company

    Returns the final classification of the company based on the Primary SIC Code and the SIC
    Code Column.
    Theoretically these should be the same (both fossil fuel, both clean energy, or neither)
    Function should be used with .apply() on a row of the DataFrame.

    Args:
        row: a row of the InfoGroup DataFrame

    Returns:
        the str classification of the company (f or c). if neither SIC code is relevant,
        returns None. if they return different classifications, returns "ambiguous"
    """
    # if the same classification, just return one of them
    if row["classification_primary_code"] == row["classification_code"]:
        return row["classification_primary_code"]
    if (row["classification_primary_code"] is not None) & (
        row["classification_code"] is None
    ):
        return row["classification_primary_code"]
    if (row["classification_primary_code"] is None) & (
        row["classification_code"] is not None
    ):
        return row["classification_code"]
    # if different classifications, return ambiguous
    return "ambiguous"


def prepare_infogroup_data(
    infogroup_csv: str,
    sic6_codes_df: pd.DataFrame,
    output_file_path: str,
    testing: bool = False,
) -> pd.DataFrame:
    """Subsets InfoGroup company data into only relevant CE, FF, and energy companies

    Also transforms Infogroup data into formatting that is compatible with FFF data

    Args:
        infogroup_csv: the InfoGroup csv file
        sic6_codes_df: DataFrame of the relevant SIC6 codes w/ corresponding regex codes
            and descriptions
        output_file_path: the resulting df will be written as a csv to this file path location
        testing: Boolean - True if code is being tested on only several chunks, False if
            whole InfoGroup csv should be used
        chunksize: the number of rows per chunk in the IG dataset (default 10,000 but can be
            changed for testing purposes)
        num_testing_chunks: number of chunks to iterate through when testing = True

    Returns:
        a DataFrame with information for only the relevant companies from the InfoGroup
        dataset that is formatted in the same schema as the aggregated company df for downstream
        use in the pipeline.
        also writes the subsetted DataFrame to an output data file that can be found in
        the Google Drive folder.
    """
    # the set of parent company ids to search in each chunk
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
            "SIC CODE",
            "classification_primary_code",
            "classification_code",
        ],
        index=[],
    )

    # df of potential parent companies throughout chunks
    potential_parent_companies_df = pd.DataFrame()

    cleaned_aggregated_df = pd.DataFrame(COMPANY_CLASSIFICATION_OUTPUT_SCHEMA, index=[])

    counter = 0  # will keep track of which chunk is being processed
    business_data_df = pd.read_csv(
        infogroup_csv, sep=",", header=0, chunksize=10000
    )  # chunksize = 10,000
    for chunk in business_data_df:
        print("processing chunk", counter, "...")

        # applies SIC matcher to each company in the chunk based on both
        # primary SIC code as well as SIC Code (which can be different)
        chunk["classification_primary_code"] = chunk["PRIMARY SIC CODE"].apply(
            lambda x: sic_matcher(x, sic6_codes_df)
        )

        chunk["classification_code"] = chunk["SIC CODE"].apply(
            lambda x: sic_matcher(x, sic6_codes_df)
        )

        # will late drop any rows in chunk that do not have classification
        # (companies that are not classified are not relevant)
        no_classification_rows = chunk[
            (chunk["classification_primary_code"].isna())
            & (chunk["classification_code"].isna())
        ]

        relevant_rows = chunk[
            (chunk["classification_primary_code"].notna())
            | (chunk["classification_code"].notna())
        ]

        # get the potential parent companies from the chunk
        # (companies that do not have a parent company)

        potential_parent_companies = chunk[chunk["PARENT NUMBER"].isna()]

        # and add them to the potential parent company df from prev chunks
        potential_parent_companies_df = pd.concat(
            [potential_parent_companies, potential_parent_companies_df]
        )

        # get all the unique values from parent numbers
        parent_companies_in_chunk = set(relevant_rows["PARENT NUMBER"].unique())

        # after the first chunk, want to see if each parent company is found
        # in any of the chunks, so make a set of these companies to iterate through
        if counter != 0:
            parent_company_ids = parent_companies_in_chunk.union(
                parent_companies_in_chunk, parent_company_ids
            )

        # creating a set of the parent company ids that have been confirmed
        # these ids will be removed for the next chunk iteration
        confirmed_parent_company_ids = set()

        # look at each parent company id and see if it is in the chunk.
        # if so, add this row to the confirmed_parent_company df
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

            # if the parent company id does not have an associated name in the df and it is NOT
            # found within the chunk, add it to the set of parent company IDs to match against
            # other chunks
            else:
                parent_company_ids.add(parent_company_id)

        # removing the ids that have been confirmed in this chunk iteration
        parent_company_ids -= confirmed_parent_company_ids

        chunk = chunk.drop(no_classification_rows.index)

        # get the final classification of the chunk (based on classification of SIC code
        # and SIC6 code)
        chunk["classification"] = chunk.apply(
            lambda row: get_classification(row), axis=1
        )

        # subsetting the chunk by only relevant columns
        chunk = chunk[RELEVANT_INFOGROUP_COLUMNS]

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
            "ABI": "ABI",
        }
        cleaned_chunk = chunk.rename(mapper=column_name_mapper, axis=1)

        # appending chunk to the aggregated output DataFrame
        cleaned_aggregated_df = pd.concat([cleaned_chunk, cleaned_aggregated_df])
        counter += 1
        num_testing_chunks = 4
        if testing & (counter > num_testing_chunks):
            # adding confirmed parent company columns back into DataFrame (ensures that
            # parent companies that are not classified based on sic codes are still in the
            # output DataFrame)
            confirmed_parent_companies_df["classification"] = (
                confirmed_parent_companies_df.apply(
                    lambda row: get_classification(row), axis=1
                )
            )
            confirmed_parent_companies_df = confirmed_parent_companies_df[
                RELEVANT_INFOGROUP_COLUMNS
            ]
            cleaned_parent_companies_df = confirmed_parent_companies_df.rename(
                mapper=column_name_mapper, axis=1
            )

            cleaned_aggregated_df = pd.concat(
                [cleaned_aggregated_df, cleaned_parent_companies_df]
            )
            cleaned_aggregated_df = cleaned_aggregated_df.drop_duplicates()
            cleaned_aggregated_df["unique_id"] = [
                uuid.uuid4() for i in range(len(cleaned_aggregated_df))
            ]
            cleaned_aggregated_df.to_csv(output_file_path, mode="w", index=False)
            return cleaned_aggregated_df

    # adding confirmed parent company columns back into DataFrame (ensures that
    # parent companies that are not classified based on sic codes are still in the
    # output DataFrame)
    confirmed_parent_companies_df["classification"] = (
        confirmed_parent_companies_df.apply(lambda row: get_classification(row), axis=1)
    )
    confirmed_parent_companies_df = confirmed_parent_companies_df[
        RELEVANT_INFOGROUP_COLUMNS
    ]
    cleaned_parent_companies_df = confirmed_parent_companies_df.rename(
        mapper=column_name_mapper, axis=1
    )

    cleaned_aggregated_df = pd.concat(
        [cleaned_aggregated_df, cleaned_parent_companies_df]
    )
    cleaned_aggregated_df = cleaned_aggregated_df.drop_duplicates()
    cleaned_aggregated_df["unique_id"] = [
        uuid.uuid4() for i in range(len(cleaned_aggregated_df))
    ]

    # write cleaned DF to output file in data/raw_classification
    cleaned_aggregated_df.to_csv(output_file_path, mode="w", index=False)

    return cleaned_aggregated_df


def get_infogroup_df(
    sic6_codes_csv: str,
    infogroup_csv: str,
    output_file_path: str,
    cached: bool,
    testing_subset: bool = False,
) -> pd.DataFrame:
    """Returns a DataFrame of all relevant InfoGroup data for use in pipeline

    Args:
      sic6_codes_csv: a csv of relevant SIC6 codes and NCAIS codes and their descriptions
      infogroup_csv: the InfoGroup csv file
      output_file_path: the output df will be written as a csv to this file path
      cached: True if you want to use existing files to bypass creating the InfoGroup data.
        Will return a df of the output file path in this case
      testing_subset: Boolean - True if code is being tested on only several chunks,
        False if whole InfoGroup csv should be used
    Returns:
        a cleaned InfoGroup DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline and the dictionary of ABI codes that
        correspond to parent companies
    """
    # preparing InfoGroup data
    print("preparing InfoGroup data...")
    if cached:
        return pd.read_csv(output_file_path)
    sic6_codes_df = convert_sic_codes_regex(sic6_codes_csv)
    infogroup_df = prepare_infogroup_data(
        infogroup_csv=infogroup_csv,
        sic6_codes_df=sic6_codes_df,
        output_file_path=output_file_path,
        testing=testing_subset,
    )
    return infogroup_df
