"""Script to combine raw datasets from FFF into one dataset for classification in the pipeline"""

import uuid

import pandas as pd

from utils.constants import COMPANY_CLASSIFICATION_OUTPUT_SCHEMA


def merge_fff_data(fff_data_classification_dict: dict) -> pd.DataFrame:
    """Merge FFF CSV files into one DataFrame

    Args:
        fff_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv where
        'f' is fossil fuel, 'c' is clean energy, 'n' is neutral
        example dict: {FFF_oil: "f", FFF_clean: "c"}

    Returns:
        a DataFrame with the merged CSV files w/ new column

    """
    fff_dfs = []
    # reading in each CSV to df and adding a classification column
    for csv in fff_data_classification_dict:
        fff_df = pd.read_csv(csv)
        # add the classification column
        classification = fff_data_classification_dict[csv]
        fff_df["classification"] = [classification] * len(fff_df)
        fff_dfs.append(fff_df)

    # combining the CSVs bc they have the same structure
    merged_fff_dfs = pd.concat(fff_dfs)

    # rename the columns for consistency with output CSV
    merged_fff_dfs = merged_fff_dfs.rename(
        mapper={
            "Company": "company_name",
            "Symbol": "stock_symbol",
            "Legal name": "legal_name",
        },
        axis=1,
    )
    return merged_fff_dfs


def prepare_fff_data(merged_fff_df: pd.DataFrame) -> pd.DataFrame:
    """Clean the merged FFF DataFrame and apply consistent data schema

    Args:
        merged_fff_df: a DataFrame with the data from all FFF CSVs merged

    Returns:
        a cleaned DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline.
    """
    # establishing schema of outuput DataFrame
    df_schema = COMPANY_CLASSIFICATION_OUTPUT_SCHEMA
    cleaned_aggregated_df = pd.DataFrame(df_schema, index=[])
    merged_fff_df = merged_fff_df.drop_duplicates()
    cleaned_aggregated_df = pd.concat([cleaned_aggregated_df, merged_fff_df])
    cleaned_aggregated_df["unique_id"] = [
        uuid.uuid4() for i in range(len(cleaned_aggregated_df))
    ]
    return cleaned_aggregated_df


def get_fff_df(
    fff_data_classification_dict: dict, file_output_path: str, cached: bool
) -> pd.DataFrame:
    """Returns a DataFrame of all FFF data for use in pipeline

    Args:
        fff_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv where
        'f' is fossil fuel, 'c' is clean energy, 'n' is neutral
        file_output_path: a path to write the resulting df to as a csv
        cached: True if you want to use existing files to bypass creating the InfoGroup data.
        Will return a df of the output file path in this case

    Returns:
        a cleaned FFF DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline.
    """
    print("preparing FFF data...")
    if cached:
        return pd.read_csv(file_output_path)
    merged_fff_data = merge_fff_data(fff_data_classification_dict)
    cleaned_fff_df = prepare_fff_data(merged_fff_data)
    cleaned_fff_df.to_csv(file_output_path, mode="w", index=False)
    return cleaned_fff_df
