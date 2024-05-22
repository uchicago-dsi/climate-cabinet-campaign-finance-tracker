"""Script to combine raw datasets from FFF into one dataset for classification in the pipeline"""

# TODO: #92 Make orgs classification script into more well-defined pipeline
import uuid

import pandas as pd

from utils.constants import company_classification_output_schema


def merge_FFF_data(FFF_data_classification_dict: dict) -> pd.DataFrame:
    """Merge FFF CSV files into one DataFrame

    Args:
        FFF_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv where
        'f' is fossil fuel, 'c' is clean energy, 'n' is neutral
        example dict: {FFF_oil: "f", FFF_clean: "c"}

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
    df_schema = company_classification_output_schema
    cleaned_aggregated_df = pd.DataFrame(df_schema, index=[])
    merged_FFF_df = merged_FFF_df.drop_duplicates()
    cleaned_aggregated_df = pd.concat([cleaned_aggregated_df, merged_FFF_df])
    cleaned_aggregated_df["unique_id"] = [
        uuid.uuid4() for i in range(len(cleaned_aggregated_df))
    ]
    return cleaned_aggregated_df


def get_FFF_df(
    FFF_data_classification_dict: dict, file_output_path: str, cached: bool
) -> pd.DataFrame:
    """Returns a DataFrame of all FFF data for use in pipeline

    Args:
        FFF_data_classification_dict: a dictionary where a key is a FFF csv str and
        the value is the corresponding classification for that csv where
        'f' is fossil fuel, 'c' is clean energy, 'n' is neutral
        file_output_path: a path to write the resulting df to as a csv
        cached: True if you want to use existing files to bypass creating the InfoGroup data. Will return a df of the output
        file path in this case

    Returns:
        a cleaned FFF DataFrame that is formatted in the same schema as the aggregated
        company df for downstream use in the pipeline.
    """
    print("preparing FFF data...")
    if cached:
        return pd.read_csv(file_output_path)
    merged_FFF_data = merge_FFF_data(FFF_data_classification_dict)
    cleaned_FFF_df = prepare_FFF_data(merged_FFF_data)
    cleaned_FFF_df.to_csv(file_output_path, mode="w", index=False)
    return cleaned_FFF_df
