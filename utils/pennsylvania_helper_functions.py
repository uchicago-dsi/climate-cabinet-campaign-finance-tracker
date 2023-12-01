# import sys

import pandas as pd
import uuid
import numpy as np
import plotly.express as px
import uuid
# sys.path.append("/home/alankagiri/2023-fall-clinic-climate-cabinet")
from utils import constants as const


def assign_col_names(filepath: str, year: int) -> list:
    """Assigns the right column names to the right datasets.

    Args:
        filepath: the path in which the data is stored/located.

        year: to make parsing through the data more manageable, the year from
        which the data originates is also taken.

    Returns:
        a list of the appropriate column names for the dataset
    """
    dir = filepath.split("/")
    file_type = dir[len(dir) - 1]

    if "contrib" in file_type:
        if year < 2022:
            return const.PA_CONT_COLS_NAMES_PRE2022
        else:
            return const.PA_CONT_COLS_NAMES_POST2022
    elif "filer" in file_type:
        if year < 2022:
            return const.PA_FILER_COLS_NAMES_PRE2022
        else:
            return const.PA_FILER_COLS_NAMES_POST2022
    elif "expense" in file_type:
        if year < 2022:
            return const.PA_EXPENSE_COLS_NAMES_PRE2022
        else:
            return const.PA_EXPENSE_COLS_NAMES_POST2022


def replace_id_with_uuid(df:pd.DataFrame, col1:str, col2:str)-> tuple[dict, pd.DataFrame]:
    """Creates a dictionary whose keys are generated UUIDs that map to values
    corresponding to unique IDs from the donor and recipient IDs columns in df
    
    Args:
        A pandas dataframe with at least two columns (col1, col1)
        col1, col2: columns of df that should have IDs
    Returns:
        A tuple whose first value is the modified df, where the IDs have been
        replaced with the UUIDS, and a dictionary correspondings to the UUIDs as
        keys and the original IDs from col1 and col2 as the values
    """
    # a set is used because there could be IDs in the donor column that also
    # appear in the recipient column due to concatenation, and I want to keep
    # the IDs unique throughout
    ids_1 = set(df[col1])
    ids_2= set(df[col2])
    unique_ids = list(ids_1.union(ids_2))

    with_uuid = []
    for id in unique_ids:
        with_uuid.append([id,str(uuid.uuid4())])

    mapped_dict = {lst[0]: (lst[1]) for lst in with_uuid}
    df[col1] = df[col1].map(mapped_dict)
    df[col2] = df[col2].map(mapped_dict)
    mapped_dict = {value: key for key, value in mapped_dict.items()}
    return mapped_dict, df


def classify_contributor(donor: str) -> str:
    """Takes a string input and compares it against a list of identifiers most
    commonly associated with organizations/corporations/PACs, and classifies the
    string input as belong to an individual or organization

    Args:
        contributor: a string
    Returns:
        string "ORGANIZATION" or "INDIVIDUAL" depending on the classification of
        the parameter
    """
    split = donor.split()
    loc = 0
    while loc < len(split):
        if split[loc].upper() in const.PA_ORGANIZATION_IDENTIFIERS:
            return "Organization"
        loc += 1
    return "Individual"


def pre_process_contributor_dataset(df: pd.DataFrame):
    """pre-processes a contributor dataset by sifting through the columns and
    keeping the relevant columns for EDA and AbstractStateCleaner purposes

    Args:
        df: the contributor dataset

    Returns:
        a pandas dataframe whose columns are appropriately formatted.
    """
    df["AMOUNT"] = df["CONT_AMT_1"] + df["CONT_AMT_2"] + df["CONT_AMT_3"]
    df["RECIPIENT_ID"] = df["RECIPIENT_ID"].astype("str")
    df["DONOR"] = df["DONOR"].astype("str")
    df["DONOR"] = df["DONOR"].str.title()
    df["DONOR_TYPE"] = df["DONOR"].apply(classify_contributor)
    df.drop(
        columns={
            "ADDRESS_1",
            "ADDRESS_2",
            "CITY",
            "STATE",
            "ZIPCODE",
            "OCCUPATION",
            "E_NAME",
            "E_ADDRESS_1",
            "E_ADDRESS_2",
            "E_CITY",
            "E_STATE",
            "E_ZIPCODE",
            "SECTION",
            "CYCLE",
            "CONT_DESCRIP",
            "CONT_DATE_1",
            "CONT_AMT_1",
            "CONT_DATE_2",
            "CONT_AMT_2",
            "CONT_DATE_3",
            "CONT_AMT_3",
        },
        inplace=True,
    )

    if "TIMESTAMP" in df.columns:
        df.drop(columns={"TIMESTAMP", "REPORTER_ID"}, inplace=True)
        df["DONOR"] = df["DONOR"].apply(lambda x: str(x).title())

    return df


def pre_process_filer_dataset(df: pd.DataFrame):
    """pre-processes a filer dataset by sifting through the columns and
    keeping the relevant columns for EDA and AbstractStateCleaner purposes

    Args:
        df: the filer dataset

    Returns:
        a pandas dataframe whose columns are appropriately formatted.
    """
    df["RECIPIENT_ID"] = df["RECIPIENT_ID"].astype("str")
    df.drop(
        columns={
            "YEAR",
            "CYCLE",
            "AMEND",
            "TERMINATE",
            "DISTRICT",
            "ADDRESS_1",
            "ADDRESS_2",
            "CITY",
            "STATE",
            "ZIPCODE",
            "COUNTY",
            "PHONE",
            "BEGINNING",
            "MONETARY",
            "INKIND",
        },
        inplace=True,
    )
    if "TIMESTAMP" in df.columns:
        df.drop(columns={"TIMESTAMP", "REPORTER_ID"}, inplace=True)

    df.drop_duplicates(subset=["RECIPIENT_ID"], inplace=True)
    df["RECIPIENT_TYPE"] = df.RECIPIENT_TYPE.map(const.PA_FILER_ABBREV_DICT)
    df["RECIPIENT"] = df["RECIPIENT"].apply(lambda x: str(x).title())
    return df


def pre_process_expense_dataset(df: pd.DataFrame):
    """pre-processes an expenditure dataset by sifting through the columns and
    keeping the relevant columns for EDA and AbstractStateCleaner purposes

    Args:
        df: the expenditure dataset

    Returns:
        a pandas dataframe whose columns are appropriately formatted.
    """
    df["DONOR_ID"] = df["DONOR_ID"].astype("str")
    df.drop(
        columns={
            "EXPENSE_CYCLE",
            "EXPENSE_ADDRESS_1",
            "EXPENSE_ADDRESS_2",
            "EXPENSE_CITY",
            "EXPENSE_STATE",
            "EXPENSE_ZIPCODE",
            "EXPENSE_DATE",
        },
        inplace=True,
    )
    if "EXPENSE_REPORTER_ID" in df.columns:
        df.drop(columns={"EXPENSE_TIMESTAMP", "EXPENSE_REPORTER_ID"}, inplace=True)
    df["PURPOSE"] = df["PURPOSE"].apply(lambda x: str(x).title())
    df["RECIPIENT"] = df["RECIPIENT"].apply(lambda x: str(x).title())
    return df


def initialize_PA_dataset(data_filepath: str, year: int) -> pd.DataFrame:
    """initializes the PA data appropriately based on whether the data contains
    filer, contributor, or expense information

    Args:
        data_filepath: the path in which the data is stored/located.

        year: the year from which the data originates

    Returns:
        a pandas dataframe whose columns are appropriately formatted, and
        any dirty rows with inconsistent columns names dropped.
    """

    df = pd.read_csv(
        data_filepath,
        names=assign_col_names(data_filepath, year),
        sep=",",
        encoding="latin-1",
        on_bad_lines="warn",
    )

    df["YEAR"] = year
    dir = data_filepath.split("/")
    file_type = dir[len(dir) - 1]

    if "contrib" in file_type:
        return pre_process_contributor_dataset(df)

    elif "filer" in file_type:
        return pre_process_filer_dataset(df)

    elif "expense" in file_type:
        return pre_process_expense_dataset(df)

    else:
        raise ValueError(
            "This function is currently formatted for filer, \
            expense, and contributor datasets. Make sure your data \
            is from these sources."
        )

def merge_contrib_filer_datasets(
    cont_file: pd.DataFrame, filer_file: pd.DataFrame
) -> pd.DataFrame:
    """merges the contributor and filer datasets from the same year using the
    unique filerID
    Args:
        cont_file: The contributor dataset

        filer_file: the filer dataset from the same year as the contributor
        file.
    Returns
        The merged pandas dataframe
    """
    merged_df = pd.merge(
        cont_file, filer_file, how="left", on="RECIPIENT_ID")
    return merged_df

def merge_expend_filer_datasets(
    expend_file: pd.DataFrame, filer_file: pd.DataFrame
) -> pd.DataFrame:
    """merges the expenditure and filer datasets from the same year using the
    unique filerID
    Args:
        expend_file: The expenditure dataset

        filer_file: the filer dataset from the same year as the expenditure file
    Returns
        The merged pandas dataframe
    """
    merged_df = pd.merge(
        expend_file, filer_file, 
        left_on="DONOR_ID", 
        right_on="RECIPIENT_ID").drop("RECIPIENT_ID",axis=1)
    return merged_df


def merge_all_datasets(datasets: list) -> pd.DataFrame:
    """concatenates datasets from different years into one super dataset
    Args:
        datasets: a list of datasets

    Returns:
        The merged pandas dataframe
    """
    return pd.concat(datasets)

def format_contrib_data_for_concat(df: pd.DataFrame) -> pd.DataFrame:
    """ Reformartes the merged contributor-filer dataset such that it has the 
    same columns as the merged expenditure-filer dataset so that concatenation
    can occur

    Args:
        The merged contributor-filer dataset

    Returns:
        A new dataframe with the appropriate column formatting for concatenation
    """
    df["DONOR_ID"] = np.nan
    df["DONOR_PARTY"] = np.nan
    df["DONOR_OFFICE"] = np.nan
    df["PURPOSE"] = "Donation"
    columns = df.columns.to_list()
    columns.sort()
    df = df.loc[:,columns]
    return df


def format_expend_data_for_concat(df: pd.DataFrame)-> pd.DataFrame:
    """ Reformartes the merged expenditure-filer dataset such that it has the 
    same columns as the merged contributor-filer dataset so that concatenation
    can occur

    Args:
        The merged expenditure-filer dataset

    Returns:
        A new dataframe with the appropriate column formatting for concatenation
    """
    df["RECIPIENT_ID"] = np.nan
    df.rename(columns = {"RECIPIENT_x":"RECIPIENT",
                         "RECIPIENT_y":"DONOR",
                         "RECIPIENT_TYPE": "DONOR_TYPE",
                         "RECIPIENT_PARTY":"DONOR_PARTY",
                         "RECIPIENT_OFFICE":"DONOR_OFFICE"}, inplace=True)
    df["RECIPIENT_TYPE"] = np.nan
    df["RECIPIENT_OFFICE"] = np.nan
    df["RECIPIENT_PARTY"] = np.nan
    columns = df.columns.to_list()
    columns.sort()
    df = df.loc[:,columns]
    return df

def combine_contributor_expenditure_datasets(
        contrib_ds:list[pd.DataFrame], 
        filer_ds:list[pd.DataFrame], 
        expend_ds:list[pd.DataFrame])-> pd.DataFrame:
    """This function takes datasets with information from the contributor,
    filer, and expenditure datasets in each given year, merges the contributor
    and expenditure datasets with pertinent information from the filer dataset,
    and concatenates the 3 datasets into 1 dataset with.

    Args:
        3 datasets: contributor, filer, and expenditure datasets. Each of the
        datasets is a list of dataframes, with each entry in the dataframes
        being a given file from a select year

    Returns:
        A concatenated dataframe with transaction information, contributor
        information, and recipient information.
    """
    merged_cont_datasets_per_yr = []
    merged_exp_dataset_per_yr = []
    
    for i in range(len(contrib_ds)):
        cont_merged = merge_contrib_filer_datasets(contrib_ds[i],filer_ds[i])
        expend_merged = merge_expend_filer_datasets(expend_ds[i],filer_ds[i])
        merged_cont_datasets_per_yr.append(cont_merged)
        merged_exp_dataset_per_yr.append(expend_merged)
    
    contrib_filer_info = format_contrib_data_for_concat(merge_all_datasets(merged_cont_datasets_per_yr))
    expend_filer_info = format_expend_data_for_concat(merge_all_datasets(merged_exp_dataset_per_yr))
    return merge_all_datasets([contrib_filer_info,expend_filer_info])

def output_ID_mapping(dictionary:dict, df:pd.DataFrame):
    pass

def split_dataframe_into_tables(df:pd.DataFrame)->tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    pass