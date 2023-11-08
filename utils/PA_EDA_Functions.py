import pandas as pd
import plotly.express as px
import sys
sys.path.append("/home/alankagiri/2023-fall-clinic-climate-cabinet")
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

    dir = data_filepath.split("/")
    file_type = dir[len(dir) - 1]

    if "contrib" in file_type:
        df["TOTAL_CONT_AMT"] = (df["CONT_AMT_1"] + df["CONT_AMT_2"] 
                                + df["CONT_AMT_3"])
        df["YEAR"] = year
        if "TIMESTAMP" in df.columns:
            df = df.drop(columns="TIMESTAMP")

        df.rename(
            columns={
                "ADDRESS_1": "CONTRIB_ADDRESS_1",
                "ADDRESS_2": "CONTRIB_ADDRESS_2",
                "CITY": "CONTRIB_CITY",
                "STATE": "CONTRIB_STATE",
                "ZIPCODE": "CONTRIB_ZIPCODE",
                "CYCLE": "CONTRIB_CYCLE_CODE",
            },
            inplace=True,
        )
        return df

    elif "filer" in file_type:
        df = df.drop(columns="YEAR")
        df.rename(
            columns={
                "CYCLE": "FILER_CYCLE_CODE",
                "ADDRESS_1": "FILER_ADDRESS_1",
                "ADDRESS_2": "FILER_ADDRESS_2",
                "CITY": "FILER_CITY",
                "STATE": "FILER_STATE",
                "ZIPCODE": "FILER_ZIPCODE",
            },
            inplace=True,
        )
        return df
    elif "expense" in file_type:
        return df
    else:
        raise ValueError("This function is currently formatted for filer, \
                        expense, and contributor datasets. Make sure your data \
                        is from these sources.")


def top_n_recipients(df: pd.DataFrame, num_recipients: int) -> object:
    """given a dataframe, retrieves the top n recipients of that year based on
    contributions and returns a table
    Args:
        df: a pandas DataFrame with a contributions column
        
        num_recipients: an integer specifying how many recipients are desired.
        If this value is larger than the possible amount of recipients, then all
        recipients are returned instead.
    Returns:
        A pandas table (object)"""
    recipients = (
        df.groupby(["FILER_NAME"])
        .agg({"TOTAL_CONT_AMT": sum})
        .sort_values(by="TOTAL_CONT_AMT", ascending=False)
    )

    if num_recipients > len(recipients):
        return recipients
    else:
        return recipients.head(num_recipients)


def top_n_contributors(df: pd.DataFrame, num_contributors: int) -> object:
    """given a dataframe, retrieves the top n contributors of that year based on
    contributions and returns a table

    Args:
        df: a pandas DataFrame with a contributions column
        
        num_contributors: an integer specifying how many contributors are
        desired. If this value is larger than the possible amount of
        contributors, then all contributors are returned instead.
    Returns:
        a pandas table (object)"""

    contributors = (
        df.groupby(["CONTRIBUTOR"])
        .agg({"TOTAL_CONT_AMT": sum})
        .sort_values(by="TOTAL_CONT_AMT", ascending=False)
    )

    if num_contributors > len(contributors):
        return contributors
    else:
        return contributors.head(num_contributors)


def merge_same_year_datasets(
    cont_file: pd.DataFrame, filer_file: pd.DataFrame
) -> pd.DataFrame:
    """merges the contributor and filer datasets from the same year using the
    unique filerID
    Args:
        cont_file: The contributor dataset
         
        filer_file: the filer dataset from the same year as the cont_file.
    Returns
        The merged pandas dataframe
    """
    merged_df = pd.merge(cont_file, filer_file, how="left", on="FILER_ID")
    merged_df.rename(
        columns={
            "REPORTER_ID_x": "CONTRIB_REPORTER_ID",
            "REPORTER_ID_y": "FILER_REPORTED_ID",
        },
        inplace=True,
    )
    return merged_df


def merge_all_datasets(datasets: list) -> pd.DataFrame:
    """concatenates datasets from different years into one super dataset
    Args:
        datasets: a list of datasets
    
    Returns:
        The merged pandas dataframe
    """
    return pd.concat(datasets)


def group_filerType_Party(dataset: pd.DataFrame) -> object:
    """takes a dataset and returns a grouped table highlighting the kinds
    of people who file the campaign reports (FilerType Key -> 1:Candidate,
    2:Committee, 3:Lobbyist.) and their political party affiliation

    Args: 
        dataset: a pandas DataFrame containing columns and values from the filer
        dataset.

    Returns: 
        A table object"""
    return dataset.groupby(["FILER_TYPE", "PARTY"]).agg({"TOTAL_CONT_AMT": sum})


def plot_recipients_by_office(merged_dataset: pd.DataFrame) -> object:
    """returns a table and plots a bargraph of data highlighting the amount of
    contributions each statewide race received over the years

    Args:
        merged_dataset: A (merged) pandas DataFrame containing columns and
        values from the contributor and filer datasets.

    Return:
        A table object"""

    recep_per_office = (
        merged_dataset.groupby(
            ["OFFICE"]).agg({"TOTAL_CONT_AMT": sum}).reset_index()
    )
    recep_per_office.replace(
        {"OFFICE": const.PA_OFFICE_ABBREV_DICT}, inplace=True)

    fig = px.bar(
        data_frame=recep_per_office,
        x="OFFICE",
        y="TOTAL_CONT_AMT",
        title="PA Contributions Received by Office-Type From 2018-2023",
        labels={"TOTAL_CONT_AMT": "Total Contribution Amount"},
    )
    fig.show()

    return recep_per_office


def compare_cont_by_donorType(merged_dataset: pd.DataFrame) -> object:
    """returns a table and plots a barplot highlighting the annual contributions
    campaign finance report-filers received based on whether they are candidates
    , committees, or lobbyists.

    Args: 
        merged_dataset: A (merged) pandas DataFrame containing columns from both
        the filer and contributor datasets.
    Return: 
        A pandas DataFrame
    """
    pd.set_option("display.float_format", "{:.2f}".format)
    cont_by_donor = (
        merged_dataset.groupby(["YEAR", "FILER_TYPE"])
        .agg({"TOTAL_CONT_AMT": sum})
        .reset_index()
    )
    cont_by_donor["FILER_TYPE"] = cont_by_donor["FILER_TYPE"].map(
        const.PA_FILER_ABBREV_DICT
    )

    fig = px.bar(
        data_frame=cont_by_donor,
        x="YEAR",
        y="TOTAL_CONT_AMT",
        color="FILER_TYPE",
        title="PA Recipients of Annual Contributions (2018 - 2023)",
        labels={
            "TOTAL_CONT_AMT": "Total Contribution Amount",
            "FILER_TYPE": "Type of Filer",
        },
    )
    fig.show()
    return cont_by_donor