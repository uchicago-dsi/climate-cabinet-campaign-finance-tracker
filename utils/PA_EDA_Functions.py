import pandas as pd
import plotly.express as px
import sys
sys.path.append('/home/alankagiri/2023-fall-clinic-climate-cabinet')
from utils import constants as const


def assign_col_names(filepath: str, year: int) -> list:
    """Assigns the right column names to the right datasets.

    Args:
        the location of the file, and the year from which the data originates
    Returns:
        the proper column names for the dataset
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

def initialize_PA_dataset(data_filepath:str, year:int)->pd.DataFrame:
    """initializes the PA data appropriately based on whether the data contains
    filer, contributor, or expense information
    
    Args: the filepath to the actual dataframe and the year from which the data
    originates
    Returns: the dataframe
    """
    df = pd.read_csv(
        data_filepath,
        names=assign_col_names(data_filepath,year),
        sep=",",
        encoding="latin-1",
        on_bad_lines="warn")
    
    dir = data_filepath.split("/")
    file_type = dir[len(dir) - 1]    

    if "contrib" in file_type:
        df["TotalContAmt"] = df["ContAmt1"] + df["ContAmt2"] + df["ContAmt3"]
        df["EYear"] = year
        if 'Timestamp' in df.columns:
            df = df.drop(columns="Timestamp")
        
        df.rename(columns={'EYear':'Year',
        'Address1':'Contributor Address1',
        'Address2':'Contributor Address2',
        'City':'Contributor City',
        'State':'Contributor State',
        'Zipcode':'Contributor Zipcode',
        'Cycle':'Contributor Cycle Code'
        }, inplace=True)
        return df
    
    elif "filer" in file_type:
        df = df.drop(columns="EYear")
        df.rename(columns={'Cycle':'Filer Cycle Code',
        'Address1':'Filer Address1',
        'Address2':'Filer Address2',
        'City':'Filer City',
        'State':'Filer State',
        'Zipcode':'Filer Zipcode'}, inplace=True)
        return df
    
    #elif "expense" in file_type:

    else: return df

def top_n_recipients(df: pd.DataFrame, num_recipients: int) -> object:
    """given a dataframe, retrieves the top n recipients of that year based on
    contributions and returns a table
    Args:
        a pandas DataFrame and the number of recipients
    Returns:
        A pandas table (object)"""
    recipients = (
        df.groupby(["FilerName"])
        .agg({"TotalContAmt": sum})
        .sort_values(by="TotalContAmt", ascending=False)
    )

    if num_recipients > len(recipients):
        return recipients
    else:
        return recipients.head(num_recipients)


def top_n_contributors(df: pd.DataFrame, num_contributors: int) -> object:
    """given a dataframe, retrieves the top n contributors of that year based on
    contributions and returns a table

    Args:
        a pandas DataFrame and the number of recipients
    Returns:
        a pandas table (object)"""

    contributors = (
        df.groupby(["Contributor"])
        .agg({"TotalContAmt": sum})
        .sort_values(by="TotalContAmt", ascending=False)
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
        The contributor and filer datasets of a given year
    Returns
        The merged pandas dataframe
    """
    merged_df = pd.merge(cont_file, filer_file, how="left", on="FilerID")
    merged_df.rename(columns={'EYear':'Year',
    'ReporterID_x':'Contributor ReporterID',
    'ReporterID_y':'FilerReporterID',

    }, inplace=True)
    return merged_df


def merge_all_datasets(datasets: list) -> pd.DataFrame:
    """concatenates datasets from different years into one super dataset
    Args:
        a list of datasets
    Returns
        The merged pandas dataframe
    """
    return pd.concat(datasets)


def group_filerType_Party(merged_dataset: pd.DataFrame) -> object:
    """takes a merged dataset and returns a grouped table highlighting the kinds
    of people who file the campaign reports (FilerType Key -> 1:Candidate,
    2:Committee, 3:Lobbyist.) and their political party affiliation

    Args: a pandas DataFrame
    Returns: A table object"""
    return merged_dataset.groupby(["FilerType", "Party"]).agg({"TotalContAmt": sum})


def plot_recipients_by_office(merged_dataset: pd.DataFrame) -> object:
    """returns a table and plots a bargraph of data highlighting the amount of
    contributions each statewide race received over the years

    Args: pandas DataFrame
    Return A table object"""

    recep_per_office = (
        merged_dataset.groupby(["Office"]).agg({"ContAmt1": sum}).reset_index()
    )
    recep_per_office.replace({"Office":const.PA_OFFICE_ABBREV_DICT},inplace=True)
    #recep_per_office["Office"] = recep_per_office["Office"].fillna(
    #    const.PA_OFFICE_ABBREV_DICT["MISC"])

    fig = px.bar(
        data_frame=recep_per_office,
        x="Office",
        y="ContAmt1",
        title="PA Contributions Received by Office-Type From 2018-2023",
        labels={"ContAmt1":"Total Contribution Amount"}
    )
    fig.show()

    return recep_per_office


def compare_cont_by_donorType(merged_dataset: pd.DataFrame) -> object:
    """returns a table and plots a barplot highlighting the annual contributions
    campaign finance report-filers received based on whether they are candidates
    . committees, or lobbyists.

    Args: pandas DataFrame
    Return: pandas DataFrame
    """
    pd.set_option('display.float_format', '{:.2f}'.format)
    cont_by_donor = (
        merged_dataset.groupby(["Year", "FilerType"])
        .agg({"TotalContAmt": sum})
        .reset_index()
    )
    cont_by_donor["FilerType"] = cont_by_donor["FilerType"].map(const.PA_FILER_ABBREV_DICT)
    #cont_by_donor.style.format(precision=9, thousands=",",decimal=".")

    fig = px.bar(
        data_frame=cont_by_donor,
        x="Year",
        y="TotalContAmt",
        color="FilerType",
        title="PA Recipients of Annual Contributions (2018 - 2023)",
        labels={"TotalContAmt":"Total Contribution Amount",
                "FilerType":"Type of Filer"}
    )
    fig.show()
    return cont_by_donor