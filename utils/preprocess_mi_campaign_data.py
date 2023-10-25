import ipywidgets as widgets
import pandas as pd
import plotly.express as px
from IPython.display import clear_output

from utils.constants import VALUES_TO_CHECK


def fix_mi_dataframes(filepath, columns):
    """
    Fixes the bug that corrupts some files by (insert how to fix when the error
    is resolved)

    Inputs: filepath (str): filepath to the MI Campaign Data txt file
            columns (lst): list of string names of the campaign data columns

    Returns: dataframe (Pandas DataFrame):
    """
    pass

def read_expenditure_data(filepath, columns):
    """ Reads in the MI expenditure data
    
    Inputs: 
            filepath (str): filepath to the MI Expenditure Data txt file
            columns (lst): list of string names of the campaign data columns
    
    Returns: df (Pandas DataFrame): dataframe of the MI Expenditure data
    """
    if filepath.endswith("txt"):
        df = pd.read_csv(filepath, delimiter="\t", index_col=False,
                        usecols=columns, encoding="mac_roman", low_memory=False)
    
    return df

def read_and_skip_errors(filepath, columns):
    """
    Reads in the MI campaign data and skips the errors, giving a warning only

    Inputs: filepath (str): filepath to the MI Campaign Data txt file
            columns (lst): list of string names of the campaign data columns

    Returns: df (Pandas DataFrame): dataframe of the MI campaign data
    """
    if filepath.endswith("00.txt"):
        # MI files that contain 00 or between 1998 and 2003 contain headers
        # VALUES_TO_CHECK contains the years between 1998 and 2003
        df = pd.read_csv(
            filepath,
            delimiter="\t",
            index_col=False,
            encoding="mac_roman",
            usecols=columns,
            low_memory=False,
            on_bad_lines="skip",
        )
    elif any(year in filepath for year in VALUES_TO_CHECK):
        df = pd.read_csv(
            filepath,
            delimiter="\t",
            index_col=False,
            encoding="mac_roman",
            usecols=columns,
            low_memory=False,
            on_bad_lines="skip",
        )
    else:
        # all other MI contribution files do not contain headers
        # read in with columns defined
        df = pd.read_csv(
            filepath,
            delimiter="\t",
            index_col=False,
            encoding="mac_roman",
            header=None,
            names=columns,
            low_memory=False,
            on_bad_lines="skip",
        )
    return df


def plot_year_contribution_types(year, merged_campaign_dataframe):
    """
    Plots contributions per year by type

    Inputs: year (int): year to plot

    Return: None
    """
    filtered_data = merged_campaign_dataframe[
        merged_campaign_dataframe["doc_stmnt_year"] == year
    ]
    filtered_data = filtered_data["contribtype"].value_counts().reset_index()
    filtered_data.columns = ["Cont_Type", "Count"]

    fig = px.bar(
        filtered_data,
        x="Cont_Type",
        y="Count",
        title=f"Polital Contributions Count from {year} by Type",
        text="Count",
    )
    fig.update_layout(
        xaxis_title="Contribution Types",
        yaxis_title=f"{year} Count",
    )
    fig.show()


def plot_committee_types_by_year(year, merged_campaign_dataframe):
    """
    Plots committees contributions per year by type

    Inputs: year (int): year to plot

    Return: None
    """
    filtered_data = merged_campaign_dataframe[
        merged_campaign_dataframe["doc_stmnt_year"] == year
    ]
    filtered_data = filtered_data[
        filtered_data["com_type"] != "MENOMINEE COUNTY DEMOCRATIC PARTY"
    ]
    filtered_data = filtered_data["com_type"].value_counts().reset_index()
    filtered_data.columns = ["Committee_Type", "Count"]

    fig = px.bar(
        filtered_data,
        x="Committee_Type",
        y="Count",
        title=f"Donations by Committee Type from {year}",
        text="Count",
    )
    fig.update_layout(xaxis_title="Committee Types")
    fig.update_layout(yaxis_title=f"{year} Count")
    fig.show()


def update_plots(year_selector, merged_campaign_dataframe):
    """
    Interactively update the plots using the Ipywidget

    Inputs: year_selector (object): widget dropdown for 1999 - 2023
            merged_campaign_dataframe (dataframe): MI campaign data

    Returns: None
    """
    selected_year = year_selector.value
    output = widgets.Output()
    with output:
        clear_output(wait=True)
        plot_committee_types_by_year(selected_year, merged_campaign_dataframe)
        plot_year_contribution_types(selected_year, merged_campaign_dataframe)
