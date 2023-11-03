import ipywidgets as widgets
import pandas as pd
import plotly.express as px
from IPython.display import clear_output


def fix_mi_dataframes(filepath, columns):
    """
    Fixes the bug that corrupts some files by (insert how to fix when the error
    is resolved)

    Inputs: filepath (str): filepath to the MI Campaign Data txt file
            columns (lst): list of string names of the campaign data columns

    Returns: dataframe (Pandas DataFrame):
    """
    pass


def read_expenditure_data(filepath: str, columns: list) -> pd.DataFrame:
    """Reads in the MI expenditure data

    Inputs:
            filepath (str): filepath to the MI Expenditure Data txt file
            columns (lst): list of string names of the campaign data columns

    Returns: df (Pandas DataFrame): dataframe of the MI Expenditure data
    """
    if filepath.endswith("txt"):
        df = pd.read_csv(
            filepath,
            delimiter="\t",
            index_col=False,
            usecols=columns,
            encoding="mac_roman",
            low_memory=False,
        )

    return df


def read_and_skip_errors(filepath: str, columns: list) -> pd.DataFrame:
    """Reads in the MI campaign data and skips the errors

    Inputs: filepath (str): filepath to the MI Campaign Data txt file
            columns (lst): list of string names of the campaign data columns

    Returns: df (Pandas DataFrame): dataframe of the MI campaign data
    """
    if filepath.endswith("00.txt"):
        # MI files that contain 00 or between 1998 and 2003 contain headers
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


def plot_year_contribution_types(
    year: int, merged_campaign_dataframe: pd.DataFrame
) -> None:
    """Plots contributions per year by type

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


def plot_committee_types_by_year(
    year: int, merged_campaign_dataframe: pd.DataFrame
) -> None:
    """Plots committees contributions per year by type

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


def update_plots(
    year_selector: object, merged_campaign_dataframe: pd.DataFrame
) -> None:
    """Interactively update the MI Contribution plots using the Ipywidget

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


def plot_expenditure_committee_types_by_year(
    year: int, merged_campaign_dataframe: pd.DataFrame
) -> None:
    """Plots committees contributions per year by type

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
        title=f"Expenditures by Committee Type from {year}",
        text="Count",
    )
    fig.update_layout(xaxis_title="Committee Types")
    fig.update_layout(
        yaxis_title=f"{year} Count", xaxis={"categoryorder": "total ascending"}
    )

    fig.show()


def plot_year_schedule_types(
    year: int, merged_campaign_dataframe: pd.DataFrame
) -> None:
    """Plots committees contributions per year by type

    Inputs: year (int): year to plot

    Return: None
    """
    filtered_data = merged_campaign_dataframe[
        merged_campaign_dataframe["doc_stmnt_year"] == year
    ]
    filtered_data = filtered_data[
        filtered_data["com_type"] != "MENOMINEE COUNTY DEMOCRATIC PARTY"
    ]
    filtered_data = filtered_data["schedule_desc"].value_counts().reset_index()
    filtered_data.columns = ["Schedule_Type", "Count"]

    fig = px.bar(
        filtered_data,
        x="Schedule_Type",
        y="Count",
        title=f"Expenditures by Schedule Type from {year}",
        text="Count",
    )
    fig.update_layout(
        xaxis_title="Schedule Types",
        yaxis_title=f"{year} Count",
        xaxis={"categoryorder": "total ascending"},
    )
    fig.show()


def update_expenditure_plots(
    year_selector: object, merged_campaign_dataframe: pd.DataFrame
) -> None:
    """Interactively update the MI Expenditure plots using the Ipywidget

      Inputs: year_selector (object): widget dropdown for 2018 - 2023
              merged_campaign_dataframe (dataframe): MI expenditure data

    Returns: None
    """
    selected_year = year_selector.value
    output = widgets.Output()
    with output:
        clear_output(wait=True)
        plot_expenditure_committee_types_by_year(
            selected_year, merged_campaign_dataframe
        )
        plot_year_schedule_types(selected_year, merged_campaign_dataframe)
