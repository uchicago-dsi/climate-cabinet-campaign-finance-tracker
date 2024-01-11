import pandas as pd
import plotly.express as px


def read_expenditure_data(filepath: str, columns: list[str]) -> pd.DataFrame:
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


def read_contribution_data(filepath: str, columns: list[str]) -> pd.DataFrame:
    """Reads in the MI campaign data and skips the errors

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


# NOTE: DEPRECIATED
def plot_year_contribution_types(
    all_year_contribution_dataframe: pd.DataFrame,
) -> None:
    """Plots contributions per year by type

    Inputs: year: year to plot
            all_year_contribution_dataframe: dataframe containing the 2018
            to 2023 Michigan campaign contributon data

    Return: None
    """

    contribution_type_by_year = (
        all_year_contribution_dataframe.groupby("doc_stmnt_year")["contribtype"]
        .value_counts()
        .reset_index()
    )

    fig = px.bar(
        contribution_type_by_year,
        x="doc_stmnt_year",
        y="count",
        color="contribtype",
        barmode="stack",
        labels={
            "doc_stmnt_year": "Year",
            "count": "Number of Contrbution Types",
            "contribtype": "Contribution Type",
        },
        title="Michigan Campaign Contribution Types by Year",
    )
    fig.show()


# NOTE: DEPRECIATED
def plot_committee_types_by_year(
    all_year_contribution_dataframe: pd.DataFrame,
) -> None:
    """Plots committees contributions per year by type

    Inputs: year : year to plot
            all_year_contribution_dataframe: dataframe containing the 2018
            to 2023 Michigan campaign contributon data

    Return: None
    """
    committee_names = {
        "DIS": "District Party Committee",
        "STA": "State Party Committee",
        "BAL": "Ballot Question Committee",
        "COU": "County Party Committee",
        "POL": "Political Action Committee",
        "GUB": "Gubernatorial Committee",
        "CAN": "Candidate Committee",
        "IND": "Independent Political Action Committee",
    }
    contribution_committee_type_by_year = (
        all_year_contribution_dataframe.groupby("doc_stmnt_year")["com_type"]
        .value_counts()
        .reset_index()
    )
    contribution_committee_type_by_year[
        "com_type_full"
    ] = contribution_committee_type_by_year["com_type"].map(committee_names)

    fig = px.bar(
        contribution_committee_type_by_year,
        x="doc_stmnt_year",
        y="count",
        color="com_type_full",
        barmode="stack",
        labels={
            "doc_stmnt_year": "Year",
            "count": "Number of Contributions",
            "com_type_full": "Committee Type",
        },
        title="Michigan Campaign Contributions Committee Types by Year",
    )
    fig.show()


# NOTE: DEPRECIATED
def plot_expenditure_committee_types_by_year(
    all_year_expenditure_dataframe: pd.DataFrame,
) -> None:
    """Plots committees contributions per year by type

    Inputs: year: year to plot
            all_year_expenditure_dataframe: dataframe containing the 2018
            to 2023 Michigan campaign expenditure data

    Return: None
    """
    committee_names = {
        "DIS": "District Party Committee",
        "STA": "State Party Committee",
        "BAL": "Ballot Question Committee",
        "COU": "County Party Committee",
        "POL": "Political Action Committee",
        "GUB": "Gubernatorial Committee",
        "CAN": "Candidate Committee",
        "IND": "Independent Political Action Committee",
    }
    expenditure_committee_type_by_year = (
        all_year_expenditure_dataframe.groupby("doc_stmnt_year")["com_type"]
        .value_counts()
        .reset_index()
    )
    expenditure_committee_type_by_year[
        "com_type_full"
    ] = expenditure_committee_type_by_year["com_type"].map(committee_names)

    fig = px.bar(
        expenditure_committee_type_by_year,
        x="doc_stmnt_year",
        y="count",
        color="com_type_full",
        barmode="stack",
        labels={
            "doc_stmnt_year": "Year",
            "count": "Number of Expenditures",
            "com_type_full": "Committee Type",
        },
        title="Michigan Campaign Expenditure Committee Types by Year",
    )

    fig.show()


# NOTE: DEPRECIATED
def plot_year_schedule_types(
    all_year_expenditure_dataframe: pd.DataFrame,
) -> None:
    """Plots committees contributions per year by type

    Inputs: year: year to plot
            all_year_expenditure_dataframe: dataframe containing the 2018
            to 2023 Michigan campaign expenditure data

    Return: None
    """
    expenditure_schedule_types_by_year = (
        all_year_expenditure_dataframe.groupby("doc_stmnt_year")[
            "schedule_desc"
        ]
        .value_counts()
        .reset_index()
    )

    fig = px.bar(
        expenditure_schedule_types_by_year,
        x="doc_stmnt_year",
        y="count",
        color="schedule_desc",
        barmode="stack",
        labels={
            "doc_stmnt_year": "Year",
            "count": "Number of Expenditures",
            "schedule_desc": "Schedule Description",
        },
        title="Michigan Campaign Expenditure Schedule Types by Year",
    )
    fig.show()


# NOTE: DEPRECIATED
def create_all_plots(
    all_year_expenditure_dataframe: pd.DataFrame,
    all_year_contribution_dataframe: pd.DataFrame,
):
    """Creates all of the plots for the MI expenditure and Contribution data

    Inputs:
            all_year_expenditure_dataframe: dataframe containing the 2018
            to 2023 Michigan campaign expenditure data
            all_year_contribution_dataframe: dataframe containing the 2018
            to 2023 Michigan campaign contributon data

    Returns: None
    """
    plot_year_schedule_types(all_year_expenditure_dataframe)
    plot_expenditure_committee_types_by_year(all_year_expenditure_dataframe)
    plot_committee_types_by_year(all_year_contribution_dataframe)
    plot_year_contribution_types(all_year_contribution_dataframe)
