import pandas as pd
import plotly.express as px

from utils import PA_constants as const


def assign_col_names(filepath: str, year: int) -> list:
    """Assigns the right column names to the right datasets.

    Args:
        the location of the file, and the year from which the data originates
    Returns:
        the proper column names for the dataset
    """
    words = filepath.split("/")
    file_type = words[len(words) - 1]

    if "contrib" in file_type:
        if year < 2022:
            return const.cont_cols_names_pre2022
        else:
            return const.cont_cols_names_post22
    elif "filer" in file_type:
        if year < 2022:
            return const.filer_cols_names_pre2022
        else:
            return const.filer_cols_names_post2022


def initialize_PA_cont_year(cont_filepath: str, year: int) -> pd.DataFrame:
    """Initializes a contributor .csv dataset.

    Args: the filepath to the actual dataframe
    Returns: the dataframe"""
    df = pd.read_csv(
        cont_filepath,
        names=assign_col_names(cont_filepath, year),
        sep=",",
        encoding="latin-1",
        on_bad_lines="warn",
    )
    df["TotalContAmt"] = df["ContAmt1"] + df["ContAmt2"] + df["ContAmt3"]
    df["EYear"] = year
    return df


def initialize_PA_filer_year(filer_filepath: str, year: int) -> pd.DataFrame:
    """Initializes the filer dataset
    Args:
        the filepath to the actual dataframe
    Returns:
        the dataframe"""
    df = pd.read_csv(
        filer_filepath,
        names=assign_col_names(filer_filepath, year),
        sep=",",
        encoding="latin-1",
        on_bad_lines="warn",
    )
    df = df.drop(columns="EYear")
    return df


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


def merge_sameYear_datasets(
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


def plot_recipients_byOffice(merged_dataset: pd.DataFrame) -> object:
    """returns a table and plots a bargraph of data highlighting the amount of
    contributions each statewide race received over the years

    Args: pandas DataFrame
    Return A table object"""

    recep_per_office = (
        merged_dataset.groupby(["Office"]).agg({"ContAmt1": sum}).reset_index()
    )
    recep_per_office["Office"] = recep_per_office["Office"].map(const.office_abb_dict)
    recep_per_office["Office"] = recep_per_office["Office"].fillna(
        const.office_abb_dict["MISC"]
    )

    fig = px.bar(
        data_frame=recep_per_office,
        x="Office",
        y="ContAmt1",
        title="Contributions received by Office type from 2018-2023",
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
    cont_by_donor = (
        merged_dataset.groupby(["EYear", "FilerType"])
        .agg({"TotalContAmt": sum})
        .reset_index()
    )
    cont_by_donor["FilerType"] = cont_by_donor["FilerType"].map(const.filer_abb_dict)

    fig = px.bar(
        data_frame=cont_by_donor,
        x="EYear",
        y="TotalContAmt",
        color="FilerType",
        title="Recipients of Annual Contributions",
    )
    fig.show()
    return cont_by_donor
