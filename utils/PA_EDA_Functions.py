import pandas as pd

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
        dtype=const.cont_year_dtypes,
    )
    df["TotalContAmt"] = df["ContAmt1"] + df["ContAmt2"] + df["ContAmt3"]
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
        dtype=const.filer_year_dypes,
    )
    return df


def top_n_recipients(df: pd.DataFrame, num_recipients: int) -> object:
    """given a dataframe, retrieves the top n recipients of that
    year based on contributions and returns a table

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
    """given a dataframe, retrieves the top n contributors of that
    year based on contributions and returns a table

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


def merge_datasets(cont_file: pd.DataFrame, filer_file: pd.DataFrame) -> pd.DataFrame:
    """merges the contributor and filer datasets using the unique filerID
    Args:
        The contributor and filer datasets of a given year
    Returns
        The merged pandas dataframe
    """
    merged_df = pd.merge(cont_file, filer_file, how="left", on="FilerID")
    return merged_df


def group_filerType_Party(merged_dataset: pd.DataFrame) -> object:
    """this func takes a merged dataset and returns a grouped table highlighting
    the kinds of people who file the campaign reports (FilerType Key:
    1:Candidate, 2:Committee, 3:Lobbyist.) and their political part
    affiliation"""
    return merged_dataset.groupby(["FilerType", "Party"]).agg({"TotalContAmt": sum})


def plot_recipients_byOffice(merged_dataset: pd.DataFrame) -> object:
    """plots a bargraph showing how much money was directed towards a state
    position in a given year"""
    group = (
        merged_dataset.groupby(["Office"])
        .agg({"ContAmt1": sum})
        .sort_values(by="ContAmt1", ascending=True)
    )
    pd.options.plotting.backend = "plotly"
    group.plot.barh(
        title="Contributions Received per Office",
        color="g",
        figsize=(10, 5),
        ylabel="Contributions Received ($)",
        logx=True,
    )
    return group
