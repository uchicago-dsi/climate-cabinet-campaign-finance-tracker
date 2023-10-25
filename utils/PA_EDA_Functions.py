import pandas as pd
import dask.dataframe as dd

# column names:
# in 2020 two additional columns were added to the original list of columns:
# ReporterId & Timestamp. Thus depending on the year there might be 24 or 26
# fields

cont_cols_names_pre2022 = [
    "FilerID",
    "EYear",
    "Cycle",
    "Section",
    "Contributor",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "occupation",
    "Ename",
    "EAddress1",
    "EAddress2",
    "ECity",
    "EState",
    "EZipcode",
    "ContDate1",
    "ContAmt1",
    "ContDate2",
    "ContAmt2",
    "ContDate3",
    "ContAmt3",
    "ContDesc",
]

cont_cols_names_post22 = [
    "FilerID",
    "ReporterID",
    "Timestamp",
    "EYear",
    "Cycle",
    "Section",
    "Contributor",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "occupation",
    "Ename",
    "EAddress1",
    "EAddress2",
    "ECity",
    "EState",
    "EZipcode",
    "ContDate1",
    "ContAmt1",
    "ContDate2",
    "ContAmt2",
    "ContDate3",
    "ContAmt3",
    "ContDesc",
]

filer_cols_names_pre2022 = [
    "FilerID",
    "EYear",
    "Cycle",
    "Amend",
    "Terminate",
    "FilerType",
    "FilerName",
    "Office",
    "District",
    "Party",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "County",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND",
]

filer_cols_names_post2022 = [
    "FilerID",
    "ReporterID",
    "Timestamp",
    "EYear",
    "Cycle",
    "Amend",
    "Terminate",
    "FilerType",
    "FilerName",
    "Office",
    "District",
    "Party",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "County",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND",
]


def specify_col_names(filepath: str, year: int) -> list:
    """
    since an incongruency exists for how many columns some years have, this
    function will ensure that the right columns fields are associated with the
    right year.

    Args: the location of the file, and the year from which the data originates
    Returns: the proper column names for the dataset
    """
    words = filepath.split("/")
    file_type = words[len(words) - 1]

    if "contrib" in file_type:
        if year < 2022:
            return cont_cols_names_pre2022
        else:
            return cont_cols_names_post22
    elif "filer" in file_type:
        if year < 2022:
            return filer_cols_names_pre2022
        else:
            return filer_cols_names_post2022


def initialize_cont_year(cont_filepath: str, year: int) -> pd.DataFrame:
    """this function initializes a contributor .csv dataset, and converts
    the filerID column into an int to make merges down the line more accessible.
    args: the filepath to the actual dataframe
    returns: the dataframe"""
    df = dd.read_csv(
        cont_filepath,
        names=specify_col_names(cont_filepath, year),
        sep=",",
        encoding="latin-1",
        on_bad_lines="warn",
        dtype={
            "FilerID": str,
            "ReporterID": str,
            "Timestamp": object,
            "EYear": int,
            "Cycle": object,
            "Section": object,
            "Contributor": str,
            "Address1": str,
            "Address2": str,
            "City": str,
            "State": str,
            "Zipcode": object,
            "occupation": str,
            "Ename": str,
            "EAddress1": str,
            "EAddress2": str,
            "ECity": str,
            "EState": str,
            "EZipcode": str,
            "ContDate1": object,
            "ContAmt1": float,
            "ContDate2": object,
            "ContAmt2": float,
            "ContDate3": object,
            "ContAmt3": float,
            "ContDesc": object,
        },
    )
    # df.FilerID = dd.to_numeric(df.FilerID, errors="coerce")
    # downcast="signed" is not supported by Dask
    df["TotalContAmt"] = df["ContAmt1"] + df["ContAmt2"] + df["ContAmt3"]
    return df


def initialize_filer_year(filer_filepath: str, year: int) -> pd.DataFrame:
    """this functions similarly to the initialize_cont_year method"""
    df = dd.read_csv(
        filer_filepath,
        names=specify_col_names(filer_filepath, year),
        sep=",",
        encoding="latin-1",
        on_bad_lines="warn",
        dtype={
            "FilerID": str,
            "ReporterID": str,
            "Timestamp": object,
            "EYear": int,
            "Cycle": int,
            "Amend": object,
            "Terminate": object,
            "FilerType": object,
            "FilerName": str,
            "Office": str,
            "District": str,
            "Party": str,
            "Address1": str,
            "Address2": str,
            "City": str,
            "State": str,
            "Zipcode": object,
            "County": object,
            "PHONE": object,
            "BEGINNING": object,
            "MONETARY": object,
            "INKIND": object,
        },
    )
    # df.FilerID = dd.to_numeric(df.FilerID, errors="coerce")
    # downcast="signed" is not supported by Dask
    return df


def top_n_recipients(df: pd.DataFrame, num_recipients: int) -> object:
    """given a dataframe, this function retrieves the top n recipients of that
    year based on contributions and returns a table"""
    return (
        df.groupby(["FilerName"])
        .agg({"TotalContAmt": sum})
        .sort_values(by="TotalContAmt", ascending=False)
        .head(num_recipients)
    )


def top_n_contributors(df: pd.DataFrame, num_contributors: int) -> object:
    """this functions similary to top_n_recipients, except it returns the total
    base on the contributors"""
    return (
        df.groupby(["Contributor"])
        .agg({"TotalContAmt": sum})
        .sort_values(by="TotalContAmt", ascending=False)
        .head(num_contributors)
    )


def columns_info(df: pd.DataFrame):
    """a basic function to provide preliminary insights on the dataframe, namely
    info regarding the type of data stored in a column and the # of null values
    """
    for column in df.columns:
        print(
            "Column: {}, NaN: {}, type: {}".format(
                column, df[column].isnull().sum(), df[column].dtype
            )
        )


def merge_datasets(cont_file: pd.DataFrame, filer_file: pd.DataFrame) -> pd.DataFrame:
    """in order for EDA to be useful, the contributor and filer datasets need to
    be merged for a given year. This function joins the two using the unique
    filerID to crossmatch"""
    merged_df = dd.merge(cont_file, filer_file, how="left", on="FilerID")
    return merged_df


def group_filerType_Party(merged_dataset: pd.DataFrame) -> object:
    """this func takes a merged dataset and returns a grouped table highlighting
    the kinds of people who file the campaign reports (FilerType Key:
    1:Candidate, 2:Committee, 3:Lobbyist.) and their political part
    affiliation"""
    return merged_dataset.groupby(["FilerType", "Party"]).agg({"TotalContAmt": sum})


def plot_recipients_byOffice(merged_dataset: pd.DataFrame) -> object:
    # Office Abbreviations:
    # GOV: Governor, LTG: Liutenant Gov, ATT: Attorney General,
    # AUD: Auditor General, TRE: State Treasurer
    # SPM: Justice of the Supreme Crt, SPR: Judge of the Superior Crt,
    # CCJ: Judge of the CommonWealth Crt
    # CPJ: Judge of the Crt of Common Pleas, MCJ: Judge of the Municipal Crt,
    # TCJ: Judge of the Traffic Crt,
    # STS: Senator (General Assembly), STH: Rep (General Assembly),
    # OTH: Other candidates for local offices
    """plots a bargraph showing how much money was directed towards a state
    position in a given year"""
    group = (
        merged_dataset.groupby(["Office"])
        .agg({"ContAmt1": sum})
        .sort_values(by="ContAmt1", ascending=True)
    )
    group.plot.barh(
        title="Contributions Received per Office",
        color="g",
        figsize=(10, 5),
        ylabel="Contributions Received ($)",
        logx=True,
    )
    return group
