import pandas as pd


def datasets_col_consistent(df_lst: list):
    """
    Checks if a list of MN DataFrames have the same columns/features

    Args:
        df_lst (list): a list of MN DataFrames whose columns will be checked
    Returns:
        Nothing, print out the checking result for column consistency
    """

    previous_columns = df_lst[0].columns
    consistent_col_count = 1

    for df in df_lst[1:]:
        if not (df.columns == previous_columns).all():
            print("Columns not consistent across races")
        else:
            consistent_col_count += 1
    if consistent_col_count == len(df_lst):
        print("All dfs have consistent columns")


def preprocess_candidate_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses all MN candidate-recipient contribution dfs.

    Args:
        df (DataFrame): the MN DataFrames to preprocess
    Returns:
        DataFrame: Preprocessed MN contribution df with candidate recipients
    """

    df_copy = df.copy(deep=True)
    columns_to_keep = [
        "OfficeSought",
        "CandRegNumb",
        "CandFirstName",
        "CandLastName",
        "CommitteeName",
        "DonationDate",
        "DonorType",
        "DonorName",
        "DonationAmount",
        "InKindDonAmount",
        "InKindDescriptionText",
    ]
    df_copy = df_copy[columns_to_keep]
    column_mapping = {"CandRegNumb": "RegNumb", "CommitteeName": "Committee"}
    df_copy.rename(columns=column_mapping, inplace=True)
    df_copy["RecipientType"] = "Candidate"

    return df_copy


def preprocess_noncandidate_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the MN non-candidate-recipient contribution df.

    Args:
        df (DataFrame): the MN DataFrames to preprocess
    Returns:
        DataFrame: Preprocessed contribution df with non-candidate recipients
    """

    df_copy = df.copy(deep=True)
    columns_to_keep = [
        "PCFRegNumb",
        "Committee",
        "ETType",
        "DonationDate",
        "DonorType",
        "DonorName",
        "DonationAmount",
        "InKindDonAmount",
        "InKindDescriptionText",
    ]
    df_copy = df_copy[columns_to_keep]
    column_mapping = {"PCFRegNumb": "RegNumb", "ETType": "RecipientType"}
    df_copy.rename(columns=column_mapping, inplace=True)

    return df_copy


def preprocess_contribution_df(df_lst: list) -> pd.DataFrame:
    """
    Preprocesses separate dfs into a complete contribution df for MN

    Args:
        df_lst (list): a list of MN DataFrames to merge and adjust columns
    Returns:
        DataFrame: the merged and preprocessed contribution df
    """

    contribution_df = pd.concat(df_lst, ignore_index=True)
    contribution_df["DonationDate"] = pd.to_datetime(
        contribution_df["DonationDate"])
    contribution_df["DonationYear"] = contribution_df["DonationDate"].dt.year
    contribution_df = contribution_df.sort_values(by="DonationYear",
                                                  ascending=False)

    contribution_df["DonorType"] = contribution_df["DonorType"].str.upper()

    contribution_df["DonationAmount"] = pd.to_numeric(
        contribution_df["DonationAmount"], errors="coerce"
    )
    contribution_df["DonationAmount"] = \
        contribution_df["DonationAmount"].fillna(0)

    contribution_df["InKindDonAmount"] = pd.to_numeric(
        contribution_df["InKindDonAmount"], errors="coerce"
    )
    contribution_df["InKindDonAmount"] = \
        contribution_df["InKindDonAmount"].fillna(0)

    contribution_df["TotalAmount"] = (
        contribution_df["DonationAmount"] + contribution_df["InKindDonAmount"]
    )

    contribution_df["DonationYear"].fillna(-1, inplace=True)
    contribution_df["RegNumb"].fillna(-1, inplace=True)
    contribution_df["DonationYear"] = \
        contribution_df["DonationYear"].astype(int)
    contribution_df["RegNumb"] = contribution_df["RegNumb"].astype(int)

    return contribution_df


def drop_nonclassifiable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop contributions with zero transaction amount or no donor registration
    number, or no donor name

    Args:
        df (DataFrame): MN contribution DataFrames to drop nonclassifiable data
    Returns:
        DataFrame: the contribution df without non-classifiable data
    """

    df = df[df["TotalAmount"] != 0]
    df = df.dropna(subset=["RegNumb", "DonorName"], how="any")
    df = df.reset_index(drop=True)

    return df
