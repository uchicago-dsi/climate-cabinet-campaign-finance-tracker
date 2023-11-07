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
    column_mapping = {
        "CandRegNumb": "RegNumb",
        "CommitteeName": "Committee",
        "DonationDate": "Date",
        "DonationAmount": "Amount",
        "InKindDonAmount": "InKindAmount",
        "InKindDescriptionText": "InKindDescription",
    }
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
    column_mapping = {
        "PCFRegNumb": "RegNumb",
        "ETType": "RecipientType",
        "DonationDate": "Date",
        "DonationAmount": "Amount",
        "InKindDonAmount": "InKindAmount",
        "InKindDescriptionText": "InKindDescription",
    }
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
    contribution_df["Date"] = pd.to_datetime(contribution_df["Date"])
    contribution_df["Year"] = contribution_df["Date"].dt.year
    contribution_df = contribution_df.sort_values(by="Year", ascending=False)

    contribution_df["DonorType"] = contribution_df["DonorType"].str.upper()

    contribution_df["Amount"] = pd.to_numeric(
        contribution_df["Amount"], errors="coerce"
    )
    contribution_df["Amount"] = contribution_df["Amount"].fillna(0)

    contribution_df["InKindAmount"] = pd.to_numeric(
        contribution_df["InKindAmount"], errors="coerce"
    )
    contribution_df["InKindAmount"] = contribution_df["InKindAmount"].fillna(0)

    contribution_df["TotalAmount"] = (
        contribution_df["Amount"] + contribution_df["InKindAmount"]
    )

    contribution_df["Year"].fillna(-1, inplace=True)
    contribution_df["RegNumb"].fillna(-1, inplace=True)
    contribution_df["Year"] = contribution_df["Year"].astype(int)
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


def preprocess_general_exp_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses MN general expenditure dataset into a DataFrame.

    Args:
        df (DataFrame): the MN general expenditure DataFrames to preprocess
    Returns:
        DataFrame: Preprocessed MN general expenditure DataFrames
    """

    df_copy = df.copy(deep=True)
    columns_to_keep = [
        'Committee reg num', 'Committee name', 'Entity type', 'Vendor name',
        'Amount', 'Unpaid amount', 'Date', 'Year', 'Purpose', 'Type',
        'In-kind?', 'In-kind descr', 'Affected committee name',
        'Affected committee reg num'
        ]
    column_mapping = {
        'Committee reg num': 'SpenderRegNum', 'Committee name': 'SpenderName',
        'Entity type': 'SpenderType', 'Vendor name': 'VendorName',
        'Unpaid amount': 'UnpaidAmount', 'In-kind descr': 'InKindDescription',
        'Affected committee name': 'AffectedCommitteeName',
        'Affected committee reg num': 'AffectedCommitteeRegNum'}

    df_copy = df_copy[columns_to_keep]
    df_copy.rename(columns=column_mapping, inplace=True)

    return df_copy


def preprocess_independent_exp_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses MN independent expenditure dataset into a DataFrame.

    Args:
        df (DataFrame): the MN independent expenditure DataFrames to preprocess
    Returns:
        DataFrame: Preprocessed MN general expenditure DataFrames
    """

    df_copy = df.copy(deep=True)
    columns_to_keep = [
        'Spender Reg Num', 'Spender', 'Spender type', 'Vendor name', 'Amount',
        'Unpaid amount', 'Date', 'Year', 'Purpose', 'Type', 'In kind?',
        'In kind descr', 'Affected Comte Name', 'Affected Cmte Reg Num']
    column_mapping = {
        'Spender Reg Num': 'SpenderRegNum', 'Spender': 'SpenderName',
        'Spender type': 'SpenderType', 'Vendor name': 'VendorName',
        'Unpaid amount': 'UnpaidAmount', 'In kind?': 'In-kind?',
        'In kind descr': 'InKindDescription',
        'Affected Comte Name': 'AffectedCommitteeName',
        'Affected Cmte Reg Num': 'AffectedCommitteeRegNum'}

    df_copy = df_copy[columns_to_keep]
    df_copy.rename(columns=column_mapping, inplace=True)

    return df_copy


def preprocess_expenditure_df(df_lst: list) -> pd.DataFrame:
    """
    Preprocesses general & individual dfs into a complete MN expenditure df

    Args:
        df_lst (list): a list of MN DataFrames to merge and adjust columns
    Returns:
        DataFrame: the merged and preprocessed expenditure df
    """

    expenditure_df = pd.concat(df_lst, ignore_index=True)

    return expenditure_df


def drop_nonclassifiable_exp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop contributions with zero transaction amount or no spender registration
    number and name

    Args:
        df (DataFrame): MN expenditure DataFrames to drop nonclassifiable data
    Returns:
        DataFrame: the expenditure df without non-classifiable data
    """

    df = df[df["Amount"] != 0]
    df = df.dropna(subset=["SpenderRegNum", "SpenderName"], how="any")
    df = df.reset_index(drop=True)

    return df
