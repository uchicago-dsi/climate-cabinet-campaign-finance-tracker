import pandas as pd


def datasets_col_consistent(df_lst):
    ''' 
    Checks if a list of DataFrames have the same columns/features

    Args:
        df_lst (list): a list of DataFrames whose columns will be checked
    Returns: 
        Nothing, print out the checking result for column consistency
    '''

    previous_columns = df_lst[0].columns
    consistent_col_count = 1

    for df in df_lst[1:]:
        if not (df.columns == previous_columns).all():
            print('Columns not consistent across races')
        else:
            consistent_col_count += 1
    if consistent_col_count == len(df_lst):
        print('All dfs have consistent columns')
 

def standardize_candidate_df(df):
    ''' 
    Preprocesses all candidate-recipient contribution dfs. The input DataFrame
    should have the following columns: recipient type, the office type (race) and 
    candidate's last and first name, candidate's registration number, committee 
    name, and donation information such as donor type and name, date, and amount
    which are of object types

    Args:
        df (DataFrame): the DataFrames to preprocess
    Returns: 
        DataFrame: Preprocessed contribution df with candidate recipients
    '''

    df_copy = df.copy(deep=True)
    columns_to_keep = ['OfficeSought', 'CandRegNumb', 'CandFirstName', 
                'CandLastName', 'CommitteeName', 'DonationDate',
                'DonorType', 'DonorName', 'DonationAmount',
                'InKindDonAmount', 'InKindDescriptionText']
    df_copy = df_copy[columns_to_keep]
    column_mapping = {'CandRegNumb': 'RegNumb', 'CommitteeName': 'Committee'}
    df_copy.rename(columns=column_mapping, inplace=True)
    df_copy['RecipientType'] = 'Candidate'

    return df_copy


def standardize_noncandidate_df(df):
    '''
    Preprocesses the non-candidate-recipient contribution df. The input DataFrame
    should have the following columns: recipient type and subtype, registration 
    number, committee, donor name and type, donation date and amount which are
    of object types.

    Args:
        df (DataFrame): the DataFrames to preprocess
    Returns:
        DataFrame: Preprocessed contribution df with non-candidate recipients
    '''

    df_copy = df.copy(deep=True)
    columns_to_keep = ['PCFRegNumb', 'Committee', 'ETType', 'DonationDate',
                    'DonorType', 'DonorName', 'DonationAmount', 
                    'InKindDonAmount', 'InKindDescriptionText']
    df_copy = df_copy[columns_to_keep]
    column_mapping = {'PCFRegNumb': 'RegNumb', 'ETType': 'RecipientType'}
    df_copy.rename(columns=column_mapping, inplace=True)

    return df_copy


def preprocess_contribution_df(df_lst):
    ''' 
    Preprocesses separate dfs into a complete contribution df. The input list of
    DataFrames should have same column types checked by datasets_col_consistent
    and including columns of DonationDate, DonationAmount and InKindDonAmount

    Args:
        df_lst (list): a list of DataFrames to merge and adjust columns
    Returns: 
        DataFrame: the merged and preprocessed contribution df
    '''


    contribution_df['DonationDate'] = pd.to_datetime(contribution_df['DonationDate'])
    contribution_df['DonationYear'] = contribution_df['DonationDate'].dt.year
    contribution_df = contribution_df.sort_values(by='DonationYear', ascending=False)

    contribution_df['DonorType'] = contribution_df['DonorType'].str.upper()

    contribution_df['DonationAmount'] = pd.to_numeric(
        contribution_df['DonationAmount'], errors='coerce')
    contribution_df['DonationAmount'] = contribution_df['DonationAmount'].fillna(0)

    contribution_df['InKindDonAmount'] = pd.to_numeric(
        contribution_df['InKindDonAmount'], errors='coerce')
    contribution_df['InKindDonAmount'] = contribution_df['InKindDonAmount'].fillna(0)

    contribution_df['TotalAmount'] = \
        contribution_df['DonationAmount'] + contribution_df['InKindDonAmount']

    return contribution_df