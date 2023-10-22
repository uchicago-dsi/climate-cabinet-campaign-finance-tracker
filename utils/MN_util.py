from pathlib import Path
import numpy as np
import pandas as pd


def datasets_col_consistent(df_lst):
    ''' 
    Check if a list of DataFrames have the same columns for future merge
    Return: nothing, just print out the result for checking column consistency
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


def standardize_cand_df(df):
    ''' 
    To preprocess all the candidate-recipient contribution datasets such that
    they are consistent with non-candidate recipient dataset
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


def standardize_noncand_df(df):
    ''' 
    To preprocess non-candidate-recipient contribution dataset such that we
    can merge all candidate-recipient and non-candidate-recipient datasets
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
    contribution_df = pd.concat(df_lst, ignore_index=True)
    
    contribution_df['DonationDate'] = pd.to_datetime(contribution_df['DonationDate'])
    contribution_df['DonationYear'] = contribution_df['DonationDate'].dt.year
    contribution_df = contribution_df.sort_values(by='DonationYear', ascending=False)
    
    contribution_df['DonorType'] = contribution_df['DonorType'].replace('b', 'B')
    contribution_df['DonorType'] = contribution_df['DonorType'].replace('u', 'U')
    contribution_df['DonorType'] = contribution_df['DonorType'].replace('s', 'S')
    contribution_df['DonorType'] = contribution_df['DonorType'].replace('c', 'C')
    contribution_df['DonorType'] = contribution_df['DonorType'].replace('l', 'L')
    contribution_df['DonorType'] = contribution_df['DonorType'].replace('i', 'I')
    contribution_df['DonorType'] = contribution_df['DonorType'].replace('h', 'H')
    
    contribution_df['DonationAmount'] = pd.to_numeric(
        contribution_df['DonationAmount'], errors='coerce') 
    contribution_df['InKindDonAmount'] = pd.to_numeric(
        contribution_df['InKindDonAmount'], errors='coerce')
    contribution_df['TotalAmount'] = \
        contribution_df['DonationAmount'] + contribution_df['InKindDonAmount']
    
    return contribution_df