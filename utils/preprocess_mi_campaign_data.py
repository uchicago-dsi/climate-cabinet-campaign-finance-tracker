import pandas as pd
import os
from utils.constants import  VALUES_TO_CHECK 

def fix_mi_dataframes(filepath, columns):
    """
    Fixes the bug that corrupts some files by (insert how to fix when the error
    is resolved)
    
    Inputs: 
            filepath (str): filepath to the MI Campaign Data txt file
            columns (lst): list of string names of the campaign data columns 
    
    Returns: dataframe (Pandas DataFrame): 
    """
    pass

    
def read_and_skip_errors(filepath, columns):
    """
    Reads in the MI campaign data and skips the errors, giving a warning only
    
    Inputs: 
            filepath (str): filepath to the MI Campaign Data txt file
            columns (lst): list of string names of the campaign data columns 
    
    Returns: df (Pandas DataFrame): dataframe of the MI campaign data 
    """
    if filepath.endswith('00.txt') or any(year in filepath for year in VALUES_TO_CHECK):
        # MI contribution files that contain 00 or between 1998 and 2003 contain headers, 
        # VALUES_TO_CHECK contsins the years between 1998 and 2003
        df = pd.read_csv(filepath, delimiter="\t", index_col=False, encoding="mac_roman", 
                        usecols=columns, low_memory=False, on_bad_lines='skip')
    else:
        # all other MI contribution files do not contain headers, read in with columns defined
        df = pd.read_csv(filepath, delimiter="\t", index_col=False,  encoding="mac_roman", 
                        header=None, names=columns, low_memory=False, on_bad_lines='skip')
    return df