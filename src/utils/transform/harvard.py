"""Election result transformer implementation for Harvard dataset""" 

import uuid

import numpy as np
import pandas as pd
from utils.ind_transform import standardize_individual_names
from utils.transform.clean import (
    ElectionResultTransformer,
)
from utils.transform.constants import HV_FILEPATH, HV_INDIVIDUAL_COLS, INDIVIDUALS_FILEPATH


class HarvardTransformer(ElectionResultTransformer):
    """Based on the StateTransformer abstract class and cleans Harvard data"""

    def preprocess(self) -> pd.DataFrame:
        """Turns filepath into a dataframe

        The raw dataverse file is in .dta format and we need to
        turn it into a pandas readable file

        returns: a dataframe
        """
        raw_df = pd.read_stata(HV_FILEPATH)

        return raw_df
    

    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        """Cleans the dataframe as needed and returns the dataframe

        Cleans the columns, converts dtypes to match database schema, and drops
        rows not representing minimal viable transactions

        Inputs:
            data: Dataframe as output from preprocess method.

        Returns: Dataframe 
        """
        clean_df = data.copy(deep=True)
        clean_df = clean_df[HV_INDIVIDUAL_COLS] 
        clean_df = clean_df[clean_df["year"] <=2016 & clean_df["year"] >=2014]
        clean_df = clean_df[~clean_df["cand"].str.startswith("namemissing")] 
        clean_df = self.match_individual(clean_df)
        clean_df["cand"] = np.where(
            clean_df["v19_20171211"].notna(), 
            clean_df["v19_20171211"], 
            clean_df["cand"]
        )

        return clean_df

    def standardize(self, data: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the dataframe into the necessary format for the schema

        Maps data types and column names as defined in schema

        Inputs:
            data: dataframe as outputted from clean method.

        Returns: Dataframe 
        """
        data["state"] = data["sab"].str.upper()
        data["last_name"] = data["last"].str.upper()
        data["first_name"] = data["first"].str.upper()
        data["full_name"] = data["cand"].str.lower()
        data["full_name"] = data["full_name"].astype(str)[
            data["full_name"].notna()
        ]
        data = data.apply(standardize_individual_names, axis = 1)

        party_map = {"d": "democratic", "r": "republican", "partymiss": np.nan}
        data["party"] = data["party"].map(party_map)

        return data
    
    
    def create_table(self, data: pd.DataFrame) -> pd.DataFrame:
        """Creates the election result table and match the data with individual data

        Inputs:
            data: Dataframe as output from standardize method.

        Returns:
            a table as defined in database schema
        """
        final_table = data.copy()

        final_table = self.create_election_result_uuid(final_table)

        return final_table
    
    # Shall I put this function in this file or in transform file?
    def create_election_result_uuid(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add uuid to each election result record
        
        Inputs: 
            data: standarized data frame
            
        Returns:
            A dataframe with case_id column added
        """
        data["case_id"] = [uuid.uuid4() for _ in range(len(data))]
        
        return data
    
    def match_individual(self, election_data: pd.DataFrame) -> pd.DataFrame:
        """Include only the individual names with existed data and find id
        
        Inputs: election result cleaned data
        
        Returns: election result with a new column of individual uuid
        """
        # Edit the pathfile
        transform_ind_df = pd.read_csv(INDIVIDUALS_FILEPATH)
        merged_data = election_data.merge(
            transform_ind_df[["full_name", "id"]], 
            left_on = "cand", 
            right_on="full_name", 
            how="inner")
        return merged_data

    
    def clean_state(self) -> pd.DataFrame:
        """Runs the ElectionResultCleaner pipeline returning a cleaned dataframes

        Returns: cleans the state and returns the standardized table showing 
        the election results.
        """
        raw_df = self.preprocess()
        clean_df = self.clean(raw_df)
        standardized_df = self.standardize(clean_df)
        return self.create_table(standardized_df)

