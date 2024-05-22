"""State transformer implementation for Texas (for data in TX_full folder only)"""

import numpy as np
import pandas as pd

from utils.transform.clean import StateTransformer
from utils.transform.constants import TX_IND_FILEPATH

# TX_ORG_FILEPATH, TX_TRA_FILEPATH
from utils.transform.utils import get_full_name


class TexasTransformer(StateTransformer):
    """State transformer implementation for Texas in tx_full folder"""

    name = "Texas"
    stable_id_across_years = True
    entity_name_dictrionary = {}

    def preprocess(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Turns filepath into dataframes

        returns: dataframes of Texas
        """
        raw_ind_df = pd.read_csv(TX_IND_FILEPATH)
        # raw_org_df = pd.read_csv(TX_ORG_FILEPATH)
        # raw_tra_df = pd.read_csv(TX_TRA_FILEPATH)
        # to save more space, I did read the actual org and tra csv
        raw_org_df = pd.DataFrame()
        raw_tra_df = pd.DataFrame()

        return raw_ind_df, raw_org_df, raw_tra_df

    def clean(
        self,
        raw_ind_df: pd.DataFrame,
        raw_org_df: pd.DataFrame,
        raw_tra_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Cleans the dataframes as needed and returns the dataframes

        Cleans the columns, converts dtypes to match database schema, and drops
        rows not representing minimal viable transactions

        Inputs:
            raw_ind_df, raw_org_df, raw_tra_df: Dataframes as outputs from preprocess method.

        Returns: Dataframe
        """
        clean_ind_df = raw_ind_df.copy(deep=True)

        clean_ind_df["FULL_NAME"] = clean_ind_df.apply(
            lambda row: get_full_name(
                row["FIRST_NAME"], row["LAST_NAME"], row["FULL_NAME"]
            ),
            axis=1,
        )
        clean_ind_df = clean_ind_df.fillna("Unknown")

        clean_ind_df["LAST_NAME"] = clean_ind_df["LAST_NAME"].str.lower().str.strip()
        clean_ind_df["FIRST_NAME"] = (
            clean_ind_df["FIRST_NAME"].str.lower().str.strip().str.split().str[0]
        )
        clean_ind_df["FULL_NAME"] = clean_ind_df["FULL_NAME"].str.lower().str.strip()
        clean_ind_df["CITY"] = clean_ind_df["CITY"].str.lower().str.strip()

        return clean_ind_df, raw_org_df, raw_tra_df

    def standardize(
        self, ind_df: pd.DataFrame, org_df: pd.DataFrame, tra_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Standardizes the dataframe into the necessary format for the schema

        Inputs:
            data: a list of 1 or 3 dataframes[ind,org,transaction] as outputted from clean method.

        Returns: a list of dataframes.
        """
        ind_df["address"] = np.nan
        ind_df["party"] = np.nan
        # for data merging use, should be in the clean process but we are not cleaning for the texas data
        ind_df["single_last_name"] = ind_df["LAST_NAME"]

        ind_df["address"] = ind_df.apply(
            lambda row: get_full_name(
                row["ADDRESS_LINE_1"], row["ADDRESS_LINE_2"], row["address"]
            ),
            axis=1,
        )
        ind_df = ind_df.drop(
            [
                "Unnamed: 0.1",
                "Unnamed: 0",
                "ADDRESS_LINE_1",
                "ADDRESS_LINE_2",
                "ENTITY_TYPE_SPECIFIC",
                "ENTITY_TYPE",
                "ORIGINAL_ID",
            ],
            axis=1,
        )

        ind_df = ind_df.rename(
            columns={
                "ID": "id",
                "ENTITY_TYPE_GENERAL": "entity_type",
                "FULL_NAME": "full_name",
                "LAST_NAME": "last_name",
                "FIRST_NAME": "first_name",
                "CITY": "city",
                "STATE": "state",
                "ZIP_CODE": "zip",
                "EMPLOYER": "company",
                "OCCUPATION": "occupation",
                ## these are the columns that are not in the current ind_table format
                "PHONE_NUMBER": "phone_number",
                "OFFICE_SOUGHT": "office_sought",
                "DISTRICT": "district",
            }
        )

        return ind_df, org_df, tra_df

    def create_tables(
        self, ind_df: pd.DataFrame, org_df: pd.DataFrame, tra_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Creates the Individuals, Organizations, and Transactions tables

        Inputs:
            data: a list of 1 or 3 dataframes as output from standardize method.

        Returns: (individuals_table, organizations_table, transactions_table)
                    tuple containing the tables as defined in database schema
        """
        return ind_df, org_df, tra_df

    def clean_state(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Runs the StateCleaner pipeline returning a tuple of cleaned dataframes

        Returns: use preprocess, clean, standardize, and create_tables methods
        to output (individuals_table, organizations_table, transactions_table)
        as defined in database schema

        Inputs:
            filepaths_list: list of absolute filepaths to relevant state data.
                required naming conventions, order, and extensions
                defined per state.

        Returns: cleans the state and returns the standardized Inidividuals,
        Organizations, and list of Transactions tables in the order:
        [ind->ind, ind->org, org->ind, org->org] tables in a tuple
        """
        raw_ind_df, raw_org_df, raw_tra_df = self.preprocess()
        clean_ind_df, clean_org_df, clean_tra_df = self.clean(
            raw_ind_df, raw_org_df, raw_tra_df
        )
        sd_ind_df, sd_org_df, sd_tra_df = self.standardize(
            clean_ind_df, clean_org_df, clean_tra_df
        )
        ind_df, org_df, tra_df = self.create_tables(sd_ind_df, sd_org_df, sd_tra_df)

        return ind_df, org_df, tra_df
