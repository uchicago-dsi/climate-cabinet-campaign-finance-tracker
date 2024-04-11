"""State transformer implementation for Harvard dataset
the dataset aims to provide more information upon the individual dataset
in terms of the individual'party, if_senate, and election outcome
"""  # noqa: D205
# TODO: remove noqa and format docstring

import numpy as np
import pandas as pd

from utils.transform.clean import StateTransformer
from utils.transform.constants import HV_FILEPATH, HV_INDIVIDUAL_COLS


# TODO: #95 Make HarvardTransformer independent of StateTransformer
# StateTransformer is built for campaign finance not election results
# code can remain largely the same, but move it out of the the transform package
# and rename methods to be more clear
class HarvardTransformer(StateTransformer):
    """Based on the StateTransformer abstract class and cleans Harvard data"""

    name = "Harvard"
    stable_id_across_years = False

    def clean_state(self) -> pd.DataFrame:
        """Calls the other methods in order

        Calling for clean, standardize, and create table functions

        args: the raw document

        returns: one table for candidate returns record.

        """
        raw_df = self.preprocess()
        cand_df = self.clean(raw_df)
        return cand_df

    def preprocess(self) -> pd.DataFrame:
        """Turns filepath into a dataframe

        The raw dataverse file is in .dta format and we need to
        turn it into a pandas readable file

        returns: a dataframe
        """
        raw_df = pd.read_stata(HV_FILEPATH)

        return raw_df

    def clean(self, hv_df: pd.DataFrame) -> pd.DataFrame:
        """Clean the candicate infromation

        Args:
            hv_df(Dataframe): raw Harvard dataset file
        Returns:
            Dataframe: cleaned dataframe
        """
        raw_df = hv_df.copy(deep=True)

        raw_df = raw_df[HV_INDIVIDUAL_COLS]
        # unidentifiable -> delete
        raw_df = raw_df[~raw_df["cand"].str.startswith("namemissing")]

        ind_df = pd.DataFrame()
        ind_df["state"] = raw_df["sab"].str.upper()
        ind_df["last_name"] = raw_df["last"].str.upper()
        ind_df["first_name"] = raw_df["first"].str.upper()
        ind_df["entity_type"] = "candidate"
        ind_df["occupation"] = np.where(raw_df["sen"] == 1, "senate", np.nan)
        # ind_df["partyz"] -> the meaning of numers?
        # ind_df city or county?
        ind_df["cname_city"] = raw_df["cname"].str.upper()
        raw_df["cand"] = np.where(
            raw_df["v19_20171211"].notna(), raw_df["v19_20171211"], raw_df["cand"]
        )
        ind_df["full_name"] = raw_df["cand"].str.lower()
        party_map = {"d": "democratic", "r": "republican", "partymiss": np.nan}

        ind_df["party"] = raw_df["party"].map(party_map)

        ind_df["outcome"] = raw_df["outcome"]

        columns_to_add = ["id", "company", "address", "zip"]
        for column in columns_to_add:
            ind_df[column] = np.nan

    def create_table(self, hv_df: pd.DataFrame) -> pd.DataFrame:
        """Crates the Candidate table

        Inputs:
            a dataframe
        Returns:
            a dataframe of which the column names could match
            the names of the other three tables
        """
        raw_df = hv_df.copy(deep=True)
        return raw_df
