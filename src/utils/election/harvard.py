"""Election result transformer implementation for Harvard dataset"""

import uuid

import numpy as np
import pandas as pd
from utils.election.clean import (
    ElectionResultTransformer,
)
from utils.election.constants import (
    HV_FILEPATH,
    HV_INDIVIDUAL_COLS,
    party_map,
    type_mapping,
)
from utils.election.utils import extract_first_name


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
        year_start = 2014
        year_end = 2016
        clean_df = clean_df[
            (clean_df["year"] <= year_end) & (clean_df["year"] >= year_start)
        ]

        clean_df = clean_df[~(clean_df["last"] == "scattering")]
        # the data is cleaned in the original dataset -- if last or first name is missing
        # that implies that the full name is incomplete as well
        # there is a posibility that records could match base on last/first name solely
        # but here for simplicity and efficiency, I jsut deleted rows with incomplete full names
        clean_df = clean_df[~(clean_df["last"].isna())]
        clean_df = clean_df[~(clean_df["first"].isna())]
        print("check")
        print(clean_df[clean_df["first"].isna()])

        clean_df.loc[clean_df["first"] == "", "first"] = clean_df["cand"].apply(
            extract_first_name
        )

        clean_df["cand"] = np.where(
            clean_df["v19_20171211"].notna(), clean_df["v19_20171211"], clean_df["cand"]
        )
        clean_df = clean_df.drop(["v19_20171211"], axis=1)

        return clean_df

    def standardize(self, data: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the dataframe into the necessary format for the schema

        Maps data types and column names as defined in schema

        Inputs:
            data: dataframe as outputted from clean method.

        Returns: Dataframe
        """
        # here last, first name should be remodified
        data["sab"] = data["sab"].str.upper()
        data["last"] = data["last"].str.lower()
        data["first"] = data["first"].str.lower()
        data["cand"] = data["cand"].str.lower()
        data["cand"] = data["cand"].astype(str)[data["cand"].notna()]

        data["partyt"] = data["partyt"].map(party_map)

        data = data.rename(
            columns={
                "ddez": "district_designation_ballot",
                "dname": "district",
                "dno": "district_number",
                "geopost": "geographic_post",
                "mmdpost": "mmd_post",
                "cname": "county",
                "sen": "senate",
                "candid": "candidate_id",
                "sab": "state",
                "last": "last_name",
                "first": "first_name",
                "cand": "full_name",
                "partyt": "party",
                "termz": "term",
                "dseats": "district_seat_number",
            }
        )

        data["day"] = data["day"].fillna(0)
        data["county"] = data["county"].fillna("Unknown")
        data["district"] = data["district"].fillna("Unknown")
        data["district_number"] = data["district_number"].fillna(0)
        data["geographic_post"] = data["geographic_post"].fillna(0)
        data["mmd_post"] = data["mmd_post"].fillna(0)
        data["vote"] = data["vote"].fillna(0)
        data["full_name"] = data["full_name"].fillna(
            "Unknown"
        )  # Fixed from mistakenly using 'district'
        data["party"] = data["party"].fillna("Unknown")

        data = data.astype(type_mapping)
        print("standardize result", data.dtypes)
        return data

    def create_table(self, data: pd.DataFrame) -> pd.DataFrame:
        """Creates the election result table and create uuid

        Inputs:
            data: Dataframe as output from standardize method.

        Returns:
            a table as defined in database schema
        """
        final_table = data.copy()

        final_table = self.create_election_result_uuid(final_table)

        return final_table

    def create_election_result_uuid(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add uuid to each election result record

        Inputs:
            data: standarized data frame

        Returns:
            A dataframe with case_id column added
        """
        data["unique_id"] = [uuid.uuid4() for _ in range(len(data))]

        return data

    def clean_state(self) -> pd.DataFrame:
        """Runs the ElectionResultCleaner pipeline returning a cleaned dataframes

        Returns: cleans the state and returns the standardized table showing
        the election results.
        """
        raw_df = self.preprocess()
        clean_df = self.clean(raw_df)
        standardized_df = self.standardize(clean_df)
        return self.create_table(standardized_df)
