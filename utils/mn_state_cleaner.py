import uuid
import warnings

import numpy as np
import pandas as pd
from clean import StateCleaner
from constants import (
    MN_CANDIDATE_CONTRIBUTION_COL,
    MN_CANDIDATE_CONTRIBUTION_MAP,
    MN_INDEPENDENT_EXPENDITURE_COL,
    MN_INDEPENDENT_EXPENDITURE_MAP,
    MN_NONCANDIDATE_CONTRIBUTION_COL,
    MN_NONCANDIDATE_CONTRIBUTION_MAP,
    MN_RACE_MAP,
)

warnings.filterwarnings("ignore")

filepaths_lst = [
    "/project/data/cand_con/AG.csv",
    "/project/data/cand_con/AP.csv",
    "/project/data/cand_con/DC.csv",
    "/project/data/cand_con/GC.csv",
    "/project/data/cand_con/House.csv",
    "/project/data/cand_con/SA.csv",
    "/project/data/cand_con/SC.csv",
    "/project/data/cand_con/Senate.csv",
    "/project/data/cand_con/SS.csv",
    "/project/data/cand_con/ST.csv",
    "/project/data/non_candidate_con.csv",
    "/project/data/independent_exp.csv",
]


class MNStateCleaner(StateCleaner):
    """
    This abstract class is MN specific finance campaign data
    """

    # @property
    def entity_name_dictionary(self) -> dict:
        """
        A dict mapping MN's raw entity names to standard versions

            Inputs: None

            Returns: _entity_name_dictionary
        """

        self.entity_name_dictionary = {
            "PCF": "Committee",
            "PTU": "Party",
            "B": "Company",
            "C": "Committee",
            "F": "Committee",
            "H": "Committee",
            "O": "Other",
            "P": "Party",
            "U": "Committee",
            "I": "Individual",
            "L": "Lobbyist",
            "S": "Candidate",
        }
        return self.entity_name_dictionary

    def preprocess_candidate_con(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper function to preprocess MN candidate-recipient contribution dfs

        Args:
            df (DataFrame): MN candidate contribution DataFrames
        Returns:
            DataFrame: Preprocessed contribution df of candidate recipients
        """

        df1 = df.copy(deep=True)
        columns_to_keep = MN_CANDIDATE_CONTRIBUTION_COL
        df1 = df1[columns_to_keep]
        column_mapping = MN_CANDIDATE_CONTRIBUTION_MAP
        df1.rename(columns=column_mapping, inplace=True)
        df1["recipient_type"] = "I"  # all recipients are individual candidate
        df1["recipient_full_name"] = np.nan
        df1["donor_first_name"] = np.nan
        df1["donor_last_name"] = np.nan
        df1["donor_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
        df1["state"] = "MN"

        pd.to_numeric(df1["amount"], errors="coerce")
        pd.to_numeric(df1["inkind_amount"], errors="coerce")
        df1["transaction_type"] = np.where(
            (df1["inkind_amount"].notna()) & (df1["inkind_amount"] != 0),
            "in-kind",
            np.nan,
        )
        return df1

    def preprocess_noncandidate_con(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper function to preprocess MN non-candidate-recipient contribution df

        Args:
            df (DataFrame): MN non-candidate contribution DataFrames
        Returns:
            DataFrame: Preprocessed contribution df of non-candidate recipients
        """

        df1 = df.copy(deep=True)
        df1["DonorType"] = df1["DonorType"].str.upper()
        columns_to_keep = MN_NONCANDIDATE_CONTRIBUTION_COL
        df1 = df1[columns_to_keep]
        column_mapping = MN_NONCANDIDATE_CONTRIBUTION_MAP
        df1.rename(columns=column_mapping, inplace=True)

        # recipients are organizations so first and last names are nan
        df1["recipient_first_name"] = np.nan
        df1["recipient_last_name"] = np.nan
        df1["donor_first_name"] = np.nan
        df1["donor_last_name"] = np.nan
        df1["state"] = "MN"
        df1["party"] = np.nan
        df1["office_sought"] = np.nan
        # non-cand con has partial donor id, create a map to store the subset
        donor_id_mapping = {}
        for index, row in df1.iterrows():
            new_uuid = str(uuid.uuid4())
            if row["donor_id"]:
                donor_id_mapping[row["donor_id"]] = new_uuid
            df1["donor_id"].iloc[index] = new_uuid
        pd.to_numeric(df1["amount"], errors="coerce")
        pd.to_numeric(df1["inkind_amount"], errors="coerce")
        df1["transaction_type"] = np.where(
            (df1["inkind_amount"].notna()) & (df1["inkind_amount"] != 0),
            "in-kind",
            np.nan,
        )
        return df1

    def preprocess_independent_exp(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper function to preprocess MN independent expenditure dataset

        Args:
            df (DataFrame): MN independent expenditure DataFrames
        Returns:
            DataFrame: Preprocessed MN general expenditure DataFrames
        """

        df1 = df.copy(deep=True)
        columns_to_keep = MN_INDEPENDENT_EXPENDITURE_COL
        df1 = df1[columns_to_keep]
        column_mapping = MN_INDEPENDENT_EXPENDITURE_MAP
        df1.rename(columns=column_mapping, inplace=True)

        df1["party"] = np.nan
        # recipient and donor are organization so first and last names nan
        df1["recipient_first_name"] = np.nan
        df1["recipient_last_name"] = np.nan
        df1["donor_first_name"] = np.nan
        df1["donor_last_name"] = np.nan
        pd.to_numeric(df1["amount"], errors="coerce")
        # negate the contribution amount if it's against the recipient
        df1.loc[df1["For /Against"] == "Against", "amount"] = -df1["amount"]
        df1["inkind_amount"] = np.nan
        df1 = df1.drop(columns=["For /Against"])
        df1["recipient_type"] = "C"
        df1["office_sought"] = np.nan
        return df1

    # @abstractmethod
    def preprocess(self, filepaths_list: list[str]) -> list[pd.DataFrame]:
        """
        Preprocesses MN data and returns a list of a single dataframe combining
        both contributions and expenditures

        Inputs:
            filepaths_list: list of absolute filepaths of 10 candidate as
            recipient contribution files, 1 non-candidate as recipient
            contribution file, and 1 independent expenditure file. Require
            consistent naming conventions, order, and extensions defined

        Returns:
            A list containing a single dataframe
        """

        cand_cons, cand_cons_pro = [], []
        for filepath1 in filepaths_list[:-2]:
            candidate = pd.read_csv(filepath1)
            cand_cons.append(candidate)
        for cand_con in cand_cons:
            processed_cand_con = self.preprocess_candidate_con(cand_con)
            cand_cons_pro.append(processed_cand_con)
        process_cand_con = pd.concat(cand_cons_pro, ignore_index=True)

        non_cand_con = pd.read_csv(filepaths_list[-2])
        process_noncand_con = self.preprocess_noncandidate_con(non_cand_con)[0]
        # donor_id_map = self.preprocess_noncandidate_con(non_cand_con)[1]
        ind_exp = pd.read_csv(filepaths_list[-1])
        process_ind_exp = self.preprocess_independent_exp(ind_exp)

        combined_df = pd.concat(
            [process_cand_con, process_noncand_con, process_ind_exp],
            ignore_index=True,
        )
        return [combined_df]

    def clean(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        """
        Cleans the MN dataframe as needed and returns the dataframe

        Cleans the columns and converts dtyes to match database schema, drop
        non-classfiable rows (not represent minimal viable transactions)

        Inputs:
            data: a list of 1 or 3 dataframes as output from preprocess method

        Returns: a list of a single cleaned MN Pandas DataFrame
        """

        df = data[0]
        df["party"] = df["party"].astype("str")
        df["state"] = df["state"].astype("str")
        df["recipient_id"] = df["recipient_id"].astype("str")
        df["donor_id"] = df["donor_id"].astype("str")
        df["recipient_first_name"] = df["recipient_first_name"].astype("str")
        df["recipient_last_name"] = df["recipient_last_name"].astype("str")
        df["recipient_full_name"] = df["recipient_full_name"].astype("str")
        df["donor_first_name"] = df["donor_first_name"].astype("str")
        df["donor_last_name"] = df["donor_last_name"].astype("str")
        df["donor_full_name"] = df["donor_full_name"].astype("str")
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        df["recipient_type"] = df["recipient_type"].astype("str")
        df["donor_type"] = df["donor_type"].astype("str")
        df["purpose"] = df["purpose"].astype("str")
        df["transaction_type"] = df["transaction_type"].astype("str")
        # non-classfiable rows of no transaction amount, no donor/recipient info
        df = df[df["amount"] != 0]
        df = df.dropna(subset=["recipient_id", "donor_id"], how="any")
        df = df.drop(columns=["inkind_amount"])
        df = df.reset_index(drop=True)
        return [df]

    def standardize(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        """
        Standardizes the dataframe into the necessary format for the schema

        Maps entity/office types and column names as defined in schema, adjust
        and add UUIDs as necessary

        Inputs:
            df: A list of a single preprocessed and cleaned Pandas Dataframe

        Returns: A list of one standarized DataFrame by database schema
        """

        df = data[0]
        df = df.drop(columns=["date"])
        df["company"] = np.nan
        df["donor_id"] = "MN" + df["donor_id"].astype(str)
        df["recipient_id"] = "MN" + df["recipient_id"].astype(str)
        # create transaction_id with randomly generated, unique strings by uuid
        # fmt: off
        df["transaction_id"] = \
            ["MN" + str(uuid.uuid4()) for _ in range(len(df))]
        # fmt: on
        df["office_sought"] = df["office_sought"].replace(MN_RACE_MAP)
        return [df]

    def standardize_entity_names(self, entity: pd.DataFrame) -> pd.DataFrame:
        """Creates a new 'standard_entity_type' column from 'raw_entity_type'

        Args:
            entity_table: an entity dataframe containing 'raw_entity_type'

        Returns: entity_table with 'standard_entity_type created from the
            entity_name_dictionary
        """

        entity["standard_entity_type"] = entity["raw_entity_type"].map(
            lambda raw_entity_type: self.entity_name_dictionary.get(
                raw_entity_type, None
            )
        )
        return entity

    # @abstractmethod
    def create_tables(
        self, data: list[pd.DataFrame]
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Creates the Individuals, Organizations, and Transactions tables from
        the dataframe list outputted from standardize

        Inputs:
            data: a list of 1 dataframe as output from standardize method

        Returns: (individuals_table, organizations_table, transactions_table)
                    tuple containing the tables as defined in database schema
        """
        df = data[0]
        ind_col = [
            "id",
            "first_name",
            "last_name",
            "full_name",
            "state",
            "party",
            "company",
        ]
        org_col = ["id", "name", "state", "entity_type"]
        tran_col = [
            "transaction_id",
            "donor_id",
            "year",
            "amount",
            "recipient_id",
            "office_sought",
            "purpose",
            "transaction_type",
        ]
        ind_df = pd.DataFrame(columns=ind_col)
        org_df = pd.DataFrame(columns=org_col)
        tran_df = pd.DataFrame(columns=tran_col)

        for _, row in df.iterrows():
            tran_df["transaction_id"] = row["transaction_id"]
            tran_df["donor_id"] = row["donor_id"]
            tran_df["year"] = row["year"]
            tran_df["amount"] = row["amount"]
            tran_df["recipient_id"] = row["recipient_id"]
            tran_df["office_sought"] = row["office_sought"]
            tran_df["purpose"] = row["purpose"]
            tran_df["transaction_type"] = row["transaction_type"]

            if row["recipient_type"] == "I":
                ind_df["id"] = row["recipient_id"]
                ind_df["first_name"] = row["recipient_first_name"]
                ind_df["last_name"] = row["recipient_last_name"]
                ind_df["full_name"] = row["recipient_full_name"]
                ind_df["state"] = row["state"]
                ind_df["party"] = row["party"]
                ind_df["company"] = row["company"]
            else:  # otherwise recipient is organization, PCF/PTU
                org_col["id"] = row["recipient_id"]
                org_col["name"] = row["recipient_full_name"]
                org_col["state"] = row["state"]
                org_col["entity_type"] = row["recipient_type"]

            if row["donor_type"] in ["I", "L", "S"]:  # individual types
                ind_df["id"] = row["donor_id"]
                ind_df["first_name"] = row["donor_first_name"]
                ind_df["last_name"] = row["donor_last_name"]
                ind_df["full_name"] = row["donor_full_name"]
                ind_df["state"] = row["state"]
                ind_df["party"] = row["party"]
                ind_df["company"] = row["company"]
            else:  # otherwise donor is organization
                org_col["id"] = row["donor_id"]
                org_col["name"] = row["donor_full_name"]
                org_col["state"] = row["state"]
                org_col["entity_type"] = row["donor_type"]

        return ind_df, org_df, tran_df

    # @abstractmethod
    def clean_state(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Runs the StateCleaner pipeline returning a tuple of cleaned dataframes

        Returns: use preprocess, clean, standardize, and create_tables methods
        to output (individuals_table, organizations_table, transactions_table)
        as defined in database schema
        """

        preprocessed_df = self.preprocess(filepaths_lst)
        cleaned_df = self.clean(preprocessed_df)[0]
        standardized_df = self.standardize(cleaned_df)[0]
        ind_df, org_df, tran_df = self.create_tables(standardized_df)
        return ind_df, org_df, tran_df


if __name__ == "__main__":
    mn_cleaner = MNStateCleaner()

    result = mn_cleaner.preprocess(filepaths_lst)
    result_df = result[0]
    clean_df = mn_cleaner.clean(result_df)[0]
    standardized_df = mn_cleaner.standardize(clean_df)
    print(clean_df.columns)
