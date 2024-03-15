import uuid

import numpy as np
import pandas as pd

from utils.clean import StateCleaner
from utils.constants import (
    MN_CANDIDATE_CONTRIBUTION_COL,
    MN_CANDIDATE_CONTRIBUTION_MAP,
    MN_FILEPATHS_LST,
    MN_INDEPENDENT_EXPENDITURE_COL,
    MN_INDEPENDENT_EXPENDITURE_MAP,
    MN_NONCANDIDATE_CONTRIBUTION_COL,
    MN_NONCANDIDATE_CONTRIBUTION_MAP,
    MN_RACE_MAP,
)


class MinnesotaCleaner(StateCleaner):
    def entity_name_dictionary(self) -> dict:
        self.entity_name_dictionary = {
            "I": "Individual",
            "L": "Lobbyist",
            "B": "Company",
            "PCF": "Committee",
            "C": "Committee",
            "F": "Committee",
            "H": "Committee",
            "S": "Committee",
            "U": "Committee",
            "PTU": "Party",
            "P": "Party",
            "O": "Other",
        }

        return self.entity_name_dictionary

    def preprocess_candidate_contribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """Helper function to preprocess MN candidate-recipient contribution df

        Args:
            df (DataFrame): a single raw MN candidate contribution df
        Returns:
            DataFrame: Preprocessed contribution df of candidate recipients
        """
        df1 = df.copy(deep=True)
        df1 = df1[MN_CANDIDATE_CONTRIBUTION_COL]
        df1 = df1.rename(columns=MN_CANDIDATE_CONTRIBUTION_MAP)
        df1["recipient_type"] = "I"

        none_columns = [
            "recipient_full_name",
            "donor_first_name",
            "donor_last_name",
            "donor_id",
        ]
        df1 = df1.assign(**{col: None for col in none_columns})

        df1["donor_type"] = df1["donor_type"].str.upper()
        df1["state"] = "MN"
        df1["amount"] = pd.to_numeric(df1["amount"], errors="coerce")
        df1["inkind_amount"] = pd.to_numeric(df1["inkind_amount"], errors="coerce")
        df1["transaction_type"] = np.where(
            (df1["inkind_amount"].notna()) & (df1["inkind_amount"] != 0),
            "in-kind",
            None,
        )

        return df1

    def preprocess_noncandidate_contribution(self, df: pd.DataFrame) -> pd.DataFrame:
        """Helper function to preprocess MN noncandidate-recipient contribution df

        Args:
            df (DataFrame): a single raw MN non-candidate contribution df
        Returns:
            DataFrame: Preprocessed contribution df of noncandidate recipients
        """
        df1 = df.copy(deep=True)
        df1 = df1[MN_NONCANDIDATE_CONTRIBUTION_COL]
        df1 = df1.rename(columns=MN_NONCANDIDATE_CONTRIBUTION_MAP)
        none_columns = [
            "recipient_first_name",
            "recipient_last_name",
            "donor_first_name",
            "donor_last_name",
            "office_sought",
        ]
        df1 = df1.assign(**{col: None for col in none_columns})

        df1["donor_type"] = df1["donor_type"].str.upper()
        df1["state"] = "MN"
        df1["amount"] = pd.to_numeric(df1["amount"], errors="coerce")
        df1["donor_id"] = df1["donor_id"].fillna(0).astype("int64")
        df1["inkind_amount"] = pd.to_numeric(df1["inkind_amount"], errors="coerce")
        df1["transaction_type"] = np.where(
            (df1["inkind_amount"].notna()) & (df1["inkind_amount"] != 0),
            "in-kind",
            None,
        )

        return df1

    def preprocess_expenditure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Helper function to preprocess MN expenditure dataset

        Args:
            df (DataFrame): MN expenditure DataFrames
        Returns:
            DataFrame: Preprocessed MN expenditure DataFrames
        """
        df1 = df.copy(deep=True)
        df1 = df1[MN_INDEPENDENT_EXPENDITURE_COL]
        df1 = df1.rename(columns=MN_INDEPENDENT_EXPENDITURE_MAP)
        # Donors and recipients are both organization, only have full name
        none_columns = [
            "recipient_first_name",
            "recipient_last_name",
            "donor_first_name",
            "donor_last_name",
            "inkind_amount",
            "office_sought",
        ]
        df1 = df1.assign(**{col: None for col in none_columns})

        df1["recipient_id"] = df1["donor_id"].fillna(0).astype("int64")
        # Negate the contribution amount if it's against the recipient
        df1.loc[df1["For /Against"] == "Against", "amount"] = -df1["amount"]
        df1 = df1.drop(columns=["For /Against"])
        df1["recipient_type"] = "PCF"  # recipient: affected political committee

        return df1

    def preprocess(self, filepaths_list: list[str]) -> list[pd.DataFrame]:
        """Preprocesses MN data and returns a list of a single dataframe combining
        both contributions and expenditures

        Inputs:
            filepaths_list (list): list of absolute filepaths of contribution
            and expenditure csv files

        Order: 10 candidate-recipient contribution files (AG, AP, DC, GC, House,
            SA, SC, Senate, SS, ST), 1 noncandidate-recipient contribution file,
            and 1 expenditure file

        Naming Convention: root + data folder (+ candidate folder) + file name

        Returns:
            A list containing 1 preprocessed DataFrame
        """
        candidate_dfs = []
        processed_dfs = []
        # candidate-recipient contribution data
        for filepath1 in filepaths_list[:-2]:
            candidate = pd.read_csv(filepath1)
            candidate_dfs.append(candidate)
        for candidate_df in candidate_dfs:
            processed_cand_con = self.preprocess_candidate_contribution(candidate_df)
            processed_dfs.append(processed_cand_con)
        # noncandidate-recipient contribution data
        noncandidate_df = pd.read_csv(filepaths_list[-2])
        processed_noncandidate_con = self.preprocess_noncandidate_contribution(
            noncandidate_df
        )
        processed_dfs.append(processed_noncandidate_con)
        # expenditure data
        expenditure_df = pd.read_csv(filepaths_list[-1])
        processed_expenditure_df = self.preprocess_expenditure(expenditure_df)
        processed_dfs.append(processed_expenditure_df)
        combined_df = pd.concat(
            processed_dfs,
            ignore_index=True,
        )

        return [combined_df]

    def clean(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        """Cleans MN data by converting dtyes to match database schema and dropping
        non-classfiable rows (not represent minimal viable transactions)

        Inputs:
            data: a list of 1 DataFrame outputted from the preprocess method

        Returns: a list of 1 cleaned MN DataFrame
        """
        df = data[0]
        df["date"] = pd.to_datetime(df["date"], format="mixed")
        df["year"] = df["date"].dt.year
        df = df.drop(columns=["date"])
        type_mapping = {
            "state": "str",
            "recipient_id": "str",
            "donor_id": "str",
            "recipient_first_name": "str",
            "recipient_last_name": "str",
            "recipient_full_name": "str",
            "donor_first_name": "str",
            "donor_last_name": "str",
            "donor_full_name": "str",
            "recipient_type": "str",
            "donor_type": "str",
            "purpose": "str",
            "transaction_type": "str",
            "year": "int64",
            "amount": "float64",
            "inkind_amount": "float64",
        }

        df = df.astype(type_mapping)
        # non-classfiable rows of no transaction amount, no donor/recipient info
        df = df[df["amount"] != 0]
        df = df.dropna(subset=["recipient_id", "donor_id"], how="any")
        df = df.drop(columns=["inkind_amount"])
        df = df.reset_index(drop=True)

        return [df]

    def standardize(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        """Standardizes the dataframe into the necessary format for the schema

        Maps entity/office types and column names as defined in schema, adjust
        and add UUIDs as necessary, store base id and provided id in MNIDMap.csv

        Inputs:
            df: A list of 1 preprocessed and cleaned Dataframe outputted from
            the clean method

        Returns: A list of 1 standarized DataFrame matching database schema
        """
        df = data[0].copy()  # Create a copy to avoid modifying the original DataFrame
        df["company"] = None  # MN dataset has no company information
        df["party"] = None  # MN dataset has no party information
        df["transaction_id"] = None
        df["office_sought"] = df["office_sought"].replace(MN_RACE_MAP)

        # Standardize entity names to match other states in the database schema
        entity_map = self.entity_name_dictionary()
        df["recipient_type"] = df["recipient_type"].map(entity_map)
        df["donor_type"] = df["donor_type"].map(entity_map)
        id_mapping = {}

        # Standardize entity names to match othe states in database schema
        df["recipient_type"] = df["recipient_type"].replace(self.entity_name_dictionary)
        df["donor_type"] = df["donor_type"].replace(self.entity_name_dictionary)
        id_mapping = {}
        for index, row in df.iterrows():
            recipient_uuid = str(uuid.uuid4())
            donor_uuid = str(uuid.uuid4())
            transaction_uuid = str(uuid.uuid4())

            # MN has partial recipient id, generate uuid, map them to original id
            if row["recipient_id"]:
                if (
                    row["recipient_type"] == "Individual"
                    or row["recipient_type"] == "Lobbyist"
                ):
                    entity_type = "Individual"
                else:
                    entity_type = "Organization"
                id_mapping[row["recipient_id"]] = (
                    row["state"],
                    row["year"],
                    entity_type,
                    row["recipient_id"],
                    recipient_uuid,
                )

                df.at[index, "recipient_id"] = recipient_uuid

            # MN has partial donor id, generate uuid, map them to original id
            if row["donor_id"]:
                if row["donor_type"] == "Individual" or row["donor_type"] == "Lobbyist":
                    entity_type = "Individual"
                else:
                    entity_type = "Organization"
                id_mapping[row["donor_id"]] = (
                    row["state"],
                    row["year"],
                    entity_type,
                    row["donor_id"],
                    donor_uuid,
                )

                df.at[index, "donor_id"] = donor_uuid

            df.at[index, "transaction_id"] = transaction_uuid

        # Convert id_mapping to DataFrame and save to CSV
        id_mapping_df = pd.DataFrame.from_dict(
            id_mapping,
            orient="index",
            columns=[
                "state",
                "year",
                "entity_type",
                "provided_id",
                "database_id",
            ],
        )
        id_mapping_df.to_csv("MNIDMap.csv", index=False)

        return [df]

    def create_tables(
        self, data: list[pd.DataFrame]
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """Creates the Individuals, Organizations, and Transactions tables from
        the dataframe list outputted from the standardize function

        Inputs:
            data: a list of 1 dataframe outputted from the standardize method

        Returns: (individuals_table, organizations_table, transactions_table)
            tuple containing the tables as defined in database schema
        """
        df = data[0]
        # Create individual table from both recipient and donor entries
        ind_recipient_df = pd.DataFrame(
            data=df[
                df["recipient_type"].isin(["Individual", "Lobbyist"])
            ].drop_duplicates(subset="recipient_id"),
            columns=[
                "recipient_id",
                "recipient_first_name",
                "recipient_last_name",
                "recipient_full_name",
                "recipient_type",
                "state",
                "party",
                "company",
            ],
        )
        ind_recipient_df = ind_recipient_df.rename(
            columns={
                "recipient_id": "id",
                "recipient_first_name": "first_name",
                "recipient_last_name": "last_name",
                "recipient_full_name": "full_name",
                "recipient_type": "entity_type",
            }
        )
        ind_donor_df = pd.DataFrame(
            data=df[df["donor_type"].isin(["Individual", "Lobbyist"])].drop_duplicates(
                subset="donor_id"
            ),
            columns=[
                "donor_id",
                "donor_first_name",
                "donor_last_name",
                "donor_full_name",
                "donor_type",
                "state",
                "party",
                "company",
            ],
        )
        ind_donor_df = ind_donor_df.rename(
            columns={
                "donor_id": "id",
                "donor_first_name": "first_name",
                "donor_last_name": "last_name",
                "donor_full_name": "full_name",
                "donor_type": "entity_type",
            }
        )
        ind_df = pd.concat([ind_recipient_df, ind_donor_df], ignore_index=True)

        # Create organization table from both recipient and donor entries
        org_recipient_df = pd.DataFrame(
            data=df[
                ~df["recipient_type"].isin(["Individual", "Lobbyist"])
            ].drop_duplicates(subset="recipient_id"),
            columns=[
                "recipient_id",
                "recipient_full_name",
                "state",
                "recipient_type",
            ],
        )
        org_recipient_df = org_recipient_df.rename(
            columns={
                "recipient_id": "id",
                "recipient_full_name": "name",
                "recipient_type": "entity_type",
            }
        )
        org_donor_df = pd.DataFrame(
            data=df[~df["donor_type"].isin(["Individual", "Lobbyist"])].drop_duplicates(
                subset="donor_id"
            ),
            columns=["donor_id", "donor_full_name", "state", "donor_type"],
        )
        org_donor_df = org_donor_df.rename(
            columns={
                "donor_id": "id",
                "donor_full_name": "name",
                "donor_type": "entity_type",
            }
        )
        org_df = pd.concat([org_recipient_df, org_donor_df], ignore_index=True)

        tran_df = pd.DataFrame(
            data=df.drop_duplicates(subset="transaction_id"),
            columns=[
                "transaction_id",
                "donor_id",
                "donor_type",
                "year",
                "amount",
                "recipient_type",
                "recipient_id",
                "office_sought",
                "purpose",
                "transaction_type",
            ],
        )

        return (ind_df, org_df, tran_df)

    def clean_state(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        preprocessed_df = self.preprocess(MN_FILEPATHS_LST)
        cleaned_df = self.clean(preprocessed_df)
        standardized_df = self.standardize(cleaned_df)
        (ind_df, org_df, tran_df) = self.create_tables(standardized_df)

        return (
            ind_df,
            org_df,
            tran_df,
        )
