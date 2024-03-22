"""State cleaner implementation for Michigan"""

import uuid

import numpy as np
import pandas as pd

from utils.clean.clean import StateCleaner
from utils.clean.constants import (
    MI_CON_FILEPATH,
    MI_CONT_DROP_COLS,
    MI_CONTRIBUTION_COLUMNS,
    MI_EXP_DROP_COLS,
    MI_EXP_FILEPATH,
    MI_EXPENDITURE_COLUMNS,
    MICHIGAN_CONTRIBUTION_COLS_RENAME,
    MICHIGAN_CONTRIBUTION_COLS_REORDER,
)
from utils.constants import BASE_FILEPATH


def read_expenditure_data(filepath: str, columns: list[str]) -> pd.DataFrame:
    """Reads in the MI expenditure data

    Inputs:
        filepath (str): filepath to the MI Expenditure Data txt file
        columns (lst): list of string names of the campaign data columns

    Returns: df (Pandas DataFrame): dataframe of the MI Expenditure data
    """
    if filepath.endswith("txt"):
        expenditure_df = pd.read_csv(
            filepath,
            delimiter="\t",
            index_col=False,
            usecols=columns,
            encoding="mac_roman",
            low_memory=False,
        )

    return expenditure_df


def read_contribution_data(filepath: str, columns: list[str]) -> pd.DataFrame:
    """Reads in the MI campaign data and skips the errors

    Inputs:
        filepath (str): filepath to the MI Campaign Data txt file
        columns (lst): list of string names of the campaign data columns

    Returns: df (Pandas DataFrame): dataframe of the MI campaign data
    """
    if filepath.endswith("00.txt"):
        # MI files that contain 00 or between 1998 and 2003 contain headers
        # VALUES_TO_CHECK contains the years between 1998 and 2003
        contribution_df = pd.read_csv(
            filepath,
            delimiter="\t",
            index_col=False,
            encoding="mac_roman",
            usecols=columns,
            low_memory=False,
            on_bad_lines="skip",
        )
    else:
        contribution_df = pd.read_csv(
            filepath,
            delimiter="\t",
            index_col=False,
            encoding="mac_roman",
            header=None,
            names=columns,
            low_memory=False,
            on_bad_lines="skip",
        )

    return contribution_df


class MichiganCleaner(StateCleaner):
    """State cleaner implementation for Michigan"""

    name = "Michigan"
    stable_id_across_years = False
    entity_name_dictionary = {
        "cfr_com_id": "original_com_id",
        "f_name": "first_name",
        "l_name_or_org": "last_name",
        "employer": "company",
        "doc_stmnt_year": "year",
        "exp_desc": "exp_desc",
        "contribtype": "transaction_type",
        "schedule_desc": "transaction_type",
    }

    id_mapping_column_order = [
        "state",
        "year",
        "entity_type",
        "provided_id",
        "database_id",
    ]
    # map to entity types listed in the schema

    def clean_state(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Runs the StateCleaner pipeline returning a tuple of cleaned dataframes

        Returns: use preprocess, clean, standardize, and create_tables methods
        to output (individuals_table, organizations_table, transactions_table)
        as defined in database schema
        """
        filepaths_lst = self.create_filepaths_list()
        preprocessed_dataframe_lst = self.preprocess(filepaths_lst)
        cleaned_dataframe_lst = self.clean(preprocessed_dataframe_lst)
        standardized_dataframe_lst = self.standardize(cleaned_dataframe_lst)
        tables = self.create_tables(standardized_dataframe_lst)

        return tables

    def create_filepaths_list(self) -> list[list[str], list[str]]:
        """Creates a list of Michigan Contribution and Expenditure filepaths

        Inputs: None

        Returns: List of lists of strings
            exp_filepath_lst: list of expenditure filepaths
            con_filepath_lst: list of contribution filepaths
        """
        exp_filepath_lst = []
        con_filepath_lst = []

        for file in MI_EXP_FILEPATH.iterdir():
            exp_filepath_lst.append(str(file))
        for file in MI_CON_FILEPATH.iterdir():
            con_filepath_lst.append(str(file))

        return [exp_filepath_lst, con_filepath_lst]

    # NOTE: Helper methods above are called throughout the class

    def preprocess(self, filepaths_list: list[str]) -> list[pd.DataFrame]:
        """Preprocesses the state data and returns a dataframe

        Reads in the state's data, makes any necessary bug fixes, and
        combines the data into a list of DataFrames, discards data not schema

        Inputs:
            filepaths_list: list of lists of absolute filepaths to relevant state data.
                required naming conventions, order, and extensions
                defined per state.

        Returns: a list of dataframes containing campaign contribution and
            expenditure data
        """
        expenditures_lst, contributions_lst = filepaths_list

        temp_exp_list = []
        temp_cont_list = []

        for file in expenditures_lst:
            temp_exp_list.append(read_expenditure_data(file, MI_EXPENDITURE_COLUMNS))
        for file in contributions_lst:
            temp_cont_list.append(read_contribution_data(file, MI_CONTRIBUTION_COLUMNS))

        contribution_dataframe = self.merge_dataframes(temp_cont_list)
        expenditure_dataframe = self.merge_dataframes(temp_exp_list)

        return [contribution_dataframe, expenditure_dataframe]

    # NOTE: Helper methods for preprocess are below

    def merge_dataframes(
        self, temp_list: list[pd.DataFrame, pd.DataFrame]
    ) -> pd.DataFrame:
        """Merges the list of dataframes into one Pandas DataFrame

        Inputs:
                temp_list: list of contribution of expenditure dataframes

        Returns:
                merged_dataframe: Pandas DataFrame of merged contribution
                                    or expenditure data
        """
        merged_dataframe = pd.concat(temp_list)
        if "schedule_desc" not in merged_dataframe.columns:
            # "schedule_desc" is only in the expenditure dataframe
            merged_dataframe = self.fix_menominee_county_bug_contribution(
                merged_dataframe
            )
        else:
            merged_dataframe = self.drop_menominee_county(merged_dataframe)

        return merged_dataframe

    def fix_menominee_county_bug_contribution(
        self, merged_campaign_dataframe: pd.DataFrame
    ) -> pd.DataFrame:
        """Fixes the Menominee County Rows within the Contribution data

        Inputs:
            merged_campaign_dataframe: Pandas DataFrame of merged MI
                contribution data


        Returns:
            merged_campaign_dataframe: Pandas DataFrame of merged MI
            contribution data edited in place to fix the
            Menominee County Democratic Party bug

        """
        subset_condition = (
            merged_campaign_dataframe["com_type"] == "MENOMINEE COUNTY DEMOCRATIC PARTY"
        )

        rows_to_fix = merged_campaign_dataframe[subset_condition]

        merged_campaign_dataframe = merged_campaign_dataframe.drop(
            merged_campaign_dataframe.loc[subset_condition].index
        )

        rows_to_fix = rows_to_fix[MICHIGAN_CONTRIBUTION_COLS_REORDER]
        rows_to_fix.columns = MICHIGAN_CONTRIBUTION_COLS_RENAME

        rows_to_fix["aggregate"] = 0.0

        merged_campaign_dataframe = pd.concat(
            [merged_campaign_dataframe, rows_to_fix], ignore_index=True
        )

        return merged_campaign_dataframe

    def drop_menominee_county(
        self, merged_campaign_dataframe: pd.DataFrame
    ) -> pd.DataFrame:
        """Drops the menominee county rows within the Michigan Expenditure data

        Inputs:
            merged_campaign_dataframe: Pandas DataFrame of merged MI expenditure data

        Returns:
            merged_campaign_dataframe: Pandas DataFrame of merged MI
            expenditure data with menominee County Democratic Party columns dropped
        """
        # There are only 20 menominee county rows read in incorrectly and
        # missing key data these rows are dropped
        subset_condition = (
            merged_campaign_dataframe["com_type"] == "MENOMINEE COUNTY DEMOCRATIC PARTY"
        )

        merged_campaign_dataframe = merged_campaign_dataframe.drop(
            merged_campaign_dataframe.loc[subset_condition].index
        )

        return merged_campaign_dataframe

    def clean(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:  # noqa: D102
        contribution_dataframe, expenditure_dataframe = data

        clean_cont = self.clean_contribution_dataframe(contribution_dataframe)

        clean_exp = self.clean_expenditure_dataframe(expenditure_dataframe)

        merged_dataframe = pd.concat([clean_cont, clean_exp], axis=0, ignore_index=True)
        # concatenate the dataframes along rows ignore the prior index

        return [merged_dataframe]

    # NOTE: Helper methods for clean are below

    def clean_contribution_dataframe(
        self,
        merged_contribution_dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        """Cleans the contribution dataframe as needed and returns the dataframe

        Inputs:
                merged_contribution_dataframe:
                Merged Michigan campaign contribution dataframe

        Returns:
                merged_contribution_dataframe:
                Merged Michigan campaign contribution dataframe cleaned in place
        """
        merged_contribution_dataframe["cfr_com_id"] = (
            merged_contribution_dataframe["cfr_com_id"]
            .apply(pd.to_numeric, errors="coerce")
            .astype("Int64")
        )
        merged_contribution_dataframe["amount"] = merged_contribution_dataframe[
            "amount"
        ].apply(pd.to_numeric, errors="coerce")
        # convert committee IDs to integer, amount and aggregate cols to float
        merged_contribution_dataframe = merged_contribution_dataframe.drop(
            columns=MI_CONT_DROP_COLS
        )
        merged_contribution_dataframe["full_name"] = (
            merged_contribution_dataframe["f_name"].fillna("")
            + " "
            + merged_contribution_dataframe["l_name_or_org"]
        )

        merged_contribution_dataframe["candidate_full_name"] = np.where(
            merged_contribution_dataframe["can_first_name"].notna()
            & merged_contribution_dataframe["can_last_name"].notna(),
            merged_contribution_dataframe["can_first_name"]
            + " "
            + merged_contribution_dataframe["can_last_name"],
            np.nan,
        )

        return merged_contribution_dataframe

    def clean_expenditure_dataframe(
        self,
        merged_expenditure_dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        """Cleans the expenditure dataframe as needed and returns the dataframe

        Inputs:
                merged_expenditure_dataframe: Merged Michigan campaign
                expenditure dataframe

        Returns:
                merged_expenditure_dataframe: Merged Michigan expenditure
                dataframe cleaned in place
        """
        merged_expenditure_dataframe[["amount", "supp_opp"]] = (
            merged_expenditure_dataframe[
                ["amount", "supp_opp"]
            ].apply(pd.to_numeric, errors="coerce")
        )
        merged_expenditure_dataframe["cfr_com_id"] = (
            merged_expenditure_dataframe["cfr_com_id"]
            .apply(pd.to_numeric, errors="coerce")
            .astype("int64")
        )
        merged_expenditure_dataframe = merged_expenditure_dataframe.rename(
            columns={"lname_or_org": "l_name_or_org"}
        )
        # convert committee IDs to integer, amount col to float
        # rename last_name column for consistency in standardize
        merged_expenditure_dataframe = merged_expenditure_dataframe.drop(
            columns=MI_EXP_DROP_COLS
        )
        merged_expenditure_dataframe["full_name"] = (
            merged_expenditure_dataframe["f_name"].fillna("")
            + " "
            + merged_expenditure_dataframe["l_name_or_org"]
        )

        return merged_expenditure_dataframe

    def standardize(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:  # noqa: D102
        data = self.add_uuid_columns(data)
        standardized_merged_dataframe = data[0]

        standardized_merged_dataframe = standardized_merged_dataframe.rename(
            columns=self.entity_name_dictionary
        )

        return [standardized_merged_dataframe]

    # NOTE: Helper methods for standardize are below

    def add_uuid_columns(
        self, cleaned_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame]:
        """Generate a UUID for a pandas DataFrame

        Inputs:
            merged_expenditure_dataframe: Merged Michigan campaign
                expenditure or contribution dataframe

        Returns:
            merged_expenditure_dataframe: Merged Michigan campaign
                expenditure or contribution dataframe modified in place
        """
        merged_dataframe = cleaned_dataframe_lst[0]

        merged_dataframe = self.generate_uuid(
            merged_dataframe,
            ["full_name", "candidate_full_name", "com_legal_name", "vend_name"],
        )

        return [merged_dataframe]

    def generate_uuid(
        self, merged_campaign_dataframe: pd.DataFrame, column_names: list[str]
    ) -> pd.DataFrame:
        """Generates uuids for the pandas DataFrame based on the column names provided

        Inputs:
            merged_campaign_dataframe:  Merged Michigan campaign
            expenditure or contribution dataframe
            column_names: List of column names for which UUIDs will be generated

        Returns:
            merged_campaign_dataframe: Merged Michigan campaign
            expenditure or contribution dataframe modified in place

        """
        for col_name in column_names:
            non_null_values = merged_campaign_dataframe[
                merged_campaign_dataframe[col_name].notna()
            ][col_name]

            ids = {value: str(uuid.uuid4()) for value in non_null_values}

            # Map the generated UUIDs to a new column in the DataFrame
            merged_campaign_dataframe[f"{col_name}_uuid"] = merged_campaign_dataframe[
                col_name
            ].map(ids)

        # create transaction ID for each row of the dataframe
        merged_campaign_dataframe["transaction_id"] = [
            uuid.uuid4() for _ in range(len(merged_campaign_dataframe))
        ]

        return merged_campaign_dataframe

    def create_tables(
        self, data: list[pd.DataFrame]
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Creates the Individuals, Organizations, and Transactions tables

        Inputs:
            data: a list of 1 or 3 dataframes as outputted from standardize method.

        Returns: (individuals_table, organizations_table, transactions_table)
                    tuple containing the tables as defined in database schema
        """
        (
            individuals_table,
            individuals_id_mapping,
        ) = self.create_individuals_table(data)
        (
            organizations_table,
            organizations_id_mapping,
        ) = self.create_organizations_table(data)
        (
            transactions_table,
            transactions_id_mapping,
        ) = self.create_transactions_table(data)
        # TODO: fix duplicated transaction_type column
        transactions_table = transactions_table.loc[
            :, ~transactions_table.columns.duplicated()
        ]
        self.output_id_mapping(
            individuals_id_mapping,
            organizations_id_mapping,
            transactions_id_mapping,
        )

        return (individuals_table, organizations_table, transactions_table)

    # NOTE: The helper functions for ID_mapping output are below

    def output_id_mapping(
        self,
        individuals_map: pd.DataFrame,
        organizations_map: pd.DataFrame,
        transactions_map: pd.DataFrame,
    ) -> None:
        """Creates MichiganIDMAp.csv

        Inputs:
            individuals_map: dataframe of individuals mapped to database
            and provided uuid
            organizations_map: dataframe of organizations mapped to database
            and provided uuid
            transactions_map: database of transactions mapped to databasee
            and provided uuid

        Returns: None, Creates data/output/MichiganIDMAp.csv
        """
        output_path = BASE_FILEPATH / "output" / "MichiganIDMap.csv"

        michigan_id_map = pd.concat(
            [individuals_map, organizations_map, transactions_map],
            ignore_index=True,
        )

        if not output_path.parent.exists():
            output_path.parent.mkdir()

        michigan_id_map.to_csv(output_path, index=False)

    def create_individuals_id_mapping(
        self, individuals: pd.DataFrame, candidates: pd.DataFrame
    ) -> pd.DataFrame:
        """Creates the ID mapping dataframe for individuals

        Inputs:
            individuals: DataFrame with individuals data
            candidates: DataFrame with candidates data

        Returns: id_mapping: dataframe in the ID mapping format
        """
        individuals = individuals[["year", "full_name_uuid"]].copy()
        candidates = candidates[["year", "candidate_full_name_uuid"]].copy()

        individuals = individuals.rename(columns={"full_name_uuid": "database_id"})
        candidates = candidates.rename(
            columns={"candidate_full_name_uuid": "database_id"}
        )

        id_mapping = pd.concat([individuals, candidates], ignore_index=True)
        id_mapping["state"] = "MI"
        id_mapping["entity_type"] = "Individual"
        id_mapping["provided_id"] = np.nan

        id_mapping = id_mapping[self.id_mapping_column_order].copy()

        return id_mapping

    def create_organizations_id_mapping(
        self,
        corporations: pd.DataFrame,
        committees: pd.DataFrame,
        vendors: pd.DataFrame,
    ) -> pd.DataFrame:
        """Creates the ID mapping dataframe for organizations

        Inputs:
            corporations: dataframe with corporations data
            committees: dataFrame with campaign committee data
            vendors: dataFrame with vendors data

        Returns: id_mapping: dataframe in the ID mapping format
        """
        corporations = corporations[["year", "full_name_uuid"]].copy()
        committees = committees[
            ["year", "com_legal_name_uuid", "original_com_id"]
        ].copy()
        vendors = vendors[["year", "vend_name_uuid"]].copy()

        corporations = corporations.rename(columns={"full_name_uuid": "database_id"})
        committees = committees.rename(
            columns={
                "com_legal_name_uuid": "database_id",
                "original_com_id": "provided_id",
            }
        )
        vendors = vendors.rename(columns={"vend_name_uuid": "database_id"})

        id_mapping = pd.concat([corporations, committees, vendors], ignore_index=True)
        id_mapping["state"] = "MI"
        id_mapping["entity_type"] = "Organization"

        id_mapping = id_mapping[self.id_mapping_column_order].copy()

        return id_mapping

    def create_transactions_id_mapping(
        self,
        org_com: pd.DataFrame,
        ind_com: pd.DataFrame,
        com_vend: pd.DataFrame,
    ) -> pd.DataFrame:
        """Creates the ID mapping dataframe for transactions

        Inputs:
            org_com: dataframe with organizations to organization (committee)
            transactions

            ind_com: dataframe with individual to organization (committee)
            transactions

            com_vend: dataframe with  committee (organization) to vendor
            (organization) transactions

        Returns: id_mapping: dataframe in the ID mapping format
        """
        org_com = org_com[["year", "transaction_id"]].copy()
        ind_com = ind_com[["year", "transaction_id"]].copy()
        com_vend = com_vend[["year", "transaction_id"]].copy()

        org_com = org_com.rename(columns={"transaction_id": "database_id"})
        ind_com = ind_com.rename(columns={"transaction_id": "database_id"})
        com_vend = com_vend.rename(columns={"transaction_id": "database_id"})

        id_mapping = pd.concat([org_com, ind_com, com_vend], ignore_index=True)
        id_mapping["provided_id"] = np.nan
        id_mapping["state"] = "MI"
        id_mapping["entity_type"] = "Transaction"

        id_mapping = id_mapping[self.id_mapping_column_order].copy()

        return id_mapping

    # NOTE: universal helper functions for creating the tables are below

    def filter_dataframe(
        self, merged_campaign_dataframe: pd.DataFrame, column_name: str
    ) -> pd.DataFrame:
        """Filters the inputted dataframe based on the column name

        Inputs:
            merged_campaign_dataframe:
            column_name: name of a column to filter the dataset on

        Returns:
            filtered_df: dataframe filtered based on the given column name

        """
        filtered_df = merged_campaign_dataframe[
            merged_campaign_dataframe[column_name].notna()
        ]
        # returns all the columns and rows associated with the column name
        # that are not null

        return filtered_df

    # NOTE: Helper functions for creating the individuals table are below

    def standardize_and_concatenate_individuals(
        self, individuals: pd.DataFrame, candidates: pd.DataFrame
    ) -> pd.DataFrame:
        """Standardizes and concatenates the individuals and candidate dataframes

        Inputs:
            individuals: DataFrame with individuals data
            candidates: DataFrame with candidates data

        Returns: individuals_df: standardized DataFrame with individual data
            following the database schema

        """
        individuals["entity_type"] = "Individual"
        candidates["entity_type"] = "Candidate"

        # rename the columns so they can be concatenated
        individuals = individuals.rename(columns={"full_name_uuid": "id"})
        candidates = candidates.rename(
            columns={
                "candidate_full_name_uuid": "id",
                "can_first_name": "first_name",
                "can_last_name": "last_name",
                "candidate_full_name": "full_name",
            }
        )
        individuals_df = pd.concat(
            [
                individuals,
                candidates,
            ],
            ignore_index=True,
            sort=False,
        )
        individuals_df["party"] = np.nan
        individuals_df["state"] = individuals_df["state"].fillna("MI")

        return individuals_df

    def create_filtered_individuals_tables(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame, pd.DataFrame]:
        """Filters the list of dataframes to create the individuals dataframe

        Inputs:
            standardized_dataframe_lst: list containing one of dataframes
            containing standardized Michigan contribution and expenditure data

        Returns:
            individuals_df: individuals dataframe as defined in the
            database schema
            id_mapping: id mapping for the individuals table
        """
        merged_dataframe = standardized_dataframe_lst[0]

        individuals_df = self.filter_dataframe(merged_dataframe, "first_name")
        candidates_df = self.filter_dataframe(
            merged_dataframe, "candidate_full_name_uuid"
        )
        id_mapping = self.create_individuals_id_mapping(individuals_df, candidates_df)
        individuals_df = individuals_df[
            [
                "full_name_uuid",
                "first_name",
                "last_name",
                "full_name",
                "state",
                "company",
            ]
        ].copy()

        candidates_df = candidates_df[
            [
                "candidate_full_name_uuid",
                "can_first_name",
                "can_last_name",
                "candidate_full_name",
            ]
        ]

        individuals_df = self.standardize_and_concatenate_individuals(
            individuals_df, candidates_df
        )
        individuals_df = individuals_df[
            [
                "id",
                "first_name",
                "last_name",
                "full_name",
                "entity_type",
                "state",
                "party",
                "company",
            ]
        ]

        return [individuals_df, id_mapping]

    # NOTE: Helper functions for creating the organizations table are below

    def standardize_and_concatenate_organizations(
        self,
        corporations: pd.DataFrame,
        committees: pd.DataFrame,
        vendors: pd.DataFrame,
    ) -> pd.DataFrame:
        """Standardizes and concatenates the corporations, committees, and vendor dfs

        Inputs:
            corporations: dataframe with corporations data
            committees: dataFrame with campaign committee data
            vendors: dataFrame with vendors data

        Returns:
            organizations: standardized DataFrame with organizations data
            following the database schema
        """
        corporations["entity_type"] = "corporation"
        committees["entity_type"] = "committee"
        vendors["entity_type"] = "vendor"

        corporations = corporations.rename(
            columns={"full_name_uuid": "id", "full_name": "name"}
        )
        committees = committees.rename(
            columns={"com_legal_name_uuid": "id", "com_legal_name": "name"}
        )
        vendors = vendors.rename(columns={"vend_name_uuid": "id", "vend_name": "name"})

        organizations = pd.concat(
            [corporations, committees, vendors],
            ignore_index=True,
            sort=False,
        )
        organizations["state"] = "MI"

        return organizations

    def create_filtered_organizations_tables(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame, pd.DataFrame]:
        """Filters the list of dataframes to create the organizations dataframe

        Inputs:
            standardized_dataframe_lst: list containing one of dataframes
            containing standardized Michigan contribution and expenditure data

        Returns:
            organizations_df: organizations dataframe as defined in the
            database schema
            id_mapping: id mapping for the organizations table

        """
        merged_dataframe = standardized_dataframe_lst[0]

        # contributing corporations have a first name that is null
        corporations_df = merged_dataframe[merged_dataframe["first_name"].isna()]
        committees_df = self.filter_dataframe(merged_dataframe, "com_legal_name_uuid")
        vendors_df = self.filter_dataframe(merged_dataframe, "vend_name_uuid")

        id_mapping = self.create_organizations_id_mapping(
            corporations_df, committees_df, vendors_df
        )

        corporations_df = corporations_df[["full_name_uuid", "full_name"]]

        committees_df = committees_df[["com_legal_name_uuid", "com_legal_name"]]

        vendors_df = vendors_df[["vend_name_uuid", "vend_name"]]

        organizations_df = self.standardize_and_concatenate_organizations(
            corporations_df, committees_df, vendors_df
        )

        organizations_df = organizations_df[["id", "name", "state", "entity_type"]]

        return [organizations_df, id_mapping]

    # NOTE: Helper functions for generating the transactions table are below

    def standardize_and_concatenate_transactions(
        self,
        org_com: pd.DataFrame,
        ind_com: pd.DataFrame,
        com_vend: pd.DataFrame,
    ) -> pd.DataFrame:
        """Standardizes and concatenates the corporations, committees, and vendors dfs

        Inputs:
            org_com: dataframe with organizations to committee transactions
            ind_com: dataframe with individual to committee transactions
            com_vend: dataframe with  committee  to vendor transactions

        Returns:
            transactions: standardized DataFrame with transactions data
            following the database schema

        """
        org_com = org_com.rename(
            columns={
                "full_name_uuid": "donor_id",
                "com_legal_name_uuid": "recipient_id",
            }
        )
        ind_com = ind_com.rename(
            columns={
                "full_name_uuid": "donor_id",
                "com_legal_name_uuid": "recipient_id",
            }
        )
        com_vend = com_vend.rename(
            columns={
                "com_legal_name_uuid": "donor_id",
                "vend_name_uuid": "recipient_id",
            }
        )
        transactions = pd.concat(
            [org_com, ind_com, com_vend], axis=0, ignore_index=True, sort=False
        )

        transactions["office_sought"] = np.nan

        return transactions

    def create_filtered_transactions_tables(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame, pd.DataFrame]:
        """Creates the Transactions tables from the dataframe list

        Inputs:
            standardized_dataframe_lst: list containing one of dataframes
            containing standardized Michigan contribution and expenditure data

        Returns:
            transactions_table: table as defined in database schema
            id_mapping: id mapping for the transactions table
        """
        merged_dataframe = standardized_dataframe_lst[0]

        # contributing corporations have a first name that is null

        # organization -> committee transaction
        org_to_com = merged_dataframe[merged_dataframe["first_name"].isna()]
        # individual -> committee transaction
        ind_to_com = self.filter_dataframe(merged_dataframe, "first_name")
        #  committee -> vendor transaction
        com_to_vend = self.filter_dataframe(merged_dataframe, "vend_name_uuid")

        id_mapping = self.create_transactions_id_mapping(
            org_to_com, ind_to_com, com_to_vend
        )
        org_to_com = org_to_com[
            [
                "transaction_id",
                "full_name_uuid",
                "year",
                "amount",
                "com_legal_name_uuid",
                "purpose",
                "transaction_type",
            ]
        ]

        ind_to_com = ind_to_com[
            [
                "transaction_id",
                "full_name_uuid",
                "year",
                "amount",
                "com_legal_name_uuid",
                "purpose",
                "transaction_type",
            ]
        ]

        com_to_vend = com_to_vend[
            [
                "transaction_id",
                "com_legal_name_uuid",
                "year",
                "amount",
                "vend_name_uuid",
                "purpose",
                "transaction_type",
            ]
        ]

        transactions_df = self.standardize_and_concatenate_transactions(
            org_to_com, ind_to_com, com_to_vend
        )

        transactions_df = transactions_df[
            [
                "donor_id",
                "year",
                "amount",
                "recipient_id",
                "office_sought",
                "purpose",
                "transaction_type",
            ]
        ]

        return [transactions_df, id_mapping]

    # NOTE: the helper functions below are used directly in create tables
    # calling the functions above

    def create_individuals_table(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame, pd.DataFrame]:
        """Creates the Individuals tables from the dataframe list

        Inputs:
            standardized_dataframe_lst: a list of 1 or 3 dataframes as
            outputted from standardize method.

        Returns:
            individuals_table: table as defined in database schema
            id_mapping: id mapping for the individuals table
        """
        individuals_table, id_mapping = self.create_filtered_individuals_tables(
            standardized_dataframe_lst
        )

        return [individuals_table, id_mapping]

    def create_organizations_table(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame, pd.DataFrame]:
        """Creates the Organizations tables from the dataframe

        Inputs:
            standardized_dataframe_lst: a list of 1 or 3 dataframes as
            outputted from standardize method.

        Returns:
            organizations_table: table as defined in database schema
            id_mapping: id mapping for the organizations table
        """
        (
            organizations_table,
            id_mapping,
        ) = self.create_filtered_organizations_tables(standardized_dataframe_lst)

        return [organizations_table, id_mapping]

    def create_transactions_table(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame, pd.DataFrame]:
        """Creates the Transactions tables from the dataframe list

        Inputs:
            standardized_dataframe_lst: a list of 1 or 3 dataframes as
            outputted from standardize method.

        Returns:
            transactions_table: table as defined in database schema
            id_mapping: id mapping for the transactions table
        """
        (
            transactions_table,
            id_mapping,
        ) = self.create_filtered_transactions_tables(standardized_dataframe_lst)
        return [transactions_table, id_mapping]
