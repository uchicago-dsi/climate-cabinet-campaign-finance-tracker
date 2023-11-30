import uuid

import numpy as np
import pandas as pd

from utils.clean import StateCleaner
from utils.constants import (
    MI_CON_FILEPATH,
    MI_CONTRIBUTION_COLUMNS,
    MI_EXP_FILEPATH,
    MI_EXPENDITURE_COLUMNS,
    MICHIGAN_CONTRIBUTION_COLS_RENAME,
    MICHIGAN_CONTRIBUTION_COLS_REORDER,
)
from utils.preprocess_mi_campaign_data import (
    read_contribution_data,
    read_expenditure_data,
)


class MichiganCleaner(StateCleaner):
    entity_name_dictionary = {
        "cfr_com_id": "original_com_id",
        "f_name": "first_name",
        "l_name_or_org": "last_name",
        "employer": "company",
        "doc_stmnt_year": "year",
        "exp_desc": "exp_desc",
        "contribtype": "transaction_type",
        "schedule_desc": "transaction_type",
    }  # map to entity types listed in the schema

    # columns to be added {full_name, entity_type, state, party, company,
    # transaction_id,
    #  "donor_id", ""}

    def is_contribution_dataframe(self, dataframe: pd.DataFrame) -> bool:
        """Checks if the DataFrame inputted is the contribution dataframe

        Inputs: dataframe: DataFrame with contribution or expenditure data

        Returns: False if the DataFrame contains a column specific
        "schedule_desc" to the MI expenditure data, and True if it does not.
        """
        if "schedule_desc" in dataframe.columns:
            # if schedule_desc is in the dataframe it is the Expenditure data
            return False
        return True

    def create_filepaths_list(self) -> [str]:
        """Creates a list of Michigan Contribution and Expenditure filepaths

        Inputs: None

        Returns: filepath_lst: list of filepaths to the MI expenditure
            and contribution data
        """
        filepath_lst = []

        for file in MI_EXP_FILEPATH.iterdir():
            filepath_lst.append(str(file))
        for file in MI_CON_FILEPATH.iterdir():
            filepath_lst.append(str(file))

        return filepath_lst

    # Helpher methods above are called throughout the class

    def preprocess(self, filepaths_list: list[str]) -> list[pd.DataFrame]:
        """
        Preprocesses the state data and returns a dataframe

        Reads in the state's data, makes any necessary bug fixes, and
        combines the data into a list of DataFrames

        Inputs:
            filepaths_list: list of absolute filepaths to relevant state data.
                required naming conventions, order, and extensions
                defined per state.

        Returns: a list of dataframes. If state data is all in one format
            (i.e. there are not separate individual and transaction tables),
            a list containing a single dataframe. Otherwise a list of three
            DataFrames that represent [transactions, individuals, organizations]
        """
        temp_exp_list = []
        temp_cont_list = []

        for file in filepaths_list:
            if "expenditures" in file.lower():
                temp_exp_list.append(
                    read_expenditure_data(file, MI_EXPENDITURE_COLUMNS)
                )
            elif "contributions" in file.lower():
                temp_cont_list.append(
                    read_contribution_data(file, MI_CONTRIBUTION_COLUMNS)
                )

        contribution_dataframe = self.merge_dataframes(temp_cont_list)
        expenditure_dataframe = self.merge_dataframes(temp_exp_list)

        return [contribution_dataframe, expenditure_dataframe]

    # Helper methods for preprocess are below

    def merge_dataframes(self, temp_list: [pd.DataFrame, pd.DataFrame]) -> pd.DataFrame:
        """Merges the list of dataframes into one Pandas DataFrame

        Inputs:
                temp_list: list of contribution of expenditure dataframes
                            from 2018 to 2023

        Returns:
                merged_dataframe: Pandas DataFrame of merged contribution
                                    or expenditure data
        """
        merged_dataframe = pd.concat(temp_list)
        if self.is_contribution_dataframe(merged_dataframe):
            merged_dataframe = self.fix_menominee_county_bug_contribution(
                merged_dataframe
            )
        else:
            merged_dataframe = self.drop_menominee_county(merged_dataframe)

        return merged_dataframe

    def fix_menominee_county_bug_contribution(
        self, merged_campaign_dataframe: pd.DataFrame
    ) -> pd.DataFrame:
        """Fixes the Memoninee County Rows within the Contribution data

        Inputs:
            merged_campaign_dataframe: Pandas DataFrame of merged MI
                contribution data


        Returns:
            merged_campaign_dataframe: Pandas DataFrame of merged MI
            contribution data edited in place to fix the
            Memoninee County Democratic Party bug

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
            expenditure data with Memoninee County Democratic Party columns dropped
        """
        subset_condition = (
            merged_campaign_dataframe["com_type"] == "MENOMINEE COUNTY DEMOCRATIC PARTY"
        )

        merged_campaign_dataframe = merged_campaign_dataframe.drop(
            merged_campaign_dataframe.loc[subset_condition].index
        )

        return merged_campaign_dataframe

    def clean(self, data_frame_lst: list[pd.DataFrame]) -> list[pd.DataFrame]:
        """Cleans the state dataframe as needed and returns the dataframe

        Cleans the columns, converts dtypes to match database schema, and drops rows
        not representing minimal viable transactions

        Inputs:
                data_frame_lst: a list of 1 or 3 dataframes
                as outputted from preprocess method.

        Returns: a list of dataframes. If state data is all in one format
            (i.e. there are not separate individual and transaction tables),
            a list containing a single dataframe. Otherwise a list of three
            DataFrames that represent [transactions, individuals, organizations]
        """
        clean_dataframe_lst = []

        contribution_dataframe, expenditure_dataframe = data_frame_lst

        clean_dataframe_lst.append(
            self.clean_contribution_dataframe(contribution_dataframe)
        )
        clean_dataframe_lst.append(
            self.clean_expenditure_dataframe(expenditure_dataframe)
        )

        return clean_dataframe_lst

    # Helper methods for clean() are below

    def clean_contribution_dataframe(
        self,
        merged_contribution_dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        """Cleans the contribution datafrmae as needed and returns the dataframe

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
            columns=[
                "doc_seq_no",
                "page_no",
                "cont_detail_id",
                "doc_type_desc",
                "address",
                "city",
                "zip",
                "occupation",
                "received_date",
                "aggregate",
                "extra_desc",
            ]
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
        """Cleans the expenditure datafrmae as needed and returns the dataframe

        Inputs:
                merged_expenditure_dataframe: Merged Michigan campaign
                expenditure dataframe

        Returns:
                merged_expenditure_dataframe: Merged Michigan expenditure
                dataframe cleaned in place
        """
        merged_expenditure_dataframe[
            ["amount", "supp_opp"]
        ] = merged_expenditure_dataframe[["amount", "supp_opp"]].apply(
            pd.to_numeric, errors="coerce"
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
            columns=[
                "doc_seq_no",
                "expenditure_type",
                "gub_account_type",
                "gub_elec_type",
                "page_no",
                "detail_id",
                "doc_type_desc",
                "extra_desc",
                "address",
                "city",
                "zip",
                "exp_date",
                "state_loc",
                "supp_opp",
                "can_or_ballot",
                "county",
                "debt_payment",
                "vend_addr",
                "vend_city",
                "vend_state",
                "vend_zip",
                "gotv_ink_ind",
                "fundraiser",
            ]
        )
        merged_expenditure_dataframe["full_name"] = (
            merged_expenditure_dataframe["f_name"].fillna("")
            + " "
            + merged_expenditure_dataframe["l_name_or_org"]
        )

        return merged_expenditure_dataframe

    def standardize(
        self, cleaned_dataframe_lst: list[pd.DataFrame]
    ) -> list[pd.DataFrame]:
        """Standardizes the dataframe into the necessary format for the schema

        Maps entity/office types and column names as defined in schema, adjust and add
        UUIDs as necessary

        Inputs:
            data: a list of 1 or 3 dataframes as outputted from clean method.

        Returns:
            cleaned_dataframe_ls: a list of dataframes.
            If state data is all in one format
            (i.e. there are not separate individual and transaction tables),
            a list containing a single dataframe. Otherwise a list of three
            DataFrames that represent [transactions, individuals, organizations]
        """
        contribution_dataframe, expenditure_dataframe = self.add_uuid_columns(
            cleaned_dataframe_lst
        )

        contribution_dataframe = contribution_dataframe.rename(
            columns=self.entity_name_dictionary
        )
        expenditure_dataframe = expenditure_dataframe.rename(
            columns=self.entity_name_dictionary
        )

        return [contribution_dataframe, expenditure_dataframe]

    # Helper methods for standardize are below

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
        contribution_dataframe, expenditure_dataframe = cleaned_dataframe_lst

        contribution_dataframe = self.generate_uuid(
            contribution_dataframe,
            ["full_name", "candidate_full_name", "com_legal_name"],
        )
        # generate uuid for contribution dataframe
        expenditure_dataframe = self.generate_uuid(
            expenditure_dataframe, ["full_name", "com_legal_name", "vend_name"]
        )
        # generate uuid for expenditure dataframe

        return [contribution_dataframe, expenditure_dataframe]

    def generate_uuid(
        self, merged_campaign_dataframe: pd.DataFrame, column_names: [str]
    ) -> pd.DataFrame:
        """Generates uuids for the pandas DataFrame based on the colummn names provided

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
                merged_campaign_dataframe[col_name].notnull()
            ][col_name]

            ids = {value: str(uuid.uuid4()) for value in non_null_values}

            # Map the generated UUIDs to a new column in the DataFrame
            merged_campaign_dataframe[
                "{}_uuid".format(col_name)
            ] = merged_campaign_dataframe[col_name].map(ids)

        # create tranaction ID for each row of the dataframe
        merged_campaign_dataframe["transaction_id"] = [
            uuid.uuid4() for _ in range(len(merged_campaign_dataframe))
        ]
        # add a step here to add this to a csv mapp?
        return merged_campaign_dataframe

    # TODO: IMPLEMENT THE CSV OUTPUT OF THE UUID MAPPING ABOVE
    def create_mapping_csv():
        """

        Inputs:

        Returns: None, Creates output/MI_ID_MAP.csv
        """
        pass

    def create_tables(
        self, data: list[pd.DataFrame]
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Creates the Individuals, Organizations, and Transactions tables from
        the dataframe list outputted from standardize

        Inputs:
            data: a list of 1 or 3 dataframes as outputted from standardize method.

        Returns: (individuals_table, organizations_table, transactions_table)
                    tuple containing the tables as defined in database schema
        """
        individuals_table = self.create_individuals_table(data)
        organizations_table = self.create_organizations_table(data)
        transactions_table = self.create_transactions_table(data)

        return (individuals_table, organizations_table, transactions_table)

    def filter_dataframe(
        self, merged_campaign_dataframe: pd.DataFrame, column_name: str
    ):
        """Filters the inputted dataframe based on the column name

        Inputs:
            merged_campaign_dataframe:
            column_name: name of a column to filter the dataset on

        Returns:
            filtered_df: dataframe filtered based on the given column name

        """
        filtered_df = merged_campaign_dataframe[
            merged_campaign_dataframe[column_name].notnull()
        ]
        # returns all the columns and rows associated with the column name
        # that are not null

        return filtered_df

    def create_filtered_individuals_tables(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> pd.DataFrame:
        """Filters the list of dataframes to create the individuals dataframe

        Inputs:
            standardized_dataframe_lst:

        Returns:
            individuals_dataframe:
        """
        contribution_dataframe, expenditure_dataframe = standardized_dataframe_lst
        contribution_individuals_df = self.filter_dataframe(
            contribution_dataframe, "full_name_uuid"
        )
        contribution_candidates_df = self.filter_dataframe(
            contribution_dataframe, "candidate_full_name_uuid"
        )
        expenditure_individuals_df = self.filter_dataframe(
            expenditure_dataframe, "first_name"
        )

        # only keep the relevant columns for the individuals table
        contribution_individuals_df = contribution_individuals_df[
            [
                "full_name_uuid",
                "first_name",
                "last_name",
                "full_name",
                "state",
                "company",
            ]
        ].copy()
        contribution_candidates_df = contribution_candidates_df[
            [
                "candidate_full_name_uuid",
                "can_first_name",
                "can_last_name",
                "candidate_full_name",
            ]
        ].copy()
        expenditure_individuals_df = expenditure_individuals_df[
            ["full_name_uuid", "first_name", "last_name"]
        ].copy()

        # create entity_type column
        contribution_individuals_df["entity_type"] = "individual"
        contribution_candidates_df["entity_type"] = "candidate"
        expenditure_individuals_df["entity_type"] = "individual"

        # rename the columns so they can be concatenated
        contribution_individuals_df = contribution_individuals_df.rename(
            columns={"full_name_uuid": "id"}
        )
        contribution_candidates_df = contribution_candidates_df.rename(
            columns={
                "candidate_full_name_uuid": "id",
                "can_first_name": "first_name",
                "can_last_name": "last_name",
                "candidate_full_name": "full_name",
            }
        )
        expenditure_individuals_df = expenditure_individuals_df.rename(
            columns={"full_name_uuid": "id"}
        )

        # concatenate individuals and add party column as null
        individuals_df = pd.concat(
            [
                contribution_individuals_df,
                contribution_candidates_df,
                expenditure_individuals_df,
            ],
            ignore_index=True,
            sort=False,
        )
        individuals_df["party"] = np.nan
        individuals_df["state"] = individuals_df["state"].fillna("MI")

        # reorder columns
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
        ].copy()

        return individuals_df

    def create_filtered_organizations_tables(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> pd.DataFrame:
        """Filters the list of dataframes to create the organizations dataframe

        Inputs:
            standardized_dataframe_lst:

        Returns:
            individuals_dataframe:

        """
        contribution_dataframe, expenditure_dataframe = standardized_dataframe_lst
        # contributing corporations have a first name that is null
        contribution_corporations_df = contribution_dataframe[
            contribution_dataframe["first_name"].isna()
        ]
        contribution_committees_df = self.filter_dataframe(
            contribution_dataframe, "com_legal_name_uuid"
        )
        expenditure_committees_df = self.filter_dataframe(
            expenditure_dataframe, "com_legal_name_uuid"
        )
        expenditure_vendors_df = self.filter_dataframe(
            expenditure_dataframe, "vend_name_uuid"
        )
        expenditure_corporations_df = expenditure_dataframe[
            expenditure_dataframe["first_name"].isna()
        ]

        contribution_corporations_df = contribution_corporations_df[
            ["full_name_uuid", "full_name"]
        ].copy()
        contribution_committees_df = contribution_committees_df[
            ["com_legal_name_uuid", "com_legal_name"]
        ]
        expenditure_committees_df = expenditure_committees_df[
            ["com_legal_name_uuid", "com_legal_name"]
        ].copy()
        expenditure_vendors_df = expenditure_vendors_df[
            ["vend_name_uuid", "vend_name"]
        ].copy()
        expenditure_corporations_df = expenditure_corporations_df[
            ["full_name_uuid", "full_name"]
        ].copy()

        # create entity_type column
        contribution_corporations_df["entity_type"] = "corporation"
        contribution_committees_df["entity_type"] = "committee"
        expenditure_committees_df["entity_type"] = "committee"
        expenditure_vendors_df["entity_type"] = "vendor"
        expenditure_corporations_df["entity_type"] = "corporation"

        # rename the columns so they can be concatenated
        contribution_corporations_df = contribution_corporations_df.rename(
            columns={"full_name_uuid": "id", "full_name": "name"}
        )
        contribution_committees_df = contribution_committees_df.rename(
            columns={"com_legal_name_uuid": "id", "com_legal_name": "name"}
        )
        expenditure_committees_df = expenditure_committees_df.rename(
            columns={"com_legal_name_uuid": "id", "com_legal_name": "name"}
        )
        expenditure_vendors_df = expenditure_vendors_df.rename(
            columns={"vend_name_uuid": "id", "vend_name": "name"}
        )
        expenditure_corporations_df = expenditure_corporations_df.rename(
            columns={"full_name_uuid": "id", "full_name": "name"}
        )

        # concatenate organizations and add state column set to MI
        organizations_df = pd.concat(
            [
                contribution_corporations_df,
                contribution_committees_df,
                expenditure_committees_df,
                expenditure_vendors_df,
                expenditure_corporations_df,
            ],
            ignore_index=True,
            sort=False,
        )
        organizations_df["state"] = "MI"

        # reorder columns
        organizations_df = organizations_df[
            ["id", "name", "state", "entity_type"]
        ].copy()

        return organizations_df

    def create_filtered_transactions_tables(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> pd.DataFrame:
        """Creates the Transactions tables from the dataframe list outputted
        from standardize

        Inputs:
            standardized_dataframe_lst: a list of 1 or 3 dataframes as
            outputted from standardize method.

        Returns:
            transactions_table: table as defined in database schema
        """
        contribution_dataframe, expenditure_dataframe = standardized_dataframe_lst

        # contributing corporations have a first name that is null

        # contribution organization -> committee
        contribution_org_to_com_transaction = contribution_dataframe[
            contribution_dataframe["first_name"].isna()
        ]
        # contribution individual -> committee
        contribution_ind_to_com_transaction = self.filter_dataframe(
            contribution_dataframe, "first_name"
        )

        # expenditure organization -> committee
        expenditure_org_to_com_transaction = expenditure_dataframe[
            expenditure_dataframe["first_name"].isna()
        ]
        # expenditure individual -> committee
        expenditure_ind_to_com_transaction = self.filter_dataframe(
            expenditure_dataframe, "first_name"
        )

        # expenditure committee -> vendor
        expenditure_com_to_vendor_transaction = self.filter_dataframe(
            expenditure_dataframe, "vend_name_uuid"
        )

        contribution_org_to_com_transaction = contribution_org_to_com_transaction[
            [
                "transaction_id",
                "full_name_uuid",
                "year",
                "amount",
                "com_legal_name_uuid",
                "transaction_type",
            ]
        ].copy()
        contribution_ind_to_com_transaction = contribution_ind_to_com_transaction[
            [
                "transaction_id",
                "full_name_uuid",
                "year",
                "amount",
                "com_legal_name_uuid",
                "transaction_type",
            ]
        ].copy()

        expenditure_org_to_com_transaction = expenditure_org_to_com_transaction[
            [
                "transaction_id",
                "full_name_uuid",
                "year",
                "amount",
                "com_legal_name_uuid",
                "purpose",
                "transaction_type",
            ]
        ].copy()
        expenditure_ind_to_com_transaction = expenditure_ind_to_com_transaction[
            [
                "transaction_id",
                "full_name_uuid",
                "year",
                "amount",
                "com_legal_name_uuid",
                "purpose",
                "transaction_type",
            ]
        ].copy()
        expenditure_com_to_vendor_transaction = expenditure_com_to_vendor_transaction[
            [
                "transaction_id",
                "com_legal_name_uuid",
                "year",
                "amount",
                "vend_name_uuid",
                "purpose",
                "transaction_type",
            ]
        ].copy()

        # create purpose columns in the contribution data set to NaN

        contribution_org_to_com_transaction["purpose"] = np.nan
        contribution_ind_to_com_transaction["purpose"] = np.nan

        # rename columns to match schema

        contribution_org_to_com_transaction = (
            contribution_org_to_com_transaction.rename(
                columns={
                    "full_name_uuid": "donor_id",
                    "com_legal_name_uuid": "recipient_id",
                }
            )
        )
        contribution_ind_to_com_transaction = (
            contribution_ind_to_com_transaction.rename(
                columns={
                    "full_name_uuid": "donor_id",
                    "com_legal_name_uuid": "recipient_id",
                }
            )
        )
        expenditure_org_to_com_transaction = expenditure_org_to_com_transaction.rename(
            columns={
                "full_name_uuid": "donor_id",
                "com_legal_name_uuid": "recipient_id",
            }
        )
        expenditure_ind_to_com_transaction = expenditure_ind_to_com_transaction.rename(
            columns={
                "full_name_uuid": "donor_id",
                "com_legal_name_uuid": "recipient_id",
            }
        )
        expenditure_com_to_vendor_transaction = (
            expenditure_com_to_vendor_transaction.rename(
                columns={
                    "com_legal_name_uuid": "donor_id",
                    "vend_name_uuid": "recipient_id",
                }
            )
        )

        # concatenate dataframes
        transactions_df = pd.concat(
            [
                contribution_org_to_com_transaction,
                contribution_ind_to_com_transaction,
                expenditure_org_to_com_transaction,
                expenditure_ind_to_com_transaction,
                expenditure_com_to_vendor_transaction,
            ],
            ignore_index=True,
            sort=False,
        )

        transactions_df["office_sought"] = np.nan

        # reorder columns
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
        ].copy()

        return transactions_df

    # TODO: Helper functions for create_tables() are below

    def create_individuals_table(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Creates the Individuals tables from the dataframe list outputted
        from standardize

        Inputs:
            standardized_dataframe_lst: a list of 1 or 3 dataframes as
            outputted from standardize method.

        Returns:
            individuals_table: table as defined in database schema
        """
        individuals_table = self.create_filtered_individuals_tables(
            standardized_dataframe_lst
        )

        return individuals_table

    def create_organizations_table(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Creates the Organizations tables from the dataframe list outputted
        from standardize

        Inputs:
            standardized_dataframe_lst: a list of 1 or 3 dataframes as
            outputted from standardize method.

        Returns:
            organizations_table: table as defined in database schema
        """
        organizations_table = self.create_filtered_organizations_tables(
            standardized_dataframe_lst
        )

        return organizations_table

    def create_transactions_table(
        self, standardized_dataframe_lst: list[pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Creates the Transactions tables from the dataframe list outputted
        from standardize

        Inputs:
            standardized_dataframe_lst: a list of 1 or 3 dataframes as
            outputted from standardize method.

        Returns:
            transactions_table: table as defined in database schema
        """
        transactions_table = self.create_filtered_transactions_tables(
            standardized_dataframe_lst
        )
        return transactions_table

    # TODO: IMPLEMENT clean_state() below

    def clean_state(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Runs the StateCleaner pipeline returning a tuple of cleaned dataframes

        Returns: use preprocess, clean, standardize, and create_tables methods
        to output (individuals_table, organizations_table, transactions_table)
        as defined in database schema
        """
        filepath_lst = self.create_filepaths_list()
        preprocessed_dataframe_lst = self.preprocess(filepath_lst)
        cleaned_dataframe_lst = self.clean(preprocessed_dataframe_lst)
        standardized_dataframe_lst = self.standardize(cleaned_dataframe_lst)
        tables = self.create_tables(standardized_dataframe_lst)

        return tables

    # TODO: GET RID OF INPLACE = TRUE CREATE A NEW DATASTRUCTURE INSTEAD
