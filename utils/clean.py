from abc import ABC, abstractmethod

import pandas as pd
from constants import entity_name_dictionary

from utils import (
    az_individuals_convert,
    az_organizations_convert,
    az_transactions_convert,
    convert_date,
    name_clean,
)


class StateCleaner(ABC):
    """
    This abstract class is the one that all the state cleaners will be built on
    """

    @property
    def entity_name_dictionary(self) -> dict:
        """
        A dict mapping a state's raw entity names to standard versions

            Inputs: None

            Returns: _entity_name_dictionary
        """
        return self.entity_name_dictionary

    @abstractmethod
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
        pass

    @abstractmethod
    def clean(self, merged_campaign_df: pd.DataFrame) -> pd.DataFrame:
        """Cleans the state dataframe as needed and returns the dataframe

        Cleans the columns and converts the dtyes as needed to return one
        cleaned pandas DataFrame

        Inputs:
                merged_campaign_df: merged campaign dataframe

        Returns:
                clean_df: cleaned Pandas DataFrame
        """
        pass

    @abstractmethod
    def standardize(self, cleaned_dataframe: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the dataframe into the necessary format for the schema

        Inputs:
            cleaned_dataframe: cleaned Pandas Dataframe

        Returns:
             standardized_dataframe: Pandas Dataframe with standatdized names
                                    based on the entity_name_dictionary
        """
        pass

    def standardize_entity_names(self, entity_table: pd.DataFrame) -> pd.DataFrame:
        """Creates a new 'standard_entity_type' column from 'raw_entity_type'
        Args:
            entity_table: an entity dataframe containing 'raw_entity_type'

            Returns: entity_table with 'standard_entity_type created from
            the entity_name_dictionary
        """
        entity_table["standard_entity_type"] = entity_table["raw_entity_type"].map(
            lambda raw_entity_type: self.entity_name_dictionary.get(
                raw_entity_type, None
            )
        )
        return entity_table

    @abstractmethod
    def create_tables(
        self, standardized_df: pd.DataFrame
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Creates the Individuals, ORganizations, and Transactions tables from
        the standardized Pandas DataFrame

        Inputs: df standardized dataframe

        Returuns: (invividuals_table, organizations_table, transactions_table)
                    tuple containing the tables for the database schema
        """
        pass

    def clean_state(
        self, filepaths_list: list[str]
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Runs the StateCleaner pipeline returning a tuple of cleaned dataframes

        Inputs:
            filepaths_list: list of absolute filepaths to relevant state data.
                required naming conventions, order, and extensions
                defined per state.

        Returns: cleans the state and returns the standardized Inidividuals,
        Organizations, and Transactions tables in a tuple
        """
        # initial_dataframes = self.preprocess(filepaths_list)

        # should run create tables, which runs through the functions above
        # to preprocess, clean, standardizes and create the following tables
        # (invividuals_table, organizations_table, transactions_table)


class AzCleaner(StateCleaner):
    """This class is based on the StateCleaner abstract class,
    and cleans Arizona data"""

    def preprocess(self: list[str]) -> list[pd.DataFrame]:
        """Turns filepaths into dataframes

        The input must be a list of valid filepaths which lead
        to pandas dataframes. Typically, these should be just two
        files: a transactions file and a details file, as
        harvested by az_curl_crawler

        """

        df_list = []

        for filepath in self:
            df_list.append(pd.read_csv(filepath))

        return df_list

    def clean_state(self):
        """Calls the other methods in order

        args: list of two filepaths which lead to dataframes

        returns: three schema-compliant tables for
        transactions, individuals, and organizations

        """

        transactions, details = self.preprocess()

        # # cleans transactions dates
        # try:
        #     transactions["TransactionDate"] = transactions[
        #         "TransactionDate"].apply(convert_date)
        # except TypeError:
        #     transactions["TransactionDate"] = transactions[
        #         "TransactionDate"]

        # details = name_clean(details)

        # employer = transactions.groupby("CommitteeId")[
        #     "TransactionEmployer"].apply(lambda x: max(
        #         x, key=lambda y: y if y is not None else "")).values
        # #is only going to select one emplyer

        # details["company"] = employer

        # entity_type = transactions.groupby("CommitteeId")[
        # "CommitteeGroupName"].apply(min).values

        # details["entity_type"] = entity_type

        cleaned_transactions, cleaned_details = self.clean(transactions, details)
        # not sure the calling syntax is right here

        cleaned_details = self.standardize(cleaned_details)

        az_transactions, az_individuals, az_organizations = self.create_tables(
            cleaned_transactions, cleaned_details
        )

        return az_transactions, az_individuals, az_organizations

    def create_tables(cs_transactions, cs_details):
        """split up cleaned and standardized tables to undergo final formatting

        We split up the details tables into individuals (individual contributors
        and donors) and organizations (everything else), which then undergo
        conversion into fully schema-compatible transactions, individual details
        and organization details tables.

        args: cleaned tr

        """

        merged_df = pd.merge(
            cs_transactions["CommitteeId"],
            cs_details[["master_committee_id", "office_name"]],
            how="left",
            left_on="CommitteeId",
            right_on="master_committee_id",
        )

        office_sought = merged_df.where(pd.notnull(merged_df), None)["office_name"]

        cs_transactions["office_sought"] = office_sought

        individual_details = cs_details[
            cs_details["entity_type"] == ("Individual Contributions" or "Candidates")
        ]

        organization_details = cs_details[
            cs_details["entity_type"] != ("Individual Contributions" or "Candidates")
        ]

        az_transactions = az_transactions_convert(cs_transactions)

        az_individuals = az_individuals_convert(individual_details)

        az_organizations = az_organizations_convert(organization_details)

        return az_transactions, az_individuals, az_organizations

    def standardize(df):
        """standardize names of entities

        takes in details dataframe and replaces the names of
        organization types to fit into the schema when appropriate
        """

        # entity_name_dictionary = {
        #     'Organizations': 'Company',
        #     'PACs': 'Committee',
        #     'Parties': 'Party',
        #     'Vendors': 'Vendor',
        # }
        df.replace({"entity_type": entity_name_dictionary}, inplace=True)

        return df

    def clean(transactions: pd.DataFrame, details: pd.DataFrame) -> pd.DataFrame:
        """clean the contents of the columns

        transactions and details dataframes undergo cleaning of
        transaction dates,
        and make them the right dtypes

        also make everything lowercase

        """

        # cleans transactions dates
        try:
            transactions["TransactionDate"] = transactions["TransactionDate"].apply(
                convert_date
            )
        except TypeError:
            transactions["TransactionDate"] = transactions["TransactionDate"]

        details = name_clean(details)

        employer = (
            transactions.groupby("CommitteeId")["TransactionEmployer"]
            .apply(lambda x: max(x, key=lambda y: y if y is not None else ""))
            .values
        )
        # is only going to select one emplyer

        details["company"] = employer

        entity_type = (
            transactions.groupby("CommitteeId")["CommitteeGroupName"].apply(min).values
        )

        details["entity_type"] = entity_type

        return transactions, details
