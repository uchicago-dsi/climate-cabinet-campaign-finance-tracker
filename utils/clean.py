from abc import ABC, abstractmethod

import pandas as pd

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

                Returns: entity_table with 'standard_entity_type created from the
            entity_name_dictionary
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

    def preprocess(self, filepaths_list: list[str]) -> list[pd.DataFrame]:
        """Reads in arizona files and does some basic processing

        what gets passed into preprocess? List of as many files as we
        can, of all types.

        Might need to be nested list of files, transactions list,
        individuals list, organizations list, with varying number of files in each

        or rather, transactions files, name files,

        """

        df_list = []

        for file in filepaths_list:
            df_list.append(pd.read_csv(file))

        return df_list

    def clean_state(self):
        """Clean the arizona dataframes

        args: list fo four preprocessed dataframes

        returns: three schema-compliant tables for
        transactions, individuals, and organizations

        """

        # cleans transactions dates
        try:
            self[0]["TransactionDate"] = self[0]["TransactionDate"].apply(convert_date)
        except TypeError:
            self[0]["TransactionDate"] = self[0]["TransactionDate"]

        self[2] = name_clean(self[2])

        az_transactions = az_transactions_convert(self[0])

        # at present, it is not clear how to distinguish between
        # tables detailing individuals and organizations
        # based only on the information
        # plan to pipe in this information from elsewhere
        # but not yet implemented

        # az_individuals = az_individuals_convert(self[2], self[3])
        az_individuals = az_individuals_convert(self[0], self[1], self[2])

        # az_organizations = az_organizations_convert(self[3])
        az_organizations = az_organizations_convert(self[0], self[2])

        return az_transactions, az_individuals, az_organizations

    def standardize():
        """standardize names of entities"""

        # entity_name_dictionary = {
        #     "Organizations": "Company",
        #     "PACs": "Committee",
        #     "Parties": "Party",
        #     "": "",
        # }

    def clean(df: pd.DataFrame) -> pd.DataFrame:
        """clean the contents of the columns
        and make them the right dtypes

        also make everything lowercase

        """
