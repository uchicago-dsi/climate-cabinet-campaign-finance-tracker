from abc import ABC, abstractmethod

import pandas as pd

from utils.cleaner_utils import convert_date


class StateCleaner(ABC):
    """
    This abstract class is the one that all the state cleaners will be built on
    """

    @abstractmethod
    def preprocess_(self) -> pd.DataFrame:
        """Preprocesses the state data and returns a dataframe"""
        # should read in the dataframe and fix any bugs (MI data)
        dataframe = pd.DataFrame()
        return dataframe

    @abstractmethod
    def clean(self) -> pd.DataFrame:
        """Cleans the state dataframe as needed and returns the dataframe"""
        # should clean the dataframes's cols and change dtypes as needed
        dataframe = pd.DataFrame()
        return dataframe

    @abstractmethod
    def standardize(self) -> pd.DataFrame:
        """Standardizes the dataframe into the necessary
        format for the scheme"""
        # should standardize using the entity name dict and rename cols
        dataframe = pd.DataFrame()
        return dataframe

    @abstractmethod
    def entity_name_dictionary(self) -> dict:
        """A dict mapping a state's raw entity names to standard versions"""
        return self._entity_name_dictionary

    # this method should be the same for all subclasses so we implement it here
    def standardize_entity_names(
        self, entity_table: pd.DataFrame
    ) -> pd.DataFrame:  # noqa: E501
        """Creates a new 'standard_entity_type' column from 'raw_entity_type'
        Args:entity_table: an entity dataframe containing 'raw_entity_type'

        Returns: entity_table with 'standard_entity_type created from
        the entity_name_dictionary
        """
        entity_table["standard_entity_type"] = entity_table[
            "raw_entity_type"
        ].map(  # noqa: E501
            lambda raw_entity_type: self.entity_name_dictionary.get(
                raw_entity_type, None
            )
        )
        return entity_table

    @abstractmethod
    def standardize_amounts(
        self, transaction_table: pd.DataFrame
    ) -> pd.DataFrame:  # noqa: E501
        """Convert 'amount' column to a float representing value in USD
        Args:
            transactions_table: must contain "amount" column
        """
        pass

    # could be looped into one standardize method that captures
    # all of the necessary standardizations and format the dataframe?

    # below are non abstractmethods, each class should inherit
    # and utilize these methods
    def create_tables(self, df: pd.Dataframe) -> pd.DataFrame:
        """Takes the state data and creates pandas dataframes to
        match the schema"""
        pass

    def to_SQL(self, df: pd.DataFrame):
        """Writes the Pandas Dataframes to SQL Database records"""
        pass

    def clean_state(self):
        """runs the StateCleaner and returns the SQL Dabase records"""
        pass


# should run the methods to load in the dataset, clean and standardize it,
# and # create the tables. This should return the formatted tables


class AzCleaner:
    """This class is based on the StateCleaner abstract class,
    and cleans Arizona data"""

    def clean(self):
        """This function cleans arizona dataframes"""
        # cleans transactions dates
        self["TransactionDate"] = self["TransactionDate"].apply(convert_date)
