from abc import ABC, abstractmethod

import pandas as pd


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
    def preprocess(self) -> pd.DataFrame:
        """
        Preprocesses the state data and returns a dataframe

        Reads in the state's data, makes any necessary bug fixes, and
        combines the data into one Pandas DataFrame

        Inputs: None

        Returns: dataframe
        """
        pass

    @abstractmethod
    def clean(self, merged_campaign_df: pd.Dataframe) -> pd.DataFrame:
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
        self, standardized_df: pd.Dataframe
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Creates the Individuals, ORganizations, and Transactions tables from
        the standardized Pandas DataFrame

        Inputs: df standardized dataframe

        Returuns: (invividuals_table, organizations_table, transactions_table)
                    tuple containing the tables for the database schema
        """
        pass

    def clean_state(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
            Runs the StateCleaner and returns the SQL Dabase records

        Inputs: None

        Returns: cleans the state and returns the standardized Inidividuals,
                            Organizations, and Transactions tables in a tuple
        """
        StateCleaner.create_tables()
        # should run create tables, which runs through the functions above
        # to preprocess, clean, standardizes and create the following tables
        # (invividuals_table, organizations_table, transactions_table)
