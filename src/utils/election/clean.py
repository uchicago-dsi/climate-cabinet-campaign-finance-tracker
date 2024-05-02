"""Abstract base class for transforming election into standard schema"""

from abc import ABC, abstractmethod

import pandas as pd


class ElectionResultTransformer(ABC):
    """This abstract class is the one that all the election result cleaners will be built on

    Given a path to a directory with raw data from dataset, this class provides
    the interface for:
    - reading the data into pandas DatFrames
    - deleting empty or clearly erroneous rows
    - renaming / reshaping data to fit a single schema
    - validating data to fit schema
    - adding uuids based on the Individual table uuids

    The methods in this class are meant to be very conservative. Raw data should
    not be modified, only transformed. Rows cannot be changed, only deleted in
    obviously erroneous cases.
    """

    @abstractmethod
    def preprocess(self, directory: str = None) -> pd.DataFrame:
        """Preprocesses the election data and returns a dataframe

        Reads in the election data, makes any necessary bug fixes, and
        combines the data into a list of DataFrames, discards data not in schema

        Inputs:
            directory: absolute path to a directory with relevant election data.
                defined per dataframe.

        Returns:
            One dataframe with all relevant information
        """
        pass

    @abstractmethod
    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        """Cleans the dataframe as needed and returns the dataframe

        Cleans the columns, converts dtypes to match database schema, and drops
        rows not representing minimal viable transactions

        Inputs:
            data: Dataframe as output from preprocess method.

        Returns: Dataframe
        """
        pass

    @abstractmethod
    def standardize(self, data: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the dataframe into the necessary format for the schema

        Maps [] types and column names as defined in schema, adjust
         and add UUIDs as necessary

        Inputs:
            data: dataframe as outputted from clean method.

        Returns: Dataframe
        """
        pass

    @abstractmethod
    def create_table(self, data: pd.DataFrame) -> pd.DataFrame:
        """Creates the election result table that has matched uuid with individual dataset

        Inputs:
            data: Dataframe as output from standardize method.

        Returns: a table as defined in database schema
        """
        pass

    @abstractmethod
    def clean_state(self) -> pd.DataFrame:
        """Runs the ElectionResultCleaner pipeline returning a cleaned dataframes

        Returns: cleans the state and returns the standardized table showing
        the election results.
        """
        pass
