from abc import ABC, abstractmethod
import pandas as pd

from utils import clean
from utils import constants
from utils import PA_Data_Web_Scraper as scrape
from utils import PA_EDA_Functions as eda

class PennsylvaniaCleaner(clean.StateCleaner):

   #filepaths_list = pd.read_csv(repo_root / "data" / "results.csv")

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
        for path in filepaths_list:
            dir = path.split("_")
            year = dir[len(dir) - 1].replace(".txt","")
            year = int(year)

            contributor_datasets, filer_datasets, expense_datasets = [],[],[]
            df = eda.initialize_PA_dataset(path, year)
            if "contrib" in path:
                contributor_datasets.append(df)                
            elif "filer" in path:
                filer_datasets.append(df)
            elif "expense" in path:
                expense_datasets.append(df)
            else:
                   # do nothing 

        pass

class StateCleaner(ABC):
    """TODO: what does this class represent? What does it do?"""

    # use the @property decorator for attributes of the class
    # for example, for the property 'entity_name_dictionary' that
    # is a dict, you would define it as follows:

    @property
    def entity_name_dictionary(self) -> dict:
        """A dict mapping a state's raw entity names to standard versions"""
        return self._entity_name_dictionary

    # this method should be the same for all subclasses so we implement it here
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

    # this is probably different for all so we make it abstract and
    # leave it blank
    @abstractmethod
    def standardize_amounts(self, transaction_table: pd.DataFrame) -> pd.DataFrame:
        """Convert 'amount' column to a float representing value in USD

        Args:
            transactions_table: must contain "amount" column
        """
        pass


#class PACleaner(StateCleaner):
