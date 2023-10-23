from abc import ABC, abstractmethod

import pandas as pd


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
    def standardize_entity_names(
        self, entity_table: pd.DataFrame
    ) -> pd.DataFrame:
        """Creates a new 'standard_entity_type' column from 'raw_entity_type'

        Args:
            entity_table: an entity dataframe containing 'raw_entity_type'
        Returns: entity_table with 'standard_entity_type created from the
            entity_name_dictionary
        """
        entity_table["standard_entity_type"] = entity_table[
            "raw_entity_type"
        ].map(
            lambda raw_entity_type: self.entity_name_dictionary.get(
                raw_entity_type, None
            )
        )
        return entity_table

    # this is probably different for all so we make it abstract and
    # leave it blank
    @abstractmethod
    def standardize_amounts(
        self, transaction_table: pd.DataFrame
    ) -> pd.DataFrame:
        """Convert 'amount' column to a float representing value in USD

        Args:
            transactions_table: must contain "amount" column
        """
        pass
