# see also the README in this branch's utils folder

import pandas as pd

from utils.clean import StateCleaner
from utils.cleaner_utils import (
    az_employment_checker,
    az_individuals_convert,
    az_name_clean,
    az_organizations_convert,
    az_transactions_convert,
    az_transactor_sorter,
    convert_date,
)
from utils.constants import (
    AZ_INDIVIDUALS_FILEPATH,
    AZ_ORGANIZATIONS_FILEPATH,
    AZ_TRANSACTIONS_FILEPATH,
)


class ArizonaCleaner(StateCleaner):
    """This class is based on the StateCleaner abstract class,
    and cleans Arizona data"""

    def get_filepaths(self):
        return [
            "notebooks/all_inds_details.csv",
            "notebooks/all_orgs_details.csv",
            "notebooks/all_transactions.csv",
        ]

    def preprocess(self, filepaths_list: list[str]) -> list[pd.DataFrame]:
        """Turns filepaths into dataframes

        The input must be a list of valid filepaths which lead
        to pandas dataframes. Typically, these should be just two
        files: a transactions file and a details file, as
        harvested by az_curl_crawler. If these conditions are not
        met, the rest of the pipeline will not work

        args: list of two filepaths for dataframes,
        transactions and details, in that order

        returns: a list of two dataframes, transactions and details,
        in that order

        """

        df_list = []

        for filepath in filepaths_list:
            df_list.append(pd.read_csv(filepath))

        return df_list

    def clean_state(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """Calls the other methods in order

        This is the master function of the ArizonaCleaner
        class, and calling it will activate the cleaning
        pipeline which takes in filenames and outputs cleaned,
        standardized, and schema-compliant tables

        args: list of two filepaths which lead to dataframes

        returns: three schema-compliant tables for
        transactions, individuals, and organizations

        """

        filepaths = self.get_filepaths()

        individuals, organizations, transactions = self.preprocess(filepaths)

        details = pd.concat([individuals, organizations])

        cleaned_transactions, cleaned_details = self.clean([transactions, details])

        standardized_transactions, standardized_details = self.standardize(
            [cleaned_transactions, cleaned_details]
        )

        (
            az_individuals,
            az_organizations,
            az_transactions,
        ) = self.create_tables([standardized_transactions, standardized_details])

        return (az_individuals, az_organizations, az_transactions)

    def create_tables(
        self,
        data: list[pd.DataFrame],
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        """
        Creates the Individuals, Organizations, and Transactions tables from
        the dataframe list outputted from standardize

        Inputs:
            data: a list of 1 or 3 dataframes as outputted from standardize method.

        Returns: (individuals_table, organizations_table, transactions_table)
                    tuple containing the tables as defined in database schema
        """

        transactions, details = data

        individual_details = details[
            (details["entity_type"] == "Individual")
            | (details["entity_type"] == "Candidate")
        ]
        organization_details = details[
            (details["entity_type"] != "Individual")
            & (details["entity_type"] != "Candidate")
        ]

        # gathers relevant columns, puts them in schema order,
        # and enforces datatype
        az_transactions = az_transactions_convert(transactions)

        # does the same for individuals and organizations,
        # so long as there is some amount of data in each

        if len(individual_details) > 0:
            az_individuals = az_individuals_convert(individual_details)
        else:
            az_individuals = None

        if len(organization_details) > 0:
            az_organizations = az_organizations_convert(organization_details)
        else:
            az_organizations = None

        return (az_individuals, az_organizations, az_transactions)

    def standardize(self, details_df_list: list[pd.DataFrame]) -> list[pd.DataFrame]:
        """standardize names of entities

        takes in details dataframe and replaces the names of
        organization types to fit into the schema when appropriate

        args: details dataframe

        returns: details dataframe with relevant entity type
        names replaced by those for the regular schema
        """

        transactions_df, details_df = details_df_list[0], details_df_list[1]

        az_entity_name_dictionary = {
            "Organizations": "Company",
            "PACs": "Committee",
            "Parties": "Party",
            "Vendors": "Vendor",
            "Individual Contributors": "Individual",
            "Candidates": "Candidate",
        }
        standardized_details = details_df.replace(
            {"entity_type": az_entity_name_dictionary}
        )

        return [transactions_df, standardized_details]

    def clean(self, data: list[pd.DataFrame]) -> pd.DataFrame:
        """clean the contents of the columns

        INCOMPLETE

        transactions and details dataframes undergo cleaning of
        transaction dates, names are imputed to the right column,
        and employer information is retrieved,

        args: transactions and details dataframes

        returns: cleaned transactions and details dataframes

        NOTE: TO DO: coerce correct dtypes and make text lowercase

        """

        transactions, details = data

        merged_df = pd.merge(details, transactions, on="retrieved_id", how="inner")

        # Filter rows in the first dataframe based on the common 'ids'
        details = details[details["retrieved_id"].isin(merged_df["retrieved_id"])]

        try:
            transactions["TransactionDate"] = transactions["TransactionDate"].apply(
                convert_date
            )
        except TypeError:
            transactions["TransactionDate"] = transactions["TransactionDate"]

        details = az_name_clean(details)

        details = details.apply(az_employment_checker, args=(transactions,), axis=1)

        transactions = transactions.apply(az_transactor_sorter, axis=1)

        merged_df = pd.merge(
            transactions["base_transactor_id"],
            details[["retrieved_id", "office_name"]],
            how="left",
            left_on="base_transactor_id",
            right_on="retrieved_id",
        )

        office_sought = merged_df.where(pd.notnull(merged_df), None)["office_name"]

        transactions["office_sought"] = office_sought

        return [transactions, details]


if __name__ == "__main__":
    ArizonaCleaner.clean_state(
        [
            AZ_INDIVIDUALS_FILEPATH,
            AZ_ORGANIZATIONS_FILEPATH,
            AZ_TRANSACTIONS_FILEPATH,
            # "notebooks/all_inds_details.csv",
            # "notebooks/all_orgs_details.csv",
            # "notebooks/all_transactions.csv",
        ]
    )
