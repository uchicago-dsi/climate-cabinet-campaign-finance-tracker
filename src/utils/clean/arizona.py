"""Code for cleaning and standardizing raw data retrieved from Arizona.

Inherits from StateCleaner.
"""

import uuid

import pandas as pd

from utils.clean.clean import StateCleaner
from utils.clean.constants import (
    AZ_INDIVIDUALS_FILEPATH,
    AZ_ORGANIZATIONS_FILEPATH,
    AZ_TRANSACTIONS_FILEPATH,
    state_abbreviations,
)
from utils.clean.utils import convert_date


def az_name_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Replace empty 'candidate' value with 'committee_name' value

    Because in some cases the 'candidate' column of the tables,
    which, contrary to expected naming convention, contains the name of
    the entity, is sometimes blank while the entity's name is in the
    commitee_name column, we replace the relevant empty values
    by their imputed values

    args: df: detailed information dataframe

    returns: modified detailed information dataframe

    """
    df_working = df.copy()

    df_working["candidate"] = df.apply(
        lambda row: (
            row["committee_name"]
            if (row["candidate"] == ("" or None or "" or """"""))
            else row["candidate"]
        ),
        axis=1,
    )

    return df_working


def az_transactions_convert(df: pd.DataFrame) -> pd.DataFrame:
    """Make raw transactions table schema-compliant

    We take the relevant columns of the raw transactions
    table and extract, reorder and relabel them
    in compliance with the transactions table database schema

    args: df: raw ransactions dataframe

    returns: schema-compliant transactions dataframe
    """
    d = {
        "transaction_id": df["PublicTransactionId"].astype(int).astype(str),
        "donor_id": df["base_transactor_id"].astype(int).astype(str),
        "year": df["TransactionDateYear"].astype(int),
        "amount": df["Amount"].abs().astype(float),
        "recipient_id": df["other_transactor_id"].astype(int).astype(int),
        "office_sought": df["office_sought"].astype(str).str.lower(),
        "purpose": df["Memo"].astype(str).str.lower(),
        "transaction_type": df["TransactionType"].astype(str).str.lower(),
        "TransactionTypeDispositionId": df["TransactionTypeDispositionId"],
    }

    trans_df = pd.DataFrame(data=d)

    trans_df = trans_df.apply(az_donor_recipient_director, axis=1)

    trans_df = trans_df.drop(columns=["TransactionTypeDispositionId"])

    return trans_df


def az_individuals_convert(details_df: pd.DataFrame) -> pd.DataFrame:
    """Make individuals detail table schema compliant

    INCOMPLETE

    We take the relevant columns of the raw individual details
    table and extract, reorder and relabel them in
    compliance with the individuals table database schema

    Presently, the function does not attempt to split names,
    and instead leaves those columns blank and preserves the
    available full names

    NOTE: Employers information is presently difficult to
    extract owing to inconsistent id locations across
    various kinds of tables. A fix is underway.

    args: df: transactions dataframe and individual details dataframe

    returns: schema-compliant individual details dataframe

    """
    details_df = details_df.apply(az_individual_name_checker, axis=1)
    details_df["full_name"] = details_df["full_name"].str.replace("\t", "")

    employer = details_df["company"]

    entity_type = details_df["entity_type"]

    states_list = []

    for i in details_df["committee_address"].str.split(" "):
        if i is not None:
            try:
                abb = i[-2]
                if " " + abb.upper() + " " not in state_abbreviations:
                    states_list.append(None)
                else:
                    states_list.append(i[-2].upper())
            except TypeError:
                states_list.append(None)
        else:
            states_list.append(None)

    d = {
        "id": details_df["retrieved_id"].astype(int).astype(str),
        "first_name": None,
        "last_name": None,
        "full_name": details_df["full_name"].astype(str).str.lower(),
        "entity_type": entity_type.astype(str).str.lower(),
        "state": states_list,
        "party": details_df["party_name"].astype(str).str.lower(),
        "company": employer.astype(str).str.lower(),
    }

    return pd.DataFrame(data=d)


def az_organizations_convert(df: pd.DataFrame) -> pd.DataFrame:
    """Make organizations detail table schema compliant

    INCOMPLETE

    We take the relevant columns of the raw organizations details
    table and extract, reorder and relabel them
    in compliance with the organizations table database schema

    args: df: raw organizations dataframe

    returns: schema-compliant organizations dataframe

    """
    entity_type = df["entity_type"]

    states_list = []

    for i in df["committee_address"].str.split(" "):
        if i is not None:
            try:
                abb = i[-2]
                if " " + abb.upper() + " " not in state_abbreviations:
                    states_list.append(None)
                else:
                    states_list.append(i[-2].upper())
            except TypeError:
                states_list.append(None)
        else:
            states_list.append(None)

    d = {
        "id": df["retrieved_id"].astype(int).astype(str),
        "name": df["committee_name"].astype(str).str.lower(),
        "state": states_list,
        "entity_type": entity_type.astype(str).str.lower(),
    }

    return pd.DataFrame(data=d)


def az_transactor_sorter(row: pd.Series) -> pd.Series:
    """Sorts ids into base and other transactor

    Because the arizona transactions dataset records the ids
    of transactors differently depending on the entity type,
    it is necessary to sort through it and clarify the ids.
    The base transactor is the entity in the transaction from
    whom the transaction was collected, and the other transactor
    is the other entity involved in the transaction

    args: row of the transactions dataframe

    returns: adjusted row of the transactions dataframe,
    with new columns 'base_transactor_id' and
    'other_transactor_id'

    """
    if row["entity_type"] in ["Vendor", "Individual"]:
        row["base_transactor_id"] = row["TransactionNameGroupId"]
        row["other_transactor_id"] = row["CommitteeId"]
    else:
        row["base_transactor_id"] = row["CommitteeId"]
        row["other_transactor_id"] = row["TransactionNameGroupId"]

    return row


def az_donor_recipient_director(row: pd.Series) -> pd.Series:
    """Sorts ids into donor and recipient columns

    We switch the elements of a row in the transactions table if the
    'TransactionTypeDispositionId' column indicates that the transaction
    involves the other transactor receiving money and the base transactor
    giving money. This is coded as a '2' in the dataset.

    args: row of the transactions dataframe

    returns: adjusted row of the transactions dataframe

    """
    base_transactor_giving_money_flag = 2
    if row["TransactionTypeDispositionId"] == base_transactor_giving_money_flag:
        row["donor_id"], row["recipient_id"] = (
            row["recipient_id"],
            row["donor_id"],
        )
    return row


def az_employment_checker(row: pd.Series, transactions: pd.DataFrame) -> pd.Series:
    """Retrieves employment data

    We attempt to collect employment data for the
    individuals dataframe. Because of how candidates vs
    individual contributors are coded, it is necessary to
    code the candidate employment separately, otherwise
    the column shapes become different

    args: row of the individuals dataframe,
    transactions dataframe

    returns: adjusted row of the individuals dataframe

    """
    if row["entity_type"] == "Candidate":
        row["company"] = "None (Is a Candidate)"
    else:
        transactions[transactions["retrieved_id"] == row["retrieved_id"]].iloc[0][
            "TransactionEmployer"
        ]
        row["company"] = transactions[
            transactions["retrieved_id"] == row["retrieved_id"]
        ].iloc[0]["TransactionEmployer"]

    return row


def az_individual_name_checker(row: pd.Series) -> pd.Series:
    """Collect names for individuals

    Since names are coded differently for candidates vs
    individual contributors, we go row by row and collect the
    right element in each case

    args: row of the individuals dataframe

    returns: adjusted row of the individual dataframe
    with the correct names of individual contributors and
    candidates in the 'full name' column

    """
    if row["entity_type"] == "Candidate":
        row["full_name"] = row["candidate"]
    else:
        row["full_name"] = row["retrieved_name"]
    return row


def az_id_table(
    individuals_df: pd.DataFrame,
    organizations_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    *args: int,
) -> pd.DataFrame:
    """Create a table of old ids and new uuids"""
    ind_ids = individuals_df["id"]

    org_ids = organizations_df["id"]

    trans_ids = transactions_df["transaction_id"]

    years = pd.concat[
        individuals_df["year"],
        organizations_df["year"],
        transactions_df["year"],
    ]

    all_ids = pd.concat([ind_ids, org_ids, trans_ids])

    id_types = (
        ["Individual"] * len(ind_ids)
        + ["Organization"] * len(org_ids)
        + ["Transaction"] * len(trans_ids)
    )

    d = {
        "state": "AZ",
        "year": years,
        "entity_type": id_types,
        "provided_id": all_ids,
    }

    id_map_df = pd.DataFrame(data=d)
    id_map_df["database_id"] = [uuid.uuid4() for _ in range(len(id_map_df.index))]

    return id_map_df


class ArizonaCleaner(StateCleaner):
    """Based on the StateCleaner abstract class and cleans Arizona data"""

    def clean_state(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Calls the other methods in order

        This is the master function of the ArizonaCleaner
        class, and calling it will activate the cleaning
        pipeline which takes in filenames and outputs cleaned,
        standardized, and schema-compliant tables

        args: list of three filepaths which lead to dataframes

        returns: three schema-compliant tables for
        transactions, individuals, and organizations

        """
        filepaths = self.get_filepaths()

        individuals, organizations, transactions = self.preprocess(filepaths)
        transactions = transactions.head(1000)

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

    def get_filepaths(self) -> list[str]:
        """Returns paths to relevant Arizona data"""
        return [
            AZ_INDIVIDUALS_FILEPATH,
            AZ_ORGANIZATIONS_FILEPATH,
            AZ_TRANSACTIONS_FILEPATH,
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

    def create_tables(
        self,
        data: list[pd.DataFrame],
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Creates the Individuals, Organizations, and Transactions tables

        Inputs: data: a list of 3 dataframes as outputted from the standardize method.

        Returns: a nested tuple of dataframes, ordered as such: (individuals_table,
        organizations_table, (transactions: ind->ind, ind->org, org->ind, org->org))
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
        """Standardize names of entities

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
        """Clean the contents of the columns

        transactions and details dataframes undergo cleaning of
        transaction dates, names are imputed to the right column,
        and employer information is retrieved,

        args: list of transactions and details dataframes

        returns: cleaned transactions and details dataframes
        """
        transactions, entities = data

        merged_df = entities.merge(transactions, on="retrieved_id", how="inner")

        # Filter rows in the first dataframe based on the common 'ids'
        entities = entities[entities["retrieved_id"].isin(merged_df["retrieved_id"])]

        try:
            transactions["TransactionDate"] = transactions["TransactionDate"].apply(
                convert_date
            )
        except TypeError as e:
            print(f"Error converting Arizona dates: {e}")
            transactions["TransactionDate"] = transactions["TransactionDate"]

        entities = az_name_clean(entities)

        entities = entities.apply(az_employment_checker, args=(transactions,), axis=1)

        transactions = transactions.apply(az_transactor_sorter, axis=1)

        # TODO: what is going on here?
        merged_df = pd.merge(  # noqa: PD015
            transactions["base_transactor_id"],
            entities[["retrieved_id", "office_name"]],
            how="left",
            left_on="base_transactor_id",
            right_on="retrieved_id",
        )

        office_sought = merged_df.where(pd.notna(merged_df), None)["office_name"]

        transactions["office_sought"] = office_sought

        return [transactions, entities]
