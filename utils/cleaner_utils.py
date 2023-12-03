import re
from datetime import datetime

import pandas as pd
from constants import state_abbreviations


def convert_date(date_str: str) -> datetime.utcfromtimestamp:
    """Reformat UNIX timestamp

    args: UNIX-formatted date string

    returns: reformatted date string

    """
    timestamp_match = re.match(r"/Date\((\d+)\)/", date_str)
    if timestamp_match:
        timestamp = int(timestamp_match.group(1))
        return datetime.utcfromtimestamp(timestamp / 1000)
    else:
        return None  # Return None for invalid date formats


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
        lambda row: row["committee_name"]
        if (row["candidate"] == ("" or None or "" or """"""))
        else row["candidate"],
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
        "transaction_id": df["PublicTransactionId"].astype(int),
        "donor_id": df["base_transactor_id"].astype(int),
        "year": df["TransactionDateYear"].astype(int),
        "amount": df["Amount"].abs(),
        "recipient_id": df["other_transactor_id"].astype(int),
        "office_sought": df["office_sought"],
        "purpose": df["Memo"],
        "transaction_type": df["TransactionType"],
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
        "id": details_df["retrieved_id"].astype(int),
        "first_name": None,
        "last_name": None,
        "full_name": details_df["full_name"],
        "entity_type": entity_type,
        "state": states_list,
        "party": details_df["party_name"],
        "company": employer,
    }

    return pd.DataFrame(data=d)


def az_organizations_convert(df: pd.DataFrame):
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
        "id": df["retrieved_id"].astype(int),
        "name": df["committee_name"],
        "state": states_list,
        "entity_type": entity_type,
    }

    return pd.DataFrame(data=d)


def remove_nonstandard(col):
    """Remove nonstandard characters from columns

    Using regex, we remove html tags and turn inconsistent
    whitespace into single spaces

    args: column of a pandas dataframe

    returns: modified column of a pandas dataframe
    """

    col = col.str.replace(r"<[^<>]*>", " ", regex=True)
    # removes html tags

    col = (
        col.str.replace("/\\s\\s+/g", " ", regex=True)
        .replace(" ", "_", regex=True)
        .replace("\\W", "", regex=True)
    )
    # turns oversized whitespace to single space

    return col


def az_transactor_sorter(row):
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


def az_donor_recipient_director(row):
    """Sorts ids into donor and recipient columns

    We switch the elements of a row in the transactions table if the
    'TransactionTypeDispositionId' column indicates that the transaction
    involves the other transactor receiving money and the base transactor
    giving money. This is coded as a '2' in the dataset.

    args: row of the transactions dataframe

    returns: adjusted row of the transactions dataframe

    """
    if row["TransactionTypeDispositionId"] == 2:
        row["donor_id"], row["recipient_id"] = row["recipient_id"], row["donor_id"]
    return row


def az_employment_checker(row, transactions):
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
        row["company"] = transactions[
            transactions["retrieved_id"] == row["retrieved_id"]
        ].iloc[0]["TransactionEmployer"]

    return row


def az_individual_name_checker(row):
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
