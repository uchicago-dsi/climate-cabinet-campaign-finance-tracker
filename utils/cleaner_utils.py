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
        "transaction_id": df["PublicTransactionId"],
        "donor_id": df["TransactionNameId"],
        "year": df["TransactionDateYear"],
        "amount": df["Amount"],
        "recipient_id": df["CommitteeId"],
        "office_sought": df["office_sought"],
        "purpose": df["Memo"],
        "transaction_type": df["TransactionType"],
    }

    return pd.DataFrame(data=d)


def az_individuals_convert(transactions_df, details_df: pd.DataFrame) -> pd.DataFrame:
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

    #     employers = transactions.groupby("CommitteeId")[
    #         "TransactionEmployer"].apply(set).apply(list).values

    full_name = details_df["retrieved_name"]

    employer = details_df["company"]

    entity_type = details_df["entity_type"]

    states_list = []

    for i in details_df["committee_address"].str.split(" "):
        if i is not None:  # != None:
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
        "id": transactions_df[
            "TransactionNameGroupId"
        ].unique(),  # details_df["master_committee_id"],
        "first_name": None,
        "last_name": None,
        "full_name": full_name,
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
        if i is not None:  # != None:
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
        "id": df["master_committee_id"],
        "name": df["committee_name"],
        # "name": df["candidate"],
        "state": states_list,
        "entity_type": entity_type,  # df["committee_type_name"]
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
