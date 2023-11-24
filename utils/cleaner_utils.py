import re
from datetime import datetime

import pandas as pd
from constants import state_abbreviations


def convert_date(date_str: str) -> datetime.utcfromtimestamp:
    """Reformat UNIX timestamp"""
    timestamp_match = re.match(r"/Date\((\d+)\)/", date_str)
    if timestamp_match:
        timestamp = int(timestamp_match.group(1))
        return datetime.utcfromtimestamp(timestamp / 1000)
    else:
        return None  # Return None for invalid date formats


def name_clean(df: pd.DataFrame) -> pd.DataFrame:
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
    """Make raw transactions table into schema-compliant

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
        "recipient_id": df["CommitteeUniqueId"],
        "purpose": df["Memo"],
        "transaction_type": df["TransactionType"],
    }

    return pd.DataFrame(data=d)


def az_individuals_convert(
    transactions: pd.DataFrame, names: list, df: pd.DataFrame
) -> pd.DataFrame:
    """Make individuals detail table schema compliant

    INCOMPLETE

    We take the relevant columns of the raw individual details
    table and extract, reorder and relabel them
    in compliance with the individuals table database schema
    NOTE: Some names may end up a but mangled, because rare
    elements such as 'Jr' or titles like 'EdD' attached to
    named mess up the splitting. Their location is unfortunately
    inconsistent

    args: df: raw individuals dataframe

    returns: schema-compliant individuals dataframe

    """

    employers = (
        transactions.groupby("CommitteeId")["TransactionEmployer"]
        .apply(set)
        .apply(list)
        .values
    )

    entity_type = (
        transactions.groupby("CommitteeId")["CommitteeGroupName"].apply(set).values
    )

    full_name = names

    states_list = []
    for i in df["committee_address"].str.split(" "):
        if i is not None:
            states_list.append(i[-2])
        else:
            states_list.append(None)

    d = {
        "id": df["master_committee_id"],
        "first_name": None,
        "last_name": None,
        "full_name": full_name,
        "entity_type": entity_type,
        "state": states_list,
        "party": df["party_name"],
        "company": employers,
    }

    return pd.DataFrame(data=d)


def az_organizations_convert(transactions: pd.DataFrame, df: pd.DataFrame):
    """Make organizations detail table schema compliant

    INCOMPLETE

    We take the relevant columns of the raw organizations details
    table and extract, reorder and relabel them
    in compliance with the organizations table database schema

    args: df: raw organizations dataframe

    returns: schema-compliant organizations dataframe

    """
    entity_type = (
        transactions.groupby("CommitteeId")["CommitteeGroupName"].apply(set).values
    )

    states_list = []
    for i in df["committee_address"].str.split(" "):
        if i is not None:
            abb = i[-2]
            if " " + abb.upper() + " " not in state_abbreviations:
                states_list.append(None)
            else:
                states_list.append(i[-2].upper())
        else:
            states_list.append(None)

    d = {
        "id": df["master_committee_id"],
        "name": df["candidate"],
        "state": states_list,
        "entity_type": entity_type,  # df["committee_type_name"]
    }

    return pd.DataFrame(data=d)


def remove_nonstandard(col):
    """Remove nonstandard characters from columns

    Using regex, we remove html tags and turn inconsistent
    whitespace into single spaces
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
