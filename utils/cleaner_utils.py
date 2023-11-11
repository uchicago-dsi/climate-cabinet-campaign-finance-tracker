import re
from datetime import datetime

import pandas as pd


def convert_date(date_str: str) -> datetime.utcfromtimestamp:
    """Reformat UNIX timestamp"""
    timestamp_match = re.match(r"/Date\((\d+)\)/", date_str)
    if timestamp_match:
        timestamp = int(timestamp_match.group(1))
        return datetime.utcfromtimestamp(timestamp / 1000)
    else:
        return None  # Return None for invalid date formats


def name_clean(df):
    """ """

    df["candidate"] = df.apply(
        lambda row: row["committee_name"]
        if (row["candidate"] == "")
        else row["candidate"],
        axis=1,
    )

    return df


def az_transactions_convert(df):
    """ """

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


def az_individuals_convert(df):
    """ """

    d = {
        "id": df["master_committee_id"],
        # 'first_name': df["candidate"].str.split(",")[1],
        # #need to correct for future cleaning
        # 'last_name': df["candidate"].str.split(",")[0],
        # #need to correct for future cleaning
        # 'full_name': df["candidate"],
        # 'state': df[""], #pipe in from elsewhere? TransactionState?
        "party": df["party_name"],
        # 'company': df[""] #pipe in from TransactionEmployer?
    }

    return pd.DataFrame(data=d)


def az_organizations_convert(df):
    """ """

    d = {
        "id": df["master_committee_id"],
        "name": df["candidate"],
        # 'state': df[""], #pipe in from elsewhere? TransactionState?
        "entity_type": df["committee_type_name"],
    }

    return pd.DataFrame(data=d)
