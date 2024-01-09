import re
from datetime import datetime

import pandas as pd


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


def remove_nonstandard(col: pd.Series) -> pd.Series:
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


def transactions_splitter(
    individuals, organizations, transactions, *args: pd.DataFrame
) -> pd.DataFrame:
    """Split transactions into four groups

    We split the transactions dataframe into four groups depending on donor
    and recipient. If the donor and recipient are both individuals, the
    transaction is classified in inividual->individual. If the donor
    is an individual and the recipient an organization, the transaction
    is classified in individual->organization, and so on.

    NOTE: If running on a subset of the data, such as in the demo,
    the resulting transactions dataframes will be very small
    or entirely empty, as many of the ids will not be present and
    cannot be classified. If this is undesirable, do not employ
    this function and instead return the raw transactions dataframe.

    args: the individuals, organizations, and transactions dataframes
    created by ArizonaCleaner.create_tables().

    returns: a tuple of four transactions dataframes split according
    to the type of donor and recipient, ordered as follows:
    individual->individual, individual->organization,
    organization->individual, organization->organization
    """

    inds_ids = individuals["id"].astype(str)

    org_ids = organizations["id"].astype(str)

    ind_ind = transactions[
        (transactions["donor_id"].isin(inds_ids))
        & (transactions["recipient_id"].isin(inds_ids))
    ]

    ind_org = transactions[
        (transactions["donor_id"].isin(inds_ids))
        & (transactions["recipient_id"].isin(org_ids))
    ]

    org_ind = transactions[
        (transactions["donor_id"].isin(org_ids))
        & (transactions["recipient_id"].isin(inds_ids))
    ]

    org_org = transactions[
        (transactions["donor_id"].isin(org_ids))
        & (transactions["recipient_id"].isin(org_ids))
    ]

    return (ind_ind, ind_org, org_ind, org_org)
