import os.path
import re

import numpy as np
import pandas as pd
import usaddress
from splink.duckdb.linker import DuckDBLinker

from utils.constants import COMPANY_TYPES, repo_root, suffixes, titles

"""
Module for performing record linkage on state campaign finance dataset
"""


def get_address_line_1_from_full_address(address: str) -> str:
    """Given a full address, return the first line of the formatted address

    Address line 1 usually includes street address or PO Box information.

    Uses the usaddress libray which splits an address string into components,
    and labels each component.
    https://usaddress.readthedocs.io/en/latest/

    Args:
        address: raw string representing full address
    Returns:
        address_line_1 as a string

    Sample Usage:
    >>> get_address_line_1_from_full_address('6727 W. Corrine Dr.  Peoria,AZ 85381')
    '6727 W. Corrine Dr.'
    >>> get_address_line_1_from_full_address('P.O. Box 5456  Sun City West ,AZ 85375')
    'P.O. Box 5456'
    >>> get_address_line_1_from_full_address('119 S 5th St  Niles,MI 49120')
    '119 S 5th St'
    >>> get_address_line_1_from_full_address(
    ...     '1415 PARKER STREET APT 251	DETROIT	MI	48214-0000'
    ... )
    '1415 PARKER STREET'
    """

    address_tuples = usaddress.parse(
        address
    )  # takes a string address and put them into value, key pairs as tuples
    line1_components = []
    for value, key in address_tuples:
        if key == "PlaceName":
            break
        elif key in (
            "AddressNumber",
            "StreetNamePreDirectional",
            "StreetName",
            "StreetNamePostType",
            "USPSBoxType",
            "USPSBoxID",
        ):
            line1_components.append(value)
    line1 = " ".join(line1_components)
    return line1


def determine_comma_role(name: str) -> str:
    """Given a string (someone's name), attempts to determine the role of the
    comma in the name and where it ought to belong.

    Some assumptions are made:
        * If a suffix is included in the name and the name is not just the last
          name(i.e "Doe, Jr), the format is
          (last_name suffix, first and middle name) i.e Doe iv, Jane Elisabeth

        * If a comma is used anywhere else, it is in the format of
          (last_name, first and middle name) i.e Doe, Jane Elisabeth
    Args:
        name: a string representing a name/names of individuals
    Returns:
        the name with or without a comma based on some conditions

    Sample Usage:
    >>> determine_comma_role("Jane Doe, Jr")
    'Jane Doe, Jr'
    >>> determine_comma_role("Doe, Jane Elisabeth")
    ' Jane Elisabeth Doe'
    >>> determine_comma_role("Jane Doe,")
    'Jane Doe'
    >>> determine_comma_role("DOe, Jane")
    ' Jane Doe'
    """

    name_parts = name.lower().split(",")
    # if the comma is just in the end as a typo:
    if len(name_parts[1]) == 0:
        return name_parts[0].title()
    # if just the suffix in the end, leave the name as it is
    if name_parts[1].strip() in suffixes:
        return name.title()
    # at this point either it's just poor name placement, or the suffix is
    # in the beginning of the name. Either way, the first part of the list is
    # the true last name.
    last_part = name_parts.pop(0)
    first_part = " ".join(name_parts)
    return first_part.title() + " " + last_part.title()


def get_likely_name(first_name: str, last_name: str, full_name: str) -> str:
    """Given name related columns, return a person's likely name

    Given different formatting used accross states, errors in data entry
    and missing data, it can be difficult to determine someone's actual
    name. For example, some states have a last name column with values like
    "Doe, Jane", where the person's first name appears to have been erroneously
    included.

    Args:
        first_name: raw value of first name column
        last_name: raw value last name column
        full_name: raw value of name or full_name column
    Returns:
        The most likely full name of the person listed

    Sample Usage:
    >>> get_likely_name("Jane", "Doe", "")
    'Jane Doe'
    >>> get_likely_name("", "", "Jane Doe")
    'Jane Doe'
    >>> get_likely_name("", "Doe, Jane", "")
    'Jane Doe'
    >>> get_likely_name("Jane Doe", "Doe", "Jane Doe")
    'Jane Doe'
    >>> get_likely_name("Jane","","Doe, Sr")
    'Jane Doe, Sr'
    >>> get_likely_name("Jane Elisabeth Doe, IV","Elisabeth","Doe, IV")
    'Jane Elisabeth Doe, Iv'
    >>> get_likely_name("","","Jane Elisabeth Doe, IV")
    'Jane Elisabeth Doe, Iv'
    >>> get_likely_name("Jane","","Doe, Jane, Elisabeth")
    'Jane Elisabeth Doe'
    """
    # first, convert any NaNs to empty strings ''
    first_name, last_name, full_name = [
        "" if x is np.NAN else x for x in [first_name, last_name, full_name]
    ]

    # second, ensure clean input by deleting spaces:
    first_name, last_name, full_name = list(
        map(lambda x: x.lower().strip(), [first_name, last_name, full_name])
    )

    # if data is clean:
    if first_name + " " + last_name == full_name:
        return full_name.title()

    # remove titles or professions from the name
    names = [first_name, last_name, full_name]

    for i in range(len(names)):
        # if there is a ',' deal with it accordingly
        if "," in names[i]:
            names[i] = determine_comma_role(names[i])

        names[i] = names[i].replace(".", "").split(" ")
        names[i] = [
            name_part for name_part in names[i] if name_part not in titles
        ]
        names[i] = " ".join(names[i])

    # one last check to remove any pieces that might add extra whitespace
    names = list(filter(lambda x: x != "", names))
    names = " ".join(names)
    names = names.title().replace("  ", " ").split(" ")
    final_name = []
    [final_name.append(x) for x in names if x not in final_name]
    return " ".join(final_name).strip()


def get_street_from_address_line_1(address_line_1: str) -> str:
    """Given an address line 1, return the street name

    Uses the usaddress libray which splits an address string into components,
    and labels each component.
    https://usaddress.readthedocs.io/en/latest/

    Args:
        address_line_1: either street information or PO box as a string
    Returns:
        street name as a string
    Raises:
        ValueError: if string is malformed and no street can be reasonably
            found.

    >>> get_street_from_address_line_1("5645 N. UBER ST")
    'UBER ST'
    >>> get_street_from_address_line_1("")
    Traceback (most recent call last):
        ...
    ValueError: address_line_1 must have whitespace
    >>> get_street_from_address_line_1("PO Box 1111")
    Traceback (most recent call last):
        ...
    ValueError: address_line_1 is PO Box
    >>> get_street_from_address_line_1("300 59 St.")
    '59 St.'
    >>> get_street_from_address_line_1("Uber St.")
    'Uber St.'
    >>> get_street_from_address_line_1("3NW 59th St")
    '59th St'
    """
    if not address_line_1 or address_line_1.isspace():
        raise ValueError("address_line_1 must have whitespace")

    address_line_lower = address_line_1.lower()

    if "po box" in address_line_lower:
        raise ValueError("address_line_1 is PO Box")

    string = []
    address = usaddress.parse(address_line_1)
    for key, val in address:
        if val in ["StreetName", "StreetNamePostType"]:
            string.append(key)

    return " ".join(string)


def convert_duplicates_to_dict(df: pd.DataFrame) -> None:
    """For each uuid, maps it to all other uuids for which it has been deemed a
    match.

    Given a dataframe where the uuids of all rows deemed similar are stored in a
    list and all but the first row of each paired uuid is dropped, this function
    maps the matched uuids to a single uuid.

    Args:
        A pandas df containing a column called 'duplicated', where each row is a
        list of all uuids deemed a match. In each list, all uuids but the first
        have their rows already dropped.

    Returns
        None. However it outputs a file to the output directory, with 2
        columns. The first lists all the uuids in df, and is labeled
        'original_uuids.' The 2nd shows the uuids to which each entry is mapped
        to, and is labeled 'mapped_uuids'.
    """
    deduped_dict = {}
    for i in range(len(df)):
        deduped_uudis = df.iloc[i]["duplicated"]
        for j in range(len(deduped_uudis)):
            deduped_dict.update({deduped_uudis[j]: df.iloc[i]["id"]})

    # now convert dictionary into a csv file
    deduped_df = pd.DataFrame.from_dict(deduped_dict, "index")
    deduped_df = deduped_df.reset_index().rename(
        columns={"index": "original_uuids", 0: "mapped_uuid"}
    )
    deduped_df.to_csv(
        repo_root / "output" / "deduplicated_UUIDs.csv",
        index=False,
        mode="a",
        header=not os.path.exists("../output/deduplicated_UUIDs.csv"),
    )


def deduplicate_perfect_matches(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe with duplicated entries removed.

    Given a dataframe, combines rows that have identical data beyond their
    UUIDs, keeps the first UUID amond the similarly grouped UUIDs, and saves the
    rest of the UUIDS to a file in the "output" directory linking them to the
    first selected UUID.

    Args:
        a pandas dataframe containing contribution data
    Returns:
        a deduplicated pandas dataframe containing contribution data
    """
    # first remove all duplicate entries:
    new_df = df.drop_duplicates()

    # find the duplicates along all columns but the id
    new_df = (
        new_df.groupby(df.columns.difference(["id"]).tolist(), dropna=False)[
            "id"
        ]
        .agg(list)
        .reset_index()
        .rename(columns={"id": "duplicated"})
    )
    new_df.index = new_df["duplicated"].str[0].tolist()

    # convert the duplicated column into a dictionary that can will be
    # an output by only feeding the entries with duplicates
    new_df = new_df.reset_index().rename(columns={"index": "id"})
    convert_duplicates_to_dict(new_df[["id", "duplicated"]])
    new_df = new_df.drop(["duplicated"], axis=1)
    return new_df


def cleaning_company_column(company_entry: str) -> str:
    """
    Given a string, check if it contains a variation of self employed,
    unemployed, or retired and return the standardized version.

    Args:
        company: string of inputted company names
    Returns:
        standardized for retired, self employed, and unemployed,
        or original string if no match or empty string

    Sample Usage:
    >>> cleaning_company_column("Retireed")
    'Retired'
    >>> cleaning_company_column("self")
    'Self Employed'
    >>> cleaning_company_column("None")
    'Unemployed'
    >>> cleaning_company_column("N/A")
    'Unemployed'
    """

    if not company_entry:
        return company_entry

    company_edited = company_entry.lower()

    if company_edited == "n/a":
        return "Unemployed"

    company_edited = re.sub(r"[^\w\s]", "", company_edited)

    if (
        company_edited == "retired"
        or company_edited == "retiree"
        or company_edited == "retire"
        or "retiree" in company_edited
    ):
        return "Retired"

    elif (
        "self employe" in company_edited
        or "freelance" in company_edited
        or company_edited == "self"
        or company_edited == "independent contractor"
    ):
        return "Self Employed"
    elif (
        "unemploye" in company_edited
        or company_edited == "none"
        or company_edited == "not employed"
        or company_edited == "nan"
    ):
        return "Unemployed"

    else:
        return company_edited


def standardize_corp_names(company_name: str) -> str:
    """Given an employer name, return the standardized version

    Args:
        company_name: corporate name
    Returns:
        standardized company name

    Sample Usage:
    >>> standardize_corp_names('MI BEER WINE WHOLESALERS ASSOC')
    'MI BEER WINE WHOLESALERS ASSOCIATION'

    >>> standardize_corp_names('MI COMMUNITY COLLEGE ASSOCIATION')
    'MI COMMUNITY COLLEGE ASSOCIATION'

    >>> standardize_corp_names('STEPHANIES CHANGEMAKER FUND')
    'STEPHANIES CHANGEMAKER FUND'

    """

    company_name_split = company_name.title().split(" ")

    for i in range(len(company_name_split)):
        if company_name_split[i] in list(COMPANY_TYPES.keys()):
            hold = company_name_split[i]
            company_name_split[i] = COMPANY_TYPES[hold]

    new_company_name = " ".join(company_name_split)
    return new_company_name


def get_address_number_from_address_line_1(address_line_1: str) -> str:
    """Given an address line 1, return the building number or po box

    Uses the usaddress libray which splits an address string into components,
    and labels each component.
    https://usaddress.readthedocs.io/en/latest/

    Args:
        address_line_1: either street information or PO box
    Returns:
        address or po box number

    Sample Usage:
    >>> get_address_number_from_address_line_1('6727 W. Corrine Dr.  Peoria,AZ 85381')
    '6727'
    >>> get_address_number_from_address_line_1('P.O. Box 5456  Sun City West ,AZ 85375')
    '5456'
    >>> get_address_number_from_address_line_1('119 S 5th St  Niles,MI 49120')
    '119'
    >>> get_address_number_from_address_line_1(
    ...     '1415 PARKER STREET APT 251	DETROIT	MI	48214-0000'
    ... )
    '1415'
    """

    address_line_1_components = usaddress.parse(address_line_1)

    for i in range(len(address_line_1_components)):
        if address_line_1_components[i][1] == "AddressNumber":
            return address_line_1_components[i][0]
        elif address_line_1_components[i][1] == "USPSBoxID":
            return address_line_1_components[i][0]
    raise ValueError("Cannot find Address Number")


def splink_dedupe(
    df: pd.DataFrame, settings: dict, blocking: list
) -> pd.DataFrame:
    """Given a dataframe and config settings, return a
    deduplicated dataframe

    Configuration settings and blocking can be found in constants.py as
    individuals_settings, indivduals_blocking, organizations_settings,
    organizations_blocking

    Uses the splink library which employs probabilistic matching for
    record linkage
    https://moj-analytical-services.github.io/splink/index.html


    Args:
        df: dataframe
        settings: configuration settings
            (based on splink documentation and dataframe columns)
        blocking: list of columns to block on for the table
            (cuts dataframe into parts based on columns labeled blocks)
    Returns:
        deduplicated version of initial dataframe with column 'matching_id'
        that holds list of matching unique_ids

    """
    linker = DuckDBLinker(df, settings)
    linker.estimate_probability_two_random_records_match(
        blocking, recall=0.6
    )  # default
    linker.estimate_u_using_random_sampling(max_pairs=5e6)

    for i in blocking:
        linker.estimate_parameters_using_expectation_maximisation(i)

    df_predict = linker.predict()
    clusters = linker.cluster_pairwise_predictions_at_threshold(
        df_predict, threshold_match_probability=0.7
    )  # default
    clusters_df = clusters.as_pandas_dataframe()

    match_list_df = (
        clusters_df.groupby("cluster_id")["unique_id"].agg(list).reset_index()
    )  # dataframe where cluster_id maps unique_id to initial instance of row
    match_list_df.rename(columns={"unique_id": "duplicated"}, inplace=True)

    first_instance_df = clusters_df.drop_duplicates(subset="cluster_id")
    col_names = np.append("cluster_id", df.columns)
    first_instance_df = first_instance_df[col_names]

    deduped_df = pd.merge(
        first_instance_df,
        match_list_df[["cluster_id"]],
        on="cluster_id",
        how="left",
    )
    deduped_df = deduped_df.rename(columns={"cluster_id": "unique_id"})

    convert_duplicates_to_dict(deduped_df)

    deduped_df = deduped_df.drop(columns=["duplicated"])

    return deduped_df
