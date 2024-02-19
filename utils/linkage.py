import re

import numpy as np
import pandas as pd
import textdistance as td
import usaddress

from utils.constants import COMPANY_TYPES

"""
Module for performing record linkage on state campaign finance dataset
"""


def get_address_line_1_from_full_address(address: str) -> str:
    """Given a full address, return the first line of the formatted address

    Address line 1 usually includes street address or PO Box information.

    Args:
        address: raw string representing full address
    Returns:
        address_line_1

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
    pass

    address_tuples = usaddress.parse(
        address
    )  # takes a string address and put them into value,key pairs as tuples
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


def calculate_string_similarity(string1: str, string2: str) -> float:
    """Returns how similar two strings are on a scale of 0 to 1

    This version utilizes Jaro-Winkler distance, which is a metric of
    edit distance. Jaro-Winkler specially prioritizes the early
    characters in a string.

    Since the ends of strings are often more valuable in matching names
    and addresses, we reverse the strings before matching them.

    https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
    https://github.com/Yomguithereal/talisman/blob/master/src/metrics/jaro-winkler.js

    The exact meaning of the metric is open, but the following must hold true:
    1. equivalent strings must return 1
    2. strings with no similar characters must return 0
    3. strings with higher intuitive similarity must return higher scores

    Args:
        string1: any string
        string2: any string
    Returns:
        similarity score

    Sample Usage:
    >>> calculate_string_similarity("exact match", "exact match")
    1.0
    >>> calculate_string_similarity("aaaaaa", "bbbbbbbbbbb")
    0.0
    >>> similar_score = calculate_string_similarity("very similar", "vary similar")
    >>> different_score = calculate_string_similarity("very similar", "very not close")
    >>> similar_score > different_score
    True
    """

    return float(td.jaro_winkler(string1.lower()[::-1], string2.lower()[::-1]))


def calculate_row_similarity(
    row1: pd.DataFrame, row2: pd.DataFrame, weights: np.array, comparison_func
) -> float:
    """Find weighted similarity of two rows in a dataframe

    The length of the weights vector must be the same as
    the number of selected columns.

    This version is slow and not optimized, and will be
    revised in order to make it more efficient. It
    exists as to provide basic functionality. Once we have
    the comparison function locked in, using .apply will
    likely be easier and more efficient.

    >>> d = {
    ...     'name': ["bob von rosevich", "anantarya smith","bob j vonrosevich"],
    ...     'address': ["3 Blue Drive, Chicago", "4 Blue Drive, Chicago",
    ...                 "8 Fancy Way, Chicago"]
    ... }
    >>> df = pd.DataFrame(data=d)
    >>> wrong = calculate_row_similarity(df.iloc[[0]], df.iloc[[1]],
    ...                                    np.array([.8, .2]),
    ...                                    calculate_string_similarity)
    >>> right = calculate_row_similarity(df.iloc[[0]], df.iloc[[2]],
    ...                                    np.array([.8, .2]),
    ...                                    calculate_string_similarity)
    >>> right > wrong
    True
    >>> wrong = calculate_row_similarity(df.iloc[[0]], df.iloc[[1]],
    ...                                    np.array([.2, .8]),
    ...                                    calculate_string_similarity)
    >>> right = calculate_row_similarity(df.iloc[[0]], df.iloc[[2]],
    ...                                    np.array([.2, .8]),
    ...                                    calculate_string_similarity)
    >>> right > wrong
    False
    """

    row_length = len(weights)
    if not (row1.shape[1] == row2.shape[1] == row_length):
        raise ValueError("Number of columns and weights must be the same")

    similarity = np.zeros(row_length)

    for i in range(row_length):
        similarity[i] = comparison_func(
            row1.reset_index().drop(columns="index").iloc[:, i][0],
            row2.reset_index().drop(columns="index").iloc[:, i][0],
        )

    return sum(similarity * weights)


def row_matches(
    df: pd.DataFrame, weights: np.array, threshold: float, comparison_func
) -> dict:
    """Get weighted similarity score of two rows

    Run through the rows using indices: if two rows have a comparison score
    greater than a threshold, we assign the later row to the former. Any
    row which is matched to any other row is not examined again. Matches are
    stored in a dictionary object, with each index appearing no more than once.

    This is not optimized. Not presently sure how to make a good test case
    for this, will submit and ask in mentor session.
    """

    all_indices = np.array(list(df.index))

    index_dict = {}
    [index_dict.setdefault(x, []) for x in all_indices]

    discard_indices = []

    end = max(all_indices)
    for i in all_indices:
        # Skip indices that have been stored in the discard_indices list
        if i in discard_indices:
            continue

        # Iterate through the remaining numbers
        for j in range(i + 1, end):
            if j in discard_indices:
                continue

            # Our conditional
            if (
                calculate_row_similarity(
                    df.iloc[[i]], df.iloc[[j]], weights, comparison_func
                )
                > threshold
            ):
                # Store the other index and mark it for skipping in future iterations
                discard_indices.append(j)
                index_dict[i].append(j)

    return index_dict


def get_street_from_address_line_1(address_line_1: str) -> str:
    """Given an address line 1, return the street name

    Args:
        address_line_1: either street information or PO box
    Returns:
        street name
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


def cleaning_company_column(company_entry: str) -> str:
    """
    Given a string, check if it contains a variation of self employed, unemployed,
    or retired and return the standardized version.

    Args:
        company: string of inputted company names
    Returns:
        standardized for retired, self employed, and unemployed,
        or original string if no match or empty string

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

    >>> standardize_corp_names('MI BEER WINE WHOLESALERS ASSOC')
    'MI BEER WINE WHOLESALERS ASSOCIATION'

    >>> standardize_corp_names('MI COMMUNITY COLLEGE ASSOCIATION')
    'MI COMMUNITY COLLEGE ASSOCIATION'

    >>> standardize_corp_names('STEPHANIES CHANGEMAKER FUND')
    'STEPHANIES CHANGEMAKER FUND'

    """

    company_name_split = company_name.upper().split(" ")

    for i in range(len(company_name_split)):
        if company_name_split[i] in list(COMPANY_TYPES.keys()):
            hold = company_name_split[i]
            company_name_split[i] = COMPANY_TYPES[hold]

    new_company_name = " ".join(company_name_split)
    return new_company_name


def get_address_number_from_address_line_1(address_line_1: str) -> str:
    """Given an address line 1, return the building number or po box

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
    raise ValueError("Can not find Address Number")
