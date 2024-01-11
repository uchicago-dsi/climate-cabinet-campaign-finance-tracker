"""
Module for performing record linkage on state campaign finance dataset
"""


def calculate_string_similarity(string1: str, string2: str) -> float:
    """Returns how similar two strings are on a scale of 0 to 1

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
    >>> similar_socre > different_score
    True
    """
    pass


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
    "Jane Doe"
    >>> get_likely_name("", "", "Jane Doe")
    "Jane Doe"
    >>> get_likely_name("", "Doe, Jane", "")
    "Jane Doe"
    >>> get_likely_name("Jane Doe", "Doe", "Jane Doe")
    "Jane Doe"
    """
    pass


def get_address_line_1_from_full_address(address: str) -> str:
    """Given a full address, return the first line of the formatted address

    Address line 1 usually includes street address or PO Box information.

    Args:
        address: raw string representing full address
    Returns:
        address_line_1

    Sample Usage:
    >>> get_address_line_1_from_full_address("6727 W. Corrine Dr.  Peoria,AZ 85381")
    "6727 W. Corrine Dr."
    >>> get_address_line_1_from_full_address("P.O. Box 5456  Sun City West ,AZ 85375")
    "P.O. Box 5456"
    >>> get_address_line_1_from_full_address("119 S 5th St  Niles,MI 49120")
    "119 S 5th St"
    >>> get_address_line_1_from_full_address(
    ...     "1415 PARKER STREET APT 251	DETROIT	MI	48214-0000"
    ... )
    "1415 PARKER STREET"
    """
    pass


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
    "UBER ST"
    >>> get_street_from_address_line_1("")
    Traceback (most recent call last):
        ...
    ValueError: address_line_1 must have whitespace
    >>> get_street_from_address_line_1("PO Box 1111")
    Traceback (most recent call last):
        ...
    ValueError: address_line_1 is PO Box
    """
    pass
