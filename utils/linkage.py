"""
Module for performing record linkage on state campaign finance dataset
"""
import pandas as pd
import usaddress


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


"""
Module for standardizing the 'company' columnn of the state campaign finance dataset
"""


def cleaning_company_column(company: str) -> str:
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
    """
    if pd.isnull(company):
        return company

    company_edited = company.lower()
    company_edited = company_edited.strip()
    company_edited = company_edited.replace(".", " ")
    company_edited = company_edited.replace(",", " ")
    company_edited = company_edited.replace("-", " ")

    if "retire" in company_edited:
        return "Retired"
    elif "self employe" in company_edited or company_edited == "self":
        return "Self Employed"
    elif (
        "unemploye" in company_edited
        or company_edited == "none"
        or company_edited == "not employed"
    ):
        return "Unemployed"

    else:
        return company


# Example implementation of the function standardize_company_column for a dataframe
# df['standardized_company'] = df['company'].apply(standardize_company_column)
