"""
Module for performing record linkage on state campaign finance dataset
"""
import textdistance as td
import usaddress
import pandas as pd


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
    suffixes = [
        "sr",
        "jr",
        "i",
        "ii",
        "iii",
        "iv",
        "v",
        "vi",
        "vii",
        "viii",
        "ix",
        "x",
    ]
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
    # first ensure clean input by deleting spaces:
    first_name, last_name, full_name = list(
        map(lambda x: x.lower().strip(), [first_name, last_name, full_name])
    )

    # if data is clean:
    if first_name + " " + last_name == full_name:
        return full_name

    # some names have titles or professions associated with the name. We need to
    # remove those from the name.
    titles = [
        "mr",
        "ms",
        "mrs",
        "miss",
        "prof",
        "dr",
        "doctor",
        "sir",
        "madam",
        "professor",
    ]
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

def deduplicate_perfect_matches(df: pd.DataFrame) -> pd.DataFrame:
    '''Given a dataframe, remove rows that have identical entry data beyond
    UUIDs, and output a file mapping an entry to other the UUIDs of the
    deduplicated rows
    
    Args:
        a pandas dataframe containing contribution data
    Returns:
        a deduplicated pandas dataframe containing contribution data
    '''
    #first remove all duplicate entries:
    new_df = df.drop_duplicates()

    # now find the duplicates along all columns but the ID
    cols = new_df.columns[1:]
    duplicates = new_df[new_df.duplicated(cols)]        
    new_df = new_df.drop(index=duplicates.index.tolist())
    #for index in duplicates.index:

    return new_df