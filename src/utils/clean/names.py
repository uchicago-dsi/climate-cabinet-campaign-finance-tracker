"""Functions for cleaning and standardizing names"""

import re

import nameparser
import pandas as pd

name_suffix_patterns = [
    r"jr\.?",
    r"sr\.?",
    r"v?i{1,3}",  # roman numerals 1-3, 6-8
    r"i?v",  # roman numerals 4-5
    r"ph\.?d\.?",
    r"m\.?d\.?",
    r"e\.?sq\.?",
    r"cpa",
    r"mba",
    r"md",
    r"dds",
    r"dvm",
    r"d\.?o\.?",
    r"j\.?d\.?",
    r"p\.?e\.?",
    r"ret\.?",
    r"rph",
    r"ed\.?d\.?",
    r"p\.?c\.?",
]

name_prefix_patterns = [
    r"dr\.?",
    r"mr\.?",
    r"mrs\.?",
    r"ms\.?",
    r"miss\.?",
    r"mme\.?",
    r"prof\.?",
    r"rev\.?",
    r"sir\.?",
    r"madam",
    r"lady",
    r"hon\.?",
    r"honorable",
    r"judge",
    r"senator",
    r"representative",
    r"officer",
    r"commissioner",
    r"commissnr",
    r"agent",
    r"ceo",
    r"colonel",
    r"col",
    r"sgt|sergeant",
    r"lt|lieutenant",
    r"cpt|captain",
    r"maj|major",
    r"col|colonel",
    r"lt|lieutenant",
]

company_name_patterns = [
    r"inc",
    r"llc",
    r"ltd",
    r"corp",
    r"committee",
    r"party",
    r"foundation",
    r"union",
    r"associat",
]


def _clean_name_components(
    name_components: dict[str, str | None],
) -> dict[str, str | None]:
    """Clean name components

    Args:
        name_components: A dictionary with keys: prefix, first_name, middle_name, last_name, suffix, nickname

    """
    # remove non standard name characters
    # (only letters, spaces, hyphens, and apostrophes)
    for key, value in name_components.items():
        if value is not None:
            name_components[key] = re.sub(r"[^a-zA-Z\s'-]", "", value)
        if value == "":
            name_components[key] = None
    return name_components


def _extract_extra_name_parts(name: str) -> tuple[dict[str, str | None], str]:
    """Extract name prefixes, suffixes, and preferred names from a full name

    Args:
        name: A full name of a person

    Returns:
        dict: A dictionary with keys: prefix, suffix, nickname
        name: The name with the extra parts removed
    """
    # Extract nickname (in quotes or parentheses)
    nickname = None
    nickname_pattern = r'["\']([^"\']+)["\']|\(([^)]+)\)'
    nickname_match = re.search(nickname_pattern, name)
    if nickname_match:
        nickname = nickname_match.group(1) or nickname_match.group(2)
        name = re.sub(nickname_pattern, "", name).strip()

    # Extract prefix (Dr., Mr., Mrs., Ms., etc.)
    prefix = None
    prefix_pattern = r"^(Dr\.|Mr\.|Mrs\.|Ms\.|Prof\.|Rev\.|Sir|Madam|Lady)\s+"
    prefix_match = re.match(prefix_pattern, name, re.IGNORECASE)
    if prefix_match:
        prefix = prefix_match.group(1)
        name = re.sub(prefix_pattern, "", name, flags=re.IGNORECASE).strip()

    # Extract suffix (Jr., Sr., III, IV, Ph.D., etc.)
    suffix = None
    suffix_pattern = r",?\s+(Jr\.|Sr\.|I{2,}|IV|VI{0,3}|Ph\.D\.|M\.D\.|Esq\.|CPA|MBA)$"
    suffix_match = re.search(suffix_pattern, name, re.IGNORECASE)
    if suffix_match:
        suffix = suffix_match.group(1)
        name = re.sub(rf"{suffix_pattern}", "", name, flags=re.IGNORECASE).strip()

    return (
        {
            "prefix": prefix,
            "suffix": suffix,
            "nickname": nickname,
        },
        name,
    )


def divide_full_name(full_name: str) -> dict[str, str | None]:
    """Divide a full name into component parts

    Args:
        full_name: A full name of a person

    Returns:
        dict: A dictionary with keys: prefix, first_name, middle_name, last_name, suffix, nickname
    """
    name = full_name.strip()
    if not name:
        return {
            "name_prefix": None,
            "first_name": None,
            "middle_name": None,
            "last_name": None,
            "name_suffix": None,
            "name_preferred": None,
        }
    # isolate first, middle, and last name parts
    extra_parts, name = _extract_extra_name_parts(name)

    # Check if name is in "Last, First" format
    last_name_first_pattern = r"^(?P<last_name>[^,]+),\s*(?P<first_name_parts>.+)$"
    last_name_first_match = re.match(last_name_first_pattern, name)
    last_name_last_pattern = r"^(?P<first_name_parts>[^,]+)\s+(?P<last_name>[^,]+)$"
    last_name_last_match = re.match(last_name_last_pattern, name)
    first_name_parts = name
    if last_name_first_match:
        last_name = last_name_first_match.group("last_name").strip()
        first_name_parts = last_name_first_match.group("first_name_parts").strip()
    elif last_name_last_match:
        first_name_parts = last_name_last_match.group("first_name_parts").strip()
        last_name = last_name_last_match.group("last_name").strip()

    # fits two initial middle names
    first_name_middle_initial_pattern = (
        r"^(?P<first_name>[^,]+)\s+(?P<middle_name>([A-Z]\.?){1,2})$"
    )
    only_first_name_pattern = r"^(?P<first_name>[^,\s]+)$"
    first_name_middle_initial_match = re.match(
        first_name_middle_initial_pattern, first_name_parts
    )
    only_first_name_match = re.match(only_first_name_pattern, first_name_parts)
    if first_name_middle_initial_match:
        first_name = first_name_middle_initial_match.group("first_name").strip()
        middle_name = first_name_middle_initial_match.group("middle_name").strip()
    elif only_first_name_match:
        first_name = only_first_name_match.group("first_name").strip()
        middle_name = None
    else:
        first_name = first_name_parts
        middle_name = None

    name_components = {
        "first_name": first_name,
        "middle_name": middle_name,
        "last_name": last_name,
        **extra_parts,
    }

    return _clean_name_components(name_components)


def divide_full_name_nameparser(full_name: str) -> dict[str, str | None]:
    """Divide a full name into component parts using nameparser

    Args:
        full_name: A full name of a person

    Returns:
        dict: A dictionary with keys: prefix, first_name, middle_name, last_name, suffix, nickname
    """
    if pd.isna(full_name):
        return {
            "name_prefix": None,
            "first_name": None,
            "middle_name": None,
            "last_name": None,
            "name_suffix": None,
            "name_preferred": None,
        }
    name = nameparser.HumanName(full_name)
    name_components = {
        "name_prefix": name.title,
        "first_name": name.first,
        "middle_name": name.middle,
        "last_name": name.last,
        "name_suffix": name.suffix,
        "name_preferred": name.nickname,
    }
    return _clean_name_components(name_components)


def clean_individuals_names(names: pd.DataFrame) -> pd.DataFrame:
    """Attempt to properly parse names into components

    Args:
        names: A dataframe with columns:
            full_name, first_name, middle_name, last_name, name_prefix,
            name_suffix, name_preferred

    Returns:
        pd.DataFrame: A dataframe with columns:
            full_name, first_name, middle_name, last_name, name_prefix,
            name_suffix, name_preferred
    """
    names = names.copy()
    if names.empty:
        return names
    # handle misplaced last names
    likely_full_names = (
        names["full_name"].isna()
        & names["last_name"].notna()
        & names["last_name"].str.contains(",", na=False)
    )
    names.loc[likely_full_names, "full_name"] = names.loc[
        likely_full_names, "last_name"
    ]
    # divide full names into components
    name_components = names["full_name"].apply(
        divide_full_name_nameparser,  # result_type="expand"
    )
    name_components = pd.DataFrame(name_components.tolist())
    name_columns = [
        "first_name",
        "middle_name",
        "last_name",
        "name_prefix",
        "name_suffix",
        "name_preferred",
    ]
    for col in name_columns:
        if col in name_components.columns:
            names.loc[:, col] = name_components[col].to_numpy()

    return names


def fill_in_transactor_types(names: pd.DataFrame) -> pd.DataFrame:
    """Fill in transactor types based on name components

    Args:
        names: A dataframe with columns:
            full_name, first_name, middle_name, last_name, name_prefix,
            name_suffix, name_preferred
    """
    probable_organization_mask = (
        names["transactor_type"].isna()
        & names["full_name"].notna()
        & names["full_name"].str.contains(
            "|".join(company_name_patterns), case=False, na=False
        )
    )
    names.loc[probable_organization_mask, "transactor_type"] = "Organization"
    # TODO: improve transactor_type prediction
    return names


def clean_phone_number(val: object) -> str | None:
    """Convert phone numbers to strings of length 10"""
    if pd.isna(val):
        return None
    string_phone_number = str(val)
    # strip non-numeric characters
    string_phone_number = re.sub(r"\D", "", string_phone_number)
    return string_phone_number


def clean_names(names: pd.DataFrame) -> pd.DataFrame:
    """Clean names in a dataframe

    Args:
        names: A dataframe with columns:
            full_name, first_name, middle_name, last_name, name_prefix,
            name_suffix, name_preferred
    """
    # fill in transactor types
    names = fill_in_transactor_types(names)
    # clean individuals names
    individuals_names_mask = names["transactor_type"] == "Individual"
    individuals_names = clean_individuals_names(names.loc[individuals_names_mask, :])
    # concatenate cleaned individuals names with original dataframe
    cleaned_names = pd.concat(
        [names.loc[~individuals_names_mask, :], individuals_names]
    )
    return cleaned_names


def clean_transactors(transactors: pd.DataFrame) -> pd.DataFrame:
    """Clean transactors in a dataframe

    Args:
        transactors: A dataframe with columns:
            full_name, first_name, middle_name, last_name, name_prefix,
            name_suffix, name_preferred, phone_number
    """
    transactors = transactors.drop_duplicates(subset=["id"], keep="first")
    transactors = clean_names(transactors)
    transactors["phone_number"] = transactors["phone_number"].apply(clean_phone_number)
    return transactors
