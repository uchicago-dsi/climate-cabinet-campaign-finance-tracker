"""
Module for performing record linkage on state campaign finance dataset
"""


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
        # if there is a ',' switch around the names
        if "," in names[i]:
            index = names[i].find(",")
            first_part = names[i][index + 1 :]
            last_part = names[i][0:index]
            names[i] = first_part + " " + last_part

        names[i] = names[i].lower().replace(".", "").split(" ")
        names[i] = [
            name_part for name_part in names[i] if name_part not in titles
        ]
        names[i] = " ".join(names[i])

    names = " ".join(names)
    names = names.split(" ")
    final_name = []
    [
        final_name.append(x)
        for x in names
        if ((x not in final_name) & (len(x) != 0))
    ]
    return " ".join(final_name).title()
