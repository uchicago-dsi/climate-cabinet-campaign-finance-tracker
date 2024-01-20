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
    name_parts = name.split(",")
    # if the comma is just in the end as a typo:
    if len(name_parts[1]) == 0:
        return name_parts[0]
    # if just the suffix in the end, leave the name as it is
    if name_parts[1].strip() in suffixes:
        return name
    # at this point either it's just poor name placement, or the suffix is
    # in the beginning of the name. Either way, the first part of the list is the
    # true last name.
    last_part = name_parts.pop(0)
    first_part = " ".join(name_parts)
    return first_part + " " + last_part


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
            print(names[i])
        names[i] = names[i].replace(".", "").split(" ")
        names[i] = [
            name_part for name_part in names[i] if name_part not in titles
        ]
        names[i] = " ".join(names[i])
        print(names[i])

    names = " ".join(names)
    print("after comma: ", names)
    names = names.split(" ")
    final_name = []
    [final_name.append(x) for x in names if x not in final_name]
    return " ".join(final_name).title().strip()
