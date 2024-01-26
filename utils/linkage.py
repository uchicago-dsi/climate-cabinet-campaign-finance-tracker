"""
Module for performing record linkage on state campaign finance dataset
"""
import usaddress

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

    address_tuples = usaddress.parse(address) #takes a string address and put them into value,key pairs as tuples
    line1_components = []
    for value,key in address_tuples:
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
