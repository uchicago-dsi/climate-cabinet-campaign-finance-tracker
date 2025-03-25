"""Clean!"""

# %%
import re

import pandas as pd

from utils.constants import BASE_FILEPATH

transformed_directory = BASE_FILEPATH / "output" / "transformed"


def get_address_component_from_table(address: pd.Series, column: str) -> str:
    """Returns stripped component if in table's row, otherwise empty string

    Args:
        address: row of an address database
        column: column to check
    """
    if column not in address or pd.isna(address[column]):
        return ""
    # TODO: remove accents or something?
    return re.sub(r"\s+", " ", address[column].strip().lower())


def get_address_string(address: pd.Series) -> str:
    """From an address row make the address a single string"""
    # if a raw full address is not present, create one
    if "full_address" not in address:
        po_box = get_address_component_from_table(address, "po_box")
        address_line_1 = get_address_component_from_table(address, "line_1")
        if po_box:
            line_1 = po_box
        elif address_line_1:
            line_1 = address_line_1
        else:
            line_1_elements = [
                "building_number",
                "street_predirectional",
                "street_name",
                "street_suffix",
                "street_postdirectional",
            ]
            line_1 = " ".join(
                [
                    get_address_component_from_table(address, line_1_element)
                    for line_1_element in line_1_elements
                ]
            )
        address_line_2 = get_address_component_from_table(address, "line_2")
        if address_line_2:
            line_2 = address_line_2
        else:
            line_2_elements = [
                "secondary_address_identifier",
                "secondary_address_number",
            ]
            line_2 = " ".join(
                [
                    get_address_component_from_table(address, line_2_element)
                    for line_2_element in line_2_elements
                ]
            )
        city = get_address_component_from_table(address, "city")
        state = get_address_component_from_table(address, "state")
        zipcode = get_address_component_from_table(address, "zipcode")
        line_3 = f"{city}, {state} {zipcode}"
        address["full_address"] = f"{line_1}\n{line_2}\n{line_3}"

    # clean and return the (maybe new) full_address
    full_address = get_address_component_from_table(address, "full_address")
    return full_address


if __name__ == "__main__":
    address_df = pd.read_csv(transformed_directory / "Address.csv")
    address_df["address_string"] = address_df.apply(get_address_string, axis=1)

    address_df.to_csv("address-special.csv")
# %%
