"""Script to get list of company names from CSVs of fossil fuel companies for use in constants.py for classification"""

import numpy as np
import pandas as pd

from utils.constants import BASE_FILEPATH

# METHODS


def clean_company_names(company_name: str) -> str:
    """Cleans company name from the FFF companies CSV to standardize

    This function should be applied to the "Company Name" column in a Fossil Free Fund's DataFrame.

    Args:
        company_name: a string of the company's name

    Returns:
        a cleaned string in the company name
    """
    cleaned_company_str = company_name.lower()
    cleaned_company_str = company_name.strip()
    return cleaned_company_str


# CLEANING & OUTPUT

coal_company_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "ff_companies"
    / "FFF_coal_companies.csv"
)
oil_company_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "ff_companies"
    / "FFF_oil_companies.csv"
)

# disclaimer: companies are global (perhaps not all US companies )
ff_companies_csvs = [coal_company_csv, oil_company_csv]

# assumes that the df has a "Company" column w/ the company name

clean_company_dfs = [
    pd.read_csv(csv)["Company"].apply(lambda company: clean_company_names(company))
    for csv in ff_companies_csvs
]

ff_company_names_arr = np.concatenate([df.to_numpy() for df in clean_company_dfs])

# removing duplicate companies
ff_company_names_arr = list(set(ff_company_names_arr))
