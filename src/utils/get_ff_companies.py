"""Script to get list of company names from CSVs of fossil fuel companies for use in constants.py for classification"""

import numpy as np
import pandas as pd

# disclaimer: companies are global (perhaps not all US companies )
ff_companies_csvs = ["FFF_coal_companies.csv", "FFF_oil_companies.csv"]

# assumes that the df has a "Company" column w/ the company name
ff_company_names_arr = np.concatenate(
    [pd.read_csv(csv)["Company"].to_numpy() for csv in ff_companies_csvs]
)

# removing duplicate companies
ff_company_names_arr = list(set(ff_company_names_arr))
