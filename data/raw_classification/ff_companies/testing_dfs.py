# pylint: disable=all
import pandas as pd

coal_companies_df = pd.read_csv("FFF_coal_companies.csv")
oil_companies_df = pd.read_csv("FFF_oil_companies.csv")
print(coal_companies_df.head())
print(oil_companies_df.head())
