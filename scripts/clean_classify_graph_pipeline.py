"""Script to run cleaning, classification, and graph building pipeline"""

import pandas as pd
from utils.constants import BASE_FILEPATH
from utils.linkage_and_network_pipeline import clean_data_and_build_network

transformed_data = BASE_FILEPATH / "output" / "transformed"

organizations_table = pd.read_csv(transformed_data / "organizations_table.csv")
individuals_table = pd.read_csv(transformed_data / "individuals_table.csv")
transactions_table = pd.read_csv(transformed_data / "transactions_table.csv")

clean_data_and_build_network(individuals_table, organizations_table, transactions_table)
