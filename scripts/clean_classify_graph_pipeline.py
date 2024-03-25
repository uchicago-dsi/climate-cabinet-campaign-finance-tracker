"""Script to run cleaning, classification, and graph building pipeline"""

import pandas as pd
from utils.constants import BASE_FILEPATH
from utils.linkage_and_network_pipeline import clean_data_and_build_network

transformed_data = BASE_FILEPATH / "data" / "transformed"

organizations_table = pd.read_csv(transformed_data / "orgs_mini.csv")
individuals_table = pd.read_csv(transformed_data / "inds_mini.csv")
transactions_table = pd.read_csv(transformed_data / "trans_mini.csv")

clean_data_and_build_network(individuals_table, organizations_table, transactions_table)
