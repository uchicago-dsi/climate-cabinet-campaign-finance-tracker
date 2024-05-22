"""Script to run preprocessing, cleaning pipelin to build linkage for Texas election data"""

import pandas as pd
from utils.constants import BASE_FILEPATH
from utils.election.tx_linkage_pipeline import preprocess_data_and_create_table

transformed_data = BASE_FILEPATH / "output" / "transformed"

ind_df = pd.read_csv(transformed_data / "tx_individuals_table.csv")
election_table = pd.read_csv(transformed_data / "election_results_table.csv")

tx_election_table = election_table[election_table["state"] == "TX"]

preprocess_data_and_create_table(tx_election_table, ind_df)
