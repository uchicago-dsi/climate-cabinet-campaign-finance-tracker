"""Script to run preprocessing, cleaning pipelin to build linkage for election data"""

import pandas as pd
from utils.constants import BASE_FILEPATH
from utils.election.linkage_pipeline import preprocess_data_and_create_table

transformed_data = BASE_FILEPATH / "output" / "transformed"
cleaned_data= BASE_FILEPATH / "output" / "cleaned"

ind_df = pd.read_csv(cleaned_data / "individuals_table.csv")
election_table = pd.read_csv(transformed_data / "election_results_table.csv")

preprocess_data_and_create_table(election_table,ind_df)
