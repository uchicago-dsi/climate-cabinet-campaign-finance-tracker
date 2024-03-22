"""Script for running cleaning pipeline"""

from utils.clean.pipeline import clean_and_merge_state_data
from utils.constants import BASE_FILEPATH

individuals_output_path = BASE_FILEPATH / "output" / "complete_individuals_table.csv"
organizations_output_path = (
    BASE_FILEPATH / "output" / "complete_organizations_table.csv"
)
transactions_output_path = BASE_FILEPATH / "output" / "complete_transactions_table.csv"
(
    complete_individuals_table,
    complete_organizations_table,
    complete_transactions_table,
) = clean_and_merge_state_data()
complete_individuals_table.to_csv(individuals_output_path)
complete_organizations_table.to_csv(organizations_output_path)
complete_transactions_table.to_csv(transactions_output_path)
