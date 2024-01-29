import pandas as pd

from utils.arizona import ArizonaCleaner
from utils.constants import BASE_FILEPATH
from utils.michigan import MichiganCleaner
from utils.minnesota import MinnesotaCleaner
from utils.pennsylvania import PennsylvaniaCleaner

state_cleaners = [
    ArizonaCleaner(),
    MichiganCleaner(),
    MinnesotaCleaner(),
    PennsylvaniaCleaner(),
]

if __name__ == "__main__":
    single_state_individuals_tables = []
    single_state_organizations_tables = []
    single_state_transactions_tables = []
    for state_cleaner in state_cleaners:
        print("Cleaning...")
        (
            individuals_table,
            organizations_table,
            transactions_table,
        ) = state_cleaner.clean_state()
        single_state_individuals_tables.append(individuals_table)
        single_state_organizations_tables.append(organizations_table)
        single_state_transactions_tables.append(transactions_table)

    complete_individuals_table = pd.concat(single_state_individuals_tables)
    complete_organizations_table = pd.concat(single_state_organizations_tables)
    complete_transactions_table = pd.concat(single_state_transactions_tables)

    individuals_output_path = (
        BASE_FILEPATH / "output" / "complete_individuals_table.csv"
    )
    organizations_output_path = (
        BASE_FILEPATH / "output" / "complete_organizations_table.csv"
    )
    transactions_output_path = (
        BASE_FILEPATH / "output" / "complete_transactions_table.csv"
    )

    complete_individuals_table.to_csv(individuals_output_path)
    complete_organizations_table.to_csv(organizations_output_path)
    complete_transactions_table.to_csv(transactions_output_path)
