import pandas as pd

# import state cleaners here

# uncomment your state once it is added
state_cleaners = [
    # ArizonaCleaner(),
    # MichiganCleaner(),
    # MinnesotaCleaner(),
    # PennsylvaniaCleaner(),
]

if __name__ == "__main__":
    single_state_individuals_tables = []
    single_state_organizations_tables = []
    single_state_transactions_tables = []
    for state_cleaner in state_cleaners:
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
