# Utils README
---

## Michigan Utils:
#### preprocess_mi_campaign_data.py
1. read_expenditure_data
2. read_contribution_data
3. plot_year_contribution_types (depreciated)
4. plot_committee_types_by_year (depreciated)
5. plot_expenditure_committee_types_by_year (depreciated)
6. plot_year_schedule_types (depreciated)
7. create_all_plots (depreciated)

#### michigan.py
1. entity_name_dictionary
2. create_filepaths_list
3. filter_dataframe
4. preprocess
    a. merge_dataframes
    b. fix_menominee_county_bug_contribution
    c. drop_menominee_county
5. clean
    a. clean_contribution_dataframe
    b. clean_expenditure_dataframe
6. standardize
    a. add_uuid_columns
    b. generate_uuid
7. create_tables
    a. create_individuals_table
    b. create_organizations_table
    c. create_transactions_table
    d. output_id_mapping
        - create_individuals_id_mapping
        - create_organizations_id_mapping
        - create_transactions_id_mapping
8. clean_state

## Minnesota Util:
#### MN_util.py

Util functions for MN EDA
1. datasets_col_consistent (deprecated)
2. preprocess_candidate_df (deprecated)
3. preprocess_noncandidate_df (deprecated)
4. preprocess_contribution_df (deprecated)
5. drop_nonclassifiable (deprecated)
6. preprocess_expenditure (deprecated)
7. drop_nonclassifiable_expenditure (deprecated)

#### minnesota.py
1. entity_name_dictionary
2. preprocess_candidate_contribution
3. preprocess_noncandidate_contribution
4. preprocess_expenditure
5. preprocess
6. clean
7. standardize
8. create_tables
9. clean_state
