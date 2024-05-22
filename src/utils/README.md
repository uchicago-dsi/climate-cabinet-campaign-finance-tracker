# Utils README
---
#### classify.py
1. These functions take in the deduplicated and cleaned individuals and organizations 
dataframes from the deduplication and linkage pipeline. 
2. We classify based on substrings known to indicate clean energy or fossil fuels groups. 
In particular, individuals are classified based on their employment by fossil fuels companies, 
and organizations are classified by their names, prioritizing high profile corporations/PACs 
and those which were found by a manual search of the largest donors/recipients in the dataset

#### constants.py 
Declares constants to be used in various parts of the project. Specifies relative file paths and other static information to be used 
uniformly across all code scripts. 

#### linkage.py 
Performs record linkage across the different datasets, deduplicates records. 

#### network.py 
Writes the code for building, visualizing, and analyzing network visualizations (both micro and macro level) as the final outputs. 

### linkage_and_network_pipeline.py 
The module for running the final network visualization pipeline. Writes functions to call other relevant functions to build the networks from cleaned, transformed, and classified data. 

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

#### pennsylvania.py
- General Notes to clarify some coding decisions:
    1. The Expenditure datasets do not provide explicit information about the
    recipient that can help to classify the entity_type of said recipient. For
    the sake of fitting this data within the schema, I code recipient_type as
    'Organization'.
    2. The Expenditure datasets, when merged with the filer information, have
    some donors whose entity_type isn't specified. Thus I use the same
    function used to classify contributors in the contributions dataset to
    classify the donor entities in the expenditures.
    3. The Contributors datasets have 4 kinds of recipient entities: lobbyists,
    candidates, committees, and nan. In order to fit the entries within the
    schema, I code nan entries as 'Organization'

#### classify.py
1. These functions take in the deduplicated and cleaned individuals and organizations 
dataframes from the deduplication and linkage pipeline. 
2. We classify based on substrings known to indicate clean energy or fossil fuels groups. 
In particular, individuals are classified based on their employment by fossil fuels companies, 
and organizations are classified by their names, prioritizing high profile corporations/PACs 
and those which were found by a manual search of the largest donors/recipients in the dataset

## Company Classification Utils
### classify_fff_data.py
These functions clean, standardize, and merge Fossil Fuel Funds data that has label classifications for fossil fuel or clean energy. Also performs record linkage to dedupe and creates unique ids for each company. 

### classify_infogroup_data.py
These functions clean, standardize, and merge InfoGroup data. It cleans the large 2023 InfoGroup text file and subsets the data with SIC codes that are relevant to fossil fuel or clean energy. Also performs record linkage to dedupe and creates unique ids for each company. 

### merge_transform_company_data.py
These functions merge the FFF and Infogroup data and transforms the df to create a reference to InfoGroup's parent company UUID if found. 


## Election Util:
#### Util function for harvard.py
1. extract_first_name

#### harvard.py
1. preprocess
2. clean
3. standardize
4. create_table
5. create_election_result_uuid
6. clean_state

## Texas Util:
#### texas.py
1. preprocess
2. clean
3. standardize
4. create_tables
5. clean_state
