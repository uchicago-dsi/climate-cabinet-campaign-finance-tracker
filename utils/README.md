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