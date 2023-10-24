### MN Contribution EDA util

1. datasets_col_consistent
    - Check if the input list of DataFrames have the consistent columns/features for later merge

2. standardize_cand_df
    - Standardize those contribution DataFrames whose recipients are only candidates

3. standardize_noncand_df
    - Standardize those contribution DataFrames whose recipients are non-candidates

4. preprocess_contribution_df
    - Preprocess the separate contribution DataFrames by first merging them, adjust the contribution date and create the contribution year column, and calculate total contribution amount by summing up both direct monetary ones and inkind ones
