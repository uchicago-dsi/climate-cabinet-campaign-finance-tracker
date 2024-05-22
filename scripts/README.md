### Scripts directory
* `election_linkage.py`: Script to run preprocessing, cleaning pipelin to build linkage for election data  
* `transform_election_pipeline.py`: Script for running cleaning pipeline for election results  
* `tx_transform_pipeline.py`: Script for running cleaning pipeline for Texas StateTransformer
* `tx_election_linkage_pipeline.py`: Script to run preprocessing, cleaning pipeline to build linkage for Texas election data
* `company_classification_pipeline.py`: Script to clean FFF data and InfoGroup data and merge to create a dataset of confidently classified companies. Deduping is performed on these datasets separately then they are merged. Creates a csv of the merged dataset: data/classification/merged_cleaned_company_classification.py
* `company_linkage_pipeline.py`: Script to perform record linkage on the confidently classified company dataset and a campaign finance dataset (currently using a testing subset with the Texas organizations dataset). Transforms the campaign finance data by creating a reference to the matching UUIDs in the classified company dataset. Output csv can be found in output/linked
