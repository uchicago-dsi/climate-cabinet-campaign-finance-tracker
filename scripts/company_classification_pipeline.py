"""Script to create a csv of relevant companies that can be used for classification pipeline"""

from utils.classify_FFF_data import get_FFF_df
from utils.classify_InfoGroup_data import get_InfoGroup_df
from utils.constants import BASE_FILEPATH
from utils.merge_transform_company_data import merge_company_dfs

# GETTING FFF DATA
FFF_coal_company_csv = (
    BASE_FILEPATH / "data" / "classification" / "raw" / "FFF_coal_companies.csv"
)

FFF_oil_company_csv = (
    BASE_FILEPATH / "data" / "classification" / "raw" / "FFF_oil_companies.csv"
)

FFF_output = BASE_FILEPATH / "data" / "classification" / "raw" / "merged_FFF.csv"

FFF_dict = {FFF_oil_company_csv: "f", FFF_coal_company_csv: "f"}
FFF_df = get_FFF_df(FFF_dict, FFF_output, cached=False)

# GETTING INFOGROUP DATA

SIC6_codes_csv = BASE_FILEPATH / "data" / "classification" / "raw" / "SIC6_codes.csv"

infogroup_data_2023 = (
    BASE_FILEPATH / "data" / "classification" / "raw" / "2023_InfoGroup.txt"
)

relevant_InfoGroup_2023_csv = (
    BASE_FILEPATH / "data" / "classification" / "raw" / "relevant_InfoGroup_2023.csv"
)

InfoGroup_df = get_InfoGroup_df(
    SIC6_codes_csv=SIC6_codes_csv,
    infogroup_csv=infogroup_data_2023,
    output_file_path=relevant_InfoGroup_2023_csv,
    cached=False,
)

# MERGING AND TRANSFORMING FFF AND INFOGROUP DATA

aggregated_classification_csv = (
    BASE_FILEPATH
    / "data"
    / "classification"
    / "merged_cleaned_company_classification.csv"
)

aggregated_company_df = merge_company_dfs(
    cleaned_FFF_df=FFF_df,
    cleaned_InfoGroup_df=InfoGroup_df,
    output_file_path=aggregated_classification_csv,
)
