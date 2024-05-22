"""Script to create a csv of relevant companies that can be used for classification pipeline"""

import uuid

import pandas as pd
from utils.classify_fff_data import get_fff_df
from utils.classify_infogroup_data import get_infogroup_df
from utils.constants import (
    BASE_FILEPATH,
    FFF_COMPANY_CLASSIFICATION_BLOCKING,
    FFF_COMPANY_CLASSIFICATION_SETTINGS,
    INFOGROUP_COMPANY_CLASSIFICATION_BLOCKING,
    INFOGROUP_COMPANY_CLASSIFICATION_SETTINGS,
)
from utils.linkage import splink_dedupe
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

FFF_df = get_fff_df(FFF_dict, FFF_output, cached=False)

# deduping FFF company data

# deduped FFF df
FFF_df = splink_dedupe(
    df=FFF_df,
    settings=FFF_COMPANY_CLASSIFICATION_SETTINGS,
    blocking=FFF_COMPANY_CLASSIFICATION_BLOCKING,
    mapped_uuid_file_path_name="deduplicated_UUIDs.csv",
)

# dropping old unique_id column and renaming unique_id_deduped to become new unique_id
FFF_df = FFF_df.drop("unique_id", axis="columns")
FFF_df = FFF_df.rename(columns={"unique_id_deduped": "unique_id"})

# GETTING INFOGROUP DATA

SIC6_codes_csv = BASE_FILEPATH / "data" / "classification" / "raw" / "SIC6_codes.csv"

infogroup_data_2023 = (
    BASE_FILEPATH / "data" / "classification" / "raw" / "2023_InfoGroup.txt"
)

relevant_InfoGroup_2023_csv = (
    BASE_FILEPATH / "data" / "classification" / "raw" / "relevant_InfoGroup_2023.csv"
)

relevant_InfoGroup_csv = (
    BASE_FILEPATH / "data" / "classification" / "raw" / "relevant-infogroup.csv"
)

relevant_InfoGroup_df = pd.read_csv(relevant_InfoGroup_csv)

relevant_InfoGroup_df.to_csv(relevant_InfoGroup_2023_csv)

InfoGroup_df = get_infogroup_df(
    sic6_codes_csv=SIC6_codes_csv,
    infogroup_csv=infogroup_data_2023,
    output_file_path=relevant_InfoGroup_2023_csv,
    cached=True,
)

if "unique_id" not in InfoGroup_df.columns:
    InfoGroup_df["unique_id"] = [uuid.uuid4() for i in range(len(InfoGroup_df))]

# deduping InfoGroup company data

# deduped FFF df
InfoGroup_df = splink_dedupe(
    df=InfoGroup_df,
    settings=INFOGROUP_COMPANY_CLASSIFICATION_SETTINGS,
    blocking=INFOGROUP_COMPANY_CLASSIFICATION_BLOCKING,
    mapped_uuid_file_path_name="deduplicated_UUIDs.csv",
)

# dropping old unique_id column and renaming unique_id_deduped to become new unique_id
InfoGroup_df = InfoGroup_df.drop("unique_id", axis="columns")
InfoGroup_df = InfoGroup_df.rename(columns={"unique_id_deduped": "unique_id"})

# MERGING AND TRANSFORMING FFF AND INFOGROUP DATA

aggregated_classification_csv = (
    BASE_FILEPATH
    / "data"
    / "classification"
    / "merged_cleaned_company_classification.csv"
)

# aggregating and merging the FFF df and the InfoGroup df
aggregated_company_df = merge_company_dfs(
    cleaned_fff_df=FFF_df,
    cleaned_infogroup_df=InfoGroup_df,
    output_file_path=aggregated_classification_csv,
)
