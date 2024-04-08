"""EDA on UChicago 2023 company data to improve their classification"""

import pandas as pd
from utils.constants import BASE_FILEPATH

business_data_2023 = (
    BASE_FILEPATH / "data" / "raw_classification" / "2023_Business_Academic_QCQ.txt"
)

cols = [
    "COMPANY",
    "ADDRESS LINE 1",
    "CITY",
    "STATE",
    "ZIPCODE",
    "AREA CODE",
    "PRIMARY SIC CODE",
    "SIC6_DESCRIPTIONS (SIC)",
]

ff_keywords = ["OIL", "GAS", "FOSSIL", "FUEL", "COAL", "GASOLINE"]
ce_keywords = ["SOLAR"]
ambiguous_keywords = ["ENERGY"]

ff_keywords_df = pd.DataFrame()
ce_keywords_df = pd.DataFrame()
ambiguous_keywords_df = pd.DataFrame()

pattern_ff = "|".join(ff_keywords)
pattern_ce = "|".join(ce_keywords)
pattern_ambg = "|".join(ambiguous_keywords)

business_data_df = pd.read_csv(business_data_2023, sep=",", header=0, chunksize=1000)
for chunk in business_data_df:
    chunk = chunk.dropna(subset=["SIC6_DESCRIPTIONS (SIC)"])
    ff_keywords_df = pd.concat(
        [
            ff_keywords_df,
            chunk[chunk["SIC6_DESCRIPTIONS (SIC)"].str.contains(pattern_ff)],
        ]
    )
    ce_keywords_df = pd.concat(
        [
            ce_keywords_df,
            chunk[chunk["SIC6_DESCRIPTIONS (SIC)"].str.contains(pattern_ce)],
        ]
    )
    ambiguous_keywords_df = pd.concat(
        [
            ambiguous_keywords_df,
            chunk[chunk["SIC6_DESCRIPTIONS (SIC)"].str.contains(pattern_ambg)],
        ],
    )

print(ff_keywords_df["SIC6_DESCRIPTIONS (SIC)"].unique())
print(ce_keywords_df["SIC6_DESCRIPTIONS (SIC)"].unique())
print(ambiguous_keywords_df["SIC6_DESCRIPTIONS (SIC)"].unique())
