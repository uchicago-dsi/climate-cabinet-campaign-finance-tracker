"""EDA on UChicago 2023 company data to improve their classification"""

import pandas as pd
from utils.constants import BASE_FILEPATH

# PATHS
business_data_2023 = (
    BASE_FILEPATH / "data" / "raw_classification" / "2023_Business_Academic_QCQ.txt"
)

clean_energy_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "infogroup_biz_data"
    / "infogroup_CE.csv"
)

ff_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "infogroup_biz_data"
    / "infogroup_ff.csv"
)

ambiguous_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "infogroup_biz_data"
    / "infogroup_ambiguous.csv"
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
    "initial classification",
]

ff_keywords = ["OIL", "GAS", "FOSSIL", "FUEL", "COAL", "GASOLINE"]
ce_keywords = ["SOLAR"]
ambiguous_keywords = ["ENERGY"]

ff_keywords_df = pd.DataFrame()
ce_keywords_df = pd.DataFrame()
ambiguous_keywords_df = pd.DataFrame()

# allows us to see if any of the keywords are present in the SIC code description
pattern_ff = "|".join(ff_keywords)
pattern_ce = "|".join(ce_keywords)
pattern_ambg = "|".join(ambiguous_keywords)

business_data_df = pd.read_csv(business_data_2023, sep=",", header=0, chunksize=1000)
counter = 0
max_chunks = 20  # only looking at around 20 chunks for now since file is quite large
for chunk in business_data_df:
    # TODO: #93 Use SIC6 codes instead of descriptions
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
    counter += 1
    if counter >= max_chunks:
        break

ce_keywords_df["initial classification"] = "c"
ff_keywords_df["initial classification"] = "f"
ambiguous_keywords_df["initial classification"] = None

ce_keywords_df[cols].to_csv(clean_energy_csv, mode="w", index=False)
ff_keywords_df[cols].to_csv(ff_csv, mode="w", index=False)
ambiguous_keywords_df[cols].to_csv(ambiguous_csv, mode="w", index=False)

print(pd.read_csv(clean_energy_csv).head())
print(pd.read_csv(ff_csv).head())
print(pd.read_csv(ambiguous_csv).head())
