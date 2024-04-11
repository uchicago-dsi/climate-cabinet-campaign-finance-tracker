"""Script to combine datasets of fossil fule company names from CSVs for use in classification"""

# TODO: #92 Make orgs classification script into more well-defined pipeline
import pandas as pd

from utils.constants import BASE_FILEPATH

# PATHS

coal_company_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "ff_companies"
    / "FFF_coal_companies.csv"
)
oil_company_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "ff_companies"
    / "FFF_oil_companies.csv"
)

infogroup_clean_energy_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "infogroup_biz_data"
    / "infogroup_CE.csv"
)

infogroup_ff_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "infogroup_biz_data"
    / "infogroup_ff.csv"
)

infogroup_ambiguous_csv = (
    BASE_FILEPATH
    / "data"
    / "raw_classification"
    / "infogroup_biz_data"
    / "infogroup_ambiguous.csv"
)


organization_classification_csv = (
    BASE_FILEPATH / "data" / "raw_classification" / "organization_classifications.csv"
)


# disclaimer: companies are global (perhaps not all US companies )
ff_companies_dfs = [pd.read_csv(coal_company_csv), pd.read_csv(oil_company_csv)]
ff_companies_dfs = pd.concat(ff_companies_dfs)

# adding a classification column
ff_companies_dfs["initial classification"] = ["f"] * len(ff_companies_dfs)

# preparing infogroup CSVs to merge
infogroup_CSVs = [infogroup_clean_energy_csv, infogroup_ambiguous_csv, infogroup_ff_csv]
infogroup_dfs = [pd.read_csv(csv) for csv in infogroup_CSVs]
# renaming COMPANY column to Company to merge w/ the dfs from FFF
infogroup_dfs = [
    df.rename(mapper={"COMPANY": "Company"}, axis=1) for df in infogroup_dfs
]
concatted_infogroups_dfs = pd.concat(infogroup_dfs)

# combining all the datasets into one to be written to csv file
all_ff_companies = pd.concat([ff_companies_dfs, concatted_infogroups_dfs])

# writes the df to the organization_classification csv file
all_ff_companies.to_csv(organization_classification_csv, mode="w", index=False)
