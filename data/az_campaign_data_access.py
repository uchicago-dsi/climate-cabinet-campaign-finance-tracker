import pandas as pd

ballots = pd.read_csv("AZ_BallotMeasures_04_23.csv", encoding="utf-16le")

candcsv = "AZ_Candidates_04_23.csv"
cands = pd.read_csv(candcsv, encoding="utf-16le", on_bad_lines="skip")
# note: a delimiter error in this CSV, as downloaded from the web portal,
# leads to token errors.
# in order to bypass the token error,
# 33 rows (a little under 1%) are excluded.
# a more permanent fix may come later

ie = pd.read_csv("AZ_IndependentExpenditures_04_23.csv", encoding="utf-16le")
# note that 'individual expenditures' are expenditures
# by persons other than the candidate
# or their campaign for the purposes of advertising,
# which is not coordinated with that candidate or
# their campaign. In contrast, individual contributions
# (see 'individuals' below) are simply
# donations by individuals

org = pd.read_csv("AZ_Organizations_04_23.csv", encoding="utf-16le")

pac = pd.read_csv("AZ_PoliticalActionCommittees_04_23.csv", encoding="utf-16le")

party = (
    pd.read_csv("AZ_PoliticalParties_04_23.csv", encoding="utf-16le")
    .reset_index()
    .drop(["CashBalance"], axis=1)
    .rename(
        columns={
            "Expense": "CashBalance",
            "Income": "Expense",
            "Name": "Income",
            "index": "Name",
        }
    )
)

vendors = pd.read_csv("AZ_Vendors_04_23.csv", encoding="utf-16le")

individuals = pd.read_csv("AZ_Individuals_04_23.csv", encoding="utf-16le")
