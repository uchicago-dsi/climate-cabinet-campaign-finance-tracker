"""
This document lists the constants used in web scraping and Exploratory
Data Analysis

"""
# Web Scraping Constants:

main_url = "https://www.dos.pa.gov"
zipped_url = (
    "/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Documents/"
)

# EDA constants:

cont_cols_names_pre2022: list = [
    "FilerID",
    "EYear",
    "Cycle",
    "Section",
    "Contributor",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "occupation",
    "Ename",
    "EAddress1",
    "EAddress2",
    "ECity",
    "EState",
    "EZipcode",
    "ContDate1",
    "ContAmt1",
    "ContDate2",
    "ContAmt2",
    "ContDate3",
    "ContAmt3",
    "ContDesc",
]

cont_cols_names_post22: list = [
    "FilerID",
    "ReporterID",
    "Timestamp",
    "EYear",
    "Cycle",
    "Section",
    "Contributor",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "occupation",
    "Ename",
    "EAddress1",
    "EAddress2",
    "ECity",
    "EState",
    "EZipcode",
    "ContDate1",
    "ContAmt1",
    "ContDate2",
    "ContAmt2",
    "ContDate3",
    "ContAmt3",
    "ContDesc",
]

filer_cols_names_pre2022: list = [
    "FilerID",
    "EYear",
    "Cycle",
    "Amend",
    "Terminate",
    "FilerType",
    "FilerName",
    "Office",
    "District",
    "Party",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "County",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND",
]

filer_cols_names_post2022: list = [
    "FilerID",
    "ReporterID",
    "Timestamp",
    "EYear",
    "Cycle",
    "Amend",
    "Terminate",
    "FilerType",
    "FilerName",
    "Office",
    "District",
    "Party",
    "Address1",
    "Address2",
    "City",
    "State",
    "Zipcode",
    "County",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND",
]

office_abb_dict: dict = {
    "GOV": "Governor",
    "LTG": "Liutenant Gov",
    "ATT": "Attorney General",
    "AUD": "Auditor General",
    "TRE": "State Treasurer",
    "SPM": "Justice of the Supreme Crt",
    "SPR": "Judge of the Superior Crt",
    "CCJ": "Judge of the CommonWealth Crt",
    "CPJ": "Judge of the Crt of Common Pleas",
    "MCJ": "Judge of the Municipal Crt",
    "TCJ": "Judge of the Traffic Crt",
    "STS": "Senator (General Assembly)",
    "STH": "Rep (General Assembly)",
    "OTH": "Other(local offices)",
    "MISC": "Unknown",
}
filer_abb_dict: dict = {1.0: "Candidate", 2.0: "Committee", 3.0: "Lobbyist"}
