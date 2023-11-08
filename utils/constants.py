"""
Constants to be used in various parts of the project
"""
from pathlib import Path

MI_FILEPATH = "../data/Contributions/"

MI_VALUES_TO_CHECK = ["1998", "1999", "2000", "2001", "2002", "2003"]

BASE_FILEPATH = Path(__file__).resolve().parent.parent

USER_AGENT = """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
                (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"""

HEADERS = {"User-Agent": USER_AGENT}

MI_EXP_FILEPATH = BASE_FILEPATH / "data" / "Expenditure"

MI_CON_FILEPATH = BASE_FILEPATH / "data" / "Contributions"

MI_SOS_URL = "https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/"

MI_CONTRIBUTION_COLUMNS = [
    "doc_seq_no",
    "page_no",
    "contribution_id",
    "cont_detail_id",
    "doc_stmnt_year",
    "doc_type_desc",
    "com_legal_name",
    "common_name",
    "cfr_com_id",
    "com_type",
    "can_first_name",
    "can_last_name",
    "contribtype",
    "f_name",
    "l_name_or_org",
    "address",
    "city",
    "state",
    "zip",
    "occupation",
    "employer",
    "received_date",
    "amount",
    "aggregate",
    "extra_desc",
]

# PA Web Scraping Constants:

PA_MAIN_URL = "https://www.dos.pa.gov"
PA_ZIPPED_URL = (
    "/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Documents/"
)

# PA EDA constants:

PA_CONT_COLS_NAMES_PRE2022: list = [
    "FILER_ID",
    "YEAR",
    "CYCLE",
    "SECTION",
    "CONTRIBUTOR",
    "ADDRESS_1",
    "ADDRESS_2",
    "CITY",
    "STATE",
    "ZIPCODE",
    "OCCUPATION",
    "E_NAME",
    "E_ADDRESS_1",
    "E_ADDRESS_2",
    "E_CITY",
    "E_STATE",
    "E_ZIPCODE",
    "CONT_DATE_1",
    "CONT_AMT_1",
    "CONT_DATE_2",
    "CONT_AMT_2",
    "CONT_DATE_3",
    "CONT_AMT_3",
    "CONT_DESCRIP"
]

PA_CONT_COLS_NAMES_POST2022: list = [
    "FILER_ID",
    "REPORTER_ID",
    "TIMESTAMP",
    "YEAR",
    "CYCLE",
    "SECTION",
    "CONTRIBUTOR",
    "ADDRESS_1",
    "ADDRESS_2",
    "CITY",
    "STATE",
    "ZIPCODE",
    "OCCUPATION",
    "E_NAME",
    "E_ADDRESS_1",
    "E_ADDRESS_2",
    "E_CITY",
    "E_STATE",
    "E_ZIPCODE",
    "CONT_DATE_1",
    "CONT_AMT_1",
    "CONT_DATE_2",
    "CONT_AMT_2",
    "CONT_DATE_3",
    "CONT_AMT_3",
    "CONT_DESCRIP"
]

PA_FILER_COLS_NAMES_PRE2022: list = [
    "FILER_ID",
    "YEAR",
    "CYCLE",
    "AMEND",
    "TERMINATE",
    "FILER_TYPE",
    "FILER_NAME",
    "OFFICE",
    "DISTRICT",
    "PARTY",
    "ADDRESS_1",
    "ADDRESS_2",
    "CITY",
    "STATE",
    "ZIPCODE",
    "COUNTY",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND"
]

PA_FILER_COLS_NAMES_POST2022: list = [
    "FILER_ID",
    "REPORTER_ID",
    "TIMESTAMP",
    "YEAR",
    "CYCLE",
    "AMEND",
    "TERMINATE",
    "FILER_TYPE",
    "FILER_NAME",
    "OFFICE",
    "DISTRICT",
    "PARTY",
    "ADDRESS_1",
    "ADDRESS_2",
    "CITY",
    "STATE",
    "ZIPCODE",
    "COUNTY",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND"
]

PA_EXPENSE_COLS_NAMES_PRE2022: list = [
    "FILER_ID",
    "EXPENSE_YEAR",
    "EXPENSE_CYCLE",
    "EXPENSE_NAME",
    "EXPENSE_ADDRESS1",
    "EXPENSE_ADDRESS2",
    "EXPENSE_CITY",
    "EXPESNE_STATE",
    "EXPENSE_ZIPCODE",
    "EXPENSE_DATE",
    "EXPENSE_AMT",
    "EXPENSE_DESC",
]

PA_EXPENSE_COLS_NAMES_POST2022: list = [
    "FILER_ID",
    "EXPENSE_REPORTER_ID",
    "EXPENSE_TIMESTAMP",
    "EXPENSE_YEAR",
    "EXPENSE_CYCLE",
    "EXPENSE_NAME",
    "EXPENSE_ADDRESS_1",
    "EXPENSE_ADDRESS_2",
    "EXPENSE_CITY",
    "EXPESNE_STATE",
    "EXPENSE_ZIPCODE",
    "EXPENSE_DATE",
    "EXPENSE_AMT",
    "EXPENSE_DESC",
]

PA_OFFICE_ABBREV_DICT: dict = {
    "GOV": "Governor",
    "LTG": "Liutenant Gov",
    "ATT": "Attorney General",
    "AUD": "Auditor General",
    "TRE": "State Treasurer",
    "SPM": "Justice of the Supreme Crt",
    "SPR": "Judge of the Superior Crt",
    "CCJ": "Judge of the CommonWealth Crt",
    "CPJ": "Judge of the Crt of Common Pleas",
    "CPJA": "Judge of the Crt of Common Pleas",
    "CPJP": "Judge of the Crt of Common Pleas",
    "MCJ": "Judge of the Municipal Crt",
    "TCJ": "Judge of the Traffic Crt",
    "STS": "Senator (General Assembly)",
    "STH": "Rep (General Assembly)",
    "USC": "United States Congress",
    "USS": "United States Senate",
    "DSC": "Member of Dem State Committee",
    "RSC": "Member of Rep State Committee",
    "OTH": "Other(local offices)",
}
PA_FILER_ABBREV_DICT: dict = {1.0: "Candidate", 2.0: "Committee", 3.0: "Lobbyist"}
