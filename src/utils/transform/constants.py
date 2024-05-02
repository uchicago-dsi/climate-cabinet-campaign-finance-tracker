"""Constants to be used in various parts of the project."""

from utils.constants import BASE_FILEPATH

INIDIVIDUAL_COLUMNS = [
    "DONOR",
    "DONOR_ID",
    "DONOR_PARTY",
    "DONOR_TYPE",
    "DONOR_FIRST_NAME",
    "DONOR_LAST_NAME",
    "DONOR_EMPLOYER",
    "RECIPIENT",
    "RECIPIENT_ID",
    "RECIPIENT_PARTY",
    "RECIPIENT_TYPE",
]
MI_EXP_FILEPATH = BASE_FILEPATH / "data" / "raw" / "MI" / "Expenditure"

MI_CON_FILEPATH = BASE_FILEPATH / "data" / "raw" / "MI" / "Contribution"

AZ_TRANSACTIONS_FILEPATH = (
    BASE_FILEPATH / "data" / "raw" / "AZ" / "az_transactions_demo.csv"
)

AZ_INDIVIDUALS_FILEPATH = (
    BASE_FILEPATH / "data" / "raw" / "AZ" / "az_individuals_demo.csv"
)

AZ_ORGANIZATIONS_FILEPATH = BASE_FILEPATH / "data" / "raw" / "AZ" / "az_orgs_demo.csv"


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

MN_FILEPATHS_LST = [
    BASE_FILEPATH / "data" / "raw" / "MN" / "AG.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "AP.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "DC.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "GC.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "House.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "SA.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "SC.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "Senate.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "SS.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "ST.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "non_candidate_con.csv",
    BASE_FILEPATH / "data" / "raw" / "MN" / "independent_exp.csv",
]

MN_CANDIDATE_CONTRIBUTION_COL = [
    "OfficeSought",
    "CandRegNumb",
    "CandFirstName",
    "CandLastName",
    "DonationDate",
    "DonorType",
    "DonorName",
    "DonationAmount",
    "InKindDonAmount",
    "InKindDescriptionText",
]

MN_CANDIDATE_CONTRIBUTION_MAP = {
    "OfficeSought": "office_sought",
    "CandRegNumb": "recipient_id",
    "CandFirstName": "recipient_first_name",
    "CandLastName": "recipient_last_name",
    "DonationDate": "date",
    "DonorType": "donor_type",
    "DonorName": "donor_full_name",
    "DonationAmount": "amount",
    "InKindDonAmount": "inkind_amount",
    "InKindDescriptionText": "purpose",
}

MN_NONCANDIDATE_CONTRIBUTION_COL = [
    "PCFRegNumb",
    "Committee",
    "ETType",
    "DonationDate",
    "DonorType",
    "DonorRegNumb",
    "DonorName",
    "DonationAmount",
    "InKindDonAmount",
    "InKindDescriptionText",
]

MN_NONCANDIDATE_CONTRIBUTION_MAP = {
    "PCFRegNumb": "recipient_id",
    "Committee": "recipient_full_name",
    "ETType": "recipient_type",
    "DonationDate": "date",
    "DonorType": "donor_type",
    "DonorRegNumb": "donor_id",
    "DonorName": "donor_full_name",
    "DonationAmount": "amount",
    "InKindDonAmount": "inkind_amount",
    "InKindDescriptionText": "purpose",
}

MN_INDEPENDENT_EXPENDITURE_COL = [
    "Spender",
    "Spender Reg Num",
    "Spender type",
    "Affected Comte Name",
    "Affected Cmte Reg Num",
    "For /Against",
    "Date",
    "Type",
    "Amount",
    "Purpose",
    "Vendor State",
]

MN_INDEPENDENT_EXPENDITURE_MAP = {
    "Spender": "donor_full_name",
    "Spender Reg Num": "donor_id",
    "Spender type": "donor_type",
    "Affected Comte Name": "recipient_full_name",
    "Affected Cmte Reg Num": "recipient_id",
    "Date": "date",
    "Amount": "amount",
    "Purpose": "purpose",
    "Type": "transaction_type",
    "Vendor State": "state",
}

MN_RACE_MAP = {
    "GC": "Governor",
    "AG": "Attorney General",
    "SS": "Secretary of State",
    "SA": "State Auditor",
    "ST": "State Treasurer",
    "Senate": "State Senator",
    "House": "State Representative",
    "SC": "State Supreme Court Justice",
    "AP": "State Appeals Court Judge",
    "DC": "State District Court Judge",
}


MI_CONT_DROP_COLS = [
    "doc_seq_no",
    "page_no",
    "cont_detail_id",
    "doc_type_desc",
    "address",
    "city",
    "zip",
    "occupation",
    "received_date",
    "aggregate",
    "extra_desc",
]

MI_EXP_DROP_COLS = [
    "doc_seq_no",
    "expenditure_type",
    "gub_account_type",
    "gub_elec_type",
    "page_no",
    "detail_id",
    "doc_type_desc",
    "extra_desc",
    "address",
    "city",
    "zip",
    "exp_date",
    "state_loc",
    "supp_opp",
    "can_or_ballot",
    "county",
    "debt_payment",
    "vend_addr",
    "vend_city",
    "vend_state",
    "vend_zip",
    "gotv_ink_ind",
    "fundraiser",
]


# PA EDA constants:

PA_SCHEMA_CHANGE_YEAR = 2022


PA_CONT_COLS_NAMES_PRE2022: list = [
    "RECIPIENT_ID",
    "YEAR",
    "CYCLE",
    "SECTION",
    "DONOR",
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
    "PURPOSE",
]

PA_CONT_COLS_NAMES_POST2022: list = [
    "RECIPIENT_ID",
    "REPORTER_ID",
    "TIMESTAMP",
    "YEAR",
    "CYCLE",
    "SECTION",
    "DONOR",
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
    "PURPOSE",
]

PA_FILER_COLS_NAMES_PRE2022: list = [
    "RECIPIENT_ID",
    "YEAR",
    "CYCLE",
    "AMEND",
    "TERMINATE",
    "RECIPIENT_TYPE",
    "RECIPIENT",
    "RECIPIENT_OFFICE",
    "DISTRICT",
    "RECIPIENT_PARTY",
    "ADDRESS_1",
    "ADDRESS_2",
    "CITY",
    "STATE",
    "ZIPCODE",
    "COUNTY",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND",
]

PA_FILER_COLS_NAMES_POST2022: list = [
    "RECIPIENT_ID",
    "REPORTER_ID",
    "TIMESTAMP",
    "YEAR",
    "CYCLE",
    "AMEND",
    "TERMINATE",
    "RECIPIENT_TYPE",
    "RECIPIENT",
    "RECIPIENT_OFFICE",
    "DISTRICT",
    "RECIPIENT_PARTY",
    "ADDRESS_1",
    "ADDRESS_2",
    "CITY",
    "STATE",
    "ZIPCODE",
    "COUNTY",
    "PHONE",
    "BEGINNING",
    "MONETARY",
    "INKIND",
]

PA_EXPENSE_COLS_NAMES_PRE2022: list = [
    "DONOR_ID",
    "YEAR",
    "EXPENSE_CYCLE",
    "RECIPIENT",
    "EXPENSE_ADDRESS_1",
    "EXPENSE_ADDRESS_2",
    "EXPENSE_CITY",
    "EXPENSE_STATE",
    "EXPENSE_ZIPCODE",
    "EXPENSE_DATE",
    "AMOUNT",
    "PURPOSE",
]

PA_EXPENSE_COLS_NAMES_POST2022: list = [
    "DONOR_ID",
    "EXPENSE_REPORTER_ID",
    "EXPENSE_TIMESTAMP",
    "YEAR",
    "EXPENSE_CYCLE",
    "RECIPIENT",
    "EXPENSE_ADDRESS_1",
    "EXPENSE_ADDRESS_2",
    "EXPENSE_CITY",
    "EXPENSE_STATE",
    "EXPENSE_ZIPCODE",
    "EXPENSE_DATE",
    "AMOUNT",
    "PURPOSE",
]

PA_OFFICE_ABBREV_DICT: dict = {
    "GOV": "Governor",
    "LTG": "Lieutenant Gov",
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
PA_FILER_ABBREV_DICT: dict = {
    1.0: "Candidate",
    2.0: "Committee",
    3.0: "Lobbyist",
}

PA_ORGANIZATION_IDENTIFIERS: list = [
    "FRIENDS",
    "CITIZENS",
    "UNION",
    "STATE",
    "TEAM",
    "PAC",
    "PA",
    "GOVT",
    "WARD",
    "DEM",
    "COM",
    "COMMITTEE",
    "CORP",
    "ASSOCIATIONS",
    "FOR",
    "FOR THE",
    "SENATE",
    "COMMONWEALTH",
    "ELECT",
    "POLITICAL ACTION COMMITTEE",
    "REPUBLICANS",
    "REPUBLICAN",
    "DEMOCRAT",
    "DEMOCRATS",
    "CORPORATION",
    "CORP",
    "COMPANY",
    "CO",
    "LIMITED",
    "LTD",
    "INC",
    "INCORPORATED",
    "LLC",
    "FUND",
]

MI_EXPENDITURE_COLUMNS = [
    "doc_seq_no",
    "expenditure_type",
    "gub_account_type",
    "gub_elec_type",
    "page_no",
    "expense_id",
    "detail_id",
    "doc_stmnt_year",
    "doc_type_desc",
    "com_legal_name",
    "common_name",
    "cfr_com_id",
    "com_type",
    "schedule_desc",
    "exp_desc",
    "purpose",
    "extra_desc",
    "f_name",
    "lname_or_org",
    "address",
    "city",
    "state",
    "zip",
    "exp_date",
    "amount",
    "state_loc",
    "supp_opp",
    "can_or_ballot",
    "county",
    "debt_payment",
    "vend_name",
    "vend_addr",
    "vend_city",
    "vend_state",
    "vend_zip",
    "gotv_ink_ind",
    "fundraiser",
]

MICHIGAN_CONTRIBUTION_COLS_REORDER = [
    "doc_seq_no",
    "page_no",
    "contribution_id",
    "cont_detail_id",
    "doc_stmnt_year",
    "doc_type_desc",
    "common_name",
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
    "amount",
    "received_date",
    "aggregate",
    "extra_desc",
    "amount",
]

MICHIGAN_CONTRIBUTION_COLS_RENAME = [
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
]


state_abbreviations = [
    " AK ",
    " AL ",
    " AR ",
    " AZ ",
    " CA ",
    " CO ",
    " CT ",
    " DC ",
    " DE ",
    " FL ",
    " GA ",
    " GU ",
    " HI ",
    " IA ",
    " ID ",
    " IL ",
    " IN ",
    " KS ",
    " KY ",
    " LA ",
    " MA ",
    " MD ",
    " ME ",
    " MI ",
    " MN ",
    " MO ",
    " MS ",
    " MT ",
    " NC ",
    " ND ",
    " NE ",
    " NH ",
    " NJ ",
    " NM ",
    " NV ",
    " NY ",
    " OH ",
    " OK ",
    " OR ",
    " PA ",
    " PR ",
    " RI ",
    " SC ",
    " SD ",
    " TN ",
    " TX ",
    " UT ",
    " VA ",
    " VI ",
    " VT ",
    " WA ",
    " WI ",
    " WV ",
    " WY ",
]
PA_CONTRIBUTION_COLS: list = ["RECIPIENT_ID", "DONOR", "AMOUNT", "YEAR", "PURPOSE"]

PA_FILER_COLS: list = [
    "RECIPIENT_ID",
    "RECIPIENT_TYPE",
    "RECIPIENT",
    "RECIPIENT_OFFICE",
    "RECIPIENT_PARTY",
]

PA_EXPENSE_COLS: list = [
    "DONOR_ID",
    "RECIPIENT",
    "PURPOSE",
    "AMOUNT",
    "YEAR",
]

TX_CONTRIBUTION_COLS: list = [
    "filerIdent",
    "filerName",
    "contributionDt",
    "contributionAmount",
    "contributorPersentTypeCd",
    "contributorNameOrganization",
    "contributorNameLast",
    "contributorNameFirst",
]
# TO CLARIFY: (1) does filer refer to recipient (2)no office for texas
TX_FILER_COLS: list = [
    "filerIdent",
    "filerTypeCd",
    "filerName",
    "filerNameOrganization",
]

TX_FILER_MAPPING: dict = {
    "filerIdent": "RECIPIENT_ID",
    "filerTypeCd": "RECIPIENT_TYPE",
    "filerName": "RECIPIENT",
}


# TO CLARIFY: no purpose in texas expend
TX_EXPENSE_COLS: list = [
    "filerIdent",
    "payeePersentTypeCd",
    "payeeNameOrganization",
    "payeeNameLast",
    "payeeNameFirst",
    "expendAmount",
    "expendDt",
]

TX_CONTRIBUTION_MAPPING: dict = {
    "filerIdent": "RECIPIENT_ID",
    "contributionAmount": "AMOUNT",
}

TX_FILER_MAPPING: dict = {
    "filerIdent": "RECIPIENT_ID",
    "filerTypeCd": "RECIPIENT_TYPE",
    "filerName": "RECIPIENT",
}

TX_EXPENSE_MAPPING: dict = {
    "filerIdent": "DONOR_ID",
}
