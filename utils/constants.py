"""
Constants to be used in various parts of the project
"""
from pathlib import Path

BASE_FILEPATH = Path(__file__).resolve().parent.parent

USER_AGENT = """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
                (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"""

HEADERS = {"User-Agent": USER_AGENT}

MI_EXP_FILEPATH = str(BASE_FILEPATH / "data" / "Expenditure")

MI_CON_FILEPATH = str(BASE_FILEPATH / "data" / "Contribution")

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

MN_CANDIDATE_CONTRIBUTION_COL = [
    "OfficeSought",
    "Party",
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
    "Party": "party",
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


AZ_pages_dict = {
    "Candidate": 1,
    "PAC": 2,
    "Political Party": 3,
    "Organzations": 4,
    "Independent Expenditures": 5,
    "Ballot Measures": 6,
    "Individual Contributors": 7,
    "Vendors": 8,
    "Name": 11,
    "Candidate/Income": 20,
    "Candidate/Expense": 21,
    "Candidate/IEFor": 22,
    "Candidate/IEAgainst": 23,
    "Candidate/All Transactions": 24,
    "PAC/Income": 30,
    "PAC/Expense": 31,
    "PAC/IEFor": 32,
    "PAC/IEAgainst": 33,
    "PAC/BMEFor": 34,
    "PAC/BMEAgainst": 35,
    "PAC/All Transactions": 36,
    "Political Party/Income": 40,
    "Political Party/Expense": 41,
    "Political Party/All Transactions": 42,
    "Organizations/IEFor": 50,
    "Organizations/IEAgainst": 51,
    "Organizations/BMEFor": 52,
    "Organizations/BME Against": 53,
    "Organizations/All Transactions": 54,
    "Independent Expenditures/IEFor": 60,
    "Independent Expenditures/IEAgainst": 61,
    "Independent Expenditures/All Transactions": 62,
    "Ballot Measures/Amount For": 70,
    "Ballot Measures/Amount Against": 71,
    "Ballot Measures/All Transactions": 72,
    "Individuals/All Transactions": 80,
    "Vendors/All Transactions": 90,
}

AZ_head = {
    "authority": "seethemoney.az.gov",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.7",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://seethemoney.az.gov",
    "referer": "https://seethemoney.az.gov/Reporting/Explore",
    "sec-ch-ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Brave";v="116"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "user-agent": """Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36""",
    "x-requested-with": "XMLHttpRequest",
}


AZ_valid_detailed_pages = [
    20,
    21,
    22,
    23,
    24,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    40,
    41,
    42,
    50,
    51,
    52,
    53,
    54,
    60,
    61,
    62,
    70,
    71,
    72,
    80,
    90,
]


AZ_base_data = {
    "draw": "2",
    "order[0][column]": "0",
    "order[0][dir]": "asc",
    "start": "0",
    "length": "500000",
    "search[value]": "",
    "search[regex]": "false",
}
