"""
Constants to be used in various parts of the project
"""
from pathlib import Path

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
