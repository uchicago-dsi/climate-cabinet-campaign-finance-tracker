"""
Constants to be used in various parts of the project
"""
USER_AGENT = """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
                (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"""

HEADERS = {"User-Agent": USER_AGENT}

EXP_FILEPATH = "../data/Expenditure/"

FILEPATH = "../data/Contributions/"

URL = "https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/"

VALUES_TO_CHECK = ["1998", "1999", "2000", "2001", "2002", "2003"]

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
