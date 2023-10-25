"""
Constants to be used in various parts of the project
"""
EXP_FILEPATH = "/Users/necabotheking/Documents/Github/2023-fall-clinic-climate-cabinet/data/Expenditure/"

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

# MI_EXPENDITURE_COLUMNS = [
#     "doc_seq_no", 
#     "expenditure_type", 
#     "gub_account_type", 
#     "gub_elec_type", 
#     "page_no",	
#     "expense_id",	
#     "detail_id",	
#     "doc_stmnt_year",	
#     "doc_type_desc",	
#     "com_legal_name",	
#     "common_name",	
#     "cfr_com_id", 
#     "com_type",	
#     "schedule_desc", 
#     "exp_desc",	
#     "purpose",	
#     "extra_desc",	
#     "f_name",	
#     "lname_or_org",	
#     "address",	
#     "city",	
#     "state",	
#     "zip",	
#     "exp_date",	
#     "amount",	
#     "state_loc",	
#     "supp_opp",	
#     "can_or_ballot", 
#     "county",	
#     "debt_payment",	
#     "vend_name",	
#     "vend_addr", 
#     "vend_city",	
#     "vend_state",	
#     "vend_zip",	
#     "gotv_ink_ind",	
#     "fundraiser"
# ]

MI_EXPENDITURE_COLUMNS = [ "doc_seq_no", "expenditure_type",
                          "gub_account_type", "gub_elec_type",
                          "page_no", "expense_id", "detail_id",
                          "doc_stmnt_year", "doc_type_desc", "com_legal_name", 
                          "common_name", "cfr_com_id", "com_type",
                          "schedule_desc", "exp_desc", "purpose", "extra_desc", 
                          "f_name", "lname_or_org", "address", "city", "state", 
                          "zip", "exp_date", "amount", "state_loc", "supp_opp",
                          "can_or_ballot", "county", "debt_payment", "vend_name",
                          "vend_addr", "vend_city", "vend_state", "vend_zip",
                          "gotv_ink_ind", "fundraiser" ]