import pandas as pd


def pre_process_az():

    inds_14_22 = pd.read_csv("inds_14_22.csv")
    pac_14_22 = pd.read_csv("pac_14_22.csv")
    org_14_22 = pd.read_csv("org_14_22.csv")
    cands_14_22 = pd.read_csv("cands_14_22.csv")

    miniind = inds_14_22[["Name", "total_spending", "type"]]
    miniind["Name"] = miniind["Name"].str.lower()

    function_dictionary = {"total_spending": "sum", "type": "max"}

    ind_bla = miniind.groupby(by="Name").aggregate(function_dictionary)
    ind_bla = ind_bla.sort_values(by="total_spending", ascending=False).head(11)

    minipac = pac_14_22[["Name", "total_spending", "type"]]
    minipac["Name"] = minipac["Name"].str.lower()

    function_dictionary = {"total_spending": "sum", "type": "max"}

    pac_bla = minipac.groupby(by="Name").aggregate(function_dictionary)
    pac_bla = pac_bla.sort_values(by="total_spending", ascending=False).head(10)

    miniorg = org_14_22[["Name", "total_spending", "type"]]
    miniorg["Name"] = miniorg["Name"].str.lower()

    function_dictionary = {"total_spending": "sum", "type": "max"}

    org_bla = miniorg.groupby(by="Name").aggregate(function_dictionary)
    org_bla = org_bla.sort_values(by="total_spending", ascending=False).head(10)

    minicand = cands_14_22[["Name", "total_spending", "type"]]
    minicand["Name"] = minicand["Name"].str.lower()

    function_dictionary = {"total_spending": "sum", "type": "max"}

    bla_cands = minicand.groupby(by="Name").aggregate(function_dictionary)
    bla_cands = bla_cands.sort_values(by="total_spending", ascending=False)
    bla_cands = bla_cands.head(10)

    return ind_bla, pac_bla, org_bla, bla_cands
