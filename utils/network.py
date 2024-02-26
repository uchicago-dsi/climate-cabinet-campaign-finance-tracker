import networkx as nx
import pandas as pd


def name_identifier(uuid: str, dfs: list[pd.DataFrame]) -> str:
    """Returns the name of the entity given the entity's uuid

    Args:
        uuid: the uuid of the entity
        List of dfs: dataframes that have a uuid column, and an 'name' or
        'full_name' column
    Return:
        The entity's name
    """
    for df in dfs:
        # first, check orgs df:
        if "name" in df.columns:
            name_in_org = df.loc[df["id"] == uuid]
            if len(name_in_org) > 0:
                return name_in_org.iloc[0]["name"]
        # theoretically it must be in inds if not in orgs, but for the sample
        # data this might not be the case

        if "full_name" in df.columns:
            name_in_ind = df.loc[df["id"] == uuid]
            if len(name_in_ind) > 0:
                return name_in_ind.iloc[0]["full_name"]
    return None


def create_network_nodes(df: pd.DataFrame) -> nx.MultiDiGraph:
    """Takes in a dataframe and generates a MultiDiGraph where the nodes are
    entity names, and the rest of the dataframe columns make the node attributes

    Args:
        df: a pandas dataframe (complete_individuals_table /
        complete_organizations_table)

    Returns:
        A Networkx MultiDiGraph with nodes lacking any edges
    """
    G = nx.MultiDiGraph()
    # first check if df is individuals or organizations dataset
    if "name" in df.columns:
        node_name = "name"
    else:
        node_name = "full_name"

    transact_info = [
        "office_sought",
        "purpose",
        "transaction_type",
        "recipient_id",
        "transaction_id",
        "recipient_type",
        "donor_office",
        "recipient_name",
        "amount",
    ]

    for _, row in df.iterrows():
        # add node attributes based on the columns relevant to the entity
        G.add_node(row[node_name])
        for column in df.columns.difference(transact_info):
            if not pd.isnull(row[column]):
                G.nodes[row[node_name]][column] = row[column]

        # link the donor node to the recipient node. add the attributes of the
        # edge based on relevant nodes
        for column in transact_info:
            if not pd.isnull(row[column]):
                G.add_edge(
                    row[node_name], row["recipient_name"], column=row[column]
                )

    return G
