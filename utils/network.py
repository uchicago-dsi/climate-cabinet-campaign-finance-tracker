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
        "year",
        "transaction_id",
        "donor_office",
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
        edge_dictionary = {}
        for column in transact_info:
            if not pd.isnull(row[column]):
                edge_dictionary[column] = row[column]
        G.add_edge(row[node_name], row["recipient_name"], **edge_dictionary)

        # the added 'recipient_name' node has no attributes at this moment
        # for the final code this line won't be necessary, as each recipient
        # should ideally be referenced later on. For now, all added nodes for
        # the recipient will only have one default attribute: classification
        G.nodes[row["recipient_name"]]["classification"] = "neutral"

    edge_labels = {(u, v): d["amount"] for u, v, d in G.edges(data=True)}
    entity_colors = {"neutral": "green", "c": "blue", "f": "red"}
    node_colors = [
        entity_colors[G.nodes[node]["classification"]] for node in G.nodes()
    ]

    nx.draw_planar(G, with_labels=False, node_color=node_colors)
    nx.draw_networkx_edge_labels(
        G, pos=nx.spring_layout(G), edge_labels=edge_labels, label_pos=0.5
    )

    # nx.draw_planar(G, with_labels=False)
    return G
