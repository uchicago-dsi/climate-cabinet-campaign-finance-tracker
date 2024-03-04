import networkx as nx
import pandas as pd
import plotly.graph_objects as go


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


def create_network_graph(df: pd.DataFrame) -> nx.MultiDiGraph:
    """Takes in a dataframe and generates a MultiDiGraph where the nodes are
    entity names, and the rest of the dataframe columns make the node attributes

    Args:
        df: a pandas dataframe (complete_individuals_table /
        complete_organizations_table)

    Returns:
        A Networkx MultiDiGraph with nodes and edges
    """
    G = nx.MultiDiGraph()
    # first check if df is individuals or organizations dataset
    if "name" in df.columns:
        node_name = "name"
    else:
        node_name = "full_name"

    edge_columns = [
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
        G.add_node(
            row[node_name],
            **row[df.columns.difference(edge_columns)].dropna().to_dict(),
        )
        # add the recipient as a node
        G.nodes[row["recipient_name"]]["classification"] = "neutral"

        # add the edge attributes between two nodes
        edge_attributes = row[edge_columns].dropna().to_dict()
        G.add_edge(row[node_name], row["recipient_name"], **edge_attributes)

    return G


def plot_network_graph(G: nx.MultiDiGraph):
    """Given a networkX Graph, creates a plotly visualization of the nodes and
    edges

    Args:
        A networkX MultiDiGraph with edges including the attribute 'amount'

    Returns: None. Creates a plotly graph
    """
    edge_trace = go.Scatter(
        x=[], y=[], line=dict(color="#888"), hoverinfo="text", mode="lines"
    )
    hovertext = []

    for edge in G.edges(data=True):
        # donor = edge[0], recipient = edge[1]
        hovertext.append(f"Amount: {edge[2]['amount']:.2f}")

    edge_trace["hovertext"] = hovertext

    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        mode="markers",
        hoverinfo="text",
        marker=dict(showscale=True, colorscale="YlGnBu", size=10),
    )

    for node in G.nodes():
        node_info = f"Name: {node}<br>"
        for key, value in G.nodes[node].items():
            node_info += f"{key}: {value}<br>"
        node_trace["text"] += tuple([node_info])

    # Define layout settings
    layout = go.Layout(
        title="Network Graph Indicating Campaign Contributions from 2018-2022",
        titlefont=dict(size=16),
        showlegend=False,
        hovermode="closest",
        margin=dict(b=20, l=5, r=5, t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    )

    fig = go.Figure(data=[edge_trace, node_trace], layout=layout)
    fig.show()
