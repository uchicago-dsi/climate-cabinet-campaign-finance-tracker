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


def combine_datasets_for_network_graph(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines the 3 dataframes into a single dataframe to create the graph

    Given the inds, orgs, and transactions dataframes, the func first finds the
    recipient_id in the transaction dataframe in either the org or inds
    dataframes and adds the name of the recipient to the transaction df. Then,
    the inds and orgs dfs are merged with the transaction df and concatenated
    with the contributions amount aggregated, making a final dataframe of the
    merged transactions and entity dataframes.

    Args:
        list of dataframes in the order: [inds_df, orgs_df, transactions_df]
        Transactions dataframe with at least column: 'recipient_id'
        Individuals dataframe with at least column: 'full_name'
        Organizations dataframe with at least column: 'name'

    Returns
        A merged dataframe with aggregate contribution amounts between entitites
    """

    inds_df, orgs_df, transactions_df = dfs

    # first update the transactions df to have a recipient name tied to id
    transactions_df["recipient_name"] = transactions_df["recipient_id"].apply(
        name_identifier, args=([orgs_df, inds_df],)
    )

    # next, merge the inds_df and orgs_df with the transactions_df
    inds_trans_df = pd.merge(
        inds_df, transactions_df, how="left", left_on="id", right_on="donor_id"
    )
    inds_trans_df = inds_trans_df.dropna(subset=["amount"])
    orgs_trans_df = pd.merge(
        orgs_df, transactions_df, how="left", left_on="id", right_on="donor_id"
    )
    orgs_trans_df = orgs_trans_df.dropna(subset=["amount"])
    orgs_trans_df = orgs_trans_df.rename(columns={"name": "full_name"})

    # concatenated the merged dfs
    merged_df = pd.concat([orgs_trans_df, inds_trans_df])

    # lastly, create the final dataframe with aggregated attributes
    attribute_cols = merged_df.columns.difference(
        ["donor_id", "recipient_id", "full_name", "recipient_name"]
    )
    agg_functions = {
        col: "sum" if col == "amount" else "first" for col in attribute_cols
    }
    aggreg_df = (
        merged_df.groupby(
            ["donor_id", "recipient_id", "full_name", "recipient_name"]
        )
        .agg(agg_functions)
        .reset_index()
    )
    aggreg_df = aggreg_df.drop(["id"], axis=1)
    return aggreg_df


def create_network_graph(df: pd.DataFrame) -> nx.MultiDiGraph:
    """Takes in a dataframe and generates a MultiDiGraph where the nodes are
    entity names, and the rest of the dataframe columns make the node attributes

    Args:
        df: a pandas dataframe with merged information from the inds, orgs, &
        transactions dataframes

    Returns:
        A Networkx MultiDiGraph with nodes and edges
    """
    G = nx.MultiDiGraph()
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
            row["full_name"],
            **row[df.columns.difference(edge_columns)].dropna().to_dict(),
        )
        # add the recipient as a node
        G.nodes[row["recipient_name"]]["classification"] = "neutral"

        # add the edge attributes between two nodes
        edge_attributes = row[edge_columns].dropna().to_dict()
        G.add_edge(row["full_name"], row["recipient_name"], **edge_attributes)

    return G


def plot_network_graph(G: nx.MultiDiGraph):
    """Given a networkX Graph, creates a plotly visualization of the nodes and
    edges

    Args:
        A networkX MultiDiGraph with edges including the attribute 'amount'

    Returns: None. Creates a plotly graph
    """
    edge_trace = go.Scatter(
        x=(),
        y=(),
        line=dict(color="#888", width=1.5),
        hoverinfo="text",
        mode="lines+markers",
    )
    hovertext = []
    pos = nx.spring_layout(G)

    for edge in G.edges(data=True):
        source = edge[0]
        target = edge[1]
        hovertext.append(f"Amount: {edge[2]['amount']:.2f}")
        # Adding coordinates of source and target nodes to edge_trace
        edge_trace["x"] += (
            pos[source][0],
            pos[target][0],
            None,
        )  # None creates a gap between line segments
        edge_trace["y"] += (pos[source][1], pos[target][1], None)

    edge_trace["hovertext"] = hovertext

    # Define arrow symbol for edges
    edge_trace["marker"] = dict(
        symbol="arrow", color="#888", size=10, angleref="previous"
    )

    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        mode="markers",
        hoverinfo="text",
        marker=dict(showscale=True, colorscale="YlGnBu", size=10),
    )
    node_trace["marker"]["color"] = []

    for node in G.nodes():
        node_info = f"Name: {node}<br>"
        for key, value in G.nodes[node].items():
            node_info += f"{key}: {value}<br>"
        node_trace["text"] += tuple([node_info])
        # Get the classification value for the node
        classification = G.nodes[node].get("classification", "neutral")
        # Assign a color based on the classification value
        if classification == "c":
            color = "blue"
        elif classification == "f":
            color = "red"
        else:
            color = "green"  # Default color for unknown classification
        node_trace["marker"]["color"] += tuple([color])

        # Add node positions to the trace
        node_trace["x"] += tuple([pos[node][0]])
        node_trace["y"] += tuple([pos[node][1]])

    # Define layout settings
    layout = go.Layout(
        title="Network Graph Indicating Campaign Contributions from 2018-2022",
        titlefont=dict(size=16),
        showlegend=True,
        hovermode="closest",
        margin=dict(b=20, l=5, r=5, t=40),
        xaxis=dict(showgrid=True, zeroline=True, showticklabels=False),
        yaxis=dict(showgrid=True, zeroline=True, showticklabels=False),
    )

    fig = go.Figure(data=[edge_trace, node_trace], layout=layout)
    fig.show()
