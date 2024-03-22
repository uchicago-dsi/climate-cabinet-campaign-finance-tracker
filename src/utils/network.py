"""Buidling, visualizing, and analyzing networks"""

import itertools
from pathlib import Path

import networkx as nx
import pandas as pd
import plotly.graph_objects as go


def name_identifier(uuid: str, dfs: list[pd.DataFrame]) -> str:
    """Returns the name of the entity given the entity's uuid

    Args:
        uuid: the uuid of the entity
        dfs: dataframes that have a uuid column, and an 'name' or
            'full_name' column
    Return:
        The entity's name
    """
    for df in dfs:
        if "name" in df.columns:
            name_in_org = df.loc[df["id"] == uuid]
            if len(name_in_org) > 0:
                return name_in_org.iloc[0]["name"]

        if "full_name" in df.columns:
            name_in_ind = df.loc[df["id"] == uuid]
            if len(name_in_ind) > 0:
                return name_in_ind.iloc[0]["full_name"]
    return None


def combine_datasets_for_network_graph(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines the 3 dataframes into a single dataframe to create a graph

    Given 3 dataframes, the func adds a 'recipient_name' column in the
    transactions df, merges the dfs together to record transaction info between
    entities, then concatenates the dfs into a final df of the merged
    transactions and entity dfs.

    Args:
        dfs: list of dataframes in the order: [inds_df, orgs_df, transactions_df]
        Transactions dataframe with column: 'recipient_id'
        Individuals dataframe with column: 'full_name'
        Organizations dataframe with column: 'name'

    Returns:
        A merged dataframe with aggregate contribution amounts between entitites
    """
    inds_df, orgs_df, transactions_df = dfs

    # first update the transactions df to have a recipient name tied to id
    transactions_df["recipient_name"] = transactions_df["recipient_id"].apply(
        name_identifier, args=([orgs_df, inds_df],)
    )

    # next, merge the inds_df and orgs_df ids with the transactions_df donor_id
    inds_trans_df = inds_df.merge(
        transactions_df, how="left", left_on="id", right_on="donor_id"
    )
    inds_trans_df = inds_trans_df.dropna(subset=["amount"])
    orgs_trans_df = orgs_df.merge(
        transactions_df, how="left", left_on="id", right_on="donor_id"
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
        merged_df.groupby(["donor_id", "recipient_id", "full_name", "recipient_name"])
        .agg(agg_functions)
        .reset_index()
    )
    aggreg_df = aggreg_df.drop(["id"], axis=1)
    return aggreg_df


def create_network_graph(df: pd.DataFrame) -> nx.MultiDiGraph:
    """Creates network with entities as nodes, transactions as edges

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
        G.add_node(row["recipient_name"], classification="neutral")

        # add the edge attributes between two nodes
        edge_attributes = row[edge_columns].dropna().to_dict()
        G.add_edge(row["full_name"], row["recipient_name"], **edge_attributes)

    return G


def plot_network_graph(G: nx.MultiDiGraph) -> None:
    """Creates a plotly visualization of the nodes and edges

    Args:
        G: A networkX MultiDiGraph with edges including the attribute 'amount'

    Returns: None. Creates a plotly graph
    """
    edge_trace = go.Scatter(
        x=(),
        y=(),
        line={"color": "#888", "width": 1.5},
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
    edge_trace["marker"] = {
        "symbol": "arrow",
        "color": "#888",
        "size": 10,
        "angleref": "previous",
    }

    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        mode="markers",
        hoverinfo="text",
        marker={"showscale": True, "colorscale": "YlGnBu", "size": 10},
    )
    node_trace["marker"]["color"] = []

    for node in G.nodes():
        node_info = f"Name: {node}<br>"
        for key, value in G.nodes[node].items():
            node_info += f"{key}: {value}<br>"
        node_trace["text"] += ([node_info],)
        # Get the classification value for the node
        classification = G.nodes[node].get("classification", "neutral")
        # Assign a color based on the classification value
        if classification == "c":
            color = "blue"
        elif classification == "f":
            color = "red"
        else:
            color = "green"  # Default color for unknown classification
        node_trace["marker"]["color"] += ([color],)

        # Add node positions to the trace
        node_trace["x"] += ([pos[node][0]],)
        node_trace["y"] += ([pos[node][1]],)

    # Define layout settings
    layout = go.Layout(
        title="Network Graph Indicating Campaign Contributions from 2018-2022",
        titlefont={"size": 16},
        showlegend=True,
        hovermode="closest",
        margin={"b": 20, "l": 5, "r": 5, "t": 40},
        xaxis={"showgrid": True, "zeroline": True, "showticklabels": False},
        yaxis={"showgrid": True, "zeroline": True, "showticklabels": False},
    )

    fig = go.Figure(data=[edge_trace, node_trace], layout=layout)
    fig.show()


def network_metrics(net_graph: nx.Graph) -> None:
    """Output network metrics to txt files

    Args:
        net_graph: network graph as defined by networkx

    Returns:
        a text file with list of nodes with greatest calculated
        centrality for each metric: in degree, out degree,
        eigenvector, and betweenness
    """
    in_degree = nx.in_degree_centrality(
        net_graph
    )  # calculates in degree centrality of nodes
    out_degree = nx.out_degree_centrality(
        net_graph
    )  # calculated out degree centrality of nodes
    eigenvector = nx.eigenvector_centrality_numpy(
        net_graph, weight="amount"
    )  # calculates eigenvector centrality of nodes
    betweenness = nx.betweenness_centrality(
        net_graph, weight="amount"
    )  # calculates betweenness centrality of nodes

    # sort + truncate dictionaries to 50 nodes with greatest centrality
    in_degree = sorted(in_degree.items(), key=lambda x: x[1], reverse=True)[:50]
    out_degree = sorted(out_degree.items(), key=lambda x: x[1], reverse=True)[:50]
    eigenvector = sorted(eigenvector.items(), key=lambda x: x[1], reverse=True)[:50]
    betweenness = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:50]

    assortativity = nx.attribute_assortativity_coefficient(
        net_graph, "classification"
    )  # calculates assortativity of graph

    num_nodes = len(net_graph.nodes())
    num_edges = len(net_graph.edges())
    density = num_edges / (num_nodes * (num_nodes - 1))  # calculates density of graph

    k = 5
    comp = nx.community.girvan_newman(net_graph)
    for communities in itertools.islice(comp, k):
        communities = tuple(
            sorted(c) for c in communities
        )  # creates clusters of nodes with high interactions where granularity = 5
    communities = sorted(communities, key=len, reverse=True)

    with Path("output/network_metrics.txt").open("w") as file:
        file.write(f"in degree centrality: {in_degree}\n")
        file.write(f"out degree centrality: {out_degree}\n")
        file.write(f"eigenvector centrality: {eigenvector}\n")
        file.write(f"betweenness centrality: {betweenness}\n\n")

        file.write(f"assortativity based on 'classification': {assortativity}\n\n")

        file.write(f"density': {density}\n\n")

        file.write(f"communities where k = 5': {communities}\n\n")


def construct_network_graph(
    start_year: int, end_year: int, dfs: list[pd.DataFrame]
) -> None:
    """Runs the network construction pipeline starting from 3 dataframes

    Args:
        start_year: the end range of the desired data
        end_year: the end range of the desired data
        dfs: dataframes in the order: inds_df, orgs_df, transactions_df

    Returns:
        None; however, will create 2 txt files
    """
    inds_df, orgs_df, transactions_df = dfs
    transactions_df = transactions_df.loc[
        (transactions_df.year >= start_year) & (transactions_df.year <= end_year)
    ]

    aggreg_df = combine_datasets_for_network_graph([inds_df, orgs_df, transactions_df])
    G = create_network_graph(aggreg_df)
    network_metrics(G)
    plot_network_graph(G)
    plot_network_graph(G)
