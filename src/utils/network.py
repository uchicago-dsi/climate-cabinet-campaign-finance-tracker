"""Buidling, visualizing, and analyzing networks"""

import itertools
from pathlib import Path

import networkx as nx
import pandas as pd
import plotly.graph_objects as go  # TODO: #100 explore other libraries


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

    inds_trans_df = inds_trans_df.loc[:, ~inds_trans_df.columns.duplicated()].copy()
    orgs_trans_df = orgs_trans_df.loc[:, ~orgs_trans_df.columns.duplicated()].copy()
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


# RETAINED
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


def plot_network_graph(G: nx.MultiDiGraph, start_year: int, end_year: int) -> None:
    """Creates a plotly visualization of the nodes and edges with arrows indicating direction, and colors indicating classification."""
    pos = nx.spring_layout(
        G
    )  # position nodes using the spring layout - retained from original code

    edge_x = []
    edge_y = []
    annotations = []

    # looping through edges to add lines and annotations for arrows
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

        # adding arrows with annotations for each line segment
        annotations.append(
            dict(
                ax=x0,  # arrow tail location (x)
                ay=y0,  # arrow tail location (y)
                axref="x",
                ayref="y",
                x=x1,  # arrow head location (x)
                y=y1,  # arrow head location (y)
                xref="x",
                yref="y",
                showarrow=True,
                arrowhead=3,  # size of the arrow head
                arrowsize=1,
                arrowwidth=2,
                arrowcolor="#636363",
            )
        )

    # nodes
    node_x = []
    node_y = []
    node_text = []
    node_color = []  # list for storing colors based on classification

    for node in G.nodes():
        node_x.append(pos[node][0])
        node_y.append(pos[node][1])
        node_text.append(node)
        # set color based on classification, default to 'lightgrey' if not specified
        classification = G.nodes[node].get("classification", "neutral")
        if classification == "c":
            node_color.append("blue")
        elif classification == "f":
            node_color.append("red")
        else:
            node_color.append("green")  # default color for 'neutral' and others

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers",
        hoverinfo="text",
        text=node_text,
        marker=dict(
            showscale=True,
            colorscale="Viridis",  # experimenting w colors here
            size=10,
            color=node_color,
            colorbar=dict(title="Classification"),
            line_width=2,
        ),
    )

    # setting up figure
    fig = go.Figure(
        data=[
            go.Scatter(
                x=edge_x, y=edge_y, mode="lines", line=dict(color="#888", width=2)
            )
        ],
        layout=go.Layout(
            title=f"Network Graph Indicating Campaign Contributions from {start_year} to {end_year}",
            titlefont_size=16,
            showlegend=False,
            hovermode="closest",
            margin={"b": 20, "l": 5, "r": 5, "t": 40},
            annotations=annotations,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        ),
    )

    # adding node trace separately to have nodes appear above the lines
    fig.add_trace(node_trace)

    # saving and show figure
    graphs_directory = Path("output/network_graphs")
    graphs_directory.mkdir(parents=True, exist_ok=True)
    filename = graphs_directory / f"network_graph_{start_year}_to_{end_year}.html"
    fig.write_html(str(filename))
    print(f"Graph saved to {filename}")
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


def run_network_graph_pipeline(
    start_year: int, end_year: int, dfs: list[pd.DataFrame]
) -> None:
    """Runs the network construction pipeline starting from 3 dataframes

    Args:
        start_year: the start range of the desired data
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
    G = create_network_graph(aggreg_df)  # G: graph
    network_metrics(G)
    additional_network_metrics(G)
    plot_network_graph(G, start_year, end_year)


# added for macro-level viz - Work in Progress
def additional_network_metrics(G: nx.Graph) -> None:
    """Calculate and print additional network metrics

    Args:
        G: network graph created
    Returns:
        some metrics requried for clustering viz
    """
    # switch the MultiDiGraph to DiGraph for computing
    simple_graph = nx.DiGraph(G)
    # considering 'amount' as the weight attribute, computing weighted sums of edges
    for u, v, data in G.edges(data=True):
        if simple_graph.has_edge(u, v):
            simple_graph[u][v]["amount"] += data["amount"]
        else:
            simple_graph.add_edge(u, v, amount=data["amount"])

    # get the clustering coefficient based on this computation
    clustering_coeff = nx.average_clustering(simple_graph)
    print("Average Clustering Coefficient:", clustering_coeff)


# for testing
# individuals = pd.read_csv("output/cleaned/individuals_table.csv")
# organizations = pd.read_csv("output/cleaned/organizations_table.csv")
# transactions = pd.read_csv("output/cleaned/transactions_table.csv")
# run_network_graph_pipeline(2018, 2021, [individuals, organizations, transactions])
