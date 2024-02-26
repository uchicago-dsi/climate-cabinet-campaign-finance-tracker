import networkx as nx
import pandas as pd

from utils.linkage import deduplicate_perfect_matches


def deduplicate_datasets(
    ind_df: pd.DataFrame, org_df: pd.DataFrame, transactions_df: pd.DataFrame
) -> tuple:
    """Deduplicates the uuids in the inds and orgs dfs and updates the uuids in
    transactions dataset to match those in the new inds and orgs dfs

    Args:
        ind_df: A pandas df with individual information
        org_df: A pandas df with organization information
        transactions df: A pandas df with info on transactions between entities

    Returns:
        A tuple of the ind_df, org_df, and transactions_df
    """
    # apply dedup to both inds and orgs
    inds_df = deduplicate_perfect_matches(ind_df)
    orgs_df = deduplicate_perfect_matches(org_df)

    # update the deduplicated uuids in transaction donor and recipient columns
    # to the uuids they are mapped to
    deduped = pd.read_csv("../output/deduplicated_UUIDs.csv")
    transactions_df[["donor_id", "recipient_id"]] = transactions_df[
        ["donor_id", "recipient_id"]
    ].replace(deduped)

    return inds_df, orgs_df, transactions_df


def name_identifier(uuid: str, orgs_df, inds_df) -> str:
    """Returns the name of the entity given the entity's uuid

    Args:
        uuid: the uuid of the entity
        orgs_df and inds_df: the dataframes from which the entities uuid
        is queried

    Return:
        The entity's name
    """
    # first, check orgs df:
    name_in_org = orgs_df.loc[orgs_df["id"] == uuid]
    if len(name_in_org) > 0:
        return name_in_org.iloc[0]["name"]
    # theoretically it must be in inds if not in orgs, but for the sample data
    # this might not be the case
    name_in_ind = inds_df.loc[inds_df["id"] == uuid]
    if len(name_in_ind) > 0:
        return name_in_ind.iloc[0]["full_name"]
    else:
        return None


def network_prep_pipeline(
    ind_df: pd.DataFrame, org_df: pd.DataFrame, transactions_df: pd.DataFrame
) -> tuple:
    """Pipeline for preparing the orgs, inds, and transactions dataframes for
    network linkage

    Args:
        ind_df, org_df, transactions_df: pandas dataframes with information
        regarding campaign contributions between donors and recipients

    Returns:
        a tuple containing the 3 dataframes ready for network building
    """

    ind_df, org_df, transactions_df = deduplicate_datasets(
        ind_df, org_df, transactions_df
    )

    # add recipient_name to the transactions dataset
    transactions_df["recipient_name"] = transactions_df["recipient_id"].apply(
        name_identifier, args=(org_df, ind_df)
    )
    return ind_df, org_df, transactions_df


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
