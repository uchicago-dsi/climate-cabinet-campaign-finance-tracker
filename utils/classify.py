import pandas as pd

from utils.constants import c_org_names, f_companies, f_org_names


def classify_wrapper(
    individuals_df: pd.DataFrame, organizations_df: pd.DataFrame
):
    """Wrapper for classificaiton in linkage pipeline

    Initialize the classify column in both dataframes and
    call sub-functions classifying individuals and organizations

    Args: individuals_df: cleaned and deduplicated dataframe of individuals
    organizations_df: cleaned and deduplicated dataframe of organizations

    Returns: individuals and organizations datfarames with a new
    'classification' column containing 'neutral', 'f', or 'c'
    """

    individuals_df["classification"] = "neutral"
    organizations_df["classification"] = "neutral"

    classified_individuals = classify_individuals(individuals_df)
    classified_orgs = classify_orgs(organizations_df)

    return classified_individuals, classified_orgs


def matcher(df: pd.DataFrame, substring: str, column: str, category: str):
    """Applies a label to the classification column based on substrings

    We run through a given column containing strings in the dataframe. We
    seek out rows containing substrings, and apply a certain label to
    the classification column. We initialize using the 'neutral' label and
    use the 'f' and 'c' labels to denote fossil fuel and clean energy
    entities respectively.
    """

    bool_series = df[column].str.contains(substring, na=False)

    df.loc[bool_series, "classification"] = category

    return df


def classify_individuals(individuals_df: pd.DataFrame):
    """Part of the classification pipeline

    We apply the matcher function to the individuals dataframe
    repeatedly, using a variety of substrings to identify the
    employees of fossil fuel companies.
    """

    for i in f_companies:
        individuals_df = matcher(individuals_df, i, "company", "f")

    return individuals_df


def classify_orgs(organizations_df: pd.DataFrame):
    """Part of the classification pipeline

    We apply the matcher function to the organizations dataframe
    repeatedly, using a variety of substrings to identify fossil
    fuel and clean energy companies.
    """

    for i in f_org_names:
        organizations_df = matcher(organizations_df, i, "name", "f")

    for i in c_org_names:
        organizations_df = matcher(organizations_df, i, "name", "c")

    return organizations_df
