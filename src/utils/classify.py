"""Code for classifying entities as fossil fuel, clean energy, or neither"""

import pandas as pd

from utils.constants import C_ORG_NAMES, F_COMPANIES, F_ORG_NAMES


def classify_wrapper(
    individuals_df: pd.DataFrame, organizations_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Wrapper for classification in linkage pipeline

    Initialize the classify column in both dataframes and
    call sub-functions classifying individuals and organizations

    Args:
        individuals_df: cleaned and deduplicated dataframe of individuals
        organizations_df: cleaned and deduplicated dataframe of organizations

    Returns:
        individuals and organizations datfarames with a new
        'classification' column containing 'neutral', 'f', or 'c'.
        'neutral' status is the default for all entities, and those tagged
        as 'neutral' are entities which we could not confidently identify as
        either fossil fuel or clean energy organizations or affiliates.
        Classification is very conservative, and we are very confident that
        entities classified as one group or another are related to them.

    """
    individuals_df["classification"] = "neutral"
    organizations_df["classification"] = "neutral"

    classified_individuals = classify_individuals(individuals_df)
    classified_orgs = classify_orgs(organizations_df)

    return classified_individuals, classified_orgs


def apply_classification_label(
    df: pd.DataFrame, substring: str, column: str, category: str
) -> pd.DataFrame:
    """Applies a label to the classification column based on substrings

    We run through a given column containing strings in the dataframe. We
    seek out rows containing substrings, and apply a certain label to
    the classification column. We initialize using the 'neutral' label and
    use the 'f' and 'c' labels to denote fossil fuel and clean energy
    entities respectively.

    Args:
        df: a pandas dataframe
        substring: the string to search for
        column: the column name in which to search
        category: the category to assign the row, such as 'f' 'c' or 'neutral'

    Returns:
        A pandas dataframe in which rows matching the substring conditions in
        a certain column are marked with the appropriate category
    """
    matches_target_string_bool_series = df[column].str.contains(substring, na=False)

    df.loc[matches_target_string_bool_series, "classification"] = category

    return df


def classify_individuals(individuals_df: pd.DataFrame) -> pd.DataFrame:
    """Part of the classification pipeline

    We check if individuals work for a known fossil fuel company
    and categorize them using the apply_classification_label() function.

    Args:
        individuals_df: a dataframe containing deduplicated
        standardized individuals data

    Returns:
        an individuals dataframe updated with the fossil fuels category
    """
    for company in F_COMPANIES:
        individuals_df = apply_classification_label(
            individuals_df, company, "company", "f"
        )

    return individuals_df


def classify_orgs(organizations_df: pd.DataFrame) -> pd.DataFrame:
    """Part of the classification pipeline

    We apply the apply_classification_label() function to the organizations dataframe
    repeatedly, using a variety of substrings to identify fossil
    fuel and clean energy companies.

    Args:
        organizations_df: a dataframe containing deduplicated
        standardized organizations data

    Returns:
        an organizations dataframe updated with the fossil fuels
        and clean energy category
    """
    for org in F_ORG_NAMES:
        organizations_df = apply_classification_label(
            organizations_df, org, "name", "f"
        )

    for org in C_ORG_NAMES:
        organizations_df = apply_classification_label(
            organizations_df, org, "name", "c"
        )

    return organizations_df
