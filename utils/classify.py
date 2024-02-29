import pandas as pd

from utils.constants import c_org_names, f_companies, f_org_names


def classify_wrapper(individuals_df, organizations_df):
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


def matcher(df, substring, column, category):
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


def classify_individuals(individuals_df):
    """Part of the classification pipeline

    We apply the matcher function to the individuals dataframe
    repeatedly, using a variety of substrings to identify the
    employees of fossil fuel companies.
    """

    for i in f_companies:
        individuals_df = matcher(individuals_df, i, "company", "f")

    return individuals_df


def classify_orgs(organizations_df):
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


inds_list = []

# a list of individual names


def similarity_calculator(
    df: pd.DataFrame, subject: str, n: int, comparison_func
) -> pd.DataFrame:
    """Find best matches to a subject name in a pandas dataframe

    For a given individual or organization, the subject, we search through the
    'name'column of a dataframe, select the n highest matches according to a
    selected comparison function, and return those as a dataframe. This is meant
    to be used manually to search for matches. For quick automated processing, see
    automated_classifier().

    Note that the comparison function must take in two inputs, both strings, and
    output a percentage match
    """

    similarities_df = df.copy()

    similarities = similarities_df["name"].apply(
        lambda x: comparison_func(x, subject)
    )

    similarities_df["similarities"] = similarities

    top_n_matches = similarities_df.sort_values(
        by=["similarities"], ascending=False
    )[0:n]

    return top_n_matches


def automated_classifier(
    df: pd.DataFrame, subjects_dict: dict, threshold: float, comparison_func
):
    """Using similarity_calculator, classify entities automatically

    Feeding a dictionary of names and the associated statuses, we compare
    the string matches and, if they exceed a certain threshold, classify
    them as belonging to some group specified in the subjects dictionary.
    """

    similarities_df = df.copy()

    for subject in subjects_dict:
        similarities = similarities_df["name"].apply(
            lambda x, sub=subject: comparison_func(x, sub)
        )
        matches = similarities >= threshold

        status = subjects_dict[subject]

        similarities_df["classification"] = pd.Series(matches).apply(
            lambda x, stat=status: stat if x else "neutral"
        )

    return similarities_df

    # we can use the indices and/or select manually, just add a new
    # column to the subjects table
    # that marks fossil fuels, green energy, or neither
