import numpy as np
import pandas as pd
import pytest

from utils.linkage import (
    calculate_row_similarity,
    calculate_string_similarity,
    row_matches,
)

# import pytest


# creating a test for calculate_row_similarity and row_matches

# to put in data:
d = {
    "name": ["bob von rosevich", "anantarya smith", "bob j vonrosevich"],
    "address": [
        "3 Blue Drive, Chicago",
        "4 Blue Drive, Chicago",
        "8 Fancy Way, Chicago",
    ],
}
test_df = pd.DataFrame(data=d)


@pytest.fixture
def row_similarity_scen_1():
    return test_df

@pytest.fixture
def row_similarity_scen_2():
    return test_df


def test_row_similarity_scen_1(row_similarity_scen_1):
    wrong = calculate_row_similarity(
        row_similarity_scen_1.iloc[[0]],
        row_similarity_scen_1.iloc[[1]],
        np.array([0.8, 0.2]),
        calculate_string_similarity,
    )
    right = calculate_row_similarity(
        row_similarity_scen_1.iloc[[0]],
        row_similarity_scen_1.iloc[[2]],
        np.array([0.8, 0.2]),
        calculate_string_similarity,
    )

    assert right > wrong


def test_row_similarity_scen_2(row_similarity_scen_2):
    wrong = calculate_row_similarity(
        row_similarity_scen_2.iloc[[0]],
        row_similarity_scen_2.iloc[[1]],
        np.array([0.2, 0.8]),
        calculate_string_similarity,
    )
    right = calculate_row_similarity(
        row_similarity_scen_2.iloc[[0]],
        row_similarity_scen_2.iloc[[2]],
        np.array([0.2, 0.8]),
        calculate_string_similarity,
    )

    assert right < wrong


d2 = {
    "name": [
        "bob von rosevich",
        "anantarya smith",
        "bob j vonrosevich",
        "missy elliot",
        "mr johnson",
        "quarantin directino",
        "missy eliot",
        "joseph johnson",
    ],
    "address": [
        "3 Blue Drive, Chicago",
        "4 Blue Drive, Chicago",
        "8 Fancy Way, Chicago",
        "8 Fancy Way, Evanston",
        "17 Regular Road, Chicago",
        "42 Hollywood Boulevard, Chicago",
        "8 Fancy Way, Evanston",
        "17 Regular Road, Chicago",
    ],
}
test_df2 = pd.DataFrame(data=d2)

@pytest.fixture
def row_match_scen1():
    return test_df2


def test_row_matches(row_match_scen1):
    res = row_matches(
        test_df, np.array([0.8, 0.2]), 0.9, calculate_string_similarity
    )

    assert res == {0: [2], 1: [], 2: [], 3: [6], 4: [], 5: [], 6: [], 7: []}
