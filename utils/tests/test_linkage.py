import numpy as np
import pandas as pd
import pytest
from utils.constants import BASE_FILEPATH
from utils.linkage import (
    calculate_row_similarity,
    calculate_string_similarity,
    row_matches, deduplicate_perfect_matches
)

"""
Module for testing functions in linkage.py
"""

# Creating a test for calculate_row_similarity and row_matches

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
        row_match_scen1, np.array([0.8, 0.2]), 0.9, calculate_string_similarity
    )

    assert res == {0: [2], 1: [], 2: [], 3: [6], 4: [], 5: [], 6: [], 7: []}

# Test for dedupe function
@pytest.fixture
def return_data(filename):
    path = BASE_FILEPATH / "output" / filename
    df = pd.read_csv(path, low_memory=False)
    return df


@pytest.fixture
def call_dedup_func():
    inds_sample = return_data("complete_individuals_table.csv")
    orgs_sample = return_data("complete_organizations_table.csv")

    assert not orgs_sample.empty()
    assert not inds_sample.empty()

    deduplicated_inds = deduplicate_perfect_matches(inds_sample)
    deduplicated_orgs = deduplicate_perfect_matches(orgs_sample)

    output_dedup_ids = return_data("deduplicated_UUIDs.csv")
    # outpud_ids should have all the ids that deduplicated_inds and deduplicated_orgs
    # has

    return deduplicated_inds, deduplicated_orgs, output_dedup_ids


@pytest.fixture
def confirm_dedup_uuids():
    inds, orgs, output = call_dedup_func()

    dedup_inds_id = set(inds.id.tolist())
    dedup_orgs_id = set(orgs.id.tolist())
    unique_ids = set(output.duplicated_uuids.tolist())

    assert dedup_inds_id.issubset(unique_ids)
    assert dedup_orgs_id.issubset(unique_ids)