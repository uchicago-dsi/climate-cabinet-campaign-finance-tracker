import json

import numpy as np
import pandas as pd
import pytest

from utils.linkage import calculate_row_similarity, calculate_string_similarity

# creating a test for calculate_row_similarity and row_matches


# maybe this will just be a csv for us?
def open_test_data_json(filename: str) -> dict:
    """Open json in tests/data dir into a python dict"""
    with open(test_data_directory / filename, "r") as f:
        return json.load(f)


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


# @pytest.fixture
# def row_similarity_scen_1():
#     return open_test_data_json(data)

# @pytest.fixture
# def row_similarity_scen_2():
#     return open_test_data_json(data)


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
    result = calculate_row_similarity(row_similarity_scen_2)
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
