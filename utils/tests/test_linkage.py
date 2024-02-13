import json
import pytest
import numpy as np
import pandas as pd



#creating a test for calculate_row_similarity and row_matches


from utils.linkage import(
    calculate_string_similarity, calculate_row_similarity, row_matches,
)


#maybe this will just be a csv for us?
def open_test_data_json(filename: str) -> dict:
    """Open json in tests/data dir into a python dict"""
    with open(test_data_directory / filename, "r") as f:
        return json.load(f)






#to put in data:
d = {'name': ["bob von rosevich", "anantarya smith","bob j vonrosevich"],'address': ["3 Blue Drive, Chicago", "4 Blue Drive, Chicago","8 Fancy Way, Chicago"]}
test_df = pd.DataFrame(data=d)


@pytest.fixture
def row_similarity_scen_1():
    return open_test_data_json(data)

@pytest.fixture
def row_similarity_scen_2():
    return open_test_data_json(data)


def test_row_similarity_scen_1(
    row_similarity_scen_1
):
    result = calculate_row_similarity(
        row_similarity_scen_1
    )
    wrong = calculate_row_similarity(row_similarity_scen_1.iloc[[0]], row_similarity_scen_1.iloc[[1]],np.array([.8, .2]),calculate_string_similarity)
    right = calculate_row_similarity(row_similarity_scen_1.iloc[[0]], row_similarity_scen_1.iloc[[2]],np.array([.8, .2]),calculate_string_similarity)

    assert right > wrong



def test_row_similarity_scen_2(
    row_similarity_scen_2
):
    result = calculate_row_similarity(
        row_similarity_scen_2
    )
    wrong = calculate_row_similarity(row_similarity_scen_2.iloc[[0]], row_similarity_scen_2.iloc[[1]],np.array([.2, .8]),calculate_string_similarity)
    right = calculate_row_similarity(row_similarity_scen_2.iloc[[0]], row_similarity_scen_2.iloc[[2]],np.array([.2, .8]),calculate_string_similarity)

    assert right < wrong



#how about for row matches? First pull into a notebook and see how a test should work
    




def test_create_record_from_labelstudio_results(
    labelstudio_simple_results_filtered,
):
    result = create_record_from_labelstudio_results(
        labelstudio_simple_results_filtered
    )
    expected = {
        "borrower": "Wisconsin Public Service Corporation",
        "borrower_start_idx": 23,
        "borrower_end_idx": 59,
        "interest_rate": "5.35%",
        "interest_rate_start_idx": 220,
        "interest_rate_end_idx": 225,
        "name": "Senior\\n\\nNotes",
        "name_start_idx": 205,
        "name_end_idx": 218,
        "end_date": "November 10, 2025",
        "end_date_start_idx": 237,
        "end_date_end_idx": 254,
        "type": "Bond",
        "governed_by": "eVtWJ7O08t",
    }
    assert result == expected




#calculate

def calculate_row_similarity(
    row1: pd.DataFrame, row2: pd.DataFrame, weights: np.array, comparison_func
) -> float:
    """Find weighted similarity of two rows in a dataframe

    The length of the weights vector must be the same as
    the number of selected columns.

    This version is slow and not optimized, and will be
    revised in order to make it more efficient. It
    exists as to provide basic functionality. Once we have
    the comparison function locked in, using .apply will
    likely be easier and more efficient.

    >>> d = {
    ...     'name': ["bob von rosevich", "anantarya smith","bob j vonrosevich"],
    ...     'address': ["3 Blue Drive, Chicago", "4 Blue Drive, Chicago",
    ...                 "8 Fancy Way, Chicago"]
    ... }
    >>> df = pd.DataFrame(data=d)
    >>> wrong = calculate_row_similarity(df.iloc[[0]], df.iloc[[1]],
    ...                                    np.array([.8, .2]),
    ...                                    calculate_string_similarity)
    >>> right = calculate_row_similarity(df.iloc[[0]], df.iloc[[2]],
    ...                                    np.array([.8, .2]),
    ...                                    calculate_string_similarity)
    >>> right > wrong
    True
    >>> wrong = calculate_row_similarity(df.iloc[[0]], df.iloc[[1]],
    ...                                    np.array([.2, .8]),
    ...                                    calculate_string_similarity)
    >>> right = calculate_row_similarity(df.iloc[[0]], df.iloc[[2]],
    ...                                    np.array([.2, .8]),
    ...                                    calculate_string_similarity)
    >>> right > wrong
    False
    """

    row_length = len(weights)
    if not (row1.shape[1] == row2.shape[1] == row_length):
        raise ValueError("Number of columns and weights must be the same")

    similarity = np.zeros(row_length)

    for i in range(row_length):
        similarity[i] = comparison_func(
            row1.reset_index().drop(columns="index").iloc[:, i][0],
            row2.reset_index().drop(columns="index").iloc[:, i][0],
        )

    return sum(similarity * weights)










# from banktrack.annotation.convert import (
#     create_record_from_labelstudio_results,
#     get_unique_entity_ids_from_labelstudio_results,
# )
# from banktrack.pipeline.constants import ROOT_DIR

test_data_directory = ROOT_DIR / "tests" / "data"


def open_test_data_json(filename: str) -> dict:
    """Open json in tests/data dir into a python dict"""
    with open(test_data_directory / filename, "r") as f:
        return json.load(f)


@pytest.fixture
def labelstudio_simple_results():
    return open_test_data_json("simple_labelstudio_results.json")


@pytest.fixture
def labelstudio_simple_results_filtered():
    return open_test_data_json("simple_labelstudio_results_filtered.json")


def test_create_record_from_labelstudio_results(
    labelstudio_simple_results_filtered,
):
    result = create_record_from_labelstudio_results(
        labelstudio_simple_results_filtered
    )
    expected = {
        "borrower": "Wisconsin Public Service Corporation",
        "borrower_start_idx": 23,
        "borrower_end_idx": 59,
        "interest_rate": "5.35%",
        "interest_rate_start_idx": 220,
        "interest_rate_end_idx": 225,
        "name": "Senior\\n\\nNotes",
        "name_start_idx": 205,
        "name_end_idx": 218,
        "end_date": "November 10, 2025",
        "end_date_start_idx": 237,
        "end_date_end_idx": 254,
        "type": "Bond",
        "governed_by": "eVtWJ7O08t",
    }
    assert result == expected


def test_get_unique_entity_ids_from_labelstudio_results(
    labelstudio_simple_results,
):
    result = get_unique_entity_ids_from_labelstudio_results(
        labelstudio_simple_results
    )
    expected = ["eVtWJ7O08t", "b9izoYopAS"]
    assert result == expected