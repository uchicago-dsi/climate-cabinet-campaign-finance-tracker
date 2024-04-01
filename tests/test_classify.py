"""Tests for classify.py"""

import numpy as np
import pandas as pd
import pytest
from utils.classify import apply_classification_label

d = {
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

test_df = pd.DataFrame(data=d)

test_df["classification"] = "neutral"


@pytest.fixture
def matcher_scen_1():
    return test_df


def test_matcher_scen_1(matcher_scen_1):
    apply_classification_label(matcher_scen_1, "Fancy", "address", "f")
    res = test_df[test_df["classification"] == "f"]["name"].to_numpy()

    assert np.all(res == np.array(["bob j vonrosevich", "missy elliot", "missy eliot"]))


"""
def test_apply_classification_label_different_column(matcher_scen_1):
    # Testing classification based on a different column ('name' instead of 'address')
    apply_classification_label(matcher_scen_1, "bob", "name", "f")
    res = matcher_scen_1[matcher_scen_1["classification"] == "f"]["name"].to_list()
    assert "bob von rosevich" in res and "bob j vonrosevich" in res

def test_apply_classification_label_case_sensitivity(matcher_scen_1):
    # Resetting classification to 'neutral'
    matcher_scen_1["classification"] = "neutral"
    apply_classification_label(matcher_scen_1, "fancy", "address", "c")  # Using lowercase 'fancy'
    res = matcher_scen_1[matcher_scen_1["classification"] == "c"]["name"].to_list()
    assert len(res) > 0  # Adjust this assertion based on whether the function is intended to be case-sensitive

def test_apply_classification_label_no_matches(matcher_scen_1):
    # Resetting classification to 'neutral'
    matcher_scen_1["classification"] = "neutral"
    apply_classification_label(matcher_scen_1, "xyz", "address", "f")  # Using a string that doesn't match
    assert not matcher_scen_1[matcher_scen_1["classification"] == "f"].any()

def test_apply_classification_label_special_characters(matcher_scen_1):
    # Testing with special characters
    matcher_scen_1["classification"] = "neutral"
    apply_classification_label(matcher_scen_1, "Blue Drive,", "address", "c")
    res = matcher_scen_1[matcher_scen_1["classification"] == "c"]["name"].to_list()
    assert len(res) == 2  # Expecting matches for entries with "Blue Drive,"

def test_preservation_of_neutral_rows_with_apply_classification_label(matcher_scen_1):
    # Ensuring rows that do not match remain neutral
    matcher_scen_1["classification"] = "neutral"
    apply_classification_label(matcher_scen_1, "Fancy", "address", "f")
    neutral_rows = matcher_scen_1[matcher_scen_1["classification"] == "neutral"].shape[0]
    assert neutral_rows > 0  # Expecting some rows to remain neutral
 """
