"""Tests for linkage.py"""

import pandas as pd
import pytest
from utils.constants import BASE_FILEPATH
from utils.linkage import deduplicate_perfect_matches

"""
Module for testing functions in linkage.py
"""


# Test for dedupe function
@pytest.fixture
def return_data(filename):
    path = BASE_FILEPATH / "output" / filename
    sample_df = pd.read_csv(path, low_memory=False)
    return sample_df


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
