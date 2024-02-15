# import json
# import pytest
import pandas as pd

from utils.constants import BASE_FILEPATH
from utils.linkage import deduplicate_perfect_matches

inds_sample = pd.read_csv(
    BASE_FILEPATH / "output" / "complete_individuals_table.csv"
)
orgs_sample = pd.read_csv(
    BASE_FILEPATH / "output" / "complete_organizations_table.csv"
)

deduplicated_inds = deduplicate_perfect_matches(inds_sample)
deduplicated_orgs = deduplicate_perfect_matches(orgs_sample)

output_dedup_ids = pd.read_csv(
    BASE_FILEPATH / "output" / "deduplicated_UUIDs.csv"
)
# outpud_ids should have all the ids that deduplicated_inds and deduplicated_orgs
# has

dedup_inds_id = deduplicated_inds.id.tolist()
dedup_orgs_id = deduplicated_orgs.id.tolist()
unique_ids = output_dedup_ids.duplicated_uuids.tolist()

assert all(x in unique_ids for x in dedup_inds_id)
assert all(x in unique_ids for x in dedup_orgs_id)

'''
from banktrack.annotation.convert import (
    create_record_from_labelstudio_results,
    get_unique_entity_ids_from_labelstudio_results,
)
from banktrack.pipeline.constants import ROOT_DIR

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

'''
