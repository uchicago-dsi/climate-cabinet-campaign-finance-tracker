import re
from unittest.mock import MagicMock

import pandas as pd
import pytest
from utils.ids import (
    SOURCE_IDENTIFIER_COLUMNS,
    SOURCE_IDENTIFIER_TABLE,
    UUID4_REGEX,
    SourceIdentifierMapping,
    create_new_uuid_mapping,
    drop_id_source_columns,
    get_id_sources,
    get_raw_ids_mask,
    handle_id_column,
    load_source_identifiers,
    map_ids_to_uuids,
    migrate_legacy_id_mapping,
    replace_null_ids_with_uuids,
    save_source_identifiers,
)
from utils.standardize.config import IdSourceRule

UUID_1 = "11111111-1111-4111-8111-111111111111"
UUID_2 = "22222222-2222-4222-8222-222222222222"
UUID_3 = "33333333-3333-4333-8333-333333333333"


@pytest.fixture
def sample_table():
    """Creates a sample DataFrame for testing"""
    return pd.DataFrame(
        {
            "id": [1, "invalid_uuid", "550e8400-e29b-41d4-a716-446655440000", None, 1],
            "id_source": ["src_a", "src_a", None, None, "src_a"],
            "reported_state": ["CA", "NY", "TX", "FL", "CA"],
        }
    )


def test_add_uuids_to_table(sample_table):
    """Test that missing IDs are assigned UUIDs"""
    replace_null_ids_with_uuids(sample_table, "id")
    assert sample_table["id"].notna().all(), "All ID values should be filled"
    assert re.match(UUID4_REGEX, sample_table.loc[3, "id"])


def test_get_id_sources_falls_back_to_legacy_source():
    table = pd.DataFrame(
        {
            "recipient_id": ["1", "2", "3"],
            "recipient_id_source": ["mn_cfb_registration", None, None],
            "reported_state": ["MN", "MN", None],
        }
    )
    assert get_id_sources(table, "recipient_id").to_list() == [
        "mn_cfb_registration",
        "mn_legacy",
        "unknown_legacy",
    ]
    assert get_id_sources(table, "donor_id").to_list() == [
        "mn_legacy",
        "mn_legacy",
        "unknown_legacy",
    ]


def test_map_ids_to_uuids(sample_table):
    """Test that IDs are replaced if found in the mapping"""
    id_mapping = SourceIdentifierMapping()
    id_mapping.add(("src_a", "1", "tableA"), UUID_1)
    id_mapping.add(("src_a", "invalid_uuid", "tableA"), UUID_2)
    map_ids_to_uuids(sample_table, "tableA", id_mapping, "id")

    assert sample_table.loc[0, "id"] == UUID_1
    assert sample_table.loc[4, "id"] == UUID_1
    assert sample_table.loc[1, "id"] == UUID_2
    assert sample_table.loc[2, "id"] == "550e8400-e29b-41d4-a716-446655440000"


def test_map_ids_to_uuids_keys_by_source():
    """The same raw id from two id systems maps to two entities"""
    table = pd.DataFrame(
        {
            "id": ["1001", "1001"],
            "id_source": ["az_sos_committee", "az_sos_name"],
            "reported_state": ["AZ", "AZ"],
        }
    )
    id_mapping = SourceIdentifierMapping()
    id_mapping.add(("az_sos_committee", "1001", "Transactor"), UUID_1)
    id_mapping.add(("az_sos_name", "1001", "Transactor"), UUID_2)
    map_ids_to_uuids(table, "Transactor", id_mapping, "id")
    assert table["id"].to_list() == [UUID_1, UUID_2]


def test_map_ids_to_uuids_normalizes_numeric_ids():
    """Ids read as floats or ints match the same source id as strings"""
    table = pd.DataFrame({"id": [15677.0, 15677], "reported_state": ["MN", "MN"]})
    id_mapping = SourceIdentifierMapping()
    id_mapping.add(("mn_legacy", "15677", "Transactor"), UUID_1)
    map_ids_to_uuids(table, "Transactor", id_mapping, "id")
    assert table["id"].to_list() == [UUID_1, UUID_1]


def test_create_new_uuid_mapping(sample_table):
    """Test new UUIDs are created for invalid/missing IDs"""
    new_mapping = create_new_uuid_mapping(sample_table, "tableA", "id")

    assert len(new_mapping) == 2, "All unique combinations should be mapped"
    assert set(new_mapping) == {
        ("src_a", "1", "tableA"),
        ("src_a", "invalid_uuid", "tableA"),
    }
    for key in new_mapping:
        assert re.match(UUID4_REGEX, new_mapping[key])
    assert new_mapping.reported_state(("src_a", "1", "tableA")) == "CA"


def test_create_new_uuid_mapping_skips_existing(sample_table):
    existing = SourceIdentifierMapping()
    existing.add(("src_a", "1", "tableA"), UUID_1)
    new_mapping = create_new_uuid_mapping(sample_table, "tableA", "id", existing)
    assert set(new_mapping) == {("src_a", "invalid_uuid", "tableA")}


def test_get_raw_ids_mask(sample_table):
    """Test that get_raw_ids_mask correctly identifies non-UUIDs"""
    mask = get_raw_ids_mask(sample_table, "id")
    assert mask.tolist() == [
        True,
        True,
        False,
        False,
        True,
    ], "Mask should correctly identify raw/non-UUID IDs"


def test_handle_id_column(sample_table):
    """Test full handling logic"""
    mock_schema = MagicMock()
    mock_schema.attributes = ["id"]

    id_mapping = SourceIdentifierMapping()
    id_mapping.add(("src_a", "1", "tableA"), UUID_1)
    handle_id_column(sample_table, mock_schema, "tableA", id_mapping, "id")

    assert id_mapping[("src_a", "1", "tableA")] == UUID_1, "Old mappings should remain"
    expected_id_mapping_length = 2
    assert re.match(
        UUID4_REGEX, id_mapping[("src_a", "invalid_uuid", "tableA")]
    ), "New mappings should be uuids"
    assert len(id_mapping) == expected_id_mapping_length, "No extra new mappings"

    assert sample_table.loc[0, "id"] == UUID_1
    assert (
        sample_table.loc[2, "id"] == "550e8400-e29b-41d4-a716-446655440000"
    ), "Exising ids should be unchanged"
    assert re.match(
        UUID4_REGEX, sample_table.loc[3, "id"]
    ), "Nans in provided table should become uuids"
    assert (
        sample_table["id"].str.match(UUID4_REGEX).all()
    ), "All ids should now be uuids"


def test_drop_id_source_columns(sample_table):
    assert "id_source" not in drop_id_source_columns(sample_table).columns
    assert "id" in drop_id_source_columns(sample_table).columns


def test_mapping_table_round_trip():
    mapping = SourceIdentifierMapping()
    mapping.add(("mn_cfb_registration", "15677", "Transactor"), UUID_1, "MN")
    mapping.add(("fec_committee", "C00236489", "Transactor"), UUID_2, "TX")
    table = mapping.to_table()
    assert list(table.columns) == SOURCE_IDENTIFIER_COLUMNS
    round_tripped = SourceIdentifierMapping.from_table(table)
    assert set(round_tripped) == set(mapping)
    assert round_tripped[("fec_committee", "C00236489", "Transactor")] == UUID_2
    assert (
        round_tripped.reported_state(("fec_committee", "C00236489", "Transactor"))
        == "TX"
    )


def test_add_keeps_first_reported_state():
    mapping = SourceIdentifierMapping()
    mapping.add(("fec_committee", "C00236489", "Transactor"), UUID_1, "TX")
    mapping.add(("fec_committee", "C00236489", "Transactor"), UUID_1, "MN")
    assert mapping.reported_state(("fec_committee", "C00236489", "Transactor")) == "TX"


@pytest.mark.parametrize("file_format", ["parquet", "csv"])
def test_save_and_load_source_identifiers(tmp_path, file_format):
    mapping = SourceIdentifierMapping()
    mapping.add(("tx_ethics_filer", "00015654", "Transactor"), UUID_1, "TX")
    save_source_identifiers(mapping, tmp_path, format=file_format)
    assert (tmp_path / f"{SOURCE_IDENTIFIER_TABLE}.{file_format}").exists()
    loaded = load_source_identifiers(tmp_path, format=file_format)
    # leading zeros survive because source ids are stored as strings
    assert loaded[("tx_ethics_filer", "00015654", "Transactor")] == UUID_1


def test_load_source_identifiers_empty_directory(tmp_path):
    assert len(load_source_identifiers(tmp_path)) == 0


MN_RULES = [
    IdSourceRule("mn_cfb_unregistered", when=re.compile(r"^-\d+$")),
    IdSourceRule("mn_cfb_lobbyist", when=re.compile(r"^\d{1,4}$")),
    IdSourceRule("mn_cfb_registration"),
]


def write_legacy_mapping(path, rows):
    pd.DataFrame(
        rows, columns=["raw_id", "year", "reported_state", "table_name", "uuid"]
    ).to_csv(path, sep="\t", index=False)


def test_migrate_legacy_id_mapping_infers_unambiguous_sources(tmp_path):
    legacy_path = tmp_path / "id_mapping.tsv"
    write_legacy_mapping(
        legacy_path,
        [
            ["15677", None, "MN", "Transactor", UUID_1],
            ["500", None, "MN", "Transactor", UUID_2],
            ["-2139646094", None, "MN", "Transactor", UUID_3],
        ],
    )
    mapping = migrate_legacy_id_mapping(legacy_path, MN_RULES)
    assert mapping[("mn_cfb_registration", "15677", "Transactor")] == UUID_1
    assert mapping[("mn_cfb_lobbyist", "500", "Transactor")] == UUID_2
    assert mapping[("mn_cfb_unregistered", "-2139646094", "Transactor")] == UUID_3


def test_migrate_legacy_id_mapping_uses_legacy_source_when_ambiguous(tmp_path):
    legacy_path = tmp_path / "id_mapping.tsv"
    write_legacy_mapping(legacy_path, [["1001", None, "AZ", "Transactor", UUID_1]])
    rules = [IdSourceRule("az_sos_committee"), IdSourceRule("az_sos_name")]
    mapping = migrate_legacy_id_mapping(legacy_path, rules)
    assert set(mapping) == {("az_legacy", "1001", "Transactor")}


def test_migrate_legacy_id_mapping_skips_formatted_sources(tmp_path):
    """Short PA ids can't be migrated to a source whose ids include the year"""
    legacy_path = tmp_path / "id_mapping.tsv"
    write_legacy_mapping(legacy_path, [["10014", None, "PA", "Transactor", UUID_1]])
    rules = [
        IdSourceRule(
            "pa_dos_candidate_filer",
            when=re.compile(r"^\d{1,6}$"),
            source_id_format="{reported_election_year}-{value}",
        ),
        IdSourceRule("pa_dos_filer"),
    ]
    mapping = migrate_legacy_id_mapping(legacy_path, rules)
    assert set(mapping) == {("pa_legacy", "10014", "Transactor")}


def test_load_source_identifiers_migrates_legacy_mapping(tmp_path):
    write_legacy_mapping(
        tmp_path / "id_mapping.tsv", [["15677", None, "MN", "Transactor", UUID_1]]
    )
    mapping = load_source_identifiers(tmp_path, legacy_id_source_rules=MN_RULES)
    assert mapping[("mn_cfb_registration", "15677", "Transactor")] == UUID_1


def test_load_source_identifiers_prefers_table_over_legacy_mapping(tmp_path):
    write_legacy_mapping(
        tmp_path / "id_mapping.tsv", [["15677", None, "MN", "Transactor", UUID_1]]
    )
    mapping = SourceIdentifierMapping()
    mapping.add(("mn_cfb_registration", "15677", "Transactor"), UUID_2, "MN")
    save_source_identifiers(mapping, tmp_path)
    loaded = load_source_identifiers(tmp_path, legacy_id_source_rules=MN_RULES)
    assert loaded[("mn_cfb_registration", "15677", "Transactor")] == UUID_2
