"""Source identifiers through normalize (library and CLI) and link"""

import argparse

import duckdb
import pandas as pd
import pytest
from utils.cli.core import run_normalize
from utils.constants import DEFAULT_SCHEMA_PATH
from utils.ids import (
    SOURCE_IDENTIFIER_TABLE,
    UUID4_REGEX,
    SourceIdentifierMapping,
)
from utils.link.predict import (
    LINKAGE_MAPPING_TABLE,
    update_source_identifiers,
)
from utils.link.source_ids import (
    SOURCE_IDENTIFIER_LINKAGE_TABLE,
    link_by_source_identifiers,
)
from utils.normalize import Normalizer


def az_transaction(donor_name_id="1001", committee_id="1001"):
    """Transaction where the donor NameID and recipient CommitteeID share a number"""
    return pd.DataFrame(
        {
            "amount": [10.0],
            "donor_id": [donor_name_id],
            "donor_id_source": ["az_sos_name"],
            "recipient_id": [committee_id],
            "recipient_id_source": ["az_sos_committee"],
            "reported_state": ["AZ"],
        }
    )


def test_normalizer_keys_ids_by_source():
    normalizer = Normalizer({"Transaction": az_transaction()}, DEFAULT_SCHEMA_PATH)
    database = normalizer.normalize_database()
    transaction = database["Transaction"]

    donor_id, recipient_id = transaction.loc[0, ["donor_id", "recipient_id"]]
    assert UUID4_REGEX.match(donor_id) and UUID4_REGEX.match(recipient_id)
    assert donor_id != recipient_id
    assert normalizer.id_mapping[("az_sos_name", "1001", "Transactor")] == donor_id
    assert (
        normalizer.id_mapping[("az_sos_committee", "1001", "Transactor")]
        == recipient_id
    )
    for table in database.values():
        assert not [column for column in table.columns if column.endswith("_source")]


def test_normalizer_reuses_existing_mapping():
    first = Normalizer({"Transaction": az_transaction()}, DEFAULT_SCHEMA_PATH)
    first_transaction = first.normalize_database()["Transaction"]

    second = Normalizer(
        {"Transaction": az_transaction(donor_name_id="2002")},
        DEFAULT_SCHEMA_PATH,
        first.id_mapping,
    )
    second_transaction = second.normalize_database()["Transaction"]
    assert (
        second_transaction.loc[0, "recipient_id"]
        == first_transaction.loc[0, "recipient_id"]
    )
    assert second_transaction.loc[0, "donor_id"] != first_transaction.loc[0, "donor_id"]
    # the existing mapping is copied, not modified
    assert ("az_sos_name", "2002", "Transactor") not in first.id_mapping


def test_election_result_from_reverse_relation_gets_id():
    transactor = pd.DataFrame(
        {
            "full_name": ["Jane Doe"],
            "transactor_type": ["Individual"],
            "election_result--votes_received": [100],
            "election_result--election--office_sought": ["State Senator"],
            "election_result--election--district": ["1"],
            "reported_state": ["MN"],
        }
    )
    database = Normalizer(
        {"Transactor": transactor}, DEFAULT_SCHEMA_PATH
    ).normalize_database()
    election_result = database["ElectionResult"]
    assert len(election_result) == 1
    assert UUID4_REGEX.match(election_result.loc[0, "id"])
    assert election_result.loc[0, "candidate_id"] == database["Transactor"].loc[0, "id"]


def normalize_args(tmp_path):
    return argparse.Namespace(
        input_directory=tmp_path / "standardized",
        output_directory=tmp_path / "normalized",
        state="mn",
        input_format="parquet",
        output_format="parquet",
        chunk_size=None,
        schema=DEFAULT_SCHEMA_PATH,
    )


def write_standardized_mn_transaction(tmp_path):
    state_directory = tmp_path / "standardized" / "mn"
    state_directory.mkdir(parents=True)
    pd.DataFrame(
        {
            "amount": [250.0],
            "donor_id": ["500"],
            "donor_id_source": ["mn_cfb_lobbyist"],
            "recipient_id": ["15677"],
            "recipient_id_source": ["mn_cfb_registration"],
            "reported_state": ["MN"],
        }
    ).to_parquet(state_directory / "Transaction.parquet")


def test_run_normalize_writes_source_identifiers(tmp_path):
    write_standardized_mn_transaction(tmp_path)
    args = normalize_args(tmp_path)
    run_normalize(args)

    output_directory = tmp_path / "normalized" / "mn"
    source_identifiers = pd.read_parquet(
        output_directory / f"{SOURCE_IDENTIFIER_TABLE}.parquet"
    )
    transaction = pd.read_parquet(output_directory / "Transaction.parquet")
    by_source = source_identifiers.set_index("source")
    assert by_source.loc["mn_cfb_registration", "source_id"] == "15677"
    assert (
        by_source.loc["mn_cfb_registration", "entity_id"]
        == (transaction.loc[0, "recipient_id"])
    )
    assert (
        by_source.loc["mn_cfb_lobbyist", "entity_id"] == transaction.loc[0, "donor_id"]
    )
    assert (source_identifiers["entity_table"] == "Transactor").all()

    # a second run keeps the same entity ids
    run_normalize(args)
    rerun_transaction = pd.read_parquet(output_directory / "Transaction.parquet")
    assert (
        rerun_transaction.loc[0, "recipient_id"] == transaction.loc[0, "recipient_id"]
    )


def test_run_normalize_migrates_legacy_id_mapping(tmp_path):
    write_standardized_mn_transaction(tmp_path)
    output_directory = tmp_path / "normalized" / "mn"
    output_directory.mkdir(parents=True)
    legacy_uuid = "11111111-1111-4111-8111-111111111111"
    pd.DataFrame(
        [["15677", None, "MN", "Transactor", legacy_uuid]],
        columns=["raw_id", "year", "reported_state", "table_name", "uuid"],
    ).to_csv(output_directory / "id_mapping.tsv", sep="\t", index=False)

    run_normalize(normalize_args(tmp_path))

    transaction = pd.read_parquet(output_directory / "Transaction.parquet")
    assert transaction.loc[0, "recipient_id"] == legacy_uuid


@pytest.fixture
def linked_database():
    """Combined database where two states gave the same FEC committee two ids"""
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE Transactor AS SELECT * FROM (VALUES
            ('e1', 'KOCH-PAC', 'TX'),
            ('e2', 'Koch PAC', 'MN'),
            ('e3', 'Koch Industries PAC', 'MN'),
            ('e4', 'Unrelated Committee', 'MN')
        ) AS t(id, full_name, reported_state)
        """
    )
    con.execute(
        """
        CREATE TABLE "Transaction" AS SELECT * FROM (VALUES
            ('e2', 'e4', 100.0),
            ('e3', 'e4', 50.0),
            ('e1', 'e4', 25.0)
        ) AS t(donor_id, recipient_id, amount)
        """
    )
    mapping = SourceIdentifierMapping()
    mapping.add(("fec_committee", "C00236489", "Transactor"), "e1", "TX")
    mapping.add(("mn_cfb_registration", "40000", "Transactor"), "e4", "MN")
    table = mapping.to_table()
    # the same FEC id seen in MN, plus a MN id shared by e2 and e3 (a chain)
    table = pd.concat(
        [
            table,
            pd.DataFrame(
                [
                    [
                        "e2",
                        "Transactor",
                        "fec_committee",
                        "C00236489",
                        "MN",
                        None,
                        None,
                    ],
                    [
                        "e2",
                        "Transactor",
                        "mn_cfb_registration",
                        "41000",
                        "MN",
                        None,
                        None,
                    ],
                    [
                        "e3",
                        "Transactor",
                        "mn_cfb_registration",
                        "41000",
                        "MN",
                        None,
                        None,
                    ],
                ],
                columns=table.columns,
            ),
        ],
        ignore_index=True,
    )
    con.register("source_identifier_frame", table)
    con.execute(
        f"CREATE TABLE {SOURCE_IDENTIFIER_TABLE} AS SELECT * FROM source_identifier_frame"
    )
    con.unregister("source_identifier_frame")
    yield con
    con.close()


def test_link_by_source_identifiers_merges_chains(linked_database):
    con = linked_database
    n_remapped = link_by_source_identifiers(con)

    assert n_remapped == 2  # e2 and e3 both become e1
    donors = {
        row[0] for row in con.execute('SELECT donor_id FROM "Transaction"').fetchall()
    }
    assert donors == {"e1"}
    assert con.execute(
        f"""
        SELECT count(*) FROM (
            SELECT source, source_id, entity_table FROM {SOURCE_IDENTIFIER_TABLE}
            GROUP BY ALL HAVING count(DISTINCT entity_id) > 1
        )
        """
    ).fetchone() == (0,)
    linkage = con.execute(
        f"SELECT old_id, canonical_id FROM {SOURCE_IDENTIFIER_LINKAGE_TABLE}"
    ).fetchall()
    assert ("e2", "e1") in linkage
    # unrelated entities are untouched
    assert con.execute(
        f"SELECT entity_id FROM {SOURCE_IDENTIFIER_TABLE} WHERE source_id = '40000'"
    ).fetchone() == ("e4",)


def test_link_by_source_identifiers_without_table():
    con = duckdb.connect()
    assert link_by_source_identifiers(con) == 0


def test_update_source_identifiers_after_linkage(linked_database):
    con = linked_database
    con.execute(
        f"""
        CREATE TABLE {LINKAGE_MAPPING_TABLE} AS
        SELECT * FROM (VALUES ('e4', 'e0')) AS t(old_id, canonical_id)
        """
    )
    update_source_identifiers(con, "Transactor")
    assert con.execute(
        f"SELECT entity_id FROM {SOURCE_IDENTIFIER_TABLE} WHERE source_id = '40000'"
    ).fetchone() == ("e0",)
