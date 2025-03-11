from uuid import UUID

import pandas as pd
import pytest
from utils.normalize import Normalizer, get_normalization_form_by_column
from utils.schema import DataSchema


@pytest.fixture()
def determined_uuids(mocker):
    side_effect = (
        UUID(f"00000000-0000-4000-8000-{i:012}", version=4) for i in range(1, 10000)
    )
    return mocker.patch("uuid.uuid4", side_effect=side_effect)


@pytest.fixture
def sample_schema(tmp_path):
    """Creates a DataSchema object from a sample YAML schema."""
    schema_yaml = """
    Transactor:
      child_types: ["Individual", "Organization"]
      required_attributes: ["id"]
      attributes: ["id", "full_name", "transactor_type_specific", "transactor_type", "phone_number"]
      enum_columns:
        transactor_type: ["Individual", "Organization"]
        transactor_type_specific: ["Lobbyist", "Candidate", "Vendor", "Corporation", "Non-profit", "Committee", "Party"]
      reverse_relations:
        address: Address
      reverse_relation_names:
        address: transactor_id

    Individual:
      parent_type: Transactor
      attributes: ["first_name", "middle_name", "last_name", "name_suffix", "name_title", "name_preferred", "occupation", "party"]
      reverse_relations:
        employer: Membership
        election_result: ElectionResult
      reverse_relation_names:
        employer: member_id
        election_result: candidate_id

    Organization:
      parent_type: Transactor
      attributes: ["naics", "sic", "stock_symbol"]

    Transaction:
      required_attributes: ["amount", "donor_id", "recipient_id"]
      attributes: ["id", "amount", "date", "transaction_type", "description", "reported_election_id", "donor_id", "recipient_id"]
      forward_relations:
        donor: Transactor
        recipient: Transactor
        reported_election: Election
      repeating_columns: ["amount", "transaction_type", "date", "description"]
      enum_columns:
        transaction_type: ["contribution"]

    Address:
      attributes: ["transactor_id", "city", "state"]
      forward_relations:
        transactor: Transactor

    Membership:
      attributes: ["member_id", "organization_id"]
      forward_relations:
        member: Individual
        organization: Organization

    Election:
      required_attributes: ["year", "district", "office_sought", "state"]
      attributes: ["id", "year", "district", "office_sought", "state"]

    ElectionResult:
      attributes: ["candidate_id", "election_id", "votes_recieved", "win"]
      forward_relations:
        candidate: Individual
        election: Election
    """

    schema_file = tmp_path / "test_schema.yaml"
    schema_file.write_text(schema_yaml)

    return DataSchema(schema_file)


@pytest.fixture
def database_1_unnormalized():
    """Provides sample data for testing normalization steps."""
    return {
        "Transaction": pd.DataFrame(
            {
                "amount-1": [100, 200],
                "amount-2": [150, 250],
                "donor--full_name": ["John Doe", "Jane Smith"],
                "donor--address--city": ["Chicago", "New York"],
                "donor--address--state": ["IL", "NY"],
                "donor--employer--organization--full_name": [
                    "University of Chicago",
                    "NYU",
                ],
                "donor--employer--organization--address--city": ["Chicago", "New York"],
                "recipient_id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                ],
                "transaction_type-1": ["contribution", "contribution"],
                "transaction_type-2": ["contribution", "contribution"],
                "date-1": ["2024-01-23", "2024-02-10"],
                "date-2": ["2024-01-26", "2024-02-18"],
            }
        ),
        "Transactor": pd.DataFrame(
            {
                "id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                ],
                "first_name": ["Alice", "Bob"],
                "last_name": ["Johnson", "Smith"],
                "full_name": ["Alice Johnson", "Bob Smith"],
            },
        ).set_index("id"),
    }


@pytest.fixture
def database_1_1NF() -> dict[str, pd.DataFrame]:
    """Sample database #1 in 1NF"""
    return {
        "Transaction": pd.DataFrame(
            {
                "amount": [100, 150, 200, 250],
                "donor--full_name": [
                    "John Doe",
                    "John Doe",
                    "Jane Smith",
                    "Jane Smith",
                ],
                "donor--address--city": [
                    "Chicago",
                    "Chicago",
                    "New York",
                    "New York",
                ],
                "donor--address--state": ["IL", "IL", "NY", "NY"],
                "donor--employer--organization--full_name": [
                    "University of Chicago",
                    "University of Chicago",
                    "NYU",
                    "NYU",
                ],
                "donor--employer--organization--address--city": [
                    "Chicago",
                    "Chicago",
                    "New York",
                    "New York",
                ],
                "recipient_id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                ],
                "transaction_type": [
                    "contribution",
                    "contribution",
                    "contribution",
                    "contribution",
                ],
                "date": ["2024-01-23", "2024-01-26", "2024-02-10", "2024-02-18"],
            }
        ),
        "Transactor": pd.DataFrame(
            {
                "id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                ],
                "first_name": ["Alice", "Bob"],
                "last_name": ["Johnson", "Smith"],
                "full_name": ["Alice Johnson", "Bob Smith"],
            },
        ).set_index("id"),
    }


@pytest.fixture
def database_1_3NF() -> dict[str, pd.DataFrame]:
    """Sample database #1 in 3NF"""
    return {
        "Transaction": pd.DataFrame(
            {
                "amount": [100, 150, 200, 250],
                "donor_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "recipient_id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                ],
                "transaction_type": [
                    "contribution",
                    "contribution",
                    "contribution",
                    "contribution",
                ],
                "date": ["2024-01-23", "2024-01-26", "2024-02-10", "2024-02-18"],
            }
        ),
        "Transactor": pd.DataFrame(
            {
                "id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                    "00000000-0000-4000-8000-000000000003",
                    "00000000-0000-4000-8000-000000000004",
                ],
                "full_name": [
                    "Alice Johnson",
                    "Bob Smith",
                    "John Doe",
                    "Jane Smith",
                    "University of Chicago",
                    "NYU",
                ],
                "first_name": ["Alice", "Bob", None, None, None, None],
                "last_name": ["Johnson", "Smith", None, None, None, None],
            },
        ).set_index("id"),
        "Membership": pd.DataFrame(
            {
                "member_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "organization_id": [
                    "00000000-0000-4000-8000-000000000003",
                    "00000000-0000-4000-8000-000000000004",
                ],
            }
        ),
        "Address": pd.DataFrame(
            {
                "transactor_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                    "00000000-0000-4000-8000-000000000003",
                    "00000000-0000-4000-8000-000000000004",
                ],
                "city": ["Chicago", "New York", "Chicago", "New York"],
                "state": ["IL", "NY", None, None],
            }
        ),
    }


@pytest.fixture
def database_1_3NF_CTI() -> dict[str, pd.DataFrame]:
    """Sample database #1 in 3rd normal form with class table inheritance"""
    return {
        "Transaction": pd.DataFrame(
            {
                "amount": [100, 150, 200, 250],
                "donor_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "recipient_id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                ],
                "transaction_type": [
                    "contribution",
                    "contribution",
                    "contribution",
                    "contribution",
                ],
                "date": ["2024-01-23", "2024-01-26", "2024-02-10", "2024-02-18"],
            }
        ),
        "Transactor": pd.DataFrame(
            {
                "id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                    "00000000-0000-4000-8000-000000000003",
                    "00000000-0000-4000-8000-000000000004",
                ],
                "full_name": [
                    "Alice Johnson",
                    "Bob Smith",
                    "John Doe",
                    "Jane Smith",
                    "University of Chicago",
                    "NYU",
                ],
                "transactor_type": [
                    "Individual",
                    "Individual",
                    "Individual",
                    "Individual",
                    "Organization",
                    "Organization",
                ],
            },
        ).set_index("id"),
        "Individual": pd.DataFrame(
            {
                "id": [
                    "bc152604-0e5b-4613-b858-d51afc259846",
                    "d7ed5203-810c-498a-824d-605f8bd22238",
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "first_name": [
                    "Alice",
                    "Bob",
                    None,
                    None,
                ],
                "last_name": ["Johnson", "Smith", None, None],
            }
        ).set_index("id"),
        "Organization": pd.DataFrame(
            {
                "id": [
                    "00000000-0000-4000-8000-000000000003",
                    "00000000-0000-4000-8000-000000000004",
                ]
            }
        ).set_index("id"),
        "Membership": pd.DataFrame(
            {
                "member_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "organization_id": [
                    "00000000-0000-4000-8000-000000000003",
                    "00000000-0000-4000-8000-000000000004",
                ],
            }
        ),
        "Address": pd.DataFrame(
            {
                "transactor_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                    "00000000-0000-4000-8000-000000000003",
                    "00000000-0000-4000-8000-000000000004",
                ],
                "city": ["Chicago", "New York", "Chicago", "New York"],
                "state": ["IL", "NY", "IL", "NY"],
            }
        ),
    }


@pytest.fixture
def database_2_1NF() -> dict[str, pd.DataFrame]:
    """Database in 1NF result of merging transactions with donor and recipient info"""
    return {
        "Transaction": pd.DataFrame(
            {
                "recipient_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                    None,
                    None,
                    None,
                ],
                "recipient--full_name": [
                    None,
                    None,
                    "Fake Name",
                    "Sample Name",
                    "Sample Name",
                ],
                "recipient--address--line_1": [
                    None,
                    None,
                    "123 Main St.",
                    "345 First Ave.",
                    "345 First Ave.",
                ],
                "amount": [
                    45,
                    32,
                    25.34,
                    103.40,
                    143,
                ],
                "date": [
                    "2023-01-12",
                    "2022-03-05",
                    "2024-07-30",
                    "2023-01-23",
                    "2023-01-23",
                ],
                "recipient--election_result--election--year": [],
            }
        )
    }


def make_df_standard_for_testing(df: pd.DataFrame, columns: pd.Index) -> pd.DataFrame:
    """Sort df by columns and reset index to facilitate testing


    pd.testing.assert_frame_equal requires the index to be considered.
    Doing this allows us to ensure two otherwise equivalent dfs will
    have the same index for each logical row
    """
    return df.sort_values(by=columns.tolist()).reset_index(drop=True)


def test_normalization_status_unnormalized(database_1_unnormalized, sample_schema):
    """Test normalization status of unnormalized database"""
    transaction_result = get_normalization_form_by_column(
        database_1_unnormalized["Transaction"], sample_schema.schema["Transaction"]
    )
    transactor_result = get_normalization_form_by_column(
        database_1_unnormalized["Transactor"].reset_index(),
        sample_schema.schema["Transactor"],
    )

    expected_value = {
        "Transaction": {
            0: {
                "amount-1",
                "amount-2",
                "transaction_type-1",
                "transaction_type-2",
                "date-1",
                "date-2",
            },
            1: {
                "donor",
            },
            3: {
                "recipient_id",
            },
        },
        "Transactor": {
            3: {
                "id",
                "first_name",
                "last_name",
                "full_name",
            },
            0: set(),
            1: set(),
        },
    }
    assert transaction_result == expected_value["Transaction"]
    assert transactor_result == expected_value["Transactor"]


def test_normalization_status_1NF(database_1_1NF, sample_schema):
    """Test normalization status of 1NF database"""
    transaction_result = get_normalization_form_by_column(
        database_1_1NF["Transaction"], sample_schema.schema["Transaction"]
    )
    transactor_result = get_normalization_form_by_column(
        database_1_1NF["Transactor"].reset_index(), sample_schema.schema["Transactor"]
    )

    expected_value = {
        "Transaction": {
            0: set(),
            1: {
                "donor",
            },
            3: {
                "recipient_id",
                "amount",
                "transaction_type",
                "date",
            },
        },
        "Transactor": {
            3: {
                "id",
                "first_name",
                "last_name",
                "full_name",
            },
            0: set(),
            1: set(),
        },
    }
    assert transaction_result == expected_value["Transaction"]
    assert transactor_result == expected_value["Transactor"]


def test_1NF_from_unnormalized(database_1_unnormalized, database_1_1NF, sample_schema):
    """Tests removing repeating columns (Level 0 to Level 1)."""
    normalizer = Normalizer(database_1_unnormalized, sample_schema)
    expected_value = database_1_1NF["Transaction"].copy()

    normalizer.convert_to_1NF_from_unnormalized("Transaction")
    normalized_transactions = normalizer.database["Transaction"]
    normalized_transactions = make_df_standard_for_testing(
        normalized_transactions, normalized_transactions.columns
    )
    expected_value = make_df_standard_for_testing(
        expected_value, normalized_transactions.columns
    )
    normalized_transactions.columns.name = None

    pd.testing.assert_frame_equal(
        normalized_transactions,
        expected_value,
        check_like=True,
        check_dtype=False,
    )


def test_3NF_from_1NF(database_1_1NF, database_1_3NF, sample_schema, determined_uuids):
    """Tests extracting foreign key attributes into separate tables (Level 1 to Level 3)."""
    normalizer = Normalizer(database_1_1NF, sample_schema)
    normalizer.convert_to_3NF_from_1NF()
    database_result_3NF = normalizer.database

    assert (
        database_result_3NF.keys() == database_1_3NF.keys()
    ), f"Result database has keys: {database_result_3NF.keys()}"

    for table_type in database_result_3NF:
        pd.testing.assert_frame_equal(
            make_df_standard_for_testing(
                database_result_3NF[table_type], database_1_3NF[table_type].columns
            ),
            make_df_standard_for_testing(
                database_1_3NF[table_type], database_1_3NF[table_type].columns
            ),
            check_like=True,
            check_dtype=False,
        )


# def test_inheritance_strategy_CTI_from_STI(
#     database_1_3NF, database_1_3NF_CTI, sample_schema
# ):
#     """Test changing the inheritance strategy from single table to class table"""
#     database_result_CTI = convert_to_class_table_from_single_table(
#         database_1_3NF, sample_schema
#     )

#     assert database_result_CTI.keys() == database_1_3NF_CTI.keys(), (
#         f"Result database has keys: {database_result_CTI.keys()}"
#     )

#     for table_type in database_result_CTI:
#         pd.testing.assert_frame_equal(
#             make_df_standard_for_testing(
#                 database_result_CTI[table_type], database_1_3NF_CTI[table_type].columns
#             ),
#             make_df_standard_for_testing(
#                 database_1_3NF_CTI[table_type], database_1_3NF_CTI[table_type].columns
#             ),
#             check_like=True,
#             check_dtype=False,
#         )
