from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest
import yaml
from utils.normalize import Normalizer
from utils.schema import DataSchema

BASE_FILEPATH = Path(__file__).resolve().parent.parent


@pytest.fixture()
def determined_uuids(mocker):
    side_effect = (
        UUID(f"00000000-0000-4000-8000-{i:012}", version=4) for i in range(1, 10000)
    )
    return mocker.patch("uuid.uuid4", side_effect=side_effect)


def make_df_standard_for_testing(df: pd.DataFrame, columns: pd.Index) -> pd.DataFrame:
    """Sort df by columns and reset index to facilitate testing


    pd.testing.assert_frame_equal requires the index to be considered.
    Doing this allows us to ensure two otherwise equivalent dfs will
    have the same index for each logical row
    """
    return df.sort_values(by=columns.tolist()).reset_index(drop=True)


def load_database(database_path):
    """Loads all database tables from a given directory."""
    full_database = {}
    for normalization_directory in ["unnormalized", "1NF", "3NF"]:
        database_normalization_level_path = database_path / normalization_directory
        if database_normalization_level_path.exists():
            database = {}
            for csv_file in Path(database_normalization_level_path).glob("*.csv"):
                table_name = csv_file.stem
                table = pd.read_csv(csv_file)
                # if "id" in table.columns:
                #     table = table.set_index("id")
                #     if "Unnamed: 0" in table.columns:
                #         table = table.drop(columns=["Unnamed: 0"])
                database[table_name] = table
            full_database[normalization_directory] = database
    return full_database


def load_schema(database_path: Path) -> DataSchema:
    """Loads a dataschema from given directory"""
    schema_file = database_path / "schema.yaml"
    if schema_file.exists():
        return DataSchema(schema_file)
    return None


@pytest.fixture
def database_fixture(request):
    """Loads full database and config dynamically"""
    database_name = request.param
    database_path = BASE_FILEPATH / "tests" / "data" / "normalization" / database_name
    return {
        "data": load_database(database_path),
        "schema": load_schema(database_path),
    }


@pytest.mark.parametrize(
    "database_fixture",
    [("campaign-finance-sample"), ("campaign-finance-tricky")],
    indirect=True,
)
def test_1NF_from_unnormalized(database_fixture):
    """Tests removing repeating columns (Level 0 to Level 1)."""
    unnormalized_database = database_fixture["data"]["unnormalized"]
    database_1NF = database_fixture["data"]["1NF"]
    schema = database_fixture["schema"]
    normalizer = Normalizer(unnormalized_database, schema)

    for table_name, table in database_1NF.items():
        expected_value = table.copy()

        normalizer.convert_to_1NF_from_unnormalized(table_name)
        normalized_transactions = normalizer.database[table_name]
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


@pytest.mark.parametrize(
    "database_fixture",
    [
        ("campaign-finance-sample"),
    ],
    indirect=True,
)
def test_3NF_from_1NF(database_fixture, determined_uuids):
    """Tests extracting foreign key attributes into separate tables (Level 1 to Level 3)."""
    database_3NF = database_fixture["data"]["3NF"]
    database_1NF = database_fixture["data"]["1NF"]
    schema = database_fixture["schema"]
    normalizer = Normalizer(database_1NF, schema)
    normalizer.convert_to_3NF_from_1NF()
    database_result_3NF = normalizer.database

    assert (
        database_result_3NF.keys() == database_3NF.keys()
    ), f"Result database has keys: {database_result_3NF.keys()}"

    for table_name in database_result_3NF:
        if database_result_3NF[table_name].index.name:
            database_result_3NF[table_name] = database_result_3NF[
                table_name
            ].reset_index()
        pd.testing.assert_frame_equal(
            make_df_standard_for_testing(
                database_result_3NF[table_name], database_3NF[table_name].columns
            ),
            make_df_standard_for_testing(
                database_3NF[table_name], database_3NF[table_name].columns
            ),
            check_like=True,
            check_dtype=False,
        )


@pytest.fixture
def schema_fixture(tmp_path):
    """Creates the schema from the docstring examples."""
    schema_data = yaml.safe_load("""
        Transaction:
          required_attributes:
            - amount
            - donor_id
            - recipient_id
          attributes:
            - amount
            - date
            - year
            - month
            - donor_id
            - recipient_id
          relations:
            - prefix: donor
              table: Transactor
              direction: forward
            - prefix: recipient
              table: Transactor
              direction: forward

        Transactor:
          required_attributes:
            - id
          attributes:
            - id
            - full_name
            - phone_number
          relations:
            - prefix: address
              table: Address
              direction: reverse
              reverse_relation_name: transactor_id
            - prefix: employer
              table: Membership
              direction: reverse
              reverse_relation_name: member_id
              relationship_column_values:
                membership_type: Member

        Address:
          required_attributes:
            - transactor_id
          attributes:
            - street_address
            - city
            - state
            - zip_code
            - transactor_id
          relations:
            - prefix: transactor
              table: Transactor
              direction: forward

        Membership:
          required_attributes:
            - member_id
            - organization_id
          attributes:
            - member_id
            - organization_id
            - membership_type
          relations:
            - prefix: member
              table: Transactor
              direction: forward
            - prefix: organization
              table: Transactor
              direction: forward
    """)

    schema_file = tmp_path / "schema.yaml"
    schema_file.write_text(yaml.dump(schema_data))
    return schema_file


@pytest.mark.parametrize(
    "description,input_data,table_name,relation_prefix,expected_output_data,expected_extracted_data,should_raise_error,error_message",
    [
        # Simple case
        (
            "simple",
            {
                "donor_id": [None],
                "donor--full_name": ["Fake Name"],
                "amount": [100],
                "recipient_id": ["708e0a88-c449-432f-b6cf-e11a0c681921"],
                "reported_state": ["CA"],
            },
            "Transaction",
            "donor",
            {
                "donor_id": ["00000000-0000-4000-8000-000000000001"],
                "amount": [100],
                "recipient_id": ["708e0a88-c449-432f-b6cf-e11a0c681921"],
                "reported_state": ["CA"],
            },
            {
                "id": ["00000000-0000-4000-8000-000000000001"],
                "full_name": ["Fake Name"],
                "reported_state": ["CA"],
            },
            False,
            None,
        ),
        # Map repeated IDs
        (
            "map-repeated-ids",
            {
                "donor_id": [None, None, None],
                "donor--full_name": ["Fake Name", "Fake Name", "Other Name"],
                "amount": [100, 100, 2.4],
                "recipient_id": [
                    "708e0a88-c449-432f-b6cf-e11a0c681921",
                    "f87748de-addb-4336-a42c-8f65f2d83990",
                    "708e0a88-c449-432f-b6cf-e11a0c681921",
                ],
                "reported_state": ["CA", "CA", "CA"],
            },
            "Transaction",
            "donor",
            {
                "donor_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "amount": [100, 100, 2.4],
                "recipient_id": [
                    "708e0a88-c449-432f-b6cf-e11a0c681921",
                    "f87748de-addb-4336-a42c-8f65f2d83990",
                    "708e0a88-c449-432f-b6cf-e11a0c681921",
                ],
                "reported_state": ["CA", "CA", "CA"],
            },
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "reported_state": ["CA", "CA"],
            },
            False,
            None,
        ),
        # Missing ID matches existing ID
        (
            "missing-id-matches-existing-id",
            {
                "donor_id": [
                    None,
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "donor--full_name": ["Fake Name", "Fake Name", "Other Name"],
                "amount": [100, 100, 2.4],
                "recipient_id": [
                    "708e0a88-c449-432f-b6cf-e11a0c681921",
                    "f87748de-addb-4336-a42c-8f65f2d83990",
                    "b4c70c73-964c-4d52-a48a-b4ad16d8b4b0",
                ],
                "reported_state": ["CA", "CA", "CA"],
            },
            "Transaction",
            "donor",
            {
                "donor_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "amount": [100, 100, 2.4],
                "recipient_id": [
                    "708e0a88-c449-432f-b6cf-e11a0c681921",
                    "f87748de-addb-4336-a42c-8f65f2d83990",
                    "b4c70c73-964c-4d52-a48a-b4ad16d8b4b0",
                ],
                "reported_state": ["CA", "CA", "CA"],
            },
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "reported_state": ["CA", "CA"],
            },
            False,
            None,
        ),
        # Reverse relation
        (
            "reverse-relation",
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "phone_number": ["1234567890", "1234567890"],
                "address--city": ["New York", "Los Angeles"],
                "address--state": ["NY", "CA"],
                "reported_state": ["CA", "CA"],
            },
            "Transactor",
            "address",
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "phone_number": ["1234567890", "1234567890"],
                "reported_state": ["CA", "CA"],
            },
            {
                "transactor_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "city": ["New York", "Los Angeles"],
                "state": ["NY", "CA"],
                "reported_state": ["CA", "CA"],
            },
            False,
            None,
        ),
        # Reverse relation with missing ID - should raise error
        (
            "reverse-relation-with-missing-id",
            {
                "id": [None, "00000000-0000-4000-8000-000000000002"],
                "full_name": ["Fake Name", "Other Name"],
                "phone_number": ["1234567890", "1234567890"],
                "address--city": ["New York", "Los Angeles"],
                "address--state": ["NY", "CA"],
                "reported_state": ["CA", "CA"],
            },
            "Transactor",
            "address",
            None,
            None,
            True,
            "Table 'Transactor' has no id column or has NaN ids.",
        ),
        # Reverse relation with null values
        (
            "reverse-relation-with-null-values",
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "phone_number": ["1234567890", "1234567890"],
                "address--city": [None, None],
                "address--state": [None, None],
                "reported_state": ["CA", "CA"],
            },
            "Transactor",
            "address",
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "phone_number": ["1234567890", "1234567890"],
                "reported_state": ["CA", "CA"],
            },
            {"transactor_id": [], "city": [], "state": [], "reported_state": []},
            False,
            None,
        ),
        # Relation table
        (
            "relation-table",
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "employer--organization--full_name": [
                    "Fake Organization",
                    "Fake Organization",
                ],
                "phone_number": ["1234567890", "1234567890"],
                "reported_state": ["CA", "CA"],
            },
            "Transactor",
            "employer",
            {
                "id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "full_name": ["Fake Name", "Other Name"],
                "phone_number": ["1234567890", "1234567890"],
                "reported_state": ["CA", "CA"],
            },
            {
                "member_id": [
                    "00000000-0000-4000-8000-000000000001",
                    "00000000-0000-4000-8000-000000000002",
                ],
                "organization--full_name": ["Fake Organization", "Fake Organization"],
                "membership_type": ["Member", "Member"],
                "reported_state": ["CA", "CA"],
            },
            False,
            None,
        ),
    ],
)
def test_split_prefixed_columns(
    description,
    input_data,
    table_name,
    relation_prefix,
    expected_output_data,
    expected_extracted_data,
    should_raise_error,
    error_message,
    schema_fixture,
    determined_uuids,
):
    """Tests _split_prefixed_columns based on docstring examples."""

    # Create normalizer
    database = {}
    normalizer = Normalizer(database, schema_fixture)

    # Create input table
    input_table = pd.DataFrame(input_data)

    if should_raise_error:
        with pytest.raises(ValueError, match=rf"{error_message}.*"):
            normalizer._split_prefixed_columns(input_table, table_name, relation_prefix)
    else:
        # Call the method
        result_table, result_extracted_table = normalizer._split_prefixed_columns(
            input_table, table_name, relation_prefix
        )

        # Create expected tables
        expected_output_table = pd.DataFrame(expected_output_data)
        expected_extracted_table = pd.DataFrame(expected_extracted_data)

        # Standardize for comparison
        result_table = make_df_standard_for_testing(result_table, result_table.columns)
        expected_output_table = make_df_standard_for_testing(
            expected_output_table, result_table.columns
        )

        result_extracted_table = make_df_standard_for_testing(
            result_extracted_table, result_extracted_table.columns
        )
        expected_extracted_table = make_df_standard_for_testing(
            expected_extracted_table, result_extracted_table.columns
        )

        # Assert results
        pd.testing.assert_frame_equal(
            result_table,
            expected_output_table,
            check_like=True,
            check_dtype=False,
        )

        pd.testing.assert_frame_equal(
            result_extracted_table,
            expected_extracted_table,
            check_like=True,
            check_dtype=False,
        )
