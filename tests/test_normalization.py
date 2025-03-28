from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest
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
