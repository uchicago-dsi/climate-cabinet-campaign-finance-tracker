"""Database utilities."""

from pathlib import Path

import duckdb

from utils.constants import DEFAULT_SCHEMA_PATH
from utils.schema import DataSchema


def _get_data_schema() -> DataSchema:
    return DataSchema(DEFAULT_SCHEMA_PATH)


# Mapping from YAML type strings to DuckDB SQL types
_TYPE_MAP: dict[str, str] = {
    "string": "VARCHAR",
    "str": "VARCHAR",
    "Int8": "TINYINT",
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",
    "Float32": "REAL",
    "Float64": "DOUBLE",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "date": "DATE",
}


def _duckdb_type(schema_type: str) -> str:
    """Convert a YAML schema type string to a DuckDB SQL type.

    Unrecognised types default to VARCHAR.
    """
    return _TYPE_MAP.get(schema_type, "VARCHAR")


def connect_duckdb(database_path: Path | str) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection to *database_path*.

    The database file will be created if it does not already exist.

    Args:
        database_path: File path to the DuckDB database.

    Returns:
        An open :class:`duckdb.DuckDBPyConnection` instance.
    """
    database_path = Path(database_path)
    return duckdb.connect(str(database_path))


def create_table_from_parquet_to_db(
    con: duckdb.DuckDBPyConnection, table_path: Path
) -> None:
    """Load a table from a parquet file into a DuckDB database."""
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table_path.stem} AS "
        f"SELECT * FROM read_parquet('{str(table_path.resolve())}')"
    )


def create_database_from_parquet(
    database_path: Path, database_dir: Path
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB database from a directory of parquet files."""
    con = duckdb.connect(str(database_path))
    for file in database_dir.glob("*.parquet"):
        create_table_from_parquet_to_db(con, file)
    return con


def create_or_append_parquet_to_db(
    con: duckdb.DuckDBPyConnection, table_path: Path
) -> None:
    """Create or append a table from a parquet file to a DuckDB database.

    If the table with name table_path.stem, already exists, the data in
    the parquet file located at table_path will be appended. Otherwise,
    a new table will be created.

    Args:
        con: DuckDB connection
        table_path: Path to the parquet file to create or append
    Modifies:
        DuckDB database will now have a table 'table_path.stem' containing
        data from the parquet file.
    """
    table_exists = (
        con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_path.stem],
        )
        .fetchdf()
        .iloc[0, 0]
        > 0
    )
    if table_exists:
        # Ensure we insert columns in the same order as the existing table to avoid
        # positional mismatches (DuckDB maps INSERT values positionally).
        # Fetch column names from the existing DuckDB table.
        table_columns = [
            col[1]
            for col in con.execute(f"PRAGMA table_info('{table_path.stem}')").fetchall()
        ]

        # Build a comma-separated column list
        column_list = ", ".join(table_columns)

        # Insert data selecting the same columns from the parquet file, allowing
        # DuckDB to match by name rather than position.
        con.execute(
            f"INSERT INTO {table_path.stem} ({column_list}) "
            f"SELECT {column_list} FROM read_parquet('{str(table_path.resolve())}')"
        )
    else:
        # Create the table explicitly using the YAML schema definitions so column
        # types are consistent regardless of the first Parquet file encountered.

        data_schema = _get_data_schema()
        try:
            table_schema = data_schema.schema[table_path.stem]
        except KeyError as exc:
            raise KeyError(
                f"Table '{table_path.stem}' not found in YAML schema located at {DEFAULT_SCHEMA_PATH}."
            ) from exc

        # Build column definitions "name TYPE"
        column_defs = ", ".join(
            f"{col} {_duckdb_type(dtype)}" for col, dtype in table_schema.types.items()
        )

        # Create the empty table with explicit schema
        con.execute(f"CREATE TABLE {table_path.stem} ({column_defs})")

        # Insert data from the Parquet file into the table, aligning by column
        # names present in the YAML schema.
        column_list = ", ".join(table_schema.types.keys())
        con.execute(
            f"INSERT INTO {table_path.stem} ({column_list}) "
            f"SELECT {column_list} FROM read_parquet('{str(table_path.resolve())}')"
        )


def create_database_from_nested_parquet_directories(
    database_path: Path, database_dir: Path, overwrite: bool = False
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB database from a nested directory of parquet files.

    The database_path should be a path to a directory containing only
    more directories, each containing parquet files whose names
    are the tables to which they should be added. Example:
    database_dir/
    ├── state1/
    │   ├── table1.parquet
    │   ├── table2.parquet
    ├── state2/
    │   ├── table3.parquet
    │   ├── table4.parquet

    Args:
        database_path: Path to the DuckDB database
        database_dir: Path to the directory containing the parquet files
        overwrite: Whether to overwrite existing tables

    Returns:
        DuckDB connection
    """
    con = duckdb.connect(str(database_path))
    if overwrite:
        # drop all data from tables
        con.execute("DROP TABLE IF EXISTS *")
    for state_dir in database_dir.iterdir():
        for file in state_dir.glob("*.parquet"):
            try:
                create_or_append_parquet_to_db(con, file)
            except Exception as e:
                print(f"Error creating table {state_dir.name} {file.stem}:\n {e}")
    return con


def create_transactor_detailed_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create a view of transactor data with both employment and address details.

    This view combines:
    - All transactor columns
    - Employer columns (from employment_view)
    - Address columns (from address view)

    Args:
        con: DuckDB connection. Should have the following tables and fields:
            - Transactor: id
            - Membership: member_id, organization_id, membership_type
            - Address: transactor_id
    """
    # Get the columns of the Membership table (excluding member_id and organization_id)
    membership_columns = [
        col[1]
        for col in con.execute("PRAGMA table_info('Membership')").fetchall()
        if col[1] not in ["member_id", "organization_id"]
    ]

    # Get the columns of the Transactor table (excluding id)
    transactor_columns = [
        col[1]
        for col in con.execute("PRAGMA table_info('Transactor')").fetchall()
        if col[1] != "id"
    ]

    # Get the columns of the Address table (excluding transactor_id)
    address_columns = [
        col[1]
        for col in con.execute("PRAGMA table_info('Address')").fetchall()
        if col[1] != "transactor_id"
    ]

    # Create select clauses with prefixes
    membership_select_clause = ",\n        ".join(
        [f"employer_data.{col} AS employer_{col}" for col in membership_columns]
    )

    employer_select_clause = ",\n        ".join(
        [f"employer_data.{col} AS employer_{col}" for col in transactor_columns]
    )

    address_select_clause = ",\n        ".join(
        [f"address_data.{col} AS address_{col}" for col in address_columns]
    )

    # Drop existing view if it exists
    view_name = "transactor_detailed_view"
    con.execute(f"DROP VIEW IF EXISTS {view_name}")

    request = f"""
        CREATE TABLE {view_name} AS
        SELECT
            t.*,
            {membership_select_clause},
            {employer_select_clause},
            {address_select_clause}
        FROM Transactor t
        LEFT JOIN (
            SELECT
                -- If there are multiple membership and membership employer details for one transactor,
                -- use the one with the highest count
                member_id,
                {", ".join([f"any_value({col}) FILTER (WHERE cnt = max_cnt) AS {col}" for col in membership_columns])},
                {", ".join([f"any_value({col}) FILTER (WHERE cnt = max_cnt) AS {col}" for col in transactor_columns])}
            FROM (
                SELECT
                    member_id,
                    {", ".join([f"{col}" for col in membership_columns])},
                    {", ".join([f"{col}" for col in transactor_columns])},
                    COUNT(*) AS cnt,
                    MAX(COUNT(*)) OVER (PARTITION BY member_id) AS max_cnt
                FROM (
                    SELECT
                        Membership.member_id,
                        {", ".join([f"Membership.{col}" for col in membership_columns])},
                        {", ".join([f"Transactor.{col}" for col in transactor_columns])}
                    FROM Membership
                    JOIN Transactor ON Membership.organization_id = Transactor.id
                    WHERE Membership.membership_type = 'Employee'
                ) joined_data
                GROUP BY member_id, {", ".join([f"{col}" for col in membership_columns])}, {", ".join([f"{col}" for col in transactor_columns])}
            ) subq
            WHERE cnt = max_cnt
            GROUP BY member_id
        ) employer_data
        ON t.id = employer_data.member_id
        LEFT JOIN (
            SELECT
                -- If there are multiple address details for one transactor, keep
                -- the one with the highest count
                transactor_id,
                {", ".join([f"any_value({col}) FILTER (WHERE cnt = max_cnt) AS {col}" for col in address_columns])}
            FROM (
                SELECT
                    transactor_id,
                    {", ".join([f"{col}" for col in address_columns])},
                    COUNT(*) AS cnt,
                    MAX(COUNT(*)) OVER (PARTITION BY transactor_id) AS max_cnt
                FROM Address
                GROUP BY transactor_id, {", ".join([f"{col}" for col in address_columns])}
            ) subq
            WHERE cnt = max_cnt
            GROUP BY transactor_id
        ) address_data
        ON t.id = address_data.transactor_id
    """

    con.execute(request)


def create_transactor_details_view_from_parquet(
    database_path: Path, database_dir: Path
) -> duckdb.DuckDBPyConnection:
    """Idempotently make duckdb view of transactor details (employment and address info).

    Args:
        database_path: Path to the DuckDB database
        database_dir: Path to the directory containing the parquet files

    Returns:
        DuckDB connection
    """
    con = create_database_from_parquet(database_path, database_dir)
    create_transactor_detailed_view(con)
    return con


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Check whether *table_name* exists in the connected database."""
    (exists,) = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return exists > 0
