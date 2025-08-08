"""Database utilities."""

from pathlib import Path

import duckdb


def create_table_from_parquet_to_db(
    con: duckdb.DuckDBPyConnection, table_path: Path
) -> None:
    """Load a table from a parquet file into a DuckDB database."""
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table_path.stem} AS "  # noqa: S608
        f"SELECT * FROM read_parquet('{str(table_path.resolve())}')"  # noqa: S608
    )


def create_database_from_parquet(
    database_path: Path, database_dir: Path
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB database from a directory of parquet files."""
    con = duckdb.connect(str(database_path))
    for file in database_dir.glob("*.parquet"):
        create_table_from_parquet_to_db(con, file)
    return con


def create_join_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    left_table_name: str,
    right_table_name: str,
    right_join_on_column: str,
    left_join_on_column: str = "id",
    right_column_prefix: str = None,
) -> None:
    """Create a view of two tables joined

    For many-to-one joins, the most common value of the join column is used.

    Args:
        con: DuckDB connection
        view_name: name of the view to create
        left_table_name: name of the table to join
        right_table_name: name of the table to join
        right_join_on_column: name of the column to join on
        left_join_on_column: name of the column to join on in the left table
        right_column_prefix: prefix to add to join table columns (e.g., 'address_')
    """
    if right_column_prefix is None:
        right_column_prefix = right_table_name.lower() + "_"
    # Get the columns of the join table
    join_table_columns = [
        col[1]
        for col in con.execute(f"PRAGMA table_info('{right_table_name}')").fetchall()
        if col[1] != right_join_on_column
    ]

    # Create select clauses with prefix
    join_table_select_clause = ",\n        ".join(
        [
            f"a.{right_column_prefix}{col} AS {right_column_prefix}{col}"
            for col in join_table_columns
        ]
    )
    join_table_group_by_clause = ",\n        ".join(
        [
            f"any_value({col}) FILTER (WHERE cnt = max_cnt) AS {right_column_prefix}{col}"
            for col in join_table_columns
        ]
    )
    join_table_final_select_clause = ",\n        ".join(
        [f"{col}" for col in join_table_columns]
    )

    # Drop existing view if it exists
    con.execute(f"DROP VIEW IF EXISTS {view_name}")

    request = f"""
        CREATE VIEW {view_name} AS
        SELECT
            t.*,
            {join_table_select_clause}
        FROM {left_table_name} t
        LEFT JOIN (
            SELECT
                {right_join_on_column},
                {join_table_group_by_clause}
            FROM (
                SELECT
                    {right_join_on_column},
                    {join_table_final_select_clause},
                    COUNT(*) AS cnt,
                    MAX(COUNT(*)) OVER (PARTITION BY {right_join_on_column}) AS max_cnt
                FROM {right_table_name}
                GROUP BY {right_join_on_column}, {", ".join(join_table_columns)}
            ) subq
            WHERE cnt = max_cnt
            GROUP BY {right_join_on_column}
        ) a
        ON t.{left_join_on_column} = a.{right_join_on_column}
    """  # noqa: S608

    con.execute(request)


def create_employment_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create a view of transactor data with employment details from the membership section

    The membership table has two one-to-many relationships with the transactor table:
      1. m.member_id -> t.id
      2. m.organization_id -> t.id
    The goal of this is to create a view of the transactor table with details from
    a given transactor's most common employer.

    Args:
        con: DuckDB connection
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

    # Create select clauses with employer_ prefix
    membership_select_clause = ",\n        ".join(
        [f"employer_data.{col} AS employer_{col}" for col in membership_columns]
    )

    transactor_select_clause = ",\n        ".join(
        [f"employer_data.{col} AS employer_{col}" for col in transactor_columns]
    )

    # Drop existing view if it exists
    view_name = "employment_view"
    con.execute(f"DROP VIEW IF EXISTS {view_name}")

    request = f"""
        CREATE VIEW {view_name} AS
        SELECT
            t.*,
            {membership_select_clause},
            {transactor_select_clause}
        FROM Transactor t
        LEFT JOIN (
            SELECT
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
    """  # noqa: S608

    con.execute(request)


def create_combined_view(con: duckdb.DuckDBPyConnection) -> None:
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

    transactor_select_clause = ",\n        ".join(
        [f"employer_data.{col} AS employer_{col}" for col in transactor_columns]
    )

    address_select_clause = ",\n        ".join(
        [f"address_data.{col} AS address_{col}" for col in address_columns]
    )

    # Drop existing view if it exists
    view_name = "combined_view"
    con.execute(f"DROP VIEW IF EXISTS {view_name}")

    request = f"""
        CREATE VIEW {view_name} AS
        SELECT
            t.*,
            {membership_select_clause},
            {transactor_select_clause},
            {address_select_clause}
        FROM Transactor t
        LEFT JOIN (
            SELECT
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
    """  # noqa: S608

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
    create_combined_view(con)
    return con
