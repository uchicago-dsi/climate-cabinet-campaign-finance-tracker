"""Database utilities."""

from pathlib import Path

import duckdb


def create_table_from_parquet_to_db(
    con: duckdb.DuckDBPyConnection, table_path: Path
) -> None:
    """Load a table from a parquet file into a DuckDB database."""
    con.execute(
        f"CREATE TABLE {table_path.stem} AS "  # noqa: S608
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
    table_name: str,
    join_table_name: str,
    join_on_column: str,
    base_join_on_column: str = "id",
    column_prefix: str = "",
) -> None:
    """Create a view of two tables joined

    For many-to-one joins, the most common value of the join column is used.

    Args:
        con: DuckDB connection
        table_name: name of the table to join
        join_table_name: name of the table to join
        join_on_column: name of the column to join on
        base_join_on_column: name of the column to join on in the base table
        column_prefix: prefix to add to join table columns (e.g., 'address_')
    """
    # Get the columns of the join table
    join_table_columns = [
        col[1]
        for col in con.execute(f"PRAGMA table_info('{join_table_name}')").fetchall()
        if col[1] != join_on_column
    ]

    # Create select clauses with prefix
    join_table_select_clause = ",\n        ".join(
        [
            f"a.{column_prefix}{col} AS {column_prefix}{col}"
            for col in join_table_columns
        ]
    )
    join_table_group_by_clause = ",\n        ".join(
        [
            f"any_value({col}) FILTER (WHERE cnt = max_cnt) AS {column_prefix}{col}"
            for col in join_table_columns
        ]
    )
    join_table_final_select_clause = ",\n        ".join(
        [f"{col}" for col in join_table_columns]
    )

    # Drop existing view if it exists
    con.execute(f"DROP VIEW IF EXISTS {table_name}_joined")

    request = f"""
        CREATE VIEW {table_name}_joined AS
        SELECT
            t.*,
            {join_table_select_clause}
        FROM {table_name} t
        LEFT JOIN (
            SELECT
                {join_on_column},
                {join_table_group_by_clause}
            FROM (
                SELECT
                    {join_on_column},
                    {join_table_final_select_clause},
                    COUNT(*) AS cnt,
                    MAX(COUNT(*)) OVER (PARTITION BY {join_on_column}) AS max_cnt
                FROM {join_table_name}
                GROUP BY {join_on_column}, {", ".join(join_table_columns)}
            ) subq
            WHERE cnt = max_cnt
            GROUP BY {join_on_column}
        ) a
        ON t.{base_join_on_column} = a.{join_on_column}
    """  # noqa: S608

    return con.execute(request).fetchall()
