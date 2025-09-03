"""Predict module for running Splink inference and updating canonical IDs.

This module provides a set of composable helper functions that:
1. Connect to an existing (or new) DuckDB database.
2. Ensure that the ``transactor_detailed_view`` view (see ``utils.database.create_transactor_detailed_view``)
   is present – creating it on-demand when missing.
3. Load a trained Splink model from the JSON produced by ``train.py``.
4. Execute record-linkage inference to obtain pairwise match probabilities **and**
   clusters.
5. Persist a canonical ID mapping to a table called ``linkage_mapping`` inside the
   same database.
6. (Optionally) Update any tables so that their ``id`` column is replaced by the
   canonical IDs.

All public helpers are deliberately small, making the module easy to test and
refactor while following the project code-quality guidelines.

Examples:
>>> from pathlib import Path
>>> from utils.link.predict import run_linkage_pipeline
>>> run_linkage_pipeline(
...     database_path=Path("data/duckdb/my_db.duckdb"),
...     model_path=Path("models/test-v2.json"),
...     parquet_dir=Path("data/parquet"),  # Only required if DB needs boot-strapping
... )
"""

import json
from pathlib import Path

import duckdb
import pandas as pd
from splink import DuckDBAPI, Linker

from utils.database import (
    connect_duckdb,
    create_transactor_detailed_view,
    create_transactor_details_view_from_parquet,
    table_exists,
)
from utils.ids import get_all_id_references

# ---------------------------------------------------------------------------
# Splink helpers
# ---------------------------------------------------------------------------


def load_linker(
    con: duckdb.DuckDBPyConnection,
    settings_path: Path | str,
    table_name: str = "transactor_detailed_view",
) -> Linker:
    """Instantiate a Splink :class:`~splink.Linker` from a saved settings JSON.

    Args:
        con: An active DuckDB connection.
        settings_path: Path to the JSON created by
            :py:meth:`splink.Linker.misc.save_model_to_json` during training.
        table_name: Name of the table/view containing the records to be deduplicated.

    Returns:
        A ready-to-use :class:`splink.Linker` instance.
    """
    settings_path = Path(settings_path)
    with settings_path.open() as fp:
        settings = json.load(fp)

    db_api = DuckDBAPI(con)
    return Linker(table_name, settings, db_api=db_api)


def cluster_transactors(
    linker: Linker,
    *,
    threshold_match_probability: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run clustering.

    Args:
        linker: A configured Splink ``Linker``.
        threshold_match_probability: Records with a match probability **>=** this
            value are considered links when forming clusters.

    Returns:
        A DataFrame of clusters
    """
    df_predictions = linker.predict()

    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predictions, threshold_match_probability=threshold_match_probability
    )
    return df_clusters


# ---------------------------------------------------------------------------
# ID mapping + replacement helpers
# ---------------------------------------------------------------------------


def _create_mapping_of_old_ids_to_cluster_ids(
    linkage_clusters: pd.DataFrame,
) -> pd.DataFrame:
    """Return a 2-column DataFrame mapping *old_id* ➔ *canonical_id*.

    The canonical ID is chosen as the minimum record ID per cluster, ensuring a
    deterministic and intuitive representative.
    """
    mapping = (
        # get all the old unique ids for each cluster
        linkage_clusters.groupby("cluster_id")["unique_id"]
        .apply(
            # choose the minimum old unique id as the canonical id
            lambda old_ids: pd.Series(
                {"canonical_id": old_ids.min(), "old_ids": list(old_ids)}
            )
        )
        # explode the list of old unique ids into separate rows
        .explode("old_ids")
        .reset_index()
        # rename columns, now each row has a single old id and a canonical id
        .rename(columns={"old_ids": "old_id"})[["old_id", "canonical_id"]]
    )
    return mapping


def upsert_linkage_mapping(
    con: duckdb.DuckDBPyConnection,
    linkage_clusters: pd.DataFrame,
    *,
    mapping_table: str = "linkage_mapping",
    replace: bool = True,
) -> None:
    """Persist a mapping of *old_id* ➔ *canonical_id* to *mapping_table*.

    Args:
        con: DuckDB connection.
        linkage_clusters: DataFrame produced by :func:`predict_and_cluster`.
        mapping_table: Target table name.
        replace: Whether to overwrite an existing table. If *False*, rows are
            **appended** (duplicates may occur).
    """
    df_mapping = _create_mapping_of_old_ids_to_cluster_ids(linkage_clusters)

    con.register("_tmp_mapping", df_mapping)
    create_stmt = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
    con.execute(f"{create_stmt} {mapping_table} AS SELECT * FROM _tmp_mapping")
    con.unregister("_tmp_mapping")


def replace_ids_with_canonical(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    id_columns: list[str],
    mapping_table: str = "linkage_mapping",
) -> None:
    """Replace *id_columns* values in *table_name* with their canonical counterpart.

    The update is performed in-place via SQL ``UPDATE`` statements.

    Args:
        con: DuckDB connection.
        table_name: Name of the table to update.
        id_columns: Columns to update.
        mapping_table: Name of the table produced by :func:`upsert_linkage_mapping`.
    """
    cols = [
        row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    ]
    for id_column in id_columns:
        # Verify column exists to avoid run-time errors
        if id_column not in cols:
            continue

        con.execute(
            f"""
            UPDATE {table_name} AS t
            SET {id_column} = m.canonical_id
            FROM {mapping_table} AS m
            WHERE t.{id_column} = m.old_id
            """
        )


def run_linkage_pipeline(
    *,
    duckdb_path: Path | str,
    model_path: Path | str,
    parquet_dir: Path | str | None = None,
    threshold: float = 0.95,
) -> None:
    """Run the full linkage pipeline end-to-end.

    Args:
        duckdb_path: DuckDB database file.
        model_path: Path to the trained Splink settings JSON.
        parquet_dir: Optional path to a directory containing Parquet files. If provided,
            any existing tables will be overwritten by the data in the parquet files.
        threshold: Match-probability threshold used when forming clusters.
    """
    # Ensure connection to duckdb database with transactor_detailed_view
    con = connect_duckdb(duckdb_path)
    if parquet_dir is not None:
        print("Overwriting existing tables")
        con.execute("DROP TABLE IF EXISTS *")
        create_transactor_details_view_from_parquet(duckdb_path, parquet_dir)
    elif not table_exists(con, "transactor_detailed_view"):
        print("Creating transactor_detailed_view")
        create_transactor_detailed_view(con)

    # Load linker and run prediction and clustering
    linker = load_linker(con, model_path)
    clusters = cluster_transactors(linker, threshold_match_probability=threshold)

    upsert_linkage_mapping(con, clusters)

    id_references = get_all_id_references(table_name="Transactor")
    for table_name, id_columns in id_references.items():
        replace_ids_with_canonical(con, table_name=table_name, id_columns=id_columns)
