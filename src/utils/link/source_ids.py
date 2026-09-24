"""Deterministic linking of entities that share a source id

States are normalized separately, so an id system shared by several states
(e.g. FEC committee ids) can give the same entity a different UUID in each
state. After the databases are combined, every entity that shares a
(source, source_id, entity_table) key with another is merged into one canonical
entity id. This runs before probabilistic linkage.
"""

import duckdb

from utils.database import table_exists
from utils.ids import SOURCE_IDENTIFIER_TABLE, get_all_id_references
from utils.link.predict import replace_ids_with_canonical, update_source_identifiers

SOURCE_IDENTIFIER_LINKAGE_TABLE = "source_identifier_linkage"
_STEP_MAPPING_TABLE = "_source_identifier_linkage_step"
_STEP_ENTITY_MAPPING_TABLE = "_source_identifier_linkage_step_entity"
MAX_ITERATIONS = 100


def _find_shared_source_ids(con: duckdb.DuckDBPyConnection) -> int:
    """Create a table mapping entity ids that share a source id to a canonical id

    Within each key held by more than one entity, the smallest entity id is
    canonical. An entity in several such keys maps to the smallest canonical id.

    Returns:
        Number of entity ids that need to be remapped
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {_STEP_MAPPING_TABLE} AS
        WITH shared AS (
            SELECT source, source_id, entity_table, min(entity_id) AS canonical_id
            FROM {SOURCE_IDENTIFIER_TABLE}
            GROUP BY source, source_id, entity_table
            HAVING count(DISTINCT entity_id) > 1
        )
        SELECT
            s.entity_table,
            s.entity_id AS old_id,
            min(shared.canonical_id) AS canonical_id
        FROM {SOURCE_IDENTIFIER_TABLE} AS s
        JOIN shared USING (source, source_id, entity_table)
        WHERE s.entity_id <> shared.canonical_id
        GROUP BY s.entity_table, s.entity_id
        """
    )
    return con.execute(f"SELECT count(*) FROM {_STEP_MAPPING_TABLE}").fetchone()[0]


def link_by_source_identifiers(con: duckdb.DuckDBPyConnection) -> int:
    """Merge entities that share a source id into one canonical entity id

    Every id column referencing a merged entity, and the SourceIdentifier table
    itself, is updated in place. Each remapping is recorded in the
    source_identifier_linkage table. Merging repeats until no source id is held
    by more than one entity, so chains (A shares a source id with B, and B with
    C) end up with one id.

    Args:
        con: DuckDB connection to the combined database

    Returns:
        Number of distinct entity ids that were remapped

    Raises:
        RuntimeError: if merging does not converge within MAX_ITERATIONS
    """
    if not table_exists(con, SOURCE_IDENTIFIER_TABLE):
        return 0
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {SOURCE_IDENTIFIER_LINKAGE_TABLE} (
            iteration INTEGER,
            entity_table VARCHAR,
            old_id VARCHAR,
            canonical_id VARCHAR
        )
        """
    )
    for iteration in range(MAX_ITERATIONS):
        if _find_shared_source_ids(con) == 0:
            return con.execute(
                f"SELECT count(DISTINCT old_id) FROM {SOURCE_IDENTIFIER_LINKAGE_TABLE}"
            ).fetchone()[0]
        con.execute(
            f"""
            INSERT INTO {SOURCE_IDENTIFIER_LINKAGE_TABLE}
            SELECT {iteration}, entity_table, old_id, canonical_id
            FROM {_STEP_MAPPING_TABLE}
            """
        )
        entity_tables = [
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT entity_table FROM {_STEP_MAPPING_TABLE}"
            ).fetchall()
        ]
        for entity_table in entity_tables:
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE {_STEP_ENTITY_MAPPING_TABLE} AS
                SELECT old_id, canonical_id FROM {_STEP_MAPPING_TABLE}
                WHERE entity_table = '{entity_table}'
                """
            )
            for table_name, id_columns in get_all_id_references(entity_table).items():
                replace_ids_with_canonical(
                    con,
                    table_name,
                    id_columns,
                    mapping_table=_STEP_ENTITY_MAPPING_TABLE,
                )
            update_source_identifiers(
                con, entity_table, mapping_table=_STEP_ENTITY_MAPPING_TABLE
            )
    raise RuntimeError(
        f"Linking by source identifiers did not converge in {MAX_ITERATIONS} rounds"
    )
