"""Code for management and creation of unique identifiers"""

import re
import uuid

import pandas as pd

from utils.schema import TableSchema

# Precompiled regex for UUID4 validation
UUID4_REGEX = re.compile(
    r"^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$",
    re.IGNORECASE,
)

UUIDMapping = dict[tuple[int | str | None, int | None, str | None, str], str]


def add_uuids_to_table(table: pd.DataFrame, id_column: str) -> None:
    """Ensure every row in table has a UUID in the given column.

    Args:
        table: DataFrame where each row requires a UUID.
        id_column: Name of the column to be populated with UUIDs.

    Modifies:
        table: Assigns UUIDs to rows where `id_column` is missing.
    """
    missing_mask = table[id_column].isna()
    table.loc[missing_mask, id_column] = [
        str(uuid.uuid4()) for _ in range(missing_mask.sum())
    ]


def map_ids_to_uuids(
    table: pd.DataFrame,
    table_name: str,
    id_mapping: UUIDMapping,
    id_column: str = "id",
    mask: pd.Series = None,
) -> None:
    """Replace values in `id_column` if their row exists in `id_mapping`.

    Args:
        table: DataFrame with an existing `id_column`.
        table_name: Table name used in the mapping.
        id_mapping: Mapping of (raw id, year, state, table_name) to UUIDs.
        id_column: Name of the column to replace with UUIDs.
        mask: Optional boolean mask to filter which rows to update

    Modifies:
        table: Updates `id_column` with mapped UUIDs where applicable.
    """
    if mask is None:
        mask = pd.Series(True, index=table.index)
    table.loc[mask, id_column] = table.loc[mask].apply(
        lambda row: id_mapping.get(
            (
                row[id_column],
                row.get("year"),
                row.get("state"),
                table_name,
            ),
            row[id_column],
        ),
        axis=1,
    )


def create_new_uuid_mapping(
    table: pd.DataFrame, table_name: str, id_column: str = "id"
) -> UUIDMapping:
    """Create UUIDs for rows with invalid or missing UUIDs in `id_column`.

    Args:
        table: DataFrame with an existing `id_column`.
        table_name: Name of the table (used in mapping).
        id_column: Name of the `id` column, defaults to 'id'.

    Returns:
        A dictionary mapping (raw_id, year, state, table_name) → new UUID.
    """
    new_mappings = {
        (row[id_column], row.get("year"), row.get("state"), table_name): str(
            uuid.uuid4()
        )
        for _, row in table.iterrows()
    }
    return new_mappings


def get_raw_ids_mask(table: pd.DataFrame, id_column: str) -> pd.Series:
    """Create a mask that is true where raw ids exist

    Args:
        table: DataFrame with existing `id_column`.
        id_column: Name of the `id` column
    """
    raw_ids_mask = ~table[id_column].astype(str).str.match(UUID4_REGEX, na=False)
    return raw_ids_mask


def handle_id_column(
    table: pd.DataFrame,
    table_schema: TableSchema,
    id_mapping: UUIDMapping,
    id_column: str = "id",
) -> tuple[pd.DataFrame, UUIDMapping]:
    """Ensure each 'id' value in table is a uuid and all raw ids are mapped

    Args:
        table: DataFrame with or without `id_column`.
        table_schema: Schema defining properties of table.
        id_mapping: Mapping of (raw id, year, state, table_name) to UUIDs.
        id_column: Name of the column to replace with UUIDs.

    Returns:
        table: Updates `id_column` with mapped UUIDs where applicable.
        id_mapping: Updates `id_mapping` with new id mappings
    """
    table_type = table_schema.table_type
    if id_column not in table_schema.attributes:
        return table
    if id_column not in table.columns:
        table[id_column] = None

    add_uuids_to_table(table, id_column)
    map_ids_to_uuids(table, table_type, id_mapping, id_column)
    raw_ids_mask = get_raw_ids_mask(table, id_column)
    new_mappings = create_new_uuid_mapping(
        table.loc[raw_ids_mask], table_type, id_column
    )
    id_mapping.update(new_mappings)
    # this can be done more efficiently if we filter table here
    map_ids_to_uuids(table, table_type, new_mappings, id_column, mask=raw_ids_mask)
    return table, id_mapping
