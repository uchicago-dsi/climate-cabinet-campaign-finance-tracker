"""Code for management and creation of unique identifiers

Every entity gets a UUID. Ids provided by sources (a committee registration
number, a filer id, ...) are mapped to those UUIDs, keyed by the id system that
issued them ("source"), the id itself, and the table of the entity it
identifies. Keying by source means the same number from two id systems maps to
two entities. The mapping is saved as the SourceIdentifier table.
"""

import re
import uuid
from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from utils.constants import DEFAULT_SCHEMA_PATH, ID_SOURCE_SUFFIX
from utils.io import FileFormat, save_database
from utils.schema import DataSchema, TableSchema
from utils.sources import SourceRegistry, get_default_registry, legacy_source

# Precompiled regex for UUID4 validation
UUID4_REGEX = re.compile(
    r"^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$",
    re.IGNORECASE,
)

# (source, source_id, entity_table)
SourceKey = tuple[str, str, str]

SOURCE_IDENTIFIER_TABLE = "SourceIdentifier"
SOURCE_IDENTIFIER_COLUMNS = [
    "entity_id",
    "entity_table",
    "source",
    "source_id",
    "reported_state",
    "earliest_known_date",
    "latest_known_date",
]
LEGACY_ID_MAPPING_FILE = "id_mapping.tsv"


class SourceIdentifierMapping:
    """Mapping of source ids to the UUIDs of the entities they identify

    Each (source, source_id, entity_table) key maps to exactly one entity_id.
    One entity may have any number of keys.
    """

    def __init__(self) -> None:
        """Create an empty mapping"""
        self._entity_ids: dict[SourceKey, str] = {}
        self._reported_states: dict[SourceKey, str | None] = {}

    def __len__(self) -> int:
        """Number of source ids in the mapping"""
        return len(self._entity_ids)

    def __contains__(self, key: SourceKey) -> bool:
        """Whether key is in the mapping"""
        return key in self._entity_ids

    def __getitem__(self, key: SourceKey) -> str:
        """Entity id that key maps to"""
        return self._entity_ids[key]

    def __iter__(self) -> Iterator[SourceKey]:
        """Iterate over keys"""
        return iter(self._entity_ids)

    def get(self, key: SourceKey, default: str | None = None) -> str | None:
        """Entity id that key maps to, or default if key is not in the mapping"""
        return self._entity_ids.get(key, default)

    def reported_state(self, key: SourceKey) -> str | None:
        """State whose data the source id was first seen in"""
        return self._reported_states.get(key)

    def add(
        self, key: SourceKey, entity_id: str, reported_state: str | None = None
    ) -> None:
        """Map key to entity_id, keeping the first reported_state seen for key"""
        self._entity_ids[key] = entity_id
        self._reported_states.setdefault(key, reported_state)

    def update(self, other: "SourceIdentifierMapping") -> None:
        """Add all keys from other, overwriting entity ids of shared keys"""
        for key in other:
            self.add(key, other[key], other.reported_state(key))

    def copy(self) -> "SourceIdentifierMapping":
        """Shallow copy of the mapping"""
        copied = SourceIdentifierMapping()
        copied.update(self)
        return copied

    def to_table(self) -> pd.DataFrame:
        """Mapping as a SourceIdentifier table"""
        rows = [
            {
                "entity_id": entity_id,
                "entity_table": entity_table,
                "source": source,
                "source_id": source_id,
                "reported_state": self._reported_states.get(
                    (source, source_id, entity_table)
                ),
                "earliest_known_date": None,
                "latest_known_date": None,
            }
            for (source, source_id, entity_table), entity_id in self._entity_ids.items()
        ]
        return pd.DataFrame(rows, columns=SOURCE_IDENTIFIER_COLUMNS)

    @classmethod
    def from_table(cls, table: pd.DataFrame) -> "SourceIdentifierMapping":
        """Build a mapping from a SourceIdentifier table"""
        mapping = cls()
        for row in table.itertuples(index=False):
            reported_state = row.reported_state
            mapping.add(
                (row.source, str(row.source_id), row.entity_table),
                row.entity_id,
                None if pd.isna(reported_state) else reported_state,
            )
        return mapping


def normalize_id_to_string(value: int | float | str | None) -> str:
    """Convert an ID value to a consistent string representation.

    Handles the case where numeric IDs might be stored as int or float,
    ensuring they always map to the same string key.
    """
    if pd.isna(value):
        return value
    # If it's a numeric type, convert to int first to remove decimal places
    try:
        if isinstance(value, int | float) and not pd.isna(value):
            # Convert to int if it's a whole number, otherwise keep as float
            if float(value).is_integer():
                return str(int(float(value)))
            else:
                return str(float(value))
    except (ValueError, TypeError):
        pass

    return str(value)


def get_id_sources(table: pd.DataFrame, id_column: str) -> pd.Series:
    """Source of each value in id_column

    Sources come from the '<id_column>_source' column added during
    standardization. Rows without one fall back to the legacy source for their
    reported_state.

    Args:
        table: DataFrame with id_column
        id_column: name of the id column

    Returns:
        Series of source names aligned with table's index
    """
    if "reported_state" in table.columns:
        legacy_sources = table["reported_state"].map(legacy_source)
    else:
        legacy_sources = pd.Series(legacy_source(None), index=table.index)
    source_column = f"{id_column}{ID_SOURCE_SUFFIX}"
    if source_column not in table.columns:
        return legacy_sources.astype(object)
    return (
        table[source_column]
        .astype(object)
        .where(table[source_column].notna(), legacy_sources)
    )


def replace_null_ids_with_uuids(table: pd.DataFrame, id_column: str) -> None:
    """For each null value in id_column, replace it with a new UUID

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
    id_mapping: SourceIdentifierMapping,
    id_column: str = "id",
    mask: pd.Series = None,
) -> None:
    """Replace values in `id_column` if their source id exists in `id_mapping`.

    Args:
        table: DataFrame with an existing `id_column`.
        table_name: Name of the table the ids identify rows of.
        id_mapping: Mapping of (source, source_id, table_name) to UUIDs.
        id_column: Name of the column to replace with UUIDs.
        mask: Optional boolean mask to filter which rows to update

    Modifies:
        table: Updates `id_column` with mapped UUIDs where applicable.
    """
    if mask is None:
        mask = pd.Series(True, index=table.index)
    sources = get_id_sources(table, id_column)
    raw_ids = [normalize_id_to_string(value) for value in table[id_column]]
    table[id_column] = pd.Series(raw_ids, index=table.index, dtype="string")
    table.loc[mask, id_column] = [
        raw_id
        if pd.isna(raw_id)
        else id_mapping.get((source, raw_id, table_name), raw_id)
        for raw_id, source in zip(
            table.loc[mask, id_column], sources[mask], strict=True
        )
    ]


def create_new_uuid_mapping(
    table: pd.DataFrame,
    table_name: str,
    id_column: str = "id",
    existing_mapping: SourceIdentifierMapping | None = None,
) -> SourceIdentifierMapping:
    """Create UUIDs for source ids in `id_column` that have none yet.

    This function will skip any rows for which the id_column is NA,
    a valid uuid, or already exists in the existing_mapping.

    Args:
        table: DataFrame with an existing `id_column`.
        table_name: Name of the table the ids identify rows of.
        id_column: Name of the `id` column, defaults to 'id'.
        existing_mapping: Existing ID mapping to check against.

    Returns:
        Mapping of (source, source_id, table_name) → new UUID.
    """
    if existing_mapping is None:
        existing_mapping = SourceIdentifierMapping()
    raw_ids_mask = get_raw_ids_mask(table, id_column)
    raw_table = table.loc[raw_ids_mask]
    sources = get_id_sources(raw_table, id_column)
    if "reported_state" in raw_table.columns:
        reported_states = raw_table["reported_state"]
    else:
        reported_states = pd.Series(None, index=raw_table.index)

    new_mapping = SourceIdentifierMapping()
    for raw_id, source, reported_state in zip(
        raw_table[id_column], sources, reported_states, strict=True
    ):
        key = (source, normalize_id_to_string(raw_id), table_name)
        if key not in existing_mapping and key not in new_mapping:
            new_mapping.add(
                key,
                str(uuid.uuid4()),
                None if pd.isna(reported_state) else reported_state,
            )
    return new_mapping


def get_raw_ids_mask(table: pd.DataFrame, id_column: str) -> pd.Series:
    """Create a mask that is true where raw ids exist

    Args:
        table: DataFrame with existing `id_column`.
        id_column: Name of the `id` column
    """
    # na=True because a null is *not* a raw id
    not_raw_id_mask = table[id_column].astype("string").str.match(UUID4_REGEX, na=True)
    # negate to get mask where all raw ids are true
    return ~not_raw_id_mask


def handle_existing_ids(
    table: pd.DataFrame,
    table_name: str,
    id_mapping: SourceIdentifierMapping,
    id_column: str,
) -> None:
    """Ensure all non null ids in id_column are mapped to a uuid in id_mapping

    Args:
        table: DataFrame with `id_column`.
        table_name: Name of table name the id represents.
        id_mapping: Mapping of (source, source_id, table_name) to UUIDs.
        id_column: Name of the column to replace with UUIDs.

    Modifies:
        table: Updates `id_column` with mapped UUIDs where applicable.
        id_mapping: Updates `id_mapping` with new id mappings
    """
    map_ids_to_uuids(table, table_name, id_mapping, id_column)
    raw_ids_mask = get_raw_ids_mask(table, id_column)
    new_mappings = create_new_uuid_mapping(
        table.loc[raw_ids_mask], table_name, id_column, id_mapping
    )
    id_mapping.update(new_mappings)
    map_ids_to_uuids(table, table_name, new_mappings, id_column, mask=raw_ids_mask)


def handle_id_column(
    table: pd.DataFrame,
    table_schema: TableSchema,
    id_table_name: str,
    id_mapping: SourceIdentifierMapping,
    id_column: str = "id",
) -> None:
    """Ensure each 'id' value in table is a uuid and all raw ids are mapped

    Args:
        table: DataFrame with or without `id_column`.
        table_schema: Schema defining properties of table.
        id_table_name: Name of the table that the id identifies a row of.
        id_mapping: Mapping of (source, source_id, table_name) to UUIDs.
        id_column: Name of the column to replace with UUIDs.

    Modifies:
        table: Creates/Updates `id_column` to have UUIDs
        id_mapping: Updates `id_mapping` with new id mappings
    """
    if id_column not in table_schema.attributes:
        return None
    if id_column not in table.columns:
        table[id_column] = None

    replace_null_ids_with_uuids(table, id_column)
    handle_existing_ids(table, id_table_name, id_mapping, id_column)


def drop_id_source_columns(table: pd.DataFrame) -> pd.DataFrame:
    """Remove the '<id column>_source' columns once ids are mapped to UUIDs"""
    return table.drop(
        columns=[
            column for column in table.columns if column.endswith(ID_SOURCE_SUFFIX)
        ]
    )


def save_source_identifiers(
    id_mapping: SourceIdentifierMapping,
    directory: Path,
    format: FileFormat = "parquet",
) -> None:
    """Save a mapping as the SourceIdentifier table in directory

    Args:
        id_mapping: Mapping to save
        directory: Directory of the normalized database
        format: File format of the database
    """
    if len(id_mapping) == 0:
        return
    save_database(
        {SOURCE_IDENTIFIER_TABLE: id_mapping.to_table()},
        directory,
        format=format,
        mode="overwrite",
    )


def load_source_identifiers(
    directory: Path,
    format: FileFormat = "parquet",
    legacy_id_source_rules: list | None = None,
    source_registry: SourceRegistry | None = None,
) -> SourceIdentifierMapping:
    """Load the SourceIdentifier table from directory, if any

    If there is no SourceIdentifier table but there is an id_mapping.tsv from an
    earlier version of the pipeline, it is migrated so existing UUIDs are kept.

    Args:
        directory: Directory of the normalized database
        format: File format of the database
        legacy_id_source_rules: id source rules declared in the state's config,
            used to infer sources when migrating id_mapping.tsv
        source_registry: registry used when migrating. Defaults to sources.yaml.

    Returns:
        The saved mapping, a migrated mapping, or an empty mapping
    """
    table_path = directory / f"{SOURCE_IDENTIFIER_TABLE}.{format}"
    if table_path.exists():
        if format == "parquet":
            table = pd.read_parquet(table_path)
        else:
            table = pd.read_csv(table_path, dtype=str)
        return SourceIdentifierMapping.from_table(table)
    legacy_path = directory / LEGACY_ID_MAPPING_FILE
    if legacy_path.exists():
        print(f"Migrating {legacy_path} to the {SOURCE_IDENTIFIER_TABLE} table")
        return migrate_legacy_id_mapping(
            legacy_path, legacy_id_source_rules or [], source_registry
        )
    return SourceIdentifierMapping()


def migrate_legacy_id_mapping(
    file_path: Path,
    id_source_rules: list,
    source_registry: SourceRegistry | None = None,
) -> SourceIdentifierMapping:
    """Convert an id_mapping.tsv from earlier pipeline versions, keeping UUIDs

    The old mapping did not record which id system a raw id came from. A source
    is inferred when exactly one of the state's declared sources accepts the raw
    id (its 'when' and its registry pattern match). Otherwise, for example when
    two id systems share a number format, the legacy source for the state is
    used. Rules with a source_id_format are skipped since the old mapping lacks
    the values needed to build their source ids.

    Args:
        file_path: Path to id_mapping.tsv with columns raw_id, year,
            reported_state, table_name, and uuid
        id_source_rules: IdSourceRules declared in the state's config
        source_registry: registry of sources. Defaults to sources.yaml.

    Returns:
        Mapping with one key per distinct (source, raw_id, table_name)
    """
    source_registry = source_registry or get_default_registry()
    candidate_rules = [
        rule for rule in id_source_rules if rule.source_id_format is None
    ]
    legacy_table = pd.read_csv(file_path, sep="\t", dtype=str)
    mapping = SourceIdentifierMapping()
    n_conflicts = 0
    for row in legacy_table.itertuples(index=False):
        raw_id = normalize_id_to_string(row.raw_id)
        reported_state = None if pd.isna(row.reported_state) else row.reported_state
        matching_sources = {
            rule.source
            for rule in candidate_rules
            if (rule.when is None or rule.when.fullmatch(raw_id))
            and source_registry[rule.source].matches(
                source_registry[rule.source].normalize(raw_id)
            )
            and not source_registry[rule.source].is_null(
                source_registry[rule.source].normalize(raw_id)
            )
        }
        if len(matching_sources) == 1:
            source = matching_sources.pop()
            source_id = source_registry[source].normalize(raw_id)
        else:
            source = legacy_source(reported_state)
            source_id = raw_id
        key = (source, source_id, row.table_name)
        if key in mapping:
            n_conflicts += mapping[key] != row.uuid
            continue
        mapping.add(key, row.uuid, reported_state)
    if n_conflicts:
        print(
            f"Warning: {n_conflicts} rows of {file_path} mapped a raw id to a "
            "different UUID than an earlier row for the same source id; the "
            "first UUID was kept."
        )
    return mapping


def get_all_id_references(
    base_table_name: str, schema: DataSchema = None
) -> dict[str, list[str]]:
    """Make mapping table name to list of all columns that reference table_name's id

    Args:
        schema: DataSchema object
        base_table_name: Name of the table to get all id references for

    Returns:
        Dictionary mapping table name to list of all columns that reference table_name's id
    """
    if schema is None:
        schema = DataSchema(DEFAULT_SCHEMA_PATH)
    id_references = {table_name: [] for table_name in schema.schema}
    if "id" in schema.schema[base_table_name].attributes:
        id_references[base_table_name].append("id")
    for table_name in schema.schema:
        for foreign_key_column, foreign_table_name in schema.schema[
            table_name
        ].relations.items():
            if foreign_table_name == base_table_name:
                id_references[table_name].append(f"{foreign_key_column}_id")
    return id_references
