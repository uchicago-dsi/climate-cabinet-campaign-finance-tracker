"""Abstract base class representing database tables

For each table, the maximal set of allowable column names in unnormalized form
includes:
  - each attribute possessed by the entity type the table represents,
  - each attribute possessed by any foreign key relations of the table,
    prefixed by the name of the foreign key relation (i.e transaction ->
    transactor may be named either donor or recipient), separated by a '-'.
  - each attribute that is a multivalued fact about the entity. Since data
    is mostly retrieved from state agency documents for transactions, info
    about other entity types is often from a particular instance in time.
    Therefore, facts about an individual that may change like address or
    employer can be multivalued but are intially presented with the individual.


#   - each attribute possessed by a limited set of reverse foreign key relations
#     of the table, prefixed by the name of the reverse foreign key relation.
#     'Limited set' refers to all of those reverse foriegn key relations that
#     are effectively 'few-to-one' and unlikely to change often enough to
#     warrant normalizind after the initial processing steps. For example,
#     transaction -> transactor is many to one and we create separate transaction
#     and transactor tables immediately. address -> transactor changes
#     infrequently, requires more sophisticated record linkage and is not worth
#     immediately normalizing.
"""

import pandas as pd

from utils.ids import add_uuids_to_table
from utils.schema import DataSchema, TableSchema

UNNORMALIZED_FLAG = 0
FIRST_NORMAL_FORM_FLAG = 1
THIRD_NORMAL_FORM_FLAG = 3
NORMALIZATION_LEVELS = [
    UNNORMALIZED_FLAG,
    FIRST_NORMAL_FORM_FLAG,
    THIRD_NORMAL_FORM_FLAG,
]

SPLIT = "--"
ID_SUFFIX = "_id"


def get_normalization_form_by_column(
    table: pd.DataFrame, table_schema: TableSchema
) -> dict[int, set]:
    """Map each normalization form to the first column tokens in that form

    Args:
        table: a dataframe representing a table in a valid table_schema
        table_schema: represents specification of table's schema
    """
    first_column_token_by_normalization_form = {
        THIRD_NORMAL_FORM_FLAG: set(),
        FIRST_NORMAL_FORM_FLAG: set(),
        UNNORMALIZED_FLAG: set(),
    }
    for column in table.columns:
        first_column_token = column.split(SPLIT)[0]
        if table_schema.repeating_columns_regex.match(first_column_token):
            first_column_token_by_normalization_form[UNNORMALIZED_FLAG].add(
                first_column_token
            )
        elif table_schema.forward_relations_regex.match(
            first_column_token
        ) or table_schema.reverse_relations_regex.match(first_column_token):
            first_column_token_by_normalization_form[FIRST_NORMAL_FORM_FLAG].add(
                first_column_token
            )
        elif table_schema.attributes_regex.match(first_column_token):
            first_column_token_by_normalization_form[THIRD_NORMAL_FORM_FLAG].add(
                first_column_token
            )
        else:
            raise ValueError(
                f"Invalid Table: {first_column_token} in {column} not expected"
                f" in {table_schema.table_type}"
            )
    return first_column_token_by_normalization_form


def get_foreign_table_type(base_type: str, column_name: str, schema: DataSchema) -> str:
    """Retrieve the type of a multivalued/foreign table"""
    column_tokens = column_name.split(SPLIT)
    current_table_type = base_type
    for column_token in column_tokens:
        current_table_schema = schema.schema[current_table_type]
        if column_token in current_table_schema.relations:
            current_table_type = current_table_schema.relations[column_token]
            continue
        elif current_table_schema.attributes_regex.match(column_token):
            return current_table_type
    return current_table_type


def convert_to_1NF_from_unnormalized(
    unnormalized_table: pd.DataFrame, table_schema: TableSchema
) -> pd.DataFrame:
    """Converts an unnormalized table into first normal form (1NF)

    Removes anticipated repeating groups

    Args:
        unnormalized_table: table with valid unnormalized schema
        table_schema: table schema that has regex to detect repeating columns
    Returns:
        table transformed to 1NF
    """
    repeated_columns = [
        column
        for column in unnormalized_table.columns
        if table_schema.repeating_columns_regex.match(column)
    ]
    if repeated_columns == []:
        return unnormalized_table
    for column in repeated_columns:
        base_column_name = column.split("-")[0]
        if base_column_name in unnormalized_table.columns:
            # base table has col and col-1. Replace col with col-{n+1}
            max_repeat = max(
                [
                    int(col.split("-")[-1])
                    for col in repeated_columns
                    if col.startswith(base_column_name)
                ]
            )
            unnormalized_table = unnormalized_table.rename(
                columns={base_column_name: f"{base_column_name}-{max_repeat + 1}"}
            )
    unnormalized_table.loc[:, "temp_id"] = range(0, len(unnormalized_table))
    static_columns = [
        column
        for column in unnormalized_table.columns
        if column not in repeated_columns
    ]
    melted_table = pd.melt(
        unnormalized_table, id_vars=static_columns, value_vars=repeated_columns
    )
    melted_table[["variable", "instance"]] = melted_table["variable"].str.rsplit(
        "-",
        n=1,
        expand=True,  # TODO: split variable
    )
    first_normal_form_table = melted_table.pivot(  # noqa: PD010
        index=static_columns + ["instance"],
        columns="variable",
        values="value",
    ).reset_index()
    first_normal_form_table = first_normal_form_table.drop(
        columns=["temp_id", "instance"]
    )
    first_normal_form_table = first_normal_form_table[
        first_normal_form_table["amount"].notna() & first_normal_form_table["amount"]
        > 0
    ]
    return first_normal_form_table


def _add_relation_to_extracted_table(
    table: pd.DataFrame, table_type: str, schema: DataSchema, foreign_key_prefix: str
) -> pd.DataFrame:
    """Checks if a column linking back to the base table is required in a foreign table"""
    backlink_column = schema.schema[table_type].reverse_relation_names[
        foreign_key_prefix
    ]
    if f"{foreign_key_prefix}{SPLIT}{backlink_column}" not in table.columns:
        # this means the required column doesn't exist so we should
        # create it
        backlink_column = f"{foreign_key_prefix}{SPLIT}{backlink_column}"
        table.loc[:, backlink_column] = table["id"]

    return table


def _split_prefixed_columns(
    table: pd.DataFrame,
    table_type: str,
    schema: DataSchema,
    relation_prefix: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Remove columns starting with prefix from table, optionally keeping foreign key

    Steps:
        0. If reverse relation, add to extracted table references back to base table
        1. identify columns that are part of foreign table
        2. create new table with all columns of foreign table
        3. drop all rows that are missing required attributes for their data type
        4. drop all rows that are complete duplicates
        5. assign id to foreign table
        6. if 3nf flag, map id to base table

    Args:
        table: any pandas dataframe
        table_type: table type present in schema
        schema: TODO
        relation_prefix: prefix of columns to be split from table. prefix should be in one of
            self.forward_relations or self.reverse_relations
        normalization_flag: the desired normalization level

    ASSUMPTIONS:
        table has an 'id' column
    Returns: original table with columns removed, new foreign table
    """
    # step 0 - handle reverse relations
    # we do this earlier than forward relation ids, because we want to
    # deduplicate forward relation ids before the ids are created
    if relation_prefix in schema.schema[table_type].reverse_relations:
        table = _add_relation_to_extracted_table(
            table, table_type, schema, relation_prefix
        )
    # step 1
    foreign_columns_in_base_table = [
        column
        for column in table.columns
        if column.startswith(f"{relation_prefix}{SPLIT}")
    ]
    foreign_columns_in_foreign_table = [
        column[len(relation_prefix) + len(SPLIT) :]
        for column in foreign_columns_in_base_table
    ]
    # step 2 - split foreign table off of old base table
    extracted_table = table[foreign_columns_in_base_table]
    extracted_table.columns = foreign_columns_in_foreign_table
    # step 3 - drop incomplete rows
    extracted_table = extracted_table.dropna(how="all")
    extracted_table_type = get_foreign_table_type(table_type, relation_prefix, schema)
    extracted_table_schema = schema.schema[extracted_table_type]
    required_columns = extracted_table_schema.required_attributes
    if not set(required_columns).issubset(extracted_table.columns):
        extracted_table = pd.DataFrame()
        print(
            f"Extracted table {extracted_table_type} from table {table_type} lacks"
            " required columns: "
            f"{set(required_columns).difference(set(extracted_table.columns))}"
        )
    extracted_table = extracted_table.dropna(subset=required_columns)
    # step 4 - drop duplicates - deduplication
    extracted_table = extracted_table.drop_duplicates()
    # step 5 - generate ids if the extracted table has an id column
    if (
        "id" not in extracted_table.columns
        and "id" in extracted_table_schema.attributes
    ):
        extracted_table = add_uuids_to_table(extracted_table, "id")
    elif "id" in extracted_table.columns:
        print("id found")
        # TODO: map existing ids to new ids
        # TODO: replace NaNs with real ids
        foreign_columns_in_foreign_table.remove("id")
    # step 6 - add relation
    if relation_prefix in schema.schema[table_type].forward_relations:
        mapping_dict = extracted_table.set_index(foreign_columns_in_foreign_table)[
            "id"
        ].to_dict()

        # Map the combinations in `table` to the ids from `foreign_key_table`
        table[f"{relation_prefix}_id"] = table[foreign_columns_in_base_table].apply(
            lambda row: mapping_dict.get(tuple(row), None), axis=1
        )

    columns_to_drop = list(foreign_columns_in_base_table)
    table = table.drop(columns=columns_to_drop)
    return table, extracted_table


def _convert_table_to_3NF_from_1NF(
    table: pd.DataFrame,
    table_type: str,
    schema: DataSchema,
) -> dict[str, list[pd.DataFrame]]:
    """Normalize table and any derivative tables to desired level given table schema

    Args:
        table: a valid table of table_type given schema
        table_type: used to identify which type of table in given schema
            table should fit
        schema: database schema that should contain table_type

    Returns:
        Dictionary mapping table_types to list of tables attaining the desired
            normalization level.
    """
    # Step 1: Figure out which columns need to be normalized
    normalization_levels_to_columns = get_normalization_form_by_column(
        table, schema.schema[table_type]
    )
    normalization_level = min(
        k for k, v in normalization_levels_to_columns.items() if v
    )
    # Step 2: Base case - if the table is at the desired level, return it.
    if normalization_level >= THIRD_NORMAL_FORM_FLAG:
        return {table_type: [table]}
    # Step 3: If the table needs to be normalized, go through
    # each column that needs to be normalized and normalize it. This will
    # return a new table and potentially a derivative table.
    updated_database = schema.empty_database()
    active_table = table
    while normalization_levels_to_columns[FIRST_NORMAL_FORM_FLAG]:
        first_column_token = normalization_levels_to_columns[
            FIRST_NORMAL_FORM_FLAG
        ].pop()
        # this is where the heavy lifting is done and a new foreign table
        # is created derived from the columns that did not belong in base table
        active_table, extracted_table = _split_prefixed_columns(
            active_table, table_type, schema, first_column_token
        )
        # Step 4 - Recursive step. Bring the foreign derivative table to the
        # desired form and all ensuing derivative tables
        extracted_table_derived_database = _convert_table_to_3NF_from_1NF(
            extracted_table,
            schema.schema[table_type].relations[first_column_token],
            schema,
        )
        for derived_table_type in extracted_table_derived_database:
            updated_database[derived_table_type].extend(
                extracted_table_derived_database[derived_table_type]
            )
    updated_database[table_type].append(active_table)
    # ensure indices are correct
    return updated_database


def convert_to_3NF_from_1NF(
    first_normal_form_database: dict[str, pd.DataFrame],
    schema: DataSchema,
) -> dict[str, pd.DataFrame]:
    """Converts database in first normal form (1NF) to third normal form (3NF)

    Derivative tables are created when information exists in the given table
    that should be placed in another table to attain the desired normalization
    level. When this is done, the derivative table must also be normalized

    Args:
        first_normal_form_database: TODO: spec of database somewhere
        schema: database schema that should contain all keys in database

    Returns:
        database where each table is at or above the desired normalization level
    """
    normalized_database = schema.empty_database()
    for table_type, table in first_normal_form_database.items():
        if table.index.name:
            table = table.reset_index()
        updated_database = _convert_table_to_3NF_from_1NF(table, table_type, schema)
        for normalized_table_type in updated_database:
            normalized_database[normalized_table_type].extend(
                updated_database[normalized_table_type]
            )
    normalized_database = _consolidate_database(normalized_database)
    return normalized_database


def _consolidate_database(
    database: dict[str, list[pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    """Consolidate the database by concatenating each table type"""
    consolidated_database = {}
    for table_type in database:
        if database[table_type] == []:
            continue
        consolidated_table = pd.concat(database[table_type], ignore_index=True)
        if "id" in consolidated_table.columns:
            consolidated_table = consolidated_table.set_index("id")
        consolidated_database[table_type] = consolidated_table
    return consolidated_database


def normalize_database(
    database: dict[str, pd.DataFrame],
    schema: DataSchema,
) -> dict[str, pd.DataFrame]:
    """Bring a database to 4NF given the provided schema

    Args:
        database: TODO probably a good place to spec database
        schema: TODO

    Returns:
        dictionary mapping table types to respective tables
    """
    # bring to 1NF
    database_1NF = {}
    for table_type in database:
        database_1NF[table_type] = convert_to_1NF_from_unnormalized(
            database[table_type], schema.schema[table_type]
        )

    # bring to 3NF
    database_3NF = convert_to_3NF_from_1NF(
        database_1NF,
        schema,
    )
    return database_3NF


def convert_to_class_table_from_single_table(
    database_single_table: dict[str, pd.DataFrame], data_schema: DataSchema
) -> dict[str, pd.DataFrame]:
    """[WIP] convert a database from a single table to class table inheritance strategy"""
    database_class_table = {}
    for table_type in database_single_table:
        original_table = database_single_table[table_type]
        data_schema.inheritance_strategy = "class table inheritance"
        for child_type in data_schema.schema[table_type].child_types:
            child_schema = data_schema.schema[child_type]
            child_table = original_table[
                [
                    col
                    for col in original_table.columns
                    if col in child_schema.attributes
                ]
            ]
            child_table = child_table.dropna(how="all")
            database_class_table[child_type] = child_table
        parent_schema = data_schema.schema[table_type]
        parent_table = original_table[
            [col for col in original_table.columns if col in parent_schema.attributes]
        ]
        database_class_table[table_type] = parent_table
    return database_class_table
