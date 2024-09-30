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

import re
import uuid
from pathlib import Path
from typing import Self

import pandas as pd
import yaml

UNNORMALIZED_FLAG = 0
FIRST_NORMAL_FORM_FLAG = 1
THIRD_NORMAL_FORM_FLAG = 3
FOURTH_NORMAL_FORM_FLAG = 4
NORMALIZATION_LEVELS = [
    UNNORMALIZED_FLAG,
    FIRST_NORMAL_FORM_FLAG,
    THIRD_NORMAL_FORM_FLAG,
    FOURTH_NORMAL_FORM_FLAG,
]

SPLIT = "--"


class TableSchema:
    """class representation of a single table schema"""

    def __init__(self, data_schema: dict, table_type: str):  # noqa ANN204
        """Creates a tableschema instance used to validate tables

        Args:
            data_schema: dict
            table_type: string must be a key in data_schema
        """
        self.table_type = table_type
        self.data_schema = data_schema
        self._child_types_are_separate = None
        self._child_types = None
        self._parent_type = None
        self._attributes = None
        self._repeating_columns = None
        self._multivalued_columns = None
        self._forward_relations = None
        self._attributes_regex = None
        self._repeating_columns_regex = None
        self._forward_relations_regex = None
        self._multivalued_columns_regex = None
        self._enum_columns = None
        self._required_attributes = None

    def _fill_properties(self, property_name: str, property_type: str) -> list | dict:
        """Calculate properties from given data schema"""
        parent_type_name = self.data_schema[self.table_type].get("parent_type", None)
        if property_type == "list":
            property_value = self.data_schema[self.table_type].get(property_name, [])
            if not self.child_types_are_separate:
                for child in self.data_schema[self.table_type].get("child_types", []):
                    property_value.extend(
                        self.data_schema[child].get(property_name, [])
                    )
                if parent_type_name:
                    parent_type = self.data_schema[parent_type_name]
                    property_value.extend(parent_type.get(property_name, []))
        elif property_type == "dict":
            property_value = self.data_schema[self.table_type].get(property_name, {})
            if not self.child_types_are_separate:
                for child in self.data_schema[self.table_type].get("child_types", []):
                    property_value.update(
                        self.data_schema[child].get(property_name, {})
                    )
                if parent_type_name:
                    parent_type = self.data_schema[parent_type_name]
                    property_value.update(parent_type.get(property_name, {}))
        else:
            raise ValueError(
                "Property type for _fill_properties should be list or dict"
            )
        return property_value

    @property
    def attributes(self) -> list:
        """List of attributes allowed for this entity"""
        if self._attributes is None:
            self._attributes = self._fill_properties("attributes", "list")
        return self._attributes

    @property
    def attributes_regex(self) -> re.Pattern:
        """Full regex to match attributes"""
        if self._attributes_regex is None:
            self._attributes_regex = re.compile("|".join(self.attributes))
        return self._attributes_regex

    @property
    def repeating_columns(self) -> list:
        """List of columns that may be repeated in unnormalized form"""
        if self._repeating_columns is None:
            self._repeating_columns = self._fill_properties("repeating_columns", "list")
        return self._repeating_columns

    @property
    def repeating_columns_regex(self) -> re.Pattern:
        """Full regex to match any repeating columns"""
        if self._repeating_columns_regex is None:
            repeating_columns_regex_list = [
                repeated_column + "-\d+$" for repeated_column in self.repeating_columns
            ]
            self._repeating_columns_regex = self._get_regex(
                repeating_columns_regex_list
            )
        return self._repeating_columns_regex

    @property
    def multivalued_columns(self) -> dict[str, Self]:
        """Columns that may have multiple values for an instance of the entity type

        For example, an individual may have multiple addresses or employers
        """
        if self._multivalued_columns is None:
            self._multivalued_columns = self._fill_properties(
                "multivalued_columns", "dict"
            )
            # self._instantiate_related_entities(self._multivalued_columns)
        return self._multivalued_columns

    @property
    def multivalued_columns_regex(self) -> re.Pattern:
        """Full regex to match any multivalued columns"""
        if self._multivalued_columns_regex is None:
            self._multivalued_columns_regex = self._get_regex(
                list(self.multivalued_columns.keys())
            )
        return self._multivalued_columns_regex

    def _get_regex(self, attribute_list: list[str]) -> re.Pattern:
        # add unmatchable regex so that empty list don't compile to '' which matches
        # everything
        unmatchable = "a^"
        possible_attributes = [f"^{attribute}$" for attribute in attribute_list]
        possible_attributes += [unmatchable]
        return re.compile("|".join(possible_attributes))

    @property
    def forward_relations(self) -> dict[str, Self]:
        """Many-to-one relationships that are an attribute of the entity type"""
        if self._forward_relations is None:
            self._forward_relations = self._fill_properties("forward_relations", "dict")
            # self._instantiate_related_entities(self._forward_relations)
        return self._forward_relations

    @property
    def forward_relations_regex(self) -> re.Pattern:
        """Full regex to match any forward relation columns"""
        if self._forward_relations_regex is None:
            self._forward_relations_regex = self._get_regex(
                list(self.forward_relations.keys())
            )
        return self._forward_relations_regex

    @property
    def enum_columns(self) -> dict[str, list]:
        """Map enum column names to their list of allowed values"""
        if self._enum_columns is None:
            self._enum_columns = self._fill_properties("enum_columns", "dict")
        return self._enum_columns

    @property
    def required_attributes(self) -> dict[str, list]:
        """Attributes required for a given record of the entity type to be relevant"""
        if self._required_attributes is None:
            self._required_attributes = self.data_schema[self.table_type].get(
                "required_attributes", []
            )
        return self._required_attributes

    @property
    def child_types_are_separate(self) -> bool:
        """True if child types are separated from paret types"""
        if self._child_types_are_separate is None:
            self._child_types_are_separate = False
        return self._child_types_are_separate

    @property
    def child_types(self) -> list:
        """Types that inherit attributes from the current type"""
        if self._child_types is None:
            self._child_types = self.data_schema[self.table_type].get("child_types", [])
        return self._child_types

    @property
    def parent_type(self) -> dict[str, list]:
        """Parent type of given entity type"""
        if self._parent_type is None:
            self._parent_type = self.data_schema[self.table_type].get(
                "parent_type", None
            )
        return self._parent_type


class DataSchema:
    """Class representation of a dataschema defined in a data schema yaml TODO better name"""

    # TODO add validation back
    @property
    def schema(self) -> dict[str, TableSchema]:
        """Maps table types to their TableSchemas"""
        if self._schema is None:
            self._schema = {}
            for table_type in self.raw_data_schema:
                self._schema[table_type] = TableSchema(self.raw_data_schema, table_type)
        return self._schema

    def empty_database(self) -> dict[str, list]:
        """Returns an empty database of the given schema"""
        return {table_type: [] for table_type in self.schema.keys()}

    def __init__(self, path_to_data_schema: Path | str) -> None:
        """Representation of a singe table"""
        self._schema = None
        with Path(path_to_data_schema).open("r") as f:
            self.raw_data_schema = yaml.safe_load(f)


def calculate_normalization_status(
    table: pd.DataFrame, table_type: str, data_schema: DataSchema
) -> tuple[dict[int, dict[str, str]], int]:
    """Return normalization level of each column of table, and table as a whole

    Args:
        table: a table that should fit the given data_schema for its table_type
        table_type: TODO
        data_schema: TODO

    Returns:
        dict mapping normalization level flag to a list of tuples representing
            columns in this table that are of that normalization level.
            Columns for derivative tables of this table may be present in deep
            nesting structures so this mapping includes all column fragments
            (strings of column tokens, where column tokens are pieces of text
            separated by a separator '--').
    """
    normalization_level = FOURTH_NORMAL_FORM_FLAG
    columns_by_normalization_level = {
        FOURTH_NORMAL_FORM_FLAG: {},
        THIRD_NORMAL_FORM_FLAG: {},
        FIRST_NORMAL_FORM_FLAG: {},
        UNNORMALIZED_FLAG: {},  # any repeating columns must be leafs
    }
    for column in table.columns:
        token_table_schema = data_schema.schema[table_type]
        column_tokens = column.split(SPLIT)
        running_tokens = ()
        for column_token in column_tokens:
            running_tokens += (column_token,)

            if token_table_schema.repeating_columns_regex.match(column_token):
                columns_by_normalization_level[UNNORMALIZED_FLAG].add(
                    {SPLIT.join(running_tokens): None}
                )
                normalization_level = min(UNNORMALIZED_FLAG, normalization_level)
            elif token_table_schema.forward_relations_regex.match(column_token):
                for (
                    forward_relation,
                    forward_relation_type,
                ) in token_table_schema.forward_relations.items():
                    if forward_relation == column_token:
                        columns_by_normalization_level[FIRST_NORMAL_FORM_FLAG].update(
                            {SPLIT.join(running_tokens): forward_relation_type}
                        )
                        token_table_schema = data_schema.schema[forward_relation_type]
                        break
                normalization_level = min(FIRST_NORMAL_FORM_FLAG, normalization_level)
            elif token_table_schema.multivalued_columns_regex.match(column_token):
                for (
                    multivalued_column,
                    multivalued_column_type,
                ) in token_table_schema.multivalued_columns.items():
                    if multivalued_column == column_token:
                        columns_by_normalization_level[THIRD_NORMAL_FORM_FLAG].update(
                            {SPLIT.join(running_tokens): multivalued_column_type}
                        )
                        token_table_schema = data_schema.schema[multivalued_column_type]
                        break
                normalization_level = min(THIRD_NORMAL_FORM_FLAG, normalization_level)
            elif token_table_schema.attributes_regex.match(column_token):
                columns_by_normalization_level[FOURTH_NORMAL_FORM_FLAG].update(
                    {SPLIT.join(running_tokens): None}
                )
            else:
                raise ValueError("Invalid Table")
    return columns_by_normalization_level, normalization_level


def convert_to_1NF_from_unnormalized(
    unnormalized_table: pd.DataFrame, repeating_columns_regex: re.Pattern
) -> pd.DataFrame:
    """Converts an unnormalized table into first normal form (1NF)

    Removes anticipated repeating groups

    Args:
        unnormalized_table: table with valid unnormalized schema
        repeating_columns_regex: regex to detect repeating columns
    Returns:
        table transformed to 1NF
    """
    repeated_columns = [
        column
        for column in unnormalized_table.columns
        if repeating_columns_regex.match(column)
    ]
    if repeated_columns == []:
        return unnormalized_table
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


def _add_foreign_key(table: pd.DataFrame, foreign_key_prefix: str):
    """Add a foreign key column to the table that will remain when foreign columns split

    When a foreign table is split form a base table, all columns starting
    with the foreign prefix are removed. In some cases, we want to keep
    a reference in the base table. A reference that we wish to keep should
    be in the format {foreign_prefix}_id instead of {foregin_prefix}--id

    Args:
        table
        foreign_key_prefix
    """
    foriegn_key_column = f"{foreign_key_prefix}_id"
    new_id_column = f"{foreign_key_prefix}{SPLIT}id"
    foreign_columns_in_base_table = [
        column
        for column in table.columns
        if column.startswith(f"{foreign_key_prefix}{SPLIT}")
    ]
    foreign_columns_in_foreign_table = [
        column[len(foreign_key_prefix) + len(SPLIT) :]
        for column in foreign_columns_in_base_table
    ]
    if foriegn_key_column not in table.columns and new_id_column not in table.columns:
        # we want an id for the given table but none exists TODO: the below code should be a function somewhere (repeat in DataSource)
        relevant_rows_mask = table[foreign_columns_in_base_table].notna().any(axis=1)
        table.loc[relevant_rows_mask, f"{foreign_key_prefix}{SPLIT}id"] = table[
            relevant_rows_mask
        ].apply(lambda _: str(uuid.uuid4()), axis=1)
        foreign_columns_in_foreign_table.append("id")
        foreign_columns_in_base_table.append(f"{foreign_key_prefix}{SPLIT}id")
    if foriegn_key_column not in table.columns:
        table[foriegn_key_column] = table.loc[
            :, foreign_key_prefix + f"{SPLIT}id"
        ]  # TODO split
    if new_id_column not in table.columns:
        table[new_id_column] = table.loc[:, foriegn_key_column]
        foreign_columns_in_foreign_table.append("id")
        foreign_columns_in_base_table.append(f"{foreign_key_prefix}{SPLIT}id")
    return table, foreign_columns_in_base_table, foreign_columns_in_foreign_table


def _add_forward_relation_to_foreign_table(
    table: pd.DataFrame, table_type: str, schema: DataSchema, foreign_key_prefix: str
) -> pd.DataFrame:
    """Checks if a column linking back to the base table is required in a foreign table"""
    foreign_columns_in_base_table = [
        column
        for column in table.columns
        if column.startswith(f"{foreign_key_prefix}{SPLIT}")
    ]
    foreign_columns_in_foreign_table = [
        column[len(foreign_key_prefix) + len(SPLIT) :]
        for column in foreign_columns_in_base_table
    ]
    derivative_table_type = schema.schema[table_type].multivalued_columns[
        foreign_key_prefix
    ]
    derivative_table_schema = schema.schema[derivative_table_type]
    # go through all of the required attributes in the derivative table.
    # If one of them is a forward relation to the base table and that
    # column does not already exist, create it
    for required_attribute in derivative_table_schema.required_attributes:
        length_of_id_suffix = len("_id")
        for (
            forward_relation,
            forward_relation_type,
        ) in derivative_table_schema.forward_relations.items():
            # forward relations are stored as only the prefix (no '_id') so we trim
            # required attribute name
            if required_attribute[
                :-length_of_id_suffix
            ] == forward_relation and table_type in [
                forward_relation_type,
                schema.schema[forward_relation_type].parent_type,
                *schema.schema[forward_relation_type].child_types,
            ]:
                # this means the derivative table requires a column linking back to
                #  the current table
                if required_attribute not in foreign_columns_in_foreign_table:
                    # this means the required column doesn't exist so we should
                    # create it
                    backlink_column = f"{foreign_key_prefix}{SPLIT}{required_attribute}"
                    relevant_rows_mask = (
                        table[foreign_columns_in_base_table].notna().any(axis=1)
                    )
                    table.loc[
                        relevant_rows_mask,
                        backlink_column,
                    ] = table.loc[relevant_rows_mask]["id"]
                    foreign_columns_in_base_table.append(backlink_column)
                    foreign_columns_in_foreign_table.append(required_attribute)
    return table, foreign_columns_in_base_table, foreign_columns_in_foreign_table


def _split_prefixed_columns(
    table: pd.DataFrame,
    table_type: str,
    schema: DataSchema,
    foreign_key_prefix: str,
    normalization_flag: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Remove columns starting with prefix from table, optionally keeping foreign key

    Args:
        table: any pandas dataframe
        table_type: table type present in schema
        schema: TODO
        foreign_key_prefix: prefix of columns to be split from table. prefix should be in one of
            self.forward_relations or self.multivalued_columns
        normalization_flag: the desired normalization level
    Returns: original table with columns removed, new foreign table
    """
    if normalization_flag == THIRD_NORMAL_FORM_FLAG:
        table, foreign_columns_in_base_table, foreign_columns_in_foreign_table = (
            _add_foreign_key(table, foreign_key_prefix)
        )
    elif normalization_flag == FOURTH_NORMAL_FORM_FLAG:
        table, foreign_columns_in_base_table, foreign_columns_in_foreign_table = (
            _add_forward_relation_to_foreign_table(
                table, table_type, schema, foreign_key_prefix
            )
        )
    else:
        raise ValueError(f"Invalid normalization flag: {normalization_flag}")

    columns_to_drop = list(foreign_columns_in_base_table)

    # only deal with rows that have some value for foreign columns
    foreign_table_na_mask = table[foreign_columns_in_base_table].isna().all(axis=1)
    relevant_table = table[~foreign_table_na_mask]
    irrelevant_table = table[foreign_table_na_mask]

    # clean up foreign table
    foreign_key_table = relevant_table[foreign_columns_in_base_table]
    foreign_key_table.columns = foreign_columns_in_foreign_table

    table = pd.concat([relevant_table, irrelevant_table])
    table = table.drop(columns=columns_to_drop)
    return table, foreign_key_table


def _normalize_table_completely(
    table: pd.DataFrame,
    table_type: str,
    schema: DataSchema,
    normalization_flag: int,
) -> dict[str, list[pd.DataFrame]]:
    """Normalize table and any derivative tables to desired level given table schema

    Args:
        table: a valid table of table_type given schema
        table_type: used to identify which type of table in given schema
            table should fit
        schema: database schema that should contain table_type
        normalization_flag: flag identifying which normalization level is
            desired.

    Returns:
        Dictionary mapping table_types to list of tables attaining the desired
            normalization level.
    """
    previous_normalization_flag = NORMALIZATION_LEVELS[
        NORMALIZATION_LEVELS.index(normalization_flag) - 1
    ]
    # Step 1: Create a mapping of normalization levels to columns in table
    # indicative of that normalization level. The table's normalization level
    # is the lowest normalization level that is not empty.
    columns_normalization, normalization_level = calculate_normalization_status(
        table, table_type, schema
    )
    # Step 2: Base case - if the table is at the desired level, return it.
    if normalization_level >= normalization_flag:
        return {table_type: [table]}
    # Step 3: If the table needs to be normalized, go through
    # each column that needs to be normalized and normalize it. This will
    # return a new table and potentially a derivative table.
    updated_database = schema.empty_database()
    active_table = table
    while columns_normalization[previous_normalization_flag]:
        relation_column_tokens, foreign_table_type = columns_normalization[
            previous_normalization_flag
        ].popitem()
        forward_relation_column = relation_column_tokens
        # since we are normalizing this forward relations, any forward relations
        # that go to that table should be removed
        columns_normalization[previous_normalization_flag] = {
            column: column_type
            for column, column_type in columns_normalization[
                previous_normalization_flag
            ].items()
            if not column.startswith(forward_relation_column)
        }
        # this is where the heavy lifting is done and a new foreign table
        # is created derived from the columns that did not belong in base table
        active_table, foreign_table = _split_prefixed_columns(
            active_table,
            table_type,
            schema,
            forward_relation_column,
            normalization_flag,
        )
        # Step 4 - Recursive step. Bring the foreign derivative table to the
        # desired form and all ensuing derivative tables
        foreign_table_derived_database = _normalize_table_completely(
            foreign_table,
            foreign_table_type,
            schema,
            normalization_flag,
        )
        for derived_table_type in foreign_table_derived_database:
            updated_database[derived_table_type].extend(
                foreign_table_derived_database[derived_table_type]
            )
    updated_database[table_type].append(active_table)
    # ensure indices are correct
    return updated_database


def normalize_database_completely(
    database: dict[str, list[pd.DataFrame]], schema: DataSchema, normalization_flag: int
) -> dict[str, list[pd.DataFrame]]:
    """Transform data in database to be at or above specified normalization level

    Derivative tables are created when information exists in the given table
    that should be placed in another table to attain the desired normalization
    level. When this is done, the derivative table must also be normalized

    Args:
        database: TODO: spec of database somewhere
        schema: database schema that should contain all keys in database
        normalization_flag: flag identifying which normalization level is
            desired.

    Returns:
        database where each table is at or above the desired normalization level
    """
    normalized_database = schema.empty_database()
    for table_type in database:
        for table in database[table_type]:
            updated_database = _normalize_table_completely(
                table, table_type, schema, normalization_flag
            )
            for normalized_table_type in updated_database:
                normalized_database[normalized_table_type].extend(
                    updated_database[normalized_table_type]
                )

    return normalized_database


def convert_to_3NF_from_1NF(
    first_normal_form_database: dict[str, list[pd.DataFrame]],
    schema: DataSchema,
) -> dict[str, list[pd.DataFrame]]:
    """Converts database in first normal form (1NF) to third normal form (3NF)

    Mainly a wrapper for the more general normalize_database_completely

    Args:
        first_normal_form_database: table that has already been converted to 1NF
        schema: TODO: general schame documentation

    Return:
        TODO specificy 'database'. Database where each table is in 3NF.
    """
    return normalize_database_completely(
        first_normal_form_database, schema, THIRD_NORMAL_FORM_FLAG
    )


def convert_to_4NF_from_3NF(
    third_normal_form_database: dict[str, list[pd.DataFrame]],
    schema: DataSchema,
) -> dict[str, list[pd.DataFrame]]:
    """Converts table in third normal form (3NF) to fourth normal form (4NF)

    Args:
        third_normal_form_database: table with no repeating values or attributes
            that are do not depend on the full key
        schema: xx
    """
    return normalize_database_completely(
        third_normal_form_database, schema, FOURTH_NORMAL_FORM_FLAG
    )


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
    database: dict[str, list[pd.DataFrame]],
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
        database_1NF[table_type] = []
        for table in database[table_type]:
            table_1NF = convert_to_1NF_from_unnormalized(
                table, schema.schema[table_type].repeating_columns_regex
            )
            database_1NF[table_type].append(table_1NF)

    # bring to 3NF
    database_3NF = convert_to_3NF_from_1NF(
        database_1NF,
        schema,
    )
    database_4NF = convert_to_4NF_from_3NF(
        database_3NF,
        schema,
    )
    normalized_database = _consolidate_database(database_4NF)
    return normalized_database
