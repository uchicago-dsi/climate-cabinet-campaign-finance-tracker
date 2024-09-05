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


class TableSchema:
    """class representation of a single table schema"""

    def __init__(self, data_schema: dict, table_type: str):
        self.table_type = table_type
        self.data_schema = data_schema
        self._child_types_are_separate = None
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
    def child_types(self):
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


def replace_id_with_uuid(
    table_with_id_column: pd.DataFrame,
    id_column: str = "id",
) -> pd.DataFrame:
    """Adds UUIDs, saving a mapping where IDs were previously given by state

    Args:
        table_with_id_column: pandas dataframe that contains a column
            representing an id
        id_column: name of expected id column. Default: id
    Returns:
        input table with added ids, table mapping old to new ids
    """
    unique_ids = table_with_id_column[id_column].dropna().unique()  # .compute() DASK
    uuid_mapping = {id: str(uuid.uuid4()) for id in unique_ids}

    id_mask = table_with_id_column[id_column].isna()
    # Replace null IDs with new UUIDs
    # this apply is slow, but extremely similar to computing n new UUIDs
    # so without a faster way of generating UUIDs, this probably won't get
    # meaningfully faster.
    table_with_id_column.loc[id_mask, id_column] = table_with_id_column[id_mask][
        id_column
    ].apply(
        lambda _: str(uuid.uuid4()),
        # meta=(id_column, "str"), DASK
    )
    table_with_id_column.loc[~id_mask, id_column] = table_with_id_column[~id_mask][
        id_column
    ].map(uuid_mapping)

    # Convert the mapping to a DataFrame
    mapping_df = pd.DataFrame(
        list(uuid_mapping.items()), columns=["original_id", "uuid"]
    )
    # mapping_ddf = dd.from_pandas(mapping_df, npartitions=1) # DASK
    return table_with_id_column, mapping_df


def calculate_normalization_status(
    table: pd.DataFrame, table_type: str, data_schema: DataSchema
) -> tuple[dict, int]:
    """Find normalization level of table"""
    normalization_level = FOURTH_NORMAL_FORM_FLAG
    columns_by_normalization_level = {
        FOURTH_NORMAL_FORM_FLAG: {},
        THIRD_NORMAL_FORM_FLAG: {},
        FIRST_NORMAL_FORM_FLAG: {},
        UNNORMALIZED_FLAG: {},  # any repeating columns must be leafs
    }
    for column in table.columns:
        token_table_schema = data_schema.schema[table_type]
        column_tokens = column.split("--")  # TODO: make separator --
        running_tokens = ()
        for column_token in column_tokens:
            running_tokens += (column_token,)

            if token_table_schema.repeating_columns_regex.match(column_token):
                columns_by_normalization_level[UNNORMALIZED_FLAG].add(
                    {"--".join(running_tokens): None}
                )
                normalization_level = min(UNNORMALIZED_FLAG, normalization_level)
            elif token_table_schema.forward_relations_regex.match(column_token):
                for (
                    forward_relation,
                    forward_relation_type,
                ) in token_table_schema.forward_relations.items():
                    if forward_relation == column_token:
                        columns_by_normalization_level[FIRST_NORMAL_FORM_FLAG].update(
                            {"--".join(running_tokens): forward_relation_type}
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
                            {"--".join(running_tokens): multivalued_column_type}
                        )
                        token_table_schema = data_schema.schema[multivalued_column_type]
                        break
                normalization_level = min(THIRD_NORMAL_FORM_FLAG, normalization_level)
            elif token_table_schema.attributes_regex.match(column_token):
                columns_by_normalization_level[FOURTH_NORMAL_FORM_FLAG].update(
                    {"--".join(running_tokens): None}
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
    """
    repeated_columns = [
        column
        for column in unnormalized_table.columns
        if repeating_columns_regex.match(column)
    ]
    if repeated_columns == []:
        return unnormalized_table
    static_columns = [
        column
        for column in unnormalized_table.columns
        if column not in repeated_columns
    ]
    melted_table = pd.melt(
        unnormalized_table, id_vars=static_columns, value_vars=repeated_columns
    )
    melted_table[["variable", "instance"]] = melted_table["variable"].str.rsplit(
        "-", n=1, expand=True
    )
    first_normal_form_table = melted_table.pivot_table(
        index=static_columns + ["instance"],
        columns="variable",
        values="value",
        aggfunc="first",
    ).reset_index()
    return first_normal_form_table


def _split_prefixed_columns(
    table: pd.DataFrame,
    foreign_key_prefix: str,
    foreign_table_type: str,
    normalization_flag: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Remove columns starting with prefix from table, optionally keeping foreign key

    Args:
        table: any pandas dataframe
        foreign_key_prefix: prefix of columns to be split from table. prefix should be in one of
            self.forward_relations or self.multivalued_columns
        foreign_table_type: type of table created from prefixed columns in table
        keep_reference: if true, keep a {prefix}-id column referring to associated
            row in split table
    Returns: original table with columns removed, new foreign table,
        and id mapping table
    """
    prefixed_columns = [
        column for column in table.columns if column.startswith(foreign_key_prefix)
    ]
    new_foreign_columns = [
        column[len(foreign_key_prefix) + 2 :] for column in prefixed_columns
    ]

    if normalization_flag == FOURTH_NORMAL_FORM_FLAG:
        keep_reference = False
    elif normalization_flag == THIRD_NORMAL_FORM_FLAG:
        keep_reference = True
    if keep_reference:
        foriegn_key_column = f"{foreign_key_prefix}_id"
        if foriegn_key_column not in table.columns:
            table[foriegn_key_column] = None
        prefixed_columns.append(foriegn_key_column)
        new_foreign_columns.append("id")
    columns_to_drop = [col for col in prefixed_columns]

    # only deal with rows that have some value for foreign columns
    foreign_table_na_mask = table[prefixed_columns].isna().all(axis=1)
    relevant_table = table[~foreign_table_na_mask]
    irrelevant_table = table[foreign_table_na_mask]

    if keep_reference:
        # standardize id column to use uuids and update database 'id_mapping'
        # table to include mapping of old ids to new uuids
        # TODO: only deal with IDs if any of the foreign columns are not na
        relevant_table, id_mapping = replace_id_with_uuid(
            relevant_table, foriegn_key_column
        )
        id_mapping["table"] = foreign_table_type
        columns_to_drop.remove(foriegn_key_column)
    else:
        id_mapping = None
    # clean up foreign table
    forign_key_table = relevant_table[prefixed_columns]
    forign_key_table.columns = new_foreign_columns
    # foreign_table_type = forign_key_table.drop_duplicates()

    if keep_reference:
        forign_key_table = forign_key_table.set_index("id")

    table = pd.concat([relevant_table, irrelevant_table])
    table = table.drop(columns=columns_to_drop)
    return table, forign_key_table, id_mapping


def normalize_table_completely(
    table: pd.DataFrame,
    table_type: str,
    schema: DataSchema,
    normalization_flag: int,
    replace_existing_ids: bool = True,
):
    previous_normalization_flag = NORMALIZATION_LEVELS[
        NORMALIZATION_LEVELS.index(normalization_flag) - 1
    ]
    columns_normalization, normalization_level = calculate_normalization_status(
        table, table_type, schema
    )
    if normalization_level >= normalization_flag:
        return {table_type: [table]}
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
        active_table, foreign_table, id_mapping = _split_prefixed_columns(
            active_table,
            forward_relation_column,
            foreign_table_type,
            normalization_flag,
        )
        updated_database["IdMap"].append(id_mapping)
        # recursive step
        foreign_table_derived_database = normalize_table_completely(
            foreign_table,
            foreign_table_type,
            schema,
            normalization_flag,
            replace_existing_ids,
        )
        for derived_table_type in foreign_table_derived_database:
            updated_database[derived_table_type].extend(
                foreign_table_derived_database[derived_table_type]
            )
    updated_database[table_type].append(active_table)
    return updated_database


def convert_to_3NF_from_1NF(
    first_normal_form_database: dict[str, list[pd.DataFrame]],
    schema: DataSchema,
    replace_existing_ids: bool = True,
) -> dict[str, list[pd.DataFrame]]:
    """Converts table in first normal form (1NF) to third normal form (3NF)

    Creates additional tables

    Iteratively converts columns that are dependent on foreign relation
    columns to

    Args:
        first_normal_form_database: table that has already been converted to 1NF
        replace_existing_ids: If the relation already has an ID present,
            replace it if True. If it is replaced produce a table storing
            the mapping of old key to new key.

    Return:
        TODO specificy 'database'. Database where each table is in 3NF.
    """
    database_3NF = schema.empty_database()
    for table_type in first_normal_form_database:
        for table in first_normal_form_database[table_type]:
            updated_database = normalize_table_completely(
                table, table_type, schema, THIRD_NORMAL_FORM_FLAG, replace_existing_ids
            )
            for normalized_table_type in updated_database:
                database_3NF[normalized_table_type].extend(
                    updated_database[normalized_table_type]
                )

    return database_3NF


def convert_to_4NF_from_3NF(
    third_normal_form_database: dict[str, list[pd.DataFrame]],
    schema: DataSchema,
    replace_existing_ids: bool = True,
) -> dict[str, list[pd.DataFrame]]:
    """Converts table in third normal form (3NF) to fourth normal form (4NF)

    Args:
        third_normal_form_table: table with no repeating values or attributes
            that are do not depend on the full key
    """
    database_4NF = schema.empty_database()
    for table_type in third_normal_form_database:
        for table in third_normal_form_database[table_type]:
            updated_database = normalize_table_completely(
                table, table_type, schema, FOURTH_NORMAL_FORM_FLAG, replace_existing_ids
            )
            for normalized_table_type in updated_database:
                database_4NF[normalized_table_type].extend(
                    updated_database[normalized_table_type]
                )
    return database_4NF


def normalize_database(
    database: dict[str, list[pd.DataFrame]],
    schema: DataSchema,
    replace_existing_ids: bool = True,
) -> dict[str, pd.DataFrame]:
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
    database_3NF = convert_to_3NF_from_1NF(database_1NF, schema, replace_existing_ids)
    database_4NF = convert_to_4NF_from_3NF(database_3NF, schema, replace_existing_ids)
    normalized_database = {}
    for table_type in database_4NF:
        if database_4NF[table_type] != []:
            normalized_database[table_type] = pd.concat(
                database_4NF[table_type], ignore_index=True
            )
    return normalized_database


# def _normalize_table_helper(
#     table: pd.DataFrame, table_type: str unique_prefix: str, level: int
# ) -> dict[str, list[pd.DataFrame]]:
#     """Normalizes tables at given level of normalization to next level

#     Args:
#         tables: TODO is this a TABLE or a dataframe? is a table multiple??
#     Returns:
#         a database TODO: this should be defined somewhere/or an object
#     """
#         if level == UNNORMALIZED_FLAG:
#             normalized_database = self.convert_to_1NF_from_unnormalized()
#         elif level == FIRST_NORMAL_FORM_FLAG:
#             normalized_database = self.convert_to_3NF_from_1NF()
#         elif level == THIRD_NORMAL_FORM_FLAG:
#             normalized_database = self.convert_to_4NF_from_3NF()
#         elif level == FOURTH_NORMAL_FORM_FLAG:
#             normalized_database = {table.table_type: [table]}
#         else:
#             raise ValueError(f"level {level} is invalid")

#     return normalized_database

# def normalize_table(
#     self, table: pd.DataFrame, table_type: str, desired_normalization_level: int = 4
# ) -> pd.DataFrame:
#     """"""
#     columns_by_normalization_level, normalization_level = (
#         self.get_normalization_level_of_columns(table, table_type)
#     )
#     while normalization_level < desired_normalization_level:
#         for unique_prefix in columns_by_normalization_level[normalization_level]:
#             modified_table, other_tables

#         columns_by_normalization_level, normalization_level = (
#             self.get_normalization_level_of_columns(table, table_type)
#         )


# class Database:
#     """Database"""

#     @property
#     def database(self) -> dict:
#         """TODO"""
#         return self._database

#     def __init__(
#         self, path_to_schema_yaml: Path | str, database: dict[str, list[pd.DataFrame]]
#     ) -> None:
#         """Representation of a data schema defined by provided yaml

#         Args:
#             path_to_yaml: Path to a yaml. Should have the TODO format
#             database: dict mapping table names to list of tables of the given type
#         """
#         with Path(path_to_schema_yaml).open("r") as f:
#             self.data_schema = yaml.safe_load(f)

#         # set database
#         self.database = {}
#         for table_type in self.data_schema:
#             self.database[table_type] = Table(
#                 self.data_schema, table_type, database.get(table_type, [])
#             )

#     def _normalize_table_helper(
#         self, tables: list[Table], unique_prefix: str, level: int
#     ) -> dict[str, list[pd.DataFrame]]:
#         """Normalizes tables at given level of normalization to next level

#         Args:
#             tables: TODO is this a TABLE or a dataframe? is a table multiple??
#         Returns:
#             a database TODO: this should be defined somewhere/or an object
#         """
#         database = None
#         for table in tables:
#             if level == UNNORMALIZED_FLAG:
#                 normalized_database = self.convert_to_1NF_from_unnormalized()
#             elif level == FIRST_NORMAL_FORM_FLAG:
#                 normalized_database = self.convert_to_3NF_from_1NF()
#             elif level == THIRD_NORMAL_FORM_FLAG:
#                 normalized_database = self.convert_to_4NF_from_3NF()
#             elif level == FOURTH_NORMAL_FORM_FLAG:
#                 normalized_database = {table.table_type: [table]}
#             else:
#                 raise ValueError(f"level {level} is invalid")
#             if database is None:
#                 database = normalized_database
#             else:
#                 for table_type in database:
#                     database[table_type].extend(normalized_database.get(table_type, []))
#         return database

#     def normalize_table(
#         self, table: pd.DataFrame, table_type: str, desired_normalization_level: int = 4
#     ) -> pd.DataFrame:
#         """"""
#         columns_by_normalization_level, normalization_level = (
#             self.get_normalization_level_of_columns(table, table_type)
#         )
#         while normalization_level < desired_normalization_level:
#             for unique_prefix in columns_by_normalization_level[normalization_level]:
#                 modified_table, other_tables

#             columns_by_normalization_level, normalization_level = (
#                 self.get_normalization_level_of_columns(table, table_type)
#             )


#     def convert_to_4NF_from_3NF(
#         self,
#         third_normal_form_table: pd.DataFrame,
#     ) -> dict[str, list[pd.DataFrame]]:
#         """Converts table in third normal form (3NF) to fourth normal form (4NF)

#         Args:
#             third_normal_form_table: table with no repeating values or attributes
#                 that are do not depend on the full key
#         """
#         database = {"id_mapping": pd.DataFrame()}  # mapping of table name to dataframe
#         active_table = third_normal_form_table
#         for (
#             multivalued_fact_column,
#             foreign_table_validator,
#         ) in self.multivalued_columns.items():
#             active_table, foreign_table, id_mapping = self._split_prefixed_columns(
#                 third_normal_form_table,
#                 multivalued_fact_column,
#                 foreign_table_validator.entity_type,
#                 False,
#             )
#             database["id_mapping"] = pd.concat([id_mapping, database["id_mapping"]])

#             database[self.entity_type] = active_table
#             database[foreign_table_validator.entity_type] = foreign_table
#         return database


# class EntityType:
#     """Base class for any entity type represented by a table in campaign finance data

#     Design notes: Initiially this was written as a base class that had
#     subclasses for each entity type. Most of the constants defined were class level
#     properties and not instance properties* and all methods were implemented in the
#     base class. This felt smelly since class instances were meaningless and had no
#     useful state. Additionally inheritence was of limited use.

#     *one issue this ran into was the deprecation of chaining @classmethod and @property
#     decorators in Python 3.11. An implementation of a @property method allowing
#     for inheritence of class properties was tested. The amount of inheritence vs
#     the added complexity of implementing this as a class made this feel like more
#     trouble than it's worth. TODO: move out of docstring??
#     """

#     def _check_yaml_validity(self) -> None:
#         """Confirm data_schema loaded in from yaml is valid"""
#         for enum_column in self.enum_columns:
#             if enum_column not in self.attributes:
#                 raise ValueError(f"{enum_column} not in attributes")
#         for repeating_column in self.repeating_columns:
#             if repeating_column not in self.attributes:
#                 raise ValueError(f"{repeating_column} not in attributes")
#         # for (
#         #     _,
#         #     multivalued_column_type,
#         # ) in self.multivalued_columns.items():
#         #     if multivalued_column_type not in self.data_schema:
#         #         raise ValueError(
#         #             f"Related type {multivalued_column_type} "
#         #             "listed in multivalued column but not in provided yaml"
#         #         )
#         # for _, forward_relation_type in self.forward_relations.items():
#         #     if forward_relation_type not in self.data_schema:
#         #         raise ValueError(
#         #             f"Related type {forward_relation_type} "
#         #             "listed in forward relations but not in provided yaml"
#         #         )
#         if self.parent_type and self.parent_type not in self.data_schema:
#             raise ValueError(f"{self.parent_type} not in prodived yaml")

#     def _check_table_validity(self, table: pd.DataFrame) -> None:
#         """Check whether an input table is a valid representation of the entity type"""
#         if any(
#             column not in self.unnormalized_form_columns for column in table.columns
#         ):
#             raise ValueError(
#                 f"Provided table has invalid columns for {self.entity_type} tables"
#             )
#         if any(column not in table.columns for column in self.required_attributes):
#             raise ValueError(
#                 f"Table is missing required columns for {self.entity_type}"
#             )
#         for column in self.enum_columns:
#             if column in table.columns:
#                 if ~table[column].isin(self.enum_columns[column]).sum() > 0:
#                     raise ValueError(
#                         f"{column} in {self.entity_type} table has invalid values"
#                     )
#         for required_column in self.required_attributes:
#             if not any(column.startswith(required_column) for column in table.columns):
#                 raise ValueError(
#                     f"Table missing required information about {required_column}"
#                 )

#     def _instantiate_related_entities(self, relation_dict: dict) -> None:
#         """Replace strings representing related columns with EntityType objects

#         # TODO: change to make sure that we're only recursing on columns that exist in the table

#         In attributes that map columns to other entity types, instantiate
#         those entity types and replace the string with EntityType objects
#         """
#         for column, column_type in relation_dict.items():
#             if column_type == self.instantiating_type:
#                 continue
#             if isinstance(column_type, EntityType):
#                 # when instantiated with data_schame_dict, relation types
#                 # may already be expanded. (TODO: is this bad?)
#                 continue
#             relation_dict[column] = EntityType(
#                 column_type,
#                 data_schema_dict=self.data_schema,
#                 instantiating_type=self.entity_type,
#             )

#     def __init__(
#         self,
#         entity_type: str,
#         path_to_yaml: None | Path = None,
#         data_schema_dict: None | dict = None,
#         instantiating_type: None | str = None,
#     ) -> None:
#         """Object that checks schema of entity type according to yaml

#         Args:
#             entity_type:
#             path_to_yaml:
#             data_schema_dict:
#             instantiating_type: if instantiated by another instance of EntityType,
#                 teh type of EnitityType. This is to prevent endless recursion
#                 when determining legal column names
#                 (i.e. donor-addresss-transactor-address-transactor-address-...)
#         """
#         print(f"instantiating {entity_type}")
#         if path_to_yaml is not None:
#             with path_to_yaml.open("r") as f:
#                 self.data_schema = yaml.safe_load(f)
#         elif data_schema_dict is not None:
#             self.data_schema = data_schema_dict
#         else:
#             raise ValueError(
#                 "One of 'path_to_yaml' or 'data_schema_dict' must be provided"
#             )
#         self.entity_type = entity_type
#         self.entity_details = self.data_schema[entity_type]
#         self._child_types_are_separate = None
#         self._instantiating_type = instantiating_type
#         self._parent_type = None
#         self._attributes = None
#         self._repeating_columns = None
#         self._multivalued_columns = None
#         self._forward_relations = None
#         self._enum_columns = None
#         self._required_attributes = None
#         self._fourth_normal_form_columns = None
#         self._third_normal_form_columns = None
#         self._first_normal_form_columns = None
#         self._unnormalized_form_columns = None
#         self._normalization_levels_allowed_columns = None
#         self._repeating_columns_regex_list = None
#         self._repeating_columns_regex = None

#         self._check_yaml_validity()

#     @property
#     def fourth_normal_form_columns(self) -> list[str]:
#         """List of columns allowed in fourth normal form"""
#         if self._fourth_normal_form_columns is None:
#             self._fourth_normal_form_columns = self.attributes
#         return self._fourth_normal_form_columns

#     @property
#     def third_normal_form_columns(self) -> list[str]:
#         """Columns allowed for table after all columns are a function of primary key"""
#         if self._third_normal_form_columns is None:
#             self._third_normal_form_columns = self.fourth_normal_form_columns + [
#                 f"{relation_name}-{relation_attribute}"
#                 for relation_name in self.multivalued_columns
#                 for relation_attribute in self.multivalued_columns[
#                     relation_name
#                 ].third_normal_form_columns
#             ]
#         return self._third_normal_form_columns

#     @property
#     def first_normal_form_columns(self) -> list[str]:
#         """All columns allowed for a table after repeating columns eliminated"""
#         if self._first_normal_form_columns is None:
#             self._first_normal_form_columns = self.third_normal_form_columns + [
#                 f"{relation_name}-{relation_attribute}"
#                 for relation_name in self.forward_relations
#                 for relation_attribute in self.forward_relations[
#                     relation_name
#                 ].first_normal_form_columns
#                 if self.forward_relations[relation_name].entity_type
#                 != self.instantiating_type
#             ]
#         return self._first_normal_form_columns

#     @property
#     def normalization_levels_allowed_columns(self) -> dict[str, list]:
#         """Maps string names of normalization levels to their list of allowed cols"""
#         if self._normalization_levels_allowed_columns is None:
#             self._normalization_levels_allowed_columns = {
#                 "unnormalized": self.unnormalized_form_columns,
#                 "first_normal_form": self.first_normal_form_columns,
#                 "normalized": self.attributes,
#             }
#         return self._normalization_levels_allowed_columns

#     @property
#     def repeating_columns_regex_list(self) -> list[str]:
#         """List of regexes matching potential repeating column names"""
#         if self._repeating_columns_regex_list is None:
#             self._repeating_columns_regex_list = [
#                 repeated_column + "-\d+$" for repeated_column in self.repeating_columns
#             ]
#         return self._repeating_columns_regex_list

#     @property
#     def repeating_columns_regex(self) -> re.Pattern:
#         """Full regex to match any repeating columns"""
#         if self._repeating_columns_regex is None:
#             self._repeating_columns_regex = re.compile(
#                 "|".join(self.repeating_columns_regex_list)
#             )
#         return self._repeating_columns_regex

#     @property
#     def unnormalized_form_columns(self) -> list[str]:
#         """All columns that could be allowed in a table for the given entity type.

#         Includes all attributes of the entity type, the attributes of all relations
#         of the entity type prefixed by the relation name and a '-', and all repeating
#         groups of columns suffixed by a '-' and their repitition number.
#         """
#         if self._unnormalized_form_columns is None:
#             self._unnormalized_form_columns = (
#                 self.first_normal_form_columns + self.repeating_columns_regex_list
#             )
#         return self._unnormalized_form_columns

#     def is_valid_schema(self, table: pd.DataFrame, normalization_level: int) -> bool:
#         """Check if a provided table has a valid schema for given normalization level

#         Checks column names and types.

#         Args:
#             table: arbitrary pandas dataframe
#             normalization_level: expected level of normalization. Should be one of:
#                 0 - unnormalized
#                 1 - first_normal_form
#                 3 - third_normal_form
#                 4 - fourth_normal_form
#         Returns: True if schema is valid
#         """
#         pass

#     def normalize_table(
#         self,
#         valid_table: pd.DataFrame,
#         desired_normalization_level: int | None = 4,
#         starting_normalization_level: str | None = None,
#     ) -> dict[str, pd.DataFrame]:
#         """Normalizes a table to desired normalization level

#         Args:
#             valid_table: Table with valid schema
#             desired_normalization_level: Desired end state of table. Default is 4NF.
#             starting_normalization_level: Normalization level of provided table.
#                 If none is provided, attempt to algorithmically determine
#         Returns:
#             dictionary mapping table names provided in data_schema to dataframes with
#             valid forms of that table type at desired_normalization_level
#         """
#         if starting_normalization_level is None:
#             starting_normalization_level = self._determine_normalization_level(
#                 valid_table
#             )
#         current_level = starting_normalization_level

#         normalization_functions = [
#             self.convert_to_1NF_from_unnormalized,
#             self.convert_to_3NF_from_1NF,
#             None,
#             self.convert_to_4NF_from_3NF,
#         ]
#         database = {self.entity_type: valid_table}

#         while current_level < desired_normalization_level:
#             for table in database:
#                 resultant_database, current_level = normalization_functions[
#                     current_level
#                 ](table)
#                 # TODO: how to reasonably join two databases?
#                 # many will be sparse and no collisions. In the case of a
#                 # collision, there are cases:
#                 #   1 - each repr of the same table has the same data.
#                 #   2 - each repr of the same table has disjoint data
#                 #   3 - one repr of the same table has updated versions of the
#                 #       same rows in the other table
#                 # 3 seems problematic but impossible?

#     def convert_to_1NF_from_unnormalized(
#         self, unnormalized_table: pd.DataFrame
#     ) -> pd.DataFrame:
#         """Converts an unnormalized table into first normal form (1NF)

#         Removes anticipated repeating groups

#         Args:
#             unnormalized_table: table with valid unnormalized schema
#         """
#         repeated_columns = [
#             column
#             for column in unnormalized_table.columns
#             if self.repeating_columns_regex.match(column)
#         ]
#         static_columns = [
#             column
#             for column in unnormalized_table.columns
#             if column not in repeated_columns
#         ]
#         melted_table = pd.melt(
#             unnormalized_table, id_vars=static_columns, value_vars=repeated_columns
#         )
#         melted_table[["variable", "instance"]] = melted_table["variable"].str.rsplit(
#             "-", n=1, expand=True
#         )
#         first_normal_form_table = melted_table.pivot_table(
#             index=static_columns + ["instance"],
#             columns="variable",
#             values="value",
#             aggfunc="first",
#         ).reset_index()
#         return first_normal_form_table

#     def convert_to_3NF_from_1NF(
#         self, first_normal_form_table: pd.DataFrame, replace_existing_ids: bool = True
#     ) -> dict[str, pd.DataFrame]:
#         """Converts table in first normal form (1NF) to third normal form (3NF)

#         Creates additional tables

#         Iteratively converts columns that are dependent on foreign relation
#         columns to

#         Args:
#             first_normal_form_table: table that has already been converted to 1NF
#             replace_existing_ids: If the relation already has an ID present,
#                 replace it if True. If it is replaced produce a table storing
#                 the mapping of old key to new key.

#         Return:
#             dictionary mapping table type to pandas dataframe with 3NF table
#         """
#         database = {"id_mapping": pd.DataFrame()}  # mapping of table name to dataframe
#         active_table = first_normal_form_table
#         for (
#             forward_relation_column,
#             foreign_table_validator,
#         ) in self.forward_relations.items():
#             active_table, foreign_table, id_mapping = self._split_prefixed_columns(
#                 first_normal_form_table,
#                 forward_relation_column,
#                 foreign_table_validator.entity_type,
#                 True,
#             )
#             database["id_mapping"] = pd.concat([id_mapping, database["id_mapping"]])

#             database[self.entity_type] = active_table
#             database[foreign_table_validator.entity_type] = foreign_table
#         return database

#     def _split_prefixed_columns(
#         self,
#         table: pd.DataFrame,
#         foreign_key_prefix: str,
#         foreign_table_type: str,
#         keep_reference: bool,
#     ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#         """Remove columns starting with prefix from table, optionally keeping foreign key

#         Args:
#             table: any pandas dataframe
#             foreign_key_prefix: prefix of columns to be split from table. prefix should be in one of
#                 self.forward_relations or self.multivalued_columns
#             foreign_table_type: type of table created from prefixed columns in table
#             keep_reference: if true, keep a {prefix}-id column referring to associated
#                 row in split table
#         Returns: original table with columns removed, new foreign table,
#             and id mapping table
#         """
#         if keep_reference:
#             foriegn_key_column = f"{foreign_key_prefix}-id"
#             if foriegn_key_column not in table.columns:
#                 table[foriegn_key_column] = None

#         # TODO: handle prefixes with prefixes.
#         # example: donor-election_result-election-office_sought
#         prefixed_columns = [
#             column for column in table.columns if column.startswith(foriegn_key_column)
#         ]
#         new_foreign_columns = [
#             column[len(foreign_key_prefix) + 1 :] for column in table.columns
#         ]
#         columns_to_drop = prefixed_columns

#         if keep_reference:
#             # standardize id column to use uuids and update database 'id_mapping'
#             # table to include mapping of old ids to new uuids
#             table, id_mapping = replace_id_with_uuid(table, foriegn_key_column)
#             id_mapping["table"] = foreign_table_type
#             columns_to_drop.remove(foriegn_key_column)
#         else:
#             id_mapping = None
#         # clean up foreign table
#         forign_key_table = table[prefixed_columns]
#         forign_key_table.colums = new_foreign_columns
#         foreign_table_type = forign_key_table.drop_duplicates()
#         # foreign_table_validator = EntityType(
#         #     foreign_table_type, data_schema_dict=self.data_schema
#         # )
#         # TODO: bring forieng table to 3nf and any derivative tables?
#         if keep_reference:
#             forign_key_table = forign_key_table.reset_index("id")

#         table = table.drop(columns=columns_to_drop)
#         return table, forign_key_table, id_mapping

#     def convert_to_4NF_from_3NF(
#         self,
#         third_normal_form_table: pd.DataFrame,
#     ) -> dict[str, pd.DataFrame]:
#         """Converts table in third normal form (3NF) to fourth normal form (4NF)

#         Args:
#             third_normal_form_table: table with no repeating values or attributes
#                 that are do not depend on the full key
#         """
#         database = {"id_mapping": pd.DataFrame()}  # mapping of table name to dataframe
#         active_table = third_normal_form_table
#         for (
#             multivalued_fact_column,
#             foreign_table_validator,
#         ) in self.multivalued_columns.items():
#             active_table, foreign_table, id_mapping = self._split_prefixed_columns(
#                 third_normal_form_table,
#                 multivalued_fact_column,
#                 foreign_table_validator.entity_type,
#                 False,
#             )
#             database["id_mapping"] = pd.concat([id_mapping, database["id_mapping"]])

#             database[self.entity_type] = active_table
#             database[foreign_table_validator.entity_type] = foreign_table
#         return database

#     normalization_levels = {
#         0: "unnormalized",
#         1: "first_normal_form",
#         3: "third_normal_form",
#         4: "fourth_normal_form",
#     }

#     def _determine_normalization_level(self, valid_table: pd.DataFrame) -> int:
#         """Based on column names, determine normalization level of table"""
#         self._check_table_validity(valid_table)
#         unnormalized_forward_relation_regex = re.compile(
#             "|".join(
#                 [
#                     f"{relation_name}-(?!id)[a-zA-Z0-9]+"
#                     for relation_name in self.forward_relations
#                 ]
#             )
#         )
#         unnormalized_multivalued_columns_regex = re.compile(
#             "|".join(self.multivalued_columns)
#         )

#         normalization_regexes = [
#             (0, self.repeating_columns_regex),
#             (1, unnormalized_forward_relation_regex),
#             (3, unnormalized_multivalued_columns_regex),
#         ]

#         for form, regex in normalization_regexes:
#             if any(regex.match(column) for column in valid_table.columns):
#                 return form
#         # if no columns are representing other forms, then the table is in 4NF
#         return 4

#     def contains_valid_column_names(self, normalization_level: str) -> bool:
#         """Checks if table's columns have the proper names for the normalization level

#         Args:
#             table: table representing EntityType
#             normalization_level: expected level of normalization. Valid values are:
#                 - unnormalized
#                 - first_normal_form
#                 - third_normal_form
#                 - fourth_normal_form
#         """
#         allowed_columns = self.normalization_levels_allowed_columns[normalization_level]
#         allowed_columns_regex = re.compile("|".join(allowed_columns))
#         for column in self.table.columns:
#             if not allowed_columns_regex.match(column):
#                 # debug
#                 print(f"{column} not in list of allowed columns")
#                 return False
#         return True

#     def check_enum_columns(self, table: pd.DataFrame) -> bool:
#         """Check if table's enumeratin columns contain valid values

#         Args:
#             table: table representing EntityType
#         """
#         for enum_column in self.enum_columns:
#             enum_column_pattern = re.compile(f"-?{enum_column}$")
#             for table_column in table.columns:
#                 if enum_column_pattern.match(table_column):
#                     if (
#                         not table[table_column]
#                         .isin(self.enum_columns[enum_column])
#                         .all()
#                     ):
#                         # debug
#                         print(table_column)
#                         return False
#         return True

#     def first_normal_form(self) -> None:
#         """Convert the unnormalized table to frist normal form"""
#         return
