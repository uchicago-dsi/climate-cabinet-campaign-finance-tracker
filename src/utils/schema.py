"""Classes for representing the schema of relevant tables

Schema is derived from configuration in a yaml configuration file, defined
here TODO
"""

import re
from pathlib import Path
from typing import Self

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
    """class representation of a single table schema

    Table schema includes

    Attributes:
        table_type (str): this is the type of the table

    """

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
