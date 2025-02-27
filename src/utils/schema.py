"""Classes for representing the schema of relevant tables

Schema is derived from configuration in a yaml configuration file, defined
here TODO
"""

import copy
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
    """class representation of a single table schema"""

    @property
    def inheritance_strategy(self) -> str:
        """How to handle inheritance in tables

        Options are single table inheritance or class table inheritance
        - https://martinfowler.com/eaaCatalog/singleTableInheritance.html
        - https://martinfowler.com/eaaCatalog/classTableInheritance.html
        """
        return self._inheritance_strategy

    @inheritance_strategy.setter
    def inheritance_strategy(self, strategy: str) -> None:
        if strategy not in {"single table inheritance", "class table inheritance"}:
            raise ValueError("Invalid inheritance strategy")
        self._inheritance_strategy = strategy

    @property
    def attributes(self) -> list:
        """List of attributes allowed for this entity"""
        return self.property_cache[self.inheritance_strategy]["attributes"]

    @property
    def attributes_regex(self) -> re.Pattern:
        """Full regex to match attributes"""
        return self.property_cache[self.inheritance_strategy]["attributes_regex"]

    @property
    def repeating_columns(self) -> list:
        """List of columns that may be repeated in unnormalized form"""
        return self.property_cache[self.inheritance_strategy]["repeating_columns"]

    @property
    def repeating_columns_regex(self) -> re.Pattern:
        """Full regex to match any repeating columns"""
        return self.property_cache[self.inheritance_strategy]["repeating_columns_regex"]

    @property
    def reverse_relations(self) -> dict[str, Self]:
        """Columns that may have multiple values for an instance of the entity type

        For example, an individual may have multiple addresses or employers
        """
        return self.property_cache[self.inheritance_strategy]["reverse_relations"]

    @property
    def reverse_relation_names(self) -> dict[str, Self]:
        """For each reverse relation, the name of the backlink to this table"""
        return self.property_cache[self.inheritance_strategy]["reverse_relation_names"]

    @property
    def relations(self) -> dict[str, Self]:
        """List of columns that are either forward or reverse relations"""
        return {
            **self.property_cache[self.inheritance_strategy]["forward_relations"],
            **self.property_cache[self.inheritance_strategy]["reverse_relations"],
        }

    @property
    def reverse_relations_regex(self) -> re.Pattern:
        """Full regex to match any multivalued columns"""
        return self.property_cache[self.inheritance_strategy]["reverse_relations_regex"]

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
        return self.property_cache[self.inheritance_strategy]["forward_relations"]

    @property
    def forward_relations_regex(self) -> re.Pattern:
        """Full regex to match any forward relation columns"""
        return self.property_cache[self.inheritance_strategy]["forward_relations_regex"]

    @property
    def enum_columns(self) -> dict[str, list]:
        """Map enum column names to their list of allowed values"""
        return self.property_cache[self.inheritance_strategy]["enum_columns"]

    @property
    def required_attributes(self) -> dict[str, list]:
        """Attributes required for a given record of the entity type to be relevant"""
        return self.property_cache[self.inheritance_strategy]["required_attributes"]

    @property
    def child_types(self) -> list:
        """Types that inherit attributes from the current type"""
        return self._child_types

    @property
    def parent_type(self) -> dict[str, list]:
        """Parent type of given entity type"""
        return self._parent_type

    def __init__(self, data_schema: dict, table_type: str):  # noqa ANN204
        """Creates a tableschema instance used to validate tables

        Args:
            data_schema: dict
            table_type: string must be a key in data_schema
        """
        if table_type not in data_schema:
            raise KeyError(
                f"{table_type} not found in {data_schema}. Available options: {data_schema.keys()}"
            )
        self.table_type = table_type
        self.data_schema = data_schema
        self.property_cache = {
            "single table inheritance": {},
            "class table inheritance": {},
        }
        self._inheritance_strategy = "single table inheritance"
        class_properties = [
            ("attributes", "list"),
            ("required_attributes", "list"),
            ("enum_columns", "dict"),
            ("repeating_columns", "list"),
            ("forward_relations", "dict"),
            ("reverse_relations", "dict"),
            ("reverse_relation_names", "dict"),
        ]
        for inheritance_strategy in self.property_cache.keys():
            for property_type, property_value_type in class_properties:
                self.property_cache[inheritance_strategy][property_type] = (
                    self._fill_properties(
                        property_type,
                        property_value_type,
                        inheritance_strategy,
                        data_schema,
                    )
                )
                if property_type in ["forward_relations", "reverse_relations"]:
                    self.property_cache[inheritance_strategy][
                        f"{property_type}_regex"
                    ] = self._get_regex(
                        list(self.property_cache[inheritance_strategy][property_type])
                    )
                elif property_type == "repeating_columns":
                    repeating_columns_regex_list = [
                        repeated_column + "-\d+$"
                        for repeated_column in self.property_cache[
                            inheritance_strategy
                        ][property_type]
                    ]
                    self.property_cache[inheritance_strategy][
                        f"{property_type}_regex"
                    ] = self._get_regex(repeating_columns_regex_list)
                elif property_type == "attributes":
                    self.property_cache[inheritance_strategy][
                        f"{property_type}_regex"
                    ] = re.compile("|".join(self.attributes))
        self._child_types = self.data_schema[table_type].get("child_types", [])
        self._parent_type = self.data_schema[table_type].get("parent_type", None)

    def _fill_properties(
        self,
        property_name: str,
        property_type: str,
        inheritance_strategy: str,
        data_schema: dict,
    ) -> list | dict:
        """Calculate properties from given data schema"""
        if property_type not in {"list", "dict"}:
            raise ValueError(
                "Property type for _fill_properties should be list or dict."
            )

        # Make sure we don't modify the original data schema
        property_value = copy.deepcopy(
            self.data_schema[self.table_type].get(
                property_name, [] if property_type == "list" else {}
            )
        )

        if inheritance_strategy == "class table inheritance":
            return property_value

        # Ensure we're working with a separate copy
        accumulated_values = (
            set(property_value)
            if property_type == "list"
            else copy.deepcopy(property_value)
        )

        # Process child types
        for child in self.data_schema[self.table_type].get("child_types", []):
            child_value = copy.deepcopy(
                self.data_schema[child].get(
                    property_name, [] if property_type == "list" else {}
                )
            )
            if property_type == "list":
                accumulated_values.update(child_value)
            else:
                accumulated_values.update(child_value)

        # Process parent type
        parent_type_name = self.data_schema[self.table_type].get("parent_type")
        if parent_type_name:
            parent_value = copy.deepcopy(
                self.data_schema[parent_type_name].get(
                    property_name, [] if property_type == "list" else {}
                )
            )
            if property_type == "list":
                accumulated_values.update(parent_value)
            else:
                accumulated_values.update(parent_value)

        if isinstance(accumulated_values, dict) and property_name in [
            "reverse_relations",
            "forward_relations",
        ]:
            updated_values = {}
            for key, related_table_type in accumulated_values.items():
                if data_schema[related_table_type].get("parent_type", None):
                    updated_values[key] = data_schema[related_table_type]["parent_type"]
                else:
                    updated_values[key] = related_table_type
            accumulated_values = updated_values

        return (
            list(accumulated_values) if property_type == "list" else accumulated_values
        )


class DataSchema:
    """Class representation of a data schema defined in a YAML file"""

    @property
    def schema(self) -> dict[str, TableSchema]:
        """Maps table types to their TableSchemas"""
        if self._schema is None:
            self._schema = {}
            for table_type in self.raw_data_schema:
                self._schema[table_type] = TableSchema(self.raw_data_schema, table_type)
        return self._schema

    @property
    def inheritance_strategy(self) -> str:
        """How to handle inherited connections between tables"""
        return self._inheritance_strategy

    @inheritance_strategy.setter
    def inheritance_strategy(self, strategy: str) -> None:
        if strategy not in {"single table inheritance", "class table inheritance"}:
            raise ValueError("Invalid inheritance strategy")
        self._inheritance_strategy = strategy
        for table in self.schema:
            self.schema[table].inheritance_strategy = strategy

    def empty_database(self) -> dict[str, list]:
        """Returns an empty database of the given schema"""
        return {table_type: [] for table_type in self.schema.keys()}

    def __init__(self, path_to_data_schema: Path | str) -> None:
        """Representation of a single table"""
        self._schema = None
        self._inheritance_strategy = "single table inheritance"
        with Path(path_to_data_schema).open("r") as f:
            self.raw_data_schema = yaml.safe_load(f)

        self.validate_schema()

    def validate_schema(self) -> None:
        """Validates the input YAML schema"""
        errors = []
        table_names = set(self.raw_data_schema.keys())

        for table_name, table_def in self.raw_data_schema.items():
            attributes = set(table_def.get("attributes", []))

            # Validate different aspects of the schema
            errors.extend(
                self._validate_forward_relations(
                    table_name, table_def, attributes, table_names
                )
            )
            errors.extend(
                self._validate_parent_child_relationships(
                    table_name, table_def, table_names
                )
            )
            errors.extend(
                self._validate_reverse_relations(table_name, table_def, table_names)
            )
            errors.extend(
                self._validate_attribute_consistency(table_name, table_def, attributes)
            )

        if errors:
            error_message = "\n".join(errors)
            raise ValueError(f"Schema validation failed:\n{error_message}")

    def _validate_forward_relations(
        self, table_name: str, table_def: dict, attributes: set, table_names: set
    ) -> list[str]:
        """Ensures all forward relations keys exist in attributes and values point to valid tables"""
        errors = []
        forward_relations = table_def.get("forward_relations", {})

        for relation_key, relation_value in forward_relations.items():
            if f"{relation_key}_id" not in attributes:
                errors.append(
                    f"Error in {table_name}: forward_relation key '{relation_key}' must be an attribute."
                )
            if relation_value not in table_names:
                errors.append(
                    f"Error in {table_name}: forward_relation value '{relation_value}' must be a valid table."
                )

        return errors

    def _validate_parent_child_relationships(
        self, table_name: str, table_def: dict, table_names: set
    ) -> list[str]:
        """Ensures all parent-child relationships exist and are bidirectional"""
        errors = []
        parent_type = table_def.get("parent_type")
        child_types = set(table_def.get("child_types", []))

        if parent_type and parent_type not in table_names:
            errors.append(
                f"Error in {table_name}: parent '{parent_type}' does not exist in schema."
            )

        for child in child_types:
            if child not in table_names:
                errors.append(
                    f"Error in {table_name}: child '{child}' does not exist in schema."
                )
            elif table_name not in self.raw_data_schema[child].get("parent_type", []):
                errors.append(
                    f"Error: {child} lists {table_name} as a child, but {table_name} does not list {child} as a parent."
                )

        return errors

    def _validate_reverse_relations(
        self, table_name: str, table_def: dict, table_names: set
    ) -> list[str]:
        """Ensures all reverse_relations values point to existing tables"""
        errors = []
        reverse_relations = table_def.get("reverse_relations", {})

        for column, related_table in reverse_relations.items():
            if related_table not in table_names:
                errors.append(
                    f"Error in {table_name}: reverse relation column '{column}' points to '{related_table}', which does not exist."
                )
            if column not in table_def.get("reverse_relation_names", {}):
                errors.append(
                    f"Error in {table_name}: reverse relation column '{column}' does "
                    "not have an entry in 'reverse_relation_names'. This is needed to "
                    "detect which column refers back to this table when normalizing"
                )

        return errors

    def _validate_attribute_consistency(
        self, table_name: str, table_def: dict, attributes: set
    ) -> list[str]:
        """Ensures enum_columns, repeating_columns, and required_attributes exist in attributes"""
        errors = []
        attribute_categories = [
            "enum_columns",
            "repeating_columns",
            "required_attributes",
        ]

        for attr_list_name in attribute_categories:
            for attr in table_def.get(attr_list_name, []):
                if attr not in attributes:
                    errors.append(
                        f"Error in {table_name}: {attr_list_name} '{attr}' must be listed in attributes."
                    )

        return errors
