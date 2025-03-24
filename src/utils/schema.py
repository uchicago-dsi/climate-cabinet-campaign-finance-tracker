"""Classes for representing the schema of relevant tables

Schema is derived from configuration in a yaml configuration file, defined
here TODO
"""

import re
from functools import cached_property
from pathlib import Path

import yaml
from typing_extensions import Self

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

    @cached_property
    def attributes(self) -> list:
        """List of attributes allowed for this entity"""
        return self._fill_properties_list("attributes")

    @cached_property
    def attributes_regex(self) -> re.Pattern:
        """Full regex to match attributes"""
        return re.compile("|".join(self.attributes))

    @cached_property
    def reverse_relations(self) -> dict[str, Self]:
        """Columns that may have multiple values for an instance of the entity type

        For example, an individual may have multiple addresses or employers
        """
        reverse_relations = self._fill_properties_dict("reverse_relations")
        if self.inheritance_strategy == "single table inheritance":
            reverse_relations = self._postprocess_relations(reverse_relations)
        return reverse_relations

    @cached_property
    def reverse_relation_names(self) -> dict[str, Self]:
        """For each reverse relation, the name of the backlink to this table"""
        return self._fill_properties_dict("reverse_relation_names")

    @cached_property
    def relations(self) -> dict[str, Self]:
        """List of columns that are either forward or reverse relations"""
        return {**self.reverse_relations, **self.forward_relations}

    @cached_property
    def reverse_relations_regex(self) -> re.Pattern:
        """Full regex to match any multivalued columns"""
        return self._get_regex(list(self.reverse_relations))

    def _get_regex(self, attribute_list: list[str]) -> re.Pattern:
        # add unmatchable regex so that empty list don't compile to '' which matches
        # everything
        unmatchable = "a^"
        possible_attributes = [f"^{attribute}$" for attribute in attribute_list]
        possible_attributes += [unmatchable]
        return re.compile("|".join(possible_attributes))

    @cached_property
    def forward_relations(self) -> dict[str, Self]:
        """Many-to-one relationships that are an attribute of the entity type"""
        forward_relations = self._fill_properties_dict("forward_relations")
        if self.inheritance_strategy == "single table inheritance":
            forward_relations = self._postprocess_relations(forward_relations)
        return forward_relations

    @cached_property
    def forward_relations_regex(self) -> re.Pattern:
        """Full regex to match any forward relation columns"""
        return self._get_regex(list(self.forward_relations))

    @cached_property
    def enum_columns(self) -> dict[str, list]:
        """Map enum column names to their list of allowed values"""
        return self._fill_properties_dict("enum_columns")

    @cached_property
    def required_attributes(self) -> dict[str, list]:
        """Attributes required for a given record of the entity type to be relevant"""
        return self._fill_properties_list("required_attributes")

    @property
    def child_types(self) -> list:
        """Types that inherit attributes from the current type"""
        return self._child_types

    @property
    def parent_type(self) -> str:
        """Parent type of given entity type"""
        return self._parent_type

    def __init__(
        self,
        data_schema: dict,
        table_name: str,
        inheritance_strategy: str = "single table inheritance",
    ) -> None:
        """Creates a tableschema instance used to validate tables

        Args:
            data_schema: dict
            table_name: string must be a key in data_schema
            inheritance_strategy: How to handle inheritance in tables
                Options are single table inheritance or class table inheritance
                - https://martinfowler.com/eaaCatalog/singleTableInheritance.html
                - https://martinfowler.com/eaaCatalog/classTableInheritance.html
        """
        if table_name not in data_schema:
            raise KeyError(
                f"{table_name} not found in {data_schema}. Available options: {data_schema.keys()}"
            )
        if inheritance_strategy not in [
            "single table inheritance",
            "class table inheritance",
        ]:
            raise KeyError(
                "Inheritance strategy must be 'single table inheritance'"
                f" or 'class table inheritance. Input was {inheritance_strategy}"
            )
        self.inheritance_strategy = inheritance_strategy
        self.table_name = table_name
        self.data_schema = data_schema
        self._child_types = self.data_schema[table_name].get("child_types", [])
        self._parent_type = self.data_schema[table_name].get("parent_type", None)

    def _fill_properties_dict(self, property_name: str) -> dict:
        """Calculate properties represented as dictionaries based on data schema"""
        property_value = {}
        if property_name in self.data_schema[self.table_name]:
            property_value = dict(self.data_schema[self.table_name][property_name])

        if self.inheritance_strategy == "class table inheritance":
            return property_value
        # If the schema is using single table inheritance, all values of parent
        # or child tables can be present in the table
        if self.parent_type:
            related_types = self.child_types + [self.parent_type]
        else:
            related_types = self.child_types
        for related_type in related_types:
            for key, value in (
                self.data_schema[related_type].get(property_name, {}).items()
            ):
                property_value[key] = value
        return property_value

    def _fill_properties_list(self, property_name: str) -> list:
        """Calculate properties represented as lists based on data schema"""
        property_value = []
        if property_name in self.data_schema[self.table_name]:
            property_value = list(self.data_schema[self.table_name][property_name])

        if self.inheritance_strategy == "class table inheritance":
            return property_value
        # If the schema is using single table inheritance, all values of parent
        # or child tables can be present in the table
        if self.parent_type:
            related_types = self.child_types + [self.parent_type]
        else:
            related_types = self.child_types
        for related_type in related_types:
            property_value.extend(self.data_schema[related_type].get(property_name, []))
        return property_value

    def _postprocess_relations(self, property_values: dict) -> dict[str, str]:
        """For single table inheritance, replace relation tables with their parents"""
        updated_values = {}
        for key, related_table_name in property_values.items():
            if self.data_schema[related_table_name].get("parent_type", None):
                updated_values[key] = self.data_schema[related_table_name][
                    "parent_type"
                ]
            else:
                updated_values[key] = related_table_name
        return updated_values


class DataSchema:
    """Class representation of a data schema defined in a YAML file"""

    @property
    def schema(self) -> dict[str, TableSchema]:
        """Maps table names to their TableSchemas"""
        if self._schema is None:
            self._schema = {}
            for table_name in self.raw_data_schema:
                self._schema[table_name] = TableSchema(self.raw_data_schema, table_name)
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
        return {table_name: [] for table_name in self.schema.keys()}

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
        """Ensures enum_columns, and required_attributes exist in attributes"""
        errors = []
        attribute_categories = [
            "enum_columns",
            "required_attributes",
        ]

        for attr_list_name in attribute_categories:
            for attr in table_def.get(attr_list_name, []):
                if attr not in attributes:
                    errors.append(
                        f"Error in {table_name}: {attr_list_name} '{attr}' must be listed in attributes."
                    )

        return errors
