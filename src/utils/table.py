"""Abstract base class representing database tables

For each table, the maximal set of allowable column names in unnormalized form
includes:
  - each attribute possessed by the entity type the table represents,
  - each attribute possessed by any foreign key relations of the table,
    prefixed by the name of the foreign key relation (i.e transaction ->
    transactor may be named either donor or recipient), separated by a '-'.
  - each attribute possessed by a limited set of reverse foreign key relations
    of the table, prefixed by the name of the reverse foreign key relation.
    'Limited set' refers to all of those reverse foriegn key relations that
    are effectively 'few-to-one' and unlikely to change often enough to
    warrant normalizind after the initial processing steps. For example,
    transaction -> transactor is many to one and we create separate transaction
    and transactor tables immediately. address -> transactor changes
    infrequently, requires more sophisticated record linkage and is not worth
    immediately normalizing.
"""

import abc
import re

import pandas as pd

from utils.decorators import classproperty


class EntityType(abc.ABC):
    """Base class for any entity type represented by a table in campaign finance data"""

    normalization_levels = {
        0: "unnormalized",
        1: "first_normal_form",
        3: "third_normal_form",
        4: "fourth_normal_form",
    }

    def determine_normalization_level(self) -> str:
        """Based on column names, determine normalization level of table"""
        if any(
            self.repeating_columns_regex.match(column) for column in self.table.columns
        ):
            return 0
        # if no relation props, return normalized
        # otherwise first_normal_form
        # check validity

    def __init__(self, table: pd.DataFrame, normalization_level: str = None) -> None:
        super().__init__()
        self.table = table
        if normalization_level is None:
            normalization_level = self.determine_normalization_level()
        self.normalization_level = normalization_level
        # check valid table
        if not self.contains_valid_column_names(normalization_level):
            raise ValueError("Table contains invalid columns")

    @classproperty
    def normalization_levels_allowed_columns(self) -> dict[str, list]:
        """Maps string names of normalization levels to their list of allowed cols"""
        return {
            "unnormalized": self.unnormalized_allowed_columns(),
            "first_normal_form": self.first_normal_form_allowed_columns(),
            "normalized": self.attributes,
        }

    _attributes = []
    _reverse_relations = {}
    _forward_relations = {}
    _repeating_columns = {}
    _multivalued_columns = {}
    _enum_column_values = {}

    @classproperty
    def attributes(cls) -> list[str]:  # noqa N805
        """List of attributes allowed for this entity"""
        return cls._attributes

    @classproperty
    def reverse_relations(cls) -> dict:
        """Reverse relations for given entity type

        For example, an 'Address' table that has resident foriegn key to an
        'Individuals' table. In this case Individuals -> Address is a reverse
        relation
        """
        return cls._reverse_relations

    @classproperty
    def forward_relations(cls) -> dict:
        """Many-to-one relationships that are an attribute of the entity type"""
        return cls._forward_relations

    @classproperty
    def repeating_columns(cls) -> list[str]:
        """List of columns that may be repeated in unnormalized form"""
        return cls.repeating_columns

    @classproperty
    def multivalued_columns(cls) -> dict:
        """Columns that may have multiple values for an instance of the entity type

        For example, an individual may have multiple addresses or employers
        """
        return cls._multivalued_columns

    @classproperty
    def repeating_columns_regex_list(cls) -> list[str]:
        """List of regexes matching potential repeating column names"""
        return [repeated_column + "-\d+$" for repeated_column in cls.repeating_columns]

    @classproperty
    def repeating_columns_regex(cls) -> re.Pattern:
        return re.compile("|".join(cls.repeating_columns_regex_list))

    @classproperty
    def enum_column_values(cls) -> dict[str, list]:
        """Map enum column names to their list of allowed values"""
        return cls._enum_column_values

    @classproperty
    def first_normal_form_allowed_columns(self) -> list[str]:
        """All columns allowed for a table after repeating columns eliminated"""
        # return (
        #     self.attributes
        #     + [
        #         f"{relation_name}-{relation_attribute}"
        #         for relation_name in self.forward_relations
        #         for relation_attribute in self.forward_relations[
        #             relation_name
        #         ].attributes
        #     ]
        #     + [
        #         f"{relation_name}-{relation_attribute}"
        #         for relation_name in self.reverse_relations
        #         for relation_attribute in self.reverse_relations[
        #             relation_name
        #         ].attributes
        #     ]
        # )
        return self.third_normal_form_allowed_columns + [
            f"{relation_name}-{relation_attribute}"
            for relation_name in self.forward_relations
            for relation_attribute in self.forward_relations[
                relation_name
            ].first_normal_form_allowed_columns()
        ]

    @property
    def third_normal_form_allowed_columns(self) -> list[str]:
        """Columns allowed for table after all columns are a function of primary key"""
        return self.attributes + [
            f"{relation_name}-{relation_attribute}"
            for relation_name in self.multivalued_columns
            for relation_attribute in self.multivalued_columns[relation_name].attributes
        ]

    @property
    def unnormalized_allowed_columns(self) -> list[str]:
        """All columns that could be allowed in a table for the given entity type.

        Includes all attributes of the entity type, the attributes of all relations
        of the entity type prefixed by the relation name and a '-', and all repeating
        groups of columns suffixed by a '-' and their repitition number.
        """
        return (
            self.first_normal_form_allowed_columns + self.repeating_columns_regex_list
        )

    def contains_valid_column_names(self, normalization_level: str) -> bool:
        """Checks if table's columns have the proper names for the normalization level

        Args:
            table: table representing EntityType
            normalization_level: expected level of normalization. Valid values are:
                - unnormalized
                - first_normal_form
                - third_normal_form
                - fourth_normal_form
        """
        allowed_columns = self.normalization_levels_allowed_columns[normalization_level]
        allowed_columns_regex = re.compile("|".join(allowed_columns))
        for column in self.table.columns:
            if not allowed_columns_regex.match(column):
                # debug
                print(f"{column} not in list of allowed columns")
                return False
        return True

    def check_enum_columns(self, table: pd.DataFrame) -> bool:
        """Check if table's enumeratin columns contain valid values

        Args:
            table: table representing EntityType
        """
        for enum_column in self.enum_column_values:
            enum_column_pattern = re.compile(f"-?{enum_column}$")
            for table_column in table.columns:
                if enum_column_pattern.match(table_column):
                    if (
                        not table[table_column]
                        .isin(self.enum_column_values[enum_column])
                        .all()
                    ):
                        # debug
                        print(table_column)
                        return False
        return True

    def first_normal_form(self) -> None:
        """Convert the unnormalized table to frist normal form"""
        return


class Transactor(EntityType):
    """Any entity that is capable of performing a legal financial transaction"""

    @property
    def attributes(self):  # noqa D102
        return super().attributes + [
            "ID",
            "FULL_NAME",
            "ADDRESS",
            "TRANSACTOR_TYPE",
        ]

    @property
    def multivalued_columns(self) -> dict:  # noqa D102
        return super().multivalued_columns.update({"ADDRESS": Address()})

    @property
    def reverse_relations(self):  # noqa D102
        return super().reverse_relations.update({"ADDRESS": Address()})

    @property
    def enum_column_values(self) -> dict[str, list]:  # noqa D102
        return {
            "TRANSACTOR_TYPE": [
                "Individual",
                "Lobbyist",
                "Candidate",
                "Organization",
                "Vendor",
                "Corporation",
                "Non-profit",
                "Committee",
                "Party",
            ]
        }


class Individual(Transactor):
    """A person"""

    @property
    def attributes(self):  # noqa D102
        return super().attributes + [
            "FIRST_NAME",
            "MIDDLE_NAME",
            "LAST_NAME",
        ]

    @property
    def multivalued_columns(self) -> dict:  # noqa D102
        return super().multivalued_columns.update(
            {"EMPLOYER": Membership(), "ELECTION_RESTULT": ElectionResult()}
        )

    @property
    def forward_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {}


class Organization(Transactor):
    """A collection of individuals capable of transferring funds"""

    @property
    def attributes(self):  # noqa D102
        return super().attributes + [
            "PARENT_ORG_ID",
            "NAICS",
            "SIC",
            "STOCK_SYMBOL",
        ]

    @property
    def forward_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {"PARENT_ORG": Organization()}


class Transaction(EntityType):
    """Documents the transfer of cash or cash equivalents between transactors"""

    @property
    def attributes(self):  # noqa D102
        return super().attributes + [
            "ID",
            "DONOR_ID",
            "RECIPIENT_ID",
            "AMOUNT",
            "DATE",
            "YEAR",
            "MONTH",
            "DAY",
            "TRANSACTION_TYPE",
            "REPORTED_ELECTION",
            "DESCRIPTION",
        ]

    @property
    def reverse_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {}

    @property
    def forward_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {
            # TODO: More elegant way to do this? actually Transactor.
            # Both Individual and Organization attributes are actually allowed.
            # Unsure how to inhereit both. In practice, only Individual
            # specific columns have been observed.
            "DONOR": Individual(),
            "RECIPIENT": Individual(),
            "REPORTED_ELECTION": Election(),
        }

    @property
    def repeating_columns(self) -> list[str]:  # noqa D102
        # TODO: potential repeating columns are probably all non-key columns
        # repeated key columns can't be handled probably. This is fine for now
        return super().repeating_columns + ["DATE", "AMOUNT"]


class Election(EntityType):
    """A particular race for a particular elected office"""

    @property
    def attributes(self):  # noqa D102
        return super().attributes + [
            "ID",
            "YEAR",
            "DISTRICT",
            "OFFICE_SOUGHT",
        ]

    @property
    def reverse_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {}

    @property
    def forward_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {}


class ElectionResult(EntityType):
    """Describes performance of an Individual in an election"""

    @property
    def attributes(self):  # noqa D102
        return super().attributes + [
            "CANDIDATE_ID",
            "ELECTION_ID",
            "VOTES_RECEIVED",
            "WIN",
        ]

    @property
    def reverse_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {}

    @property
    def forward_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {"CANDIDATE_ID": Individual(), "ELECTION_ID": Election()}


class Address(EntityType):
    """The relation between a Transactor and their reported address"""

    @property
    def attributes(self):  # noqa D102
        return (
            super().attributes
            + [
                "TRANSACTOR_ID",
                "FULL_ADDRESS",
                "LINE_1",
                "LINE_2",
                "CITY",
                "STATE",
                "ZIP_CODE",
                "EARLIEST_KNOWN_DATE",
                "LATEST_KNOWN_DATE",
                "KNOWN_LATER_ADDRESS",  # The transactor has been listed at another address later
            ]
        )

    @property
    def reverse_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {}

    @property
    def forward_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {"TRANSACTOR": Transactor()}


class Membership(EntityType):
    """Relations between individuals and organizations such as employment"""

    @property
    def attributes(self):  # noqa D102
        return super().attributes + [
            "MEMBER_ID",
            "ORGANIZATION_ID",
            "MEMBERSHIP_TYPE",
            "MEMBERSHIP_DESCRIPTION",
            "EARLIEST_KNOWN_DATE",
            "LATEST_KNOWN_DATE",
            "KNOWN_LATER_EMPLOYER",  # for membership type employee
        ]

    @property
    def reverse_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {}

    @property
    def forward_relations(self) -> dict[str, EntityType]:  # noqa D102
        return {"MEMBER": Individual(), "ORGANIZATION": Organization()}
