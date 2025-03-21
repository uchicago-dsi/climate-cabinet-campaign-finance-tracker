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
from pathlib import Path

import pandas as pd

from utils.ids import handle_existing_ids, handle_id_column
from utils.schema import DataSchema

SPLIT = "--"
ID_SUFFIX = "_id"
REPEATING_COLUMN_REGEX = r"^[A-Za-z_]+-\d+$"


class Normalizer:
    """Class for normalizing a database according to a provided schema"""

    @property
    def id_mapping(self) -> dict[tuple, str]:
        """Dictionary mapping original ids to uuids

        Original ids are stored as tuples of the form (raw id, table_name, year, state)
        """
        return self._id_mapping

    def __init__(
        self, database: dict[str, pd.DataFrame], schema: DataSchema | Path | str
    ) -> None:
        """Create new normalizer

        Args:
            database: TODO
            schema: DataSchema object or path to yaml file containing one
        """
        self.database = database
        if isinstance(schema, str | Path):
            schema = DataSchema(schema)
        elif not isinstance(schema, DataSchema):
            raise RuntimeError("schema must be path or DataSchema object")
        self.schema = schema
        self._id_mapping = {}

    def get_foreign_table_name(self, base_type: str, column_name: str) -> str:
        """Retrieve the type of a multivalued/foreign table"""
        column_tokens = column_name.split(SPLIT)
        current_table_name = base_type
        for column_token in column_tokens:
            current_table_schema = self.schema.schema[current_table_name]
            if column_token in current_table_schema.relations:
                current_table_name = current_table_schema.relations[column_token]
                continue
            elif current_table_schema.attributes_regex.match(column_token):
                return current_table_name
        return current_table_name

    def _get_repeated_columns(self, table_columns: str) -> list[str]:
        r"""Given a list of columns and potentially repeating columns

        Example: 'amount' could be considered a repeating column if 'amount-\d' were
        in the table as well, but otherwise is not.
        """
        repeating_column_base_names = [
            col.split("-")[0]
            for col in table_columns
            if re.match(REPEATING_COLUMN_REGEX, col)
        ]
        return [
            col
            for col in table_columns
            if col.split("-")[0] in repeating_column_base_names
        ]

    def convert_to_1NF_from_unnormalized(self, table_name: str) -> None:
        """Converts an unnormalized table into first normal form (1NF)

        Removes anticipated repeating groups

        Args:
            table_name: name of table name to normalize
        Modifies:
            database[table_name] transformed to 1NF
        """
        unnormalized_table = self.database[table_name]
        repeated_columns = self._get_repeated_columns(unnormalized_table.columns)
        repeat_col_table = unnormalized_table[repeated_columns]
        if repeat_col_table.empty:
            return None
        # Add "nosuffix" as the suffix for repeating columns without suffixes
        repeat_col_table = repeat_col_table.rename(
            columns={
                col: f"{col}-nosuffix" for col in repeated_columns if "-" not in col
            }
        )
        # Split the suffixes into a second index level, and unstack
        repeat_col_table.columns = pd.MultiIndex.from_tuples(
            repeat_col_table.columns.to_series().str.split("-")
        )
        unstacked_repeat_col_table = repeat_col_table.stack(1).droplevel(1)  # noqa: PD013
        # Join with unrepeated columns
        first_normal_form_table = unnormalized_table.drop(
            columns=repeated_columns
        ).join(unstacked_repeat_col_table)
        self.database[table_name] = first_normal_form_table

    def _drop_verifiably_incomplete_rows(
        self,
        table_name: str,
        table: pd.DataFrame,
    ) -> list[str]:
        """Drop all rows from table that cannot become complete after normalization

        Example: when normalizing, we may come accross a missing required foreign key
        column because the object the foreign key refers to is still stored in the
        current table (so prop--x exists, but prop_id does not yet exist, but will
        once normalization is completed)

        Args:
            table_name: TODO
            table: list of columns in table
        Modifies:
            table with verifiably incomplete rows dropped
        """
        table_schema = self.schema.schema[table_name]
        required_attributes = set(table_schema.required_attributes)
        if "id" in required_attributes:
            required_attributes.remove("id")  # id has not been generated yet
        # check if any required attributes are forward relations (and therefore
        # may not exist in a valid table that is not yet normalized)
        required_forward_relations = [
            attribute[: -len(ID_SUFFIX)]
            for attribute in required_attributes
            if attribute[: -len(ID_SUFFIX)] in table_schema.forward_relations
        ]
        for forward_relation in required_forward_relations:
            # a row is valid if the forward relation is valid. Either
            # its id column exists and is notna or all of its required columns
            required_attributes.remove(f"{forward_relation}{ID_SUFFIX}")
            # this list includes any potential forward_relation{ID_SUFFIX} column.
            # this is desired because there may be a mixture of normalized and
            # unnormalized rows for a given relation. Regardless at least one
            # such column must be notna.
            forward_relation_columns = [
                col for col in table.columns if col.startswith(forward_relation)
            ]
            if forward_relation_columns == []:
                # table has no columns for required forward relations, it is
                # verifiably incomplete
                return pd.DataFrame()
            # drop all rows that are all na for required forward relation
            table = table.dropna(subset=forward_relation_columns, how="all")
        # now drop all rows that are na for any non forward relation columns
        table = table.dropna(subset=required_attributes, how="any")

    def _add_relation_to_extracted_table(
        self,
        table: pd.DataFrame,
        table_name: str,
        schema: DataSchema,
        foreign_key_prefix: str,
    ) -> pd.DataFrame:
        """Checks if a column linking back to the base table is required in a foreign table"""
        backlink_column = schema.schema[table_name].reverse_relation_names[
            foreign_key_prefix
        ]
        if f"{foreign_key_prefix}{SPLIT}{backlink_column}" not in table.columns:
            # this means the required column doesn't exist so we should
            # create it. Since ids are handled upon table createion, we table
            # must have a valid "id" column
            backlink_column = f"{foreign_key_prefix}{SPLIT}{backlink_column}"
            table.loc[:, backlink_column] = table["id"]

        return table

    def _merge_existing_forward_relations(
        self,
        table: pd.DataFrame,
        relation_prefix: str,
    ) -> None:
        """Merges existing and computed foreign key values, raising an error in conflict

        Args:
            table: DataFrame containing {relation_prefix}_id and
                temp_{relation_prefix}_id.
            relation_prefix: name of foreign key
        Raises:
            ValueError if there exists a row where {relation_prefix}_id and
                temp_{relation_prefix}_id both exist and are notna
        """
        conflicting_rows = table[
            (table[f"{relation_prefix}_id"].notna())
            & (table[f"temp_{relation_prefix}_id"].notna())
            & (table[f"{relation_prefix}_id"] != table[f"temp_{relation_prefix}_id"])
        ]
        if not conflicting_rows.empty:
            raise ValueError(
                f"Erroneous {relation_prefix} ID created\n{conflicting_rows}"
            )
        table[f"{relation_prefix}_id"] = table[f"{relation_prefix}_id"].fillna(
            table[f"temp_{relation_prefix}_id"]
        )

    def _split_prefixed_columns(
        self,
        table: pd.DataFrame,
        table_name: str,
        relation_prefix: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Remove columns starting with prefix from table, optionally keeping foreign key

        Args:
            table: any pandas dataframe
            table_name: table name present in schema
            relation_prefix: prefix of columns to be split from table. prefix should be in one of
                self.forward_relations or self.reverse_relations
        Returns: original table with columns removed, new foreign table
        """
        # step 0 - handle reverse relations
        # we do this earlier than forward relation ids, because we want to
        # deduplicate forward relation ids before the ids are created
        if relation_prefix in self.schema.schema[table_name].reverse_relations:
            table = self._add_relation_to_extracted_table(
                table, table_name, self.schema, relation_prefix
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
        extracted_table = table[foreign_columns_in_base_table].copy()
        extracted_table.columns = foreign_columns_in_foreign_table
        extracted_table["reported_state"] = table["reported_state"]
        # some columns are trivial and should be ignored for some actions
        non_metadata_columns = [
            column for column in extracted_table.columns if column != "reported_state"
        ]
        # step 3 - drop incomplete rows
        extracted_table = extracted_table.dropna(how="all", subset=non_metadata_columns)
        extracted_table_name = self.get_foreign_table_name(
            table_name,
            relation_prefix,
        )
        extracted_table_schema = self.schema.schema[extracted_table_name]
        self._drop_verifiably_incomplete_rows(extracted_table_name, extracted_table)
        # step 4 - drop duplicates - deduplication
        extracted_table = extracted_table.drop_duplicates()
        # step 5 - generate ids if the extracted table has an id column
        handle_id_column(
            extracted_table, extracted_table_schema, self.id_mapping, id_column="id"
        )
        # step 7 - add relation
        if relation_prefix in self.schema.schema[table_name].forward_relations:
            mapping_df = extracted_table.copy()
            mapping_df = mapping_df.rename(columns={"id": f"temp_{relation_prefix}_id"})
            mapping_df = mapping_df.drop(columns=["reported_state"])
            table = table.merge(
                mapping_df,
                left_on=foreign_columns_in_base_table,
                right_on=foreign_columns_in_foreign_table,
                how="left",
            )
            # Map the combinations in `table` to the ids from `extracted_table`
            if f"{relation_prefix}_id" in table.columns:
                self._merge_existing_forward_relations(table, relation_prefix)
            else:
                table[f"{relation_prefix}_id"] = table[f"temp_{relation_prefix}_id"]

            table = table.drop(columns=[f"temp_{relation_prefix}_id"])

        columns_to_drop = list(foreign_columns_in_base_table) + list(
            foreign_columns_in_foreign_table
        )
        table = table.drop(columns=columns_to_drop, errors="ignore")
        return table, extracted_table

    def _convert_table_to_3NF_from_1NF(
        self,
        table: pd.DataFrame,
        table_name: str,
    ) -> dict[str, list[pd.DataFrame]]:
        """Normalize table and any derivative tables to desired level given table schema

        Args:
            table: a valid table of table_name given schema
            table_name: used to identify which type of table in given schema
                table should fit

        Returns:
            Dictionary mapping table_names to list of tables attaining the desired
                normalization level.
        """
        # Step 1: Figure out which columns need to be normalized
        table_schema = self.schema.schema[table_name]
        columns_in_1NF = {
            col.split(SPLIT)[0]
            for col in table.columns
            if col.split(SPLIT)[0] in table_schema.relations
        }
        # Step 2: Go through columns that need to be normalized, if any.
        # Each decomposition may return a derivative table that also needs
        # normalization so this function will run on each derived table.
        updated_database = self.schema.empty_database()
        active_table = table
        for first_column_token in sorted(columns_in_1NF):
            # this is where the heavy lifting is done and a new foreign table
            # is created derived from the columns that did not belong in base table
            active_table, extracted_table = self._split_prefixed_columns(
                active_table, table_name, first_column_token
            )
            # Step 4 - Recursive step. Bring the foreign derivative table to the
            # desired form and all ensuing derivative tables
            extracted_table_derived_database = self._convert_table_to_3NF_from_1NF(
                extracted_table,
                self.schema.schema[table_name].relations[first_column_token],
            )
            for derived_table_name in extracted_table_derived_database:
                updated_database[derived_table_name].extend(
                    extracted_table_derived_database[derived_table_name]
                )
        updated_database[table_name].append(active_table)
        return updated_database

    def convert_to_3NF_from_1NF(self) -> None:
        """Converts database in first normal form (1NF) to third normal form (3NF)

        When called, each table in self.database should have appropriate ids, and
        a filled 'reported_state' column.

        Args:
            first_normal_form_database: TODO: spec of database somewhere
            schema: database schema that should contain all keys in database

        Modifies:
            database: updates database so that each table is in at least 3NF
        """
        normalized_database = self.schema.empty_database()
        for table_name, table in self.database.items():
            if table.index.name:
                table = table.reset_index()
            updated_database = self._convert_table_to_3NF_from_1NF(
                table,
                table_name,
            )
            for normalized_table_name in updated_database:
                normalized_database[normalized_table_name].extend(
                    updated_database[normalized_table_name]
                )
        normalized_database = self._consolidate_database(normalized_database)
        self.database = normalized_database

    def _consolidate_database(
        self,
        database: dict[str, list[pd.DataFrame]],
    ) -> dict[str, pd.DataFrame]:
        """Consolidate the database by concatenating each table name"""
        consolidated_database = {}
        for table_name in database:
            if database[table_name] == []:
                continue
            consolidated_table = pd.concat(database[table_name], ignore_index=True)
            if "id" in consolidated_table.columns:
                consolidated_table = consolidated_table.set_index("id")
            consolidated_database[table_name] = consolidated_table
        return consolidated_database

    def normalize_database(self) -> dict[str, pd.DataFrame]:
        """Bring a database to 4NF given the provided schema

        Returns:
            dictionary mapping table names to respective tables
        """
        # to start, ensure all ids are properly handled in the database
        for table_name, table in self.database.items():
            handle_id_column(table, self.schema.schema[table_name], self.id_mapping)
            # TODO: handle for _id columns??
            for column in table.columns:
                if column.endswith(ID_SUFFIX):
                    column_reference_table = self.get_foreign_table_name(
                        table_name, column[: -len(ID_SUFFIX)]
                    )
                    handle_existing_ids(
                        table, column_reference_table, self.id_mapping, column
                    )

        # bring to 1NF
        for table_name in self.database:
            self.convert_to_1NF_from_unnormalized(table_name)

        # bring to 3NF
        self.convert_to_3NF_from_1NF()
        return self.database

    def convert_to_class_table_from_single_table(
        self, database_single_table: dict[str, pd.DataFrame], data_schema: DataSchema
    ) -> dict[str, pd.DataFrame]:
        """[WIP] convert a database from a single table to class table inheritance strategy"""
        database_class_table = {}
        for table_name in database_single_table:
            original_table = database_single_table[table_name]
            data_schema.inheritance_strategy = "class table inheritance"
            for child_type in data_schema.schema[table_name].child_types:
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
            parent_schema = data_schema.schema[table_name]
            parent_table = original_table[
                [
                    col
                    for col in original_table.columns
                    if col in parent_schema.attributes
                ]
            ]
            database_class_table[table_name] = parent_table
        return database_class_table
