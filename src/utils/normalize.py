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

from pathlib import Path

import pandas as pd

from utils.ids import handle_existing_ids, handle_id_column
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
        flag: set() for flag in NORMALIZATION_LEVELS
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


class Normalizer:
    """Class for normalizing a database according to a provided schema"""

    @property
    def id_mapping(self) -> dict[tuple, str]:
        """Dictionary mapping original ids to uuids

        Original ids are stored as tuples of the form (raw id, table_type, year, state)
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

    def get_foreign_table_type(self, base_type: str, column_name: str) -> str:
        """Retrieve the type of a multivalued/foreign table"""
        column_tokens = column_name.split(SPLIT)
        current_table_type = base_type
        for column_token in column_tokens:
            current_table_schema = self.schema.schema[current_table_type]
            if column_token in current_table_schema.relations:
                current_table_type = current_table_schema.relations[column_token]
                continue
            elif current_table_schema.attributes_regex.match(column_token):
                return current_table_type
        return current_table_type

    def convert_to_1NF_from_unnormalized(self, table_name: str) -> None:
        """Converts an unnormalized table into first normal form (1NF)

        Removes anticipated repeating groups

        Args:
            table_name: name of table type to normalize
        Modifies:
            database[table_name] transformed to 1NF
        """
        table_schema = self.schema.schema[table_name]
        unnormalized_table = self.database[table_name]
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
                new_column_name = f"{base_column_name}-{max_repeat + 1}"
                unnormalized_table = unnormalized_table.rename(
                    columns={base_column_name: new_column_name}
                )
                repeated_columns.append(new_column_name)
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
            first_normal_form_table["amount"].notna()
            & first_normal_form_table["amount"]
            > 0
        ]
        self.database[table_name] = first_normal_form_table

    def _drop_verifiably_incomplete_rows(
        self,
        table_type: str,
        table: pd.DataFrame,
    ) -> list[str]:
        """Drop all rows from table that cannot become complete after normalization

        Example: when normalizing, we may come accross a missing required foreign key
        column because the object the foreign key refers to is still stored in the
        current table (so prop--x exists, but prop_id does not yet exist, but will
        once normalization is completed)

        Args:
            table_type: TODO
            table: list of columns in table
        Modifies:
            table with verifiably incomplete rows dropped
        """
        table_schema = self.schema.schema[table_type]
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
        table_type: str,
        schema: DataSchema,
        foreign_key_prefix: str,
    ) -> pd.DataFrame:
        """Checks if a column linking back to the base table is required in a foreign table"""
        backlink_column = schema.schema[table_type].reverse_relation_names[
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
        table_type: str,
        relation_prefix: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Remove columns starting with prefix from table, optionally keeping foreign key

        Args:
            table: any pandas dataframe
            table_type: table type present in schema
            relation_prefix: prefix of columns to be split from table. prefix should be in one of
                self.forward_relations or self.reverse_relations
        Returns: original table with columns removed, new foreign table
        """
        # step 0 - handle reverse relations
        # we do this earlier than forward relation ids, because we want to
        # deduplicate forward relation ids before the ids are created
        if relation_prefix in self.schema.schema[table_type].reverse_relations:
            table = self._add_relation_to_extracted_table(
                table, table_type, self.schema, relation_prefix
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
        extracted_table_type = self.get_foreign_table_type(
            table_type,
            relation_prefix,
        )
        extracted_table_schema = self.schema.schema[extracted_table_type]
        self._drop_verifiably_incomplete_rows(extracted_table_type, extracted_table)
        # step 4 - drop duplicates - deduplication
        extracted_table = extracted_table.drop_duplicates()
        # step 5 - generate ids if the extracted table has an id column
        handle_id_column(
            extracted_table, extracted_table_schema, self.id_mapping, id_column="id"
        )
        # step 7 - add relation
        if relation_prefix in self.schema.schema[table_type].forward_relations:
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
        table_type: str,
    ) -> dict[str, list[pd.DataFrame]]:
        """Normalize table and any derivative tables to desired level given table schema

        Args:
            table: a valid table of table_type given schema
            table_type: used to identify which type of table in given schema
                table should fit

        Returns:
            Dictionary mapping table_types to list of tables attaining the desired
                normalization level.
        """
        # Step 1: Figure out which columns need to be normalized
        normalization_levels_to_columns = get_normalization_form_by_column(
            table, self.schema.schema[table_type]
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
        updated_database = self.schema.empty_database()
        active_table = table
        while normalization_levels_to_columns[FIRST_NORMAL_FORM_FLAG]:
            first_column_token = sorted(
                normalization_levels_to_columns[FIRST_NORMAL_FORM_FLAG]
            )[0]
            normalization_levels_to_columns[FIRST_NORMAL_FORM_FLAG].remove(
                first_column_token
            )  # not using pop to avoid non-determinism
            # this is where the heavy lifting is done and a new foreign table
            # is created derived from the columns that did not belong in base table
            active_table, extracted_table = self._split_prefixed_columns(
                active_table, table_type, first_column_token
            )
            # Step 4 - Recursive step. Bring the foreign derivative table to the
            # desired form and all ensuing derivative tables
            extracted_table_derived_database = self._convert_table_to_3NF_from_1NF(
                extracted_table,
                self.schema.schema[table_type].relations[first_column_token],
            )
            for derived_table_type in extracted_table_derived_database:
                updated_database[derived_table_type].extend(
                    extracted_table_derived_database[derived_table_type]
                )
        updated_database[table_type].append(active_table)
        # ensure indices are correct
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
        for table_type, table in self.database.items():
            if table.index.name:
                table = table.reset_index()
            updated_database = self._convert_table_to_3NF_from_1NF(
                table,
                table_type,
            )
            for normalized_table_type in updated_database:
                normalized_database[normalized_table_type].extend(
                    updated_database[normalized_table_type]
                )
        normalized_database = self._consolidate_database(normalized_database)
        self.database = normalized_database

    def _consolidate_database(
        self,
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

    def normalize_database(self) -> dict[str, pd.DataFrame]:
        """Bring a database to 4NF given the provided schema

        Returns:
            dictionary mapping table types to respective tables
        """
        # to start, ensure all ids are properly handled in the database
        for table_type, table in self.database.items():
            handle_id_column(table, self.schema.schema[table_type], self.id_mapping)
            # TODO: handle for _id columns??
            for column in table.columns:
                if column.endswith("_id"):
                    column_reference_table = self.get_foreign_table_type(
                        table_type, column[: -len("_id")]
                    )
                    handle_existing_ids(
                        table, column_reference_table, self.id_mapping, column
                    )

        # bring to 1NF
        for table_type in self.database:
            self.convert_to_1NF_from_unnormalized(table_type)

        # bring to 3NF
        self.convert_to_3NF_from_1NF()
        return self.database

    def convert_to_class_table_from_single_table(
        self, database_single_table: dict[str, pd.DataFrame], data_schema: DataSchema
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
                [
                    col
                    for col in original_table.columns
                    if col in parent_schema.attributes
                ]
            ]
            database_class_table[table_type] = parent_table
        return database_class_table
