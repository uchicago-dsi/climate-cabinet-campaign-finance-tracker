import abc
import uuid

import pandas as pd


class Form(abc.ABC):
    """General representation of a single type of document with campaign finance data

    Contains methods and attributes for transforming the data from the raw format
    provided by state election boards to a normalized and standardized schema.

    Once a form has been read into a table, there are several types that appear:

    - transactions: A table representing transactions with only references to
        information about the parties involved in the transaction
    - transactions_filer_paid: A table representing transactions with only
        references to the transaction payer/donor and columns describing
        details about the payee/recipient
    - transactions_filer_received: A table representing transactions with only
        references to the transaction payee/recipient and columns describing
        details about the payer/donor
    - transactions_fully_unnormalized: A table where each row represents a
        transaction but also contains all details about donors and recipients
    - organizations: A table which only contains details about organizations
    - individuals: A table which only contains details about individuals
    - all_entities: A a table which contains both organizations and individuals
    """

    @property
    def column_mapper(self) -> dict:
        """Dictionary for mapping raw column names to match our schema"""
        return self._column_mapper

    @property
    def allowed_columns(self) -> list[str]:
        """Columns that are allowed to be in the returned dataframe"""
        return self._required_columns

    @property
    def entity_type_mapper(self) -> dict:
        """Maps entity types to allowed values

        Allowed values are:
        Individual, Lobbyist, Candidate, Organization, Vendor, Party,
        Corporation, Committee.
        """
        return self._entity_type_mapper

    organization_entity_types = [
        "Organization",
        "Vendor",
        "Party",
        "Corporation",
        "Committee",
    ]
    individual_entity_types = ["Individual", "Lobbyist", "Candidate"]

    @property
    def stable_id_across_years(self) -> bool:
        """True if state assigned IDs are unique across years"""
        return self._stable_across_years

    @property
    def table_type(self) -> str:
        """Type of data included in raw form

        One of:
        - transactions
        - transactions_filer_paid
        - transactions_filer_received
        - transactions_fully_unnormalized
        - organizations
        - individuals
        - all_entities
        """
        return self._table_type

    """Dictionary mapping table names to tables

    Should have 'transactions', 'organizations', 'individuals',
    and 'entities' keys. By default, maps to empty dataframes
    """

    def transform_data(self, paths: list[str]) -> dict[str, pd.DataFrame]:
        """Convert raw data of form type located at paths to dict of transformed tables

        Args:
            paths: list of paths where relevant files that are of this form type are.

        Returns:
            dictionary mapping table names to their transformed tables
        """
        self.read_table(paths)
        self.standardize_columns()
        self.normalize_table()
        self.divide_entities()
        return self.transformed_data

    def __init__(self):
        self.table = None
        self.transformed_data = {
            "transactions": pd.DataFrame(),
            "organizations": pd.DataFrame(),
            "individuals": pd.DataFrame(),
            "entities": pd.DataFrame(),
        }
        self.uuid_lookup_table = pd.DataFrame()

    @abc.abstractmethod
    def read_table(self, paths: list[str]) -> pd.DataFrame:
        """Read raw tabular data from state provided files into a DataFrame

        This method should maintain the data as closely as possible. The only
        permissible modification is the dropping of segments of data that are
        malformed and not able to be read into a dataframe. These rows should
        be (TODO #107) reported.
        """
        pass

    def standardize_columns(self) -> None:
        """Renames and filters columns to match schema for unnormalized tables

        Unnormalized or semi-normalized transaction tables will have column
        names with 'DONOR' or 'RECIPIENT' prefixes. Most tables will be able to
        use the standard version which renames columns based on 'column_mapper'
        dictionary and filters to only columns in 'allowed_columns'
        """
        self._rename_columns()
        self._drop_unused_columns()
        self._get_additional_columns()

    def _rename_columns(self) -> None:
        """Rename columns"""
        self.table = self.table.rename(columns=self.column_mapper)

    @abc.abstractmethod
    def rename_entity_type(self, row: pd.Series) -> str:
        pass

    def _drop_unused_columns(self) -> None:
        """Drop columns with information that are not used in internal schema"""
        self.table = self.table.loc[:, self.table.columns.isin(self.allowed_columns)]

    def _get_additional_columns(self) -> None:
        """Add additional columns or create columns based on existing columns"""
        return

    @abc.abstractmethod
    def normalize_table(self) -> None:
        """Restructure raw table to have transactions, individuals, organizations tables

        Args:
            self.table: should be have standardized column names and irrelevant columns
                dropped.
        """
        pass

    def get_table(self) -> pd.DataFrame:
        return self.table

    def divide_entities(self) -> None:
        """Split entities into individuals and transactions tables"""
        if (
            "all_entities" in self.transformed_data
            and not self.transformed_data["all_entities"].empty
        ):
            self.transformed_data["individuals"] = self.transformed_data[
                "all_entities"
            ][
                self.transformed_data["all_entities"]["ENTITY_TYPE"].isin(
                    self.individual_entity_types
                )
            ]
            self.transformed_data["organizations"] = self.transformed_data[
                "all_entities"
            ][
                self.transformed_data["all_entities"]["ENTITY_TYPE"].isin(
                    self.organization_entity_types
                )
            ]

    def replace_id_with_uuid(
        self,
        table_with_id_column: pd.DataFrame,
        id_column: str,
    ) -> pd.DataFrame:
        """Adds UUIDs, saving a mapping where IDs were previously given by state"""
        # Handle NA IDs by generating UUIDs directly in the DataFrame
        na_id_mask = table_with_id_column[id_column].isna()
        table_with_id_column.loc[na_id_mask, id_column] = [
            uuid.uuid4() for _ in range(na_id_mask.sum())
        ]
        rows_with_existing_ids = table_with_id_column.loc[~na_id_mask]

        # Create unique identifier combinations based on 'stable_id_across_years' and 'year_column'
        if self.stable_id_across_years:
            unique_ids = rows_with_existing_ids.loc[:, id_column].drop_duplicates()
            uuid_lookup_table = pd.DataFrame(unique_ids)
            uuid_lookup_table = uuid_lookup_table.rename(
                columns={id_column: "ORIGINAL_ID"}
            )
        else:
            unique_ids = rows_with_existing_ids.loc[
                :, [id_column, "YEAR"]
            ].drop_duplicates()
            uuid_lookup_table = pd.DataFrame(unique_ids)

        if not uuid_lookup_table.empty:
            uuid_lookup_table["UUID"] = [
                uuid.uuid4() for _ in range(len(uuid_lookup_table))
            ]

            self.uuid_lookup_table = pd.concat(
                [uuid_lookup_table, self.uuid_lookup_table]
            )

            # replace ids with uuids, for rows that we didn't just add uuids to
            rows_with_existing_ids = rows_with_existing_ids.merge(
                uuid_lookup_table,
                left_on=[id_column],
                right_on=["ORIGINAL_ID"]
                + (["YEAR"] if "YEAR" and not self.stable_id_across_years else []),
                how="left",
            )

            # Replace original IDs with UUIDs
            rows_with_existing_ids[id_column] = rows_with_existing_ids["UUID"]
            rows_with_existing_ids = rows_with_existing_ids.drop(columns=["UUID"])

        uuid_table = pd.concat(
            [table_with_id_column.loc[na_id_mask], rows_with_existing_ids]
        )
        return uuid_table


class NormalizedForm(Form):
    """Form with normalized dataset"""

    def normalize_table(self) -> None:  # noqa D102
        if self.table_type not in [
            "transactions",
            "individuals",
            "organizations",
            "all_entities",
        ]:
            raise ValueError(
                f"Default table normalization does not work for {self.table_type}"
            )
        self.table = self.replace_id_with_uuid(self.table, "ID")
        self.transformed_data[self.table_type] = self.table


class SemiNormalizedForm(Form):
    """Form that is normalized for one party in each transaction"""

    def normalize_table(self) -> None:  # noqa D102
        if self.table_type == "transactions_filer_paid":
            split_prefix = "RECIPIENT"
            other_entity = "DONOR"
        elif self.table_type == "transactions_filer_received":
            split_prefix = "DONOR"
            other_entity = "RECIPIENT"
        else:
            raise ValueError(
                f"Semi-normalized table normalization does not work for {self.table_type}"
            )
        self.table = self.replace_id_with_uuid(self.table, f"{split_prefix}_ID")
        self.table = self.replace_id_with_uuid(self.table, f"{other_entity}_ID")
        entity_columns = [
            col for col in self.table.columns if col.startswith(split_prefix)
        ]
        new_entity_column_names = [
            col[len(split_prefix) + 1 :]
            for col in self.table.columns
            if col.startswith(split_prefix)
        ]
        self.entities = self.table[entity_columns]
        self.entities.columns = new_entity_column_names
        self.transformed_data["all_entities"] = self.entities
        # remove id column as we want to keep it
        entity_columns.remove(f"{split_prefix}_ID")
        self.transformed_data["transactions"] = self.table.drop(
            columns=entity_columns, errors="ignore"
        )


class TexasTransactionForm(SemiNormalizedForm):
    """Outlines general structure of Texas transaction tables (expenses + contributions)"""

    stable_id_across_years = True

    entity_type_mapper = {
        "INDIVIDUAL": "Individual",
        "ENTITY": "Organization",
    }

    @property
    def expanded_party_external_name(self) -> str:
        """Name used in raw data to refer to transaction party described in full

        In some states, only certain parties must file payments made and received.
        The filers will have a table with their details and separate tables will
        exist describing payments made to the filers and payments made by the filers.
        In these tables, the filers will be referred to as foreign keys, while the
        other party's information will be listed in full in a non-normalized manner
        """
        return self._expanded_party_name

    @property
    def expanded_party_internal_name(self) -> str:
        """Name used in internal schema to refer to transaction party described in full"""
        return self._expanded_party_internal_name

    @property
    def normalized_party_internal_name(self) -> str:
        """Name used in internal schema to refer to filer."""
        return self._normalized_party_internal_name

    @property
    def transaction_type(self) -> str:
        """Type of transaction made to the filer"""
        return self._transaction_type

    @property
    def column_mapper(self) -> dict:
        return {
            f"{self.expanded_party_external_name}NameOrganization": f"{self.expanded_party_internal_name}_FULL_NAME",
            f"{self.expanded_party_external_name}NameFirst": f"{self.expanded_party_internal_name}_FIRST_NAME",
            f"{self.expanded_party_external_name}NameLast": f"{self.expanded_party_internal_name}_LAST_NAME",
            f"{self.expanded_party_external_name}StreetAddr1": f"{self.expanded_party_internal_name}_ADDRESS_LINE_1",
            f"{self.expanded_party_external_name}StreetAddr2": f"{self.expanded_party_internal_name}_ADDRESS_LINE_2",
            f"{self.expanded_party_external_name}StreetCity": f"{self.expanded_party_internal_name}_CITY",
            f"{self.expanded_party_external_name}StreetStateCd": f"{self.expanded_party_internal_name}_STATE",
            f"{self.expanded_party_external_name}StreetPostalCode": f"{self.expanded_party_internal_name}_ZIP_CODE",
            f"{self.expanded_party_external_name}Employer": f"{self.expanded_party_internal_name}_EMPLOYER",
            f"{self.expanded_party_external_name}Occupation": f"{self.expanded_party_internal_name}_OCCUPATION",
            "filerIdent": f"{self.normalized_party_internal_name}_ID",
            f"{self.expanded_party_external_name}PersentTypeCd": f"{self.expanded_party_internal_name}_ENTITY_TYPE",
            f"{self.transaction_type}Amount": "AMOUNT",
            f"{self.transaction_type}Dt": "DATE",
            f"{self.transaction_type}Descr": "PURPOSE",
        }

    @property
    def allowed_columns(self) -> list[str]:
        return [
            f"{self.normalized_party_internal_name}_ID",
            f"{self.expanded_party_internal_name}_ID",
            "AMOUNT",
            "DATE",
            "YEAR",
            "PURPOSE",
            f"{self.expanded_party_internal_name}_FULL_NAME",
            f"{self.expanded_party_internal_name}_FIRST_NAME",
            f"{self.expanded_party_internal_name}_LAST_NAME",
            f"{self.expanded_party_internal_name}_ADDRESS_LINE_1",
            f"{self.expanded_party_internal_name}_ADDRESS_LINE_2",
            f"{self.expanded_party_internal_name}_CITY",
            f"{self.expanded_party_internal_name}_STATE",
            f"{self.expanded_party_internal_name}_ZIP_CODE",
            f"{self.expanded_party_internal_name}_EMPLOYER",
            f"{self.expanded_party_internal_name}_OCCUPATION",
            f"{self.expanded_party_internal_name}_ENTITY_TYPE",
        ]

    def __init__(self):
        super().__init__()

    def _get_additional_columns(self) -> None:
        """Enhance and prepare the dataset for final output."""
        self.table[f"{self.expanded_party_internal_name}_ENTITY_TYPE"] = (
            self.table.apply(self.rename_entity_type, axis=1)
        )
        self.table[f"{self.expanded_party_internal_name}_ID"] = pd.NA
        self.table["YEAR"] = pd.to_datetime(self.table["DATE"], format="%Y%m%d").dt.year

    def rename_entity_type(self, row: pd.Series) -> str:
        row_type = self.entity_type_mapper.get(
            row[f"{self.expanded_party_internal_name}_ENTITY_TYPE"], None
        )
        return row_type

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., expense_01.csv, expense_02.csv)
        paths = paths[:2]
        tables = [pd.read_csv(path) for path in paths]
        self.table = pd.concat(tables)
        return self.table

    def map_columns(self) -> None:
        """Refine the mapping and selection of columns specific to Texas contributions."""
        # Call the parent implementation of map_columns which automatically handles the mapping and filtering based on 'column_mapper' and 'required_columns'
        super().map_columns()
        self.table[f"{self.normalized_party_internal_name}_ID"] = self.table[
            f"{self.normalized_party_internal_name}_ID"
        ].astype("str")
        return


class TexasFilerForm(NormalizedForm):
    table_type = "all_entities"

    column_mapper = {
        "filerIdent": "ID",
        "filerNameOrganization": "FULL_NAME",
        "filerNameFirst": "FIRST_NAME",
        "filerNameLast": "LAST_NAME",
        "filerStreetAddr1": "ADDRESS_LINE_1",
        "filerStreetAddr2": "ADDRESS_LINE_2",
        "filerStreetCity": "CITY",
        "filerStreetStateCd": "STATE",
        "filerStreetPostalCode": "ZIP_CODE",
        "filerPrimaryPhoneNumber": "PHONE_NUMBER",
        "contestSeekOfficeCd": "OFFICE_SOUGHT",
        "contestSeekOfficeDistrict": "DISTRICT",
        "filerTypeCd": "ENTITY_TYPE_SPECIFIC",
        "filerPersentTypeCd": "ENTITY_TYPE_GENERAL",
    }

    allowed_columns = [
        "ID",
        "FULL_NAME",
        "FIRST_NAME",
        "LAST_NAME",
        "ADDRESS_LINE_1",
        "ADDRESS_LINE_2",
        "CITY",
        "STATE",
        "ZIP_CODE",
        "PHONE_NUMBER",
        "OFFICE_SOUGHT",
        "DISTRICT",
        "ENTITY_TYPE",
        "ENTITY_TYPE_GENERAL",
        "ENTITY_TYPE_SPECIFIC",
    ]

    entity_type_mapper = {
        "COH": "Candidate",
        # TODO: #108 TX filer data sometimes has 'cta*' columns for presumed non-candidates
        "JCOH": "Candidate",
        "GPAC": "Committee",
        "SPAC": "Committee",
        "MPAC": "Committee",
        "PTYCORP": "Party",
        "SCC": "Candidate",
        "DCE": "Candidate",  # "Report of Direct Campaign Expenditures", can be ind or org
        "ASIFSPAC": "Committee",  # As-If Special Purpose Committee
        "MCEC": "Party",  # County Executive Committee
        "CEC": "Party",  # County Executive Committee, set up by party per:
        # https://statutes.capitol.texas.gov/Docs/EL/htm/EL.171.htm
        "JSPC": "Committee",  # Judicial Special Purpose Committee
        "LEG": "Caucus",  # Legislative Caucus
        "SPK": "Candidate",  # Candidate for Speaker Of The Texas House of Representatives
        "SCPC": "Committee",  # State/County Specicic-Purppose Committee
    }

    general_entity_type_mapper = {
        "INDIVIDUAL": "Individual",
        "ENTITY": "Organization",
    }

    stable_id_across_years = True

    def rename_entity_type(self, row: pd.Series) -> str:
        sepcific_row_type = self.entity_type_mapper.get(
            row["ENTITY_TYPE_SPECIFIC"], None
        )
        general_row_type = self.general_entity_type_mapper.get(
            row["ENTITY_TYPE_GENERAL"], None
        )
        # some specific entity types can be inds or orgs (ex: DCE)
        sepcific_row_type_predicted_general = (
            "Individual"
            if sepcific_row_type in self.individual_entity_types
            else "Organization"
        )
        if general_row_type != sepcific_row_type_predicted_general:
            return general_row_type
        else:
            return sepcific_row_type

    def _get_additional_columns(self) -> None:
        self.table["ENTITY_TYPE"] = self.table.apply(self.rename_entity_type, axis=1)

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., filer_01.csv, filer_02.csv)
        try:
            tables = [pd.read_csv(path) for path in paths]
            self.table = pd.concat(tables)
        except Exception as e:
            print(f"Error reading table: {e}")
        return self.table


class TexasContributionForm(TexasTransactionForm):
    expanded_party_external_name = "contributor"
    expanded_party_internal_name = "DONOR"
    normalized_party_internal_name = "RECIPIENT"
    transaction_type = "contribution"
    table_type = "transactions_filer_received"


class TexasExpenseForm(TexasTransactionForm):
    expanded_party_external_name = "payee"
    expanded_party_internal_name = "RECIPIENT"
    normalized_party_internal_name = "DONOR"
    transaction_type = "expend"
    table_type = "transactions_filer_paid"
