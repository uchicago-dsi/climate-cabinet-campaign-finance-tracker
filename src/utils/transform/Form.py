"""Defines base class for representing and normalizing raw campaign finance data"""

import abc
import uuid
from collections import defaultdict

import dask.dataframe as dd
import numpy as np
import pandas as pd

"""
START HERE
I feel like I'm oscillating between a 'form' based representation
and a 'table' based representation. i'm not sure if this is bad...

maybe... add a 'validate_table' method to the EntityType class and
that kind of stuff isn't handled by the form. I think this is a good 
idea. 


7/3

Form just takes you from the form to a familiar table.

7/9
So form reads in table, sets types, renames columns, renames enums.

table handles transition to 1nf, transactor-transaction, 3nf
    functions for pulling out prepended


"""


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
        # TODO: standardized allowed columns -- prefixes for relations that shouldnt' be there and a function for separating and replacinging with a key
        return self._required_columns

    @property
    def entity_type_mapper(self) -> dict:
        """Maps entity types to allowed values

        Allowed values are:
        Individual, Lobbyist, Candidate, Organization, Vendor, Party,
        Corporation, Committee.
        """
        return self._entity_type_mapper

    # TODO: delete org and ind types, replaced with transactor_type
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

        The common steps for transformation are:
            1. read_tables: Read (TODO all?) data into a tabular format
            2. standardize_columns:
                rename columns, remove unused columns, add extra columns.
                This is done after first normal form as now the columns proper
                columns should exist and getting standard names now should allow
                for significant overlap in additional normalization steps.
            3. first_normal_form:
                Transform data to First Normal Form. This means the data should
                not have repeating groups (i.e. amount1, amount2, ... amountn
                columns unless each row will have exactly n values as part of
                 business logic) or non-atomic values (a cell should not contain
                 a list or another table)
            4. normalize_table:
                Transform data into Third Normal Form. (TODO: separate 2NF?)
            5. TODO: 4nf?


        Column Names: after initial conversion, column names will all match
        a list of allowed attributes for each entity type with potential
        suffixes and prefixes before normalization. Before converting to 1NF
        suffixes enumerating repeating columns may be used. Before converting
        to 3NF, columns that are not dependent on the candidate key will be
        prefixed with the name of the column they are dependent on. For
        example, a transactions table with columns for a donor's name,
        employer, and employer's address would be given DONOR_NAME,
        DONOR_EMPLOYER_NAME, and DONOR_EMPLOYER_ADDRESS. In this case
        the donor would be replaced by an ID and a table with NAME and
        EMPLOYER columns would be created. THen employer would be replaced
        by ID and a table with NAME and ADDRESS columns would be created.



        Args:
            paths: list of paths where relevant files that are of this form type are.

        Returns:
            dictionary mapping table names to their transformed tables
        """
        self.read_table(paths)
        self.first_normal_form()
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
    def read_table(self, paths: list[str]) -> dd.DataFrame:
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
    def rename_entity_type(self, row: dd.Series) -> str:
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

    def get_table(self) -> dd.DataFrame:
        return self.table

    def divide_entities(self) -> None:
        """Split entities into individuals and transactions tables"""
        if (
            "all_entities" in self.transformed_data
            and len(self.transformed_data["all_entities"].index) > 0
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
        table_with_id_column: dd.DataFrame,
        id_column: str,
    ) -> dd.DataFrame:
        """Adds UUIDs, saving a mapping where IDs were previously given by state"""
        unique_ids = (
            table_with_id_column[id_column].dropna().unique()
        )  # .compute() DASK
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
            list(uuid_mapping.items()), columns=["ORIGINAL_ID", "UUID"]
        )
        # mapping_ddf = dd.from_pandas(mapping_df, npartitions=1) # DASK
        self.uuid_lookup_table = pd.concat([self.uuid_lookup_table, mapping_df])  # DASK

        return table_with_id_column


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
        self.dtype_dict = defaultdict(lambda: str)
        self.dtype_dict.update(
            {
                "reportInfoIdent": "Int32",
                "receivedDt": "Int32",
                f"{self.transaction_type}InfoId": "Int32",
                f"{self.transaction_type}Dt": "Int32",
                f"{self.transaction_type}Amount": "Float32",
            }
        )

    def _get_additional_columns(self) -> None:
        """Enhance and prepare the dataset for final output."""
        # TODO: apply is bad, can this be replaced?
        self.table[f"{self.expanded_party_internal_name}_ENTITY_TYPE"] = self.table[
            f"{self.expanded_party_internal_name}_ENTITY_TYPE"
        ].map(
            self.entity_type_mapper,
            # axis=1,
            # meta=(f"{self.expanded_party_internal_name}_ENTITY_TYPE", str), DASK
        )
        self.table[f"{self.expanded_party_internal_name}_ID"] = pd.NA

        na_mask = self.table["DATE"].isna()
        self.table.loc[~na_mask, "YEAR"] = pd.to_datetime(
            self.table.loc[~na_mask, "DATE"], format="%Y%m%d"
        ).dt.year.astype("Int16")
        self.table.loc[na_mask, "YEAR"] = pd.NA
        # self.table["YEAR"] = self.table["YEAR"].astype("Int8")

    def rename_entity_type(self, row: pd.Series) -> str:
        row_type = self.entity_type_mapper.get(
            row[f"{self.expanded_party_internal_name}_ENTITY_TYPE"], None
        )
        return row_type

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., expense_01.csv, expense_02.csv)
        self.table = pd.read_csv(paths[0], dtype=self.dtype_dict)
        return self.table

    # def map_columns(self) -> None:
    #     """Refine the mapping and selection of columns specific to Texas contributions."""
    #     # Call the parent implementation of map_columns which automatically handles the mapping and filtering based on 'column_mapper' and 'required_columns'
    #     super().map_columns()
    #     self.table[f"{self.normalized_party_internal_name}_ID"] = self.table[
    #         f"{self.normalized_party_internal_name}_ID"
    #     ].astype("str")
    #     return


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

    def __init__(self):
        super().__init__()
        self.dtype_dict = defaultdict(lambda: str)
        self.dtype_dict.update(
            {
                "filerEffStartDt": "Int32",
                "filerEffStopDt": "Int32",
                "treasEffStartDt": "Int32",
                "treasEffStopDt": "Int32",
            }
        )

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
        entity_types_specific = self.table["ENTITY_TYPE_SPECIFIC"].map(
            self.entity_type_mapper
            # axis=1,  # meta=("ENTITY_TYPE", str) DASK
        )
        entity_types_general = self.table["ENTITY_TYPE_GENERAL"].map(
            self.general_entity_type_mapper
            # axis=1,  # meta=("ENTITY_TYPE", str) DASK
        )
        specicic_row_type_predicted_general = np.where(
            entity_types_specific.isin(self.individual_entity_types),
            "Individual",
            "Organization",
        )
        self.table["ENTITY_TYPE"] = np.where(
            specicic_row_type_predicted_general == entity_types_general,
            entity_types_specific,
            entity_types_general,
        )

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., filer_01.csv, filer_02.csv)
        self.table = pd.read_csv(paths[0], dtype=self.dtype_dict)  # DASK
        return self.table


class TexasContributionForm(TexasTransactionForm):
    expanded_party_external_name = "contributor"
    expanded_party_internal_name = "DONOR"
    normalized_party_internal_name = "RECIPIENT"
    transaction_type = "contribution"
    table_type = "transactions_filer_received"

    def __init__(self):
        super().__init__()
        self.dtype_dict.update(
            {
                "repaymentDt": "Int32",
            }
        )


class TexasExpenseForm(TexasTransactionForm):
    expanded_party_external_name = "payee"
    expanded_party_internal_name = "RECIPIENT"
    normalized_party_internal_name = "DONOR"
    transaction_type = "expend"
    table_type = "transactions_filer_paid"


class PennsylvaniaTransactionForm(SemiNormalizedForm):
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
        self.dtype_dict = defaultdict(lambda: str)
        self.dtype_dict.update(
            {
                "reportInfoIdent": "Int32",
                "receivedDt": "Int32",
                f"{self.transaction_type}InfoId": "Int32",
                f"{self.transaction_type}Dt": "Int32",
                f"{self.transaction_type}Amount": "Float32",
            }
        )
        self.columns_names = []

    def rename_entity_type(self, row: dd.Series) -> str:
        return row

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., expense_01.csv, expense_02.csv)
        self.table = pd.read_csv(
            paths[0],
            names=self.columns_names,
            dtype=self.dtype_dict,
            encoding="latin-1",
            on_bad_lines="warn",
        )
        return self.table


class PennsylvaniaContributionForm(PennsylvaniaTransactionForm):
    expanded_party_external_name = ""
    expanded_party_internal_name = "DONOR"
    normalized_party_internal_name = "RECIPIENT"
    transaction_type = "contribution"
    table_type = "transactions_filer_received"

    def __init__(self):
        super().__init__()
        self.dtype_dict = defaultdict(lambda: str)
        self.dtype_dict.update(
            {
                "EYEAR": "Int32",
                "CYCLE": "Int32",
                "AMOUNT_1": "Float32",
                "AMOUNT_2": "Float32",
                "AMOUNT_3": "Float32",
                "DATE_1": "Int32",
                "DATE_2": "Int32",
                "DATE_3": "Int32",
            }
        )
        # "name we read as", # name from documentation
        # https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Documents/readme2022.txt
        self.columns_names = [
            "RECIPIENT_ID",  # FILERID
            "REPORTID",  # REPORTID
            "EYEAR",  # TIMESTAMP  NOTE!! EYEAR and TIMESTAMP are swapped
            "TIMESTAMP",  # EYEAR
            "CYCLE",  # CYCLE
            "SECTION",  # SECTION
            "DONOR_FULL_NAME",  # CONTRIBUTOR
            "DONOR_ADDRESS_LINE_1",  # ADDRESS1
            "DONOR_ADDRESS_LINE_2",  # ADDRESS2
            "DONOR_CITY",  # CITY
            "DONOR_STATE",  # STATE
            "DONOR_ZIP_CODE",  # ZIPCODE
            "DONOR_OCCUPATION",  # OCCUPATION
            "DONOR_EMPLOYER_FULL_NAME",  # ENAME
            "DONOR_EMPLOYER_ADDRESS_LINE_1",  # EADDRESS1
            "DONOR_EMPLOYER_ADDRESS_LINE_2",  # EADDRESS2
            "DONOR_EMPLOYER_CITY",  # ECITY
            "DONOR_EMPLOYER_STATE",  # ESTATE
            "DONOR_EMPLOYER_ZIP_CODE",  # EZIPCODE
            "DATE_1",  # CONTDATE1
            "AMOUNT_1",  # CONTAMT1
            "DATE_2",  # CONTDATE2
            "AMOUNT_2",  # CONTAMT2
            "DATE_3",  # CONTDATE3
            "AMOUNT_3",  # CONTAMT3
            "PURPOSE",  # CONTDESC
        ]

    @property
    def column_mapper(self) -> dict:
        """No columns to map, since they are named on read"""
        return {}

    def first_normal_form(self) -> None:
        """Transform table into first normal form, eliminating repeating groups"""
        # first separate all columns with no additional amount values
        blank_repeated_columns_mask = (self.table["AMOUNT_2"] == 0) & (
            self.table["AMOUNT_3"] == 0
        )
        rows_with_blank_repeated_columns = self.table[blank_repeated_columns_mask]
        rows_with_repeating_columns = self.table[~blank_repeated_columns_mask]
        rows_with_blank_repeated_columns = rows_with_blank_repeated_columns.drop(
            columns=["AMOUNT_2", "AMOUNT_3", "DATE_2", "DATE_3"]
        )
        rows_with_blank_repeated_columns = rows_with_blank_repeated_columns.rename(
            columns={"AMOUNT_1": "AMOUNT", "DATE_1": "DATE"}
        )

        # Separate data and amount specific columns to melt so there is one row
        # per amount and date column
        rows_with_repeating_columns["index"] = rows_with_repeating_columns.index
        date_prefix = "DATE_"
        amount_prefix = "AMOUNT_"
        date_cols = [
            col for col in rows_with_repeating_columns.columns if date_prefix in col
        ]
        amount_cols = [
            col for col in rows_with_repeating_columns.columns if amount_prefix in col
        ]
        id_cols = [
            col
            for col in rows_with_repeating_columns.columns
            if date_prefix not in col and amount_prefix not in col
        ]
        dates_expanded = pd.melt(
            rows_with_repeating_columns,
            id_vars=id_cols,
            value_vars=date_cols,
            var_name="date_type",
            value_name="DATE",
        )
        amounts_expanded = pd.melt(
            rows_with_repeating_columns,
            id_vars=id_cols,
            value_vars=amount_cols,
            var_name="amount_type",
            value_name="AMOUNT",
        )
        dates_expanded["num"] = dates_expanded["date_type"].str.extract("(\d+)")
        amounts_expanded["num"] = amounts_expanded["amount_type"].str.extract("(\d+)")

        # Merge the two DataFrames on id_vars, num, and index
        df_long = dates_expanded.merge(amounts_expanded, on=id_cols + ["num", "index"])
        # Drop the helper columns
        df_long = df_long.drop(columns=["date_type", "amount_type", "num", "index"])
        self.table = pd.concat([df_long, rows_with_blank_repeated_columns])


class PennsylvaniaContributionFormPost2022(PennsylvaniaContributionForm):
    pass


class PennsylvaniaContributionFormPre2022(PennsylvaniaContributionForm):
    def __init__(self):
        super().__init__()
        self.dtype_dict = defaultdict(lambda: str)
        self.dtype_dict.update(
            {
                "reportInfoIdent": "Int32",
                "receivedDt": "Int32",
                f"{self.transaction_type}InfoId": "Int32",
                f"{self.transaction_type}Dt": "Int32",
                f"{self.transaction_type}Amount": "Float32",
            }
        )

        # https://www.dos.pa.gov/VotingElections/CandidatesCommittees/CampaignFinance/Resources/Documents/readmepriorto2022.txt
        self.columns_names.remove("REPORTID")
        self.columns_names.remove("TIMESTAMP")
