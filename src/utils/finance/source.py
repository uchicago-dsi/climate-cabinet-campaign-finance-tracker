"""Represents raw input data from states"""

import abc
import uuid
from pathlib import Path

import pandas as pd


class DataSource:
    """Reads in data, renames columns, assigns types, and ensures correct types

    Description:
    Represents any arbitrary data source containing campaign finance information.
    In practice, most states supply their campaign finance data through
    compilations of data from the forms candidates and other key parties are
    required to fill out. The format of these compilations are often tabular
    (like csvs) and can also be exposed through an api.

    While the format and peripheral information vary widely, the core
    information provided remains the same. The job of this class is to
    represent those raw Data Sources and transfer them into a tabular
    structure with an expected naming structure.

    This class is NOT meant to convert provided tabular data to a more
    normalized format (that is done in table.py) or to collect the data
    from the sources (that is done in scrape).

    Importantly, methods in this class should NOT change the underlying data
    in any way. A transformation of 'Sept 1 2019' to '2019-09-01' is okay
    because it is just changing the representation and is not making any
    assumptions. A transformation of full_name = "Some Person" to first_name =
    "Some" last_name = "Person" should be done later as this is based on
    assumptions.

    Notes:
    Column Names -- after initial conversion, column names will all match
    a list of allowed attributes for each entity type with potential
    suffixes and prefixes before normalization. Before converting to 1NF
    suffixes enumerating repeating columns may be used. Before converting
    to 3NF, columns that are not dependent on the candidate key will be
    prefixed with the name of the column they are dependent on. For
    example, a transactions table with columns for a donor's name,
    employer, and employer's address would be given DONOR--FULL_NAME,
    DONOR--EMPLOYER--FULL_NAME, and DONOR-EMPLOYER--ADDRESS. In this case
    the donor would be replaced by an ID and a table with NAME and
    EMPLOYER columns would be created. Then employer would be replaced
    by ID and a table with NAME and ADDRESS columns would be created.
    """

    @property
    def database(self) -> dict[str, pd.DataFrame]:
        """Dictionary mapping table type to table"""
        return self._database

    @property
    def table_type(self) -> str:
        """The type of table the metadata represents"""
        return self._table_type  # TODO: can there ever be multiple for one source?

    @property
    def default_raw_data_paths(self) -> list[str | Path]:
        """Default paths to any files of this type"""
        return self._default_paths

    @property
    def column_details(self) -> pd.DataFrame:
        """Metadata on raw columns including standardized names and types

        Dataframe should have following columns:
            raw_name - name as provided by source
            type - type as provided by source
            date_format - if column is a date, the format in
                https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
            standard_name - name fitting our schema. None if column is to be dropped
            enum - if the column is an enum, give the name of the key in the enum dict
            provided_description - any description provided by source
            notes - any notes about column (i.e. 'provided description wrong')
        """
        return self._column_details

    @property
    def enum_mapper(self) -> dict[str, dict]:
        """Maps columns names to dicts mapping raw names to allowed enum values"""
        return self._enum_mapper

    @property
    def transactor_type_mapper(self) -> dict:
        """Maps transactor types to allowed values

        Allowed values are:
        Individual, Lobbyist, Candidate, Organization, Vendor, Party,
        Corporation, Committee.
        """
        return self._entity_type_mapper

    @property
    def stable_id_across_years(self) -> bool:
        """True if state assigned IDs are unique across years

        A state may conceivably reuse IDs each election cycle.
        If this is the case, this propery should be set to False
        so that the code can prefix provided IDs to remove collisions.
        """
        return self._stable_across_years

    @property
    def id_columns_to_standardize(self) -> dict:
        """Maps id columns that should be uuids to matching columns

        Done after additional columns are added. Often a multivalued
        fact will have a version that refers back to the current table
        when it is split up. The table will add a column of the correct
        name and ensure its id is correct.
        """
        return self._id_columns_to_standardize

    def replace_id_with_uuid(
        self,
        id_column: str = "id",
        matching_id_columns: list = None,
    ) -> None:
        """Adds UUIDs, saving a mapping where IDs were previously given by state

        Args:
            table_with_id_column: pandas dataframe that contains a column
                representing an id
            id_column: name of expected id column. Default: id
            matching_id_columns: Names of columns that should have the
                same id as that row's id_column. Default None.

        Returns:
            input table with added ids, table mapping old to new ids
        """
        table_with_id_column = self.table
        if id_column not in table_with_id_column:
            return
        if matching_id_columns is None:
            matching_id_columns = []
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
            list(uuid_mapping.items()), columns=["original_id", "uuid"]
        )
        for matching_column in matching_id_columns:
            if matching_column not in table_with_id_column.columns:
                continue
            table_with_id_column.loc[:, matching_column] = table_with_id_column.loc[
                :, id_column
            ]
        # mapping_ddf = dd.from_pandas(mapping_df, npartitions=1) # DASK
        self.table = table_with_id_column
        self.id_map.append(mapping_df)

    def read_and_standardize_table(
        self, paths: list[str] = None
    ) -> dict[str, list[pd.DataFrame]]:
        """Convert raw data of form type located at paths to dict of transformed tables

        The common steps for transformation are:
            1. read_tables: Read (TODO all?) data into a tabular format
            2. standardize_column names:
                rename columns, remove unused columns, add extra columns.
            3. standardize table contents:
                make sure data is in correct format, enums are accurate

        Args:
            paths: list of paths where relevant files that are of this form type are.
                If none is provided, uses self.default_paths

        Returns:
            dictionary mapping table names to their transformed tables
        """
        if paths is None:
            paths = self.default_raw_data_paths
        self.read_table(paths)
        self._standardize_columns()
        self._standardize_data()
        return {self.table_type: [self.table], "IdMap": self.id_map}

    @abc.abstractmethod
    def read_table(self, paths: list[str]) -> pd.DataFrame:
        """Read raw tabular data from state provided files into a DataFrame

        This method should maintain the data as closely as possible. The only
        permissible modification is the dropping of segments of data that are
        malformed and not able to be read into a dataframe. These rows should
        be (TODO #107) reported.
        """
        pass

    def _standardize_columns(self) -> None:
        """Renames and filters columns to match schema for unnormalized tables"""
        self._rename_columns()
        self._drop_unused_columns()
        self._get_additional_columns()

    def _rename_columns(self) -> None:
        """Rename columns"""
        column_mapper = (
            self.column_details.set_index("raw_name")["standard_name"]
            .dropna()
            .to_dict()
        )
        self.table = self.table.rename(columns=column_mapper)

    def _drop_unused_columns(self) -> None:
        """Drop columns with information that are not used in internal schema"""
        relevant_columns = self.column_details["standard_name"].dropna()
        self.table = self.table.loc[:, self.table.columns.isin(relevant_columns)]

    def _get_additional_columns(self) -> None:
        """Add additional columns or create columns based on existing columns"""
        return

    def _standardize_data(self) -> None:
        """Ensure data in table is of correct format"""
        self._standardize_date_formats()
        self._standardize_enums()
        self._standardize_ids()

    def _standardize_date_formats(self) -> None:
        column_to_date_format = (
            self.column_details.dropna(subset=["standard_name", "date_format"])
            .set_index("standard_name")["date_format"]
            .to_dict()
        )
        for date_column in column_to_date_format:
            na_mask = self.table[date_column].isna()
            temp_column = f"tmp-{date_column}"
            self.table[temp_column] = pd.NA
            self.table.loc[~na_mask, temp_column] = pd.to_datetime(
                self.table.loc[~na_mask, date_column],
                format=column_to_date_format[date_column],
            ).dt.date
            self.table = self.table.drop(columns=date_column)
            self.table = self.table.rename(columns={temp_column: date_column})

    def _standardize_enums(self) -> None:
        """Rename entity type columns"""
        for _, column_description in self.column_details.iterrows():
            if not pd.isna(column_description["enum"]):
                if column_description["enum"] not in self.enum_mapper:
                    raise ValueError(
                        f"Provided enum {column_description['enum']} not in enum mapper"
                    )
                # map each value in the table's column according to the
                # provided enum mapper in column_details table
                self.table[column_description["standard_name"]] = self.table[
                    column_description["standard_name"]
                ].map(self.enum_mapper[column_description["enum"]])

    def _standardize_ids(self) -> None:
        """Replace IDs/Nulls with UUIDs and store a mapping in self.id_map"""
        for id_column, matching_columns in self.id_columns_to_standardize.items():
            self.replace_id_with_uuid(id_column, matching_columns)

    def __init__(self) -> None:
        self.id_map = []
