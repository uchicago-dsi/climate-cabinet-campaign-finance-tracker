"""Represents raw input data from states"""

from pathlib import Path

import pandas as pd

from utils.finance.config import ConfigHandler


class DataReader:
    """Reads raw state finance data into a pandas dataframe

    The finance data should be in a tabular form and should have a
    single schema.
    """

    @property
    def default_raw_data_paths(self) -> list[str | Path]:
        """Default paths to any files of this type"""
        return self._default_paths

    @property
    def dtype_dict(self) -> dict[str, str]:
        """Maps raw column names in"""
        return self._dtype_dict

    def __init__(self, config_handler: ConfigHandler | None = None) -> None:
        """Initalize new data reader

        Args:
            config_handler: ConfigHandler, documented # TODO
        """
        self._dtype_dict = config_handler.dtype_dict
        self.read_csv_params = config_handler.read_csv_params
        self._default_paths = config_handler.raw_data_file_paths
        self.columns = config_handler.raw_column_order

    def read_tabular_data(self, path: str = None) -> pd.DataFrame:
        """Read raw tabular data from state provided files into a DataFrame

        This method should maintain the data as closely as possible. The only
        permissible modification is the dropping of segments of data that are
        malformed and not able to be read into a dataframe. These rows should
        be (TODO #107) reported.
        """
        if path is None:
            path = self.default_raw_data_paths[0]
        table = pd.read_csv(
            str(path),
            dtype=self.dtype_dict,
            **self.read_csv_params,
        )
        return table


class SchemaTransformer:
    """Transforms column names (? and adds additional columns)"""

    def __init__(self, config_handler: ConfigHandler | None = None) -> None:
        """Create a new SchemaTransformer object

        Args:
            config_handler: ConfigHandler, documented # TODO
        """
        self.column_mapper = config_handler.column_mapper
        self.relevant_columns = config_handler.relevant_columns
        self.duplicate_columns = config_handler.duplicate_columns
        self.new_empty_columns = config_handler.new_empty_columns
        self.state_code_columns = config_handler.state_code_columns
        self.state_code = config_handler.state_code

    def _rename_columns(self, standard_data_table: pd.DataFrame) -> pd.DataFrame:
        """Rename columns"""
        standard_data_table = standard_data_table.rename(columns=self.column_mapper)
        return standard_data_table

    def _drop_unused_columns(self, standard_data_table: pd.DataFrame) -> pd.DataFrame:
        """Drop columns with information that are not used in internal schema"""
        standard_data_table = standard_data_table.loc[
            :, standard_data_table.columns.isin(self.relevant_columns)
        ]
        return standard_data_table

    def _add_duplicate_columns(self, standard_data_table: pd.DataFrame) -> pd.DataFrame:
        """Add new columns that are copies of existing columns

        This can be wanted in cases where a column represents a property of two
        separate entities. For example, a raw file might have a 'state' column
        that denotes both the state of the election the candidate is running for
        and the state in which the transaction took place. Duplicating here allows
        for the normalization logic to decompose tables without worrying about
        state specific configuration.
        """
        for base_column, duplicate_column_list in self.duplicate_columns.items():
            for duplicate_column in duplicate_column_list:
                standard_data_table[duplicate_column] = standard_data_table[base_column]
        return standard_data_table

    def _add_new_columns(self, standard_data_table: pd.DataFrame) -> pd.DataFrame:
        """Add new blank columns TODO: is this ever really needed?"""
        for column in self.new_empty_columns:
            standard_data_table[column] = None
        return standard_data_table

    def _add_state_code(self, standard_data_table: pd.DataFrame) -> pd.DataFrame:
        """Add state code to relevent columns"""
        for column in self.state_code_columns:
            standard_data_table[column] = self.state_code
        return standard_data_table

    def standardize_schema(self, raw_data_table: pd.DataFrame) -> pd.DataFrame:
        """Rename columns, remove unused columns, and add missing columns

        Args:
            raw_data_table: single dataframe with raw column names
        """
        relevant_raw_table = self._drop_unused_columns(raw_data_table)
        standard_relevant_column_table = self._rename_columns(relevant_raw_table)
        standard_relevant_column_table = self._add_state_code(
            standard_relevant_column_table
        )
        standard_relevant_column_table = self._add_new_columns(
            standard_relevant_column_table
        )
        standard_schema_table = self._add_duplicate_columns(
            standard_relevant_column_table
        )

        return standard_schema_table


class DataStandardizer:
    """Ensures data conforms to expected types"""

    def __init__(self, config_handler: ConfigHandler | None = None) -> None:
        """Create a new DataStandardizer object

        Args:
            config_handler: ConfigHandler, documented # TODO
        """
        self.enum_mapper = config_handler.enum_mapper
        self.column_to_date_format = config_handler.column_to_date_format

    def _standardize_enums(self, standard_schema_table: pd.DataFrame) -> pd.DataFrame:
        """Rename entity type columns"""
        for column_name, column_enum_map in self.enum_mapper.items():
            if column_name not in standard_schema_table.columns:
                raise ValueError(f"Provided enum: {column_name} not in table")
            # map each value in the table's column according to the provided enum mapper
            standard_schema_table[column_name] = standard_schema_table[column_name].map(
                column_enum_map
            )
        return standard_schema_table

    def _standardize_date_format(
        self, standard_schema_table: pd.DataFrame
    ) -> pd.DataFrame:
        """For each column with a configured date format, convert dates to ISO standard

        Args:
            standard_schema_table: table that has already passed through
                SchemaTransformer (most revelantly the column names should
                be standard)
        """
        for date_column, date_format in self.column_to_date_format.items():
            na_mask = standard_schema_table[date_column].isna()
            temp_column = f"tmp-{date_column}"
            standard_schema_table[temp_column] = pd.NA
            standard_schema_table.loc[~na_mask, temp_column] = pd.to_datetime(
                standard_schema_table.loc[~na_mask, date_column],
                format=date_format,
                errors="coerce",
            ).dt.date
            standard_schema_table = standard_schema_table.drop(columns=date_column)
            standard_schema_table = standard_schema_table.rename(
                columns={temp_column: date_column}
            )
        return standard_schema_table

    def standardize_data(
        self,
        standard_schema_table: pd.DataFrame,
        enum_mapper: dict[dict[str, str]] | None = None,
        column_to_date_format: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Transform data like enums and dates to correct format

        Transformation should only be done where there is no ambiguity. For
        example: 'January 30, 2025' can be safely transformed to '2025-1-30',
        but 'First Name Last Name' in a 'Full Name' column cannot be split
        without assumptions

        Args:
            standard_schema_table: table with TODO standard schema
            enum_mapper: dict mapping column names to dicts mapping
                raw values in the enum column to their standard values
            column_to_date_format: dict mapping column names to their date format
        """
        if enum_mapper is not None:
            self.enum_mapper = enum_mapper
        if column_to_date_format is not None:
            self.column_to_date_format = column_to_date_format
        standard_schema_table = self._standardize_enums(standard_schema_table)
        standard_data_table = self._standardize_date_format(standard_schema_table)
        return standard_data_table


class DataSourceStandardizationPipeline:
    """Run pipeline for a single data source from file to standardized dataframe"""

    @property
    def table_type(self) -> str:
        """Table type that pipeline standardizes"""
        return self.config_handler.table_type

    def __init__(
        self,
        state_code: str,
        form_code: str,
        config_file: Path = None,
        data_reader: DataReader = None,
        schema_transformer: SchemaTransformer = None,
        data_standardizer: DataStandardizer = None,
    ) -> None:
        """Initialize data source standardization for a single data source

        Args:
            state_code: two letter abbreviation for state
            form_code: code to identify data source form type in state config file
            config_file: Path to config file. If not provided, state's default
                config file in config/finance/{state_code}.yaml will be used
            data_reader: logic for reading data into raw dataframe. If none provided
                will use default logic with provided config
            schema_transformer: logic for transforming schema of raw data. If none
                provided, will use default logic with provided config
            data_standardizer: logic for standardizing data of the raw file. If
                none provided, will use default logic with provided config.
        """
        self.state_code = state_code.lower()
        self.form_code = form_code
        if config_file is not None:
            self.config_handler = ConfigHandler(form_code, config_file_path=config_file)
        else:
            self.config_handler = ConfigHandler(form_code, state_code=self.state_code)
        if data_reader is None:
            self.data_reader = DataReader(self.config_handler)
        else:
            self.data_reader = data_reader
        if schema_transformer is None:
            self.schema_transformer = SchemaTransformer(self.config_handler)
        else:
            self.schema_transformer = schema_transformer
        if data_standardizer is None:
            self.data_standardizer = DataStandardizer(self.config_handler)
        else:
            self.data_standardizer = data_standardizer

    def load_and_standardize_data_source(self) -> pd.DataFrame:
        """Load and process all data for a source into a single concatenated dataframe"""
        standardized_tables = []
        if self.data_reader.default_raw_data_paths == []:
            return pd.DataFrame()
        for data_path in self.data_reader.default_raw_data_paths:
            raw_data_table = self.data_reader.read_tabular_data(data_path)
            standard_schema_table = self.schema_transformer.standardize_schema(
                raw_data_table
            )
            standard_data_table = self.data_standardizer.standardize_data(
                standard_schema_table
            )
            standardized_tables.append(standard_data_table)
        return pd.concat(standardized_tables)
