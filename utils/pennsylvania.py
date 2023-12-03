import uuid
from pathlib import Path

import numpy as np
import pandas as pd

from utils import clean
from utils import constants as const


class PennsylvaniaCleaner(clean.StateCleaner):

    Base_Filepath = Path(__file__).resolve().parent.parent
    # resolve to the path of the current python file
    # repo_root will be the absolute path to the root of the repository,
    # no matter where the repository is.
    # For pathlib Path objects, `.parent` gets the parent path_splitectory and
    #  `/` can be used like `/` in unix style paths.
    filepaths_list = Base_Filepath / "data" / "raw" / "PA"

    # helper functions:
    def assign_col_names(self, filepath: str, year: int) -> list:
        """Assigns the right column names to the right datasets.

        Args:
            filepath: the path in which the data is stored/located.

            year: to make parsing through the data more manageable, the year
            from which the data originates is also taken.

        Returns:
            a list of the appropriate column names for the dataset
        """
        dir = filepath.split("/")
        file_type = dir[len(dir) - 1]

        if "contrib" in file_type:
            if year < 2022:
                return const.PA_CONT_COLS_NAMES_PRE2022
            else:
                return const.PA_CONT_COLS_NAMES_POST2022
        elif "filer" in file_type:
            if year < 2022:
                return const.PA_FILER_COLS_NAMES_PRE2022
            else:
                return const.PA_FILER_COLS_NAMES_POST2022
        elif "expense" in file_type:
            if year < 2022:
                return const.PA_EXPENSE_COLS_NAMES_PRE2022
            else:
                return const.PA_EXPENSE_COLS_NAMES_POST2022

    def replace_id_with_uuid(
        self, df: pd.DataFrame, col1: str, col2: str
    ) -> tuple[dict, pd.DataFrame]:
        """Creates a dictionary whose keys are generated UUIDs that map to values
        corresponding to unique IDs from the donor and recipient IDs columns in df

        Args:
            A pandas dataframe with at least two columns (col1, col1)
            col1, col2: columns of df that should have IDs
        Returns:
            A tuple whose first value is the modified df, where the IDs have been
            replaced with the UUIDS, and a dictionary correspondings to the UUIDs as
            keys and the original IDs from col1 and col2 as the values
        """
        # a set is used because there could be IDs in the donor column that also
        # appear in the recipient column due to concatenation, and I want to keep
        # the IDs unique throughout
        ids_1 = set(df[col1])
        ids_2 = set(df[col2])
        unique_ids = list(ids_1.union(ids_2))

        with_uuid = []
        for id in unique_ids:
            with_uuid.append([id, str(uuid.uuid4())])

        mapped_dict = {lst[0]: (lst[1]) for lst in with_uuid}
        df[col1] = df[col1].map(mapped_dict)
        df[col2] = df[col2].map(mapped_dict)
        mapped_dict = {value: key for key, value in mapped_dict.items()}
        return mapped_dict, df

    def classify_contributor(self, donor: str) -> str:
        """Takes a string input and compares it against a list of identifiers
        most commonly associated with organizations/corporations/PACs, and
        classifies the string input as belong to an individual or organization

        Args:
            contributor: a string
        Returns:
            string "ORGANIZATION" or "INDIVIDUAL" depending on the
            classification of the parameter
        """
        split = donor.split()
        loc = 0
        while loc < len(split):
            if split[loc].upper() in const.PA_ORGANIZATION_IDENTIFIERS:
                return "Organization"
            loc += 1
        return "Individual"

    def pre_process_contributor_dataset(self, df: pd.DataFrame):
        """pre-processes a contributor dataset by sifting through the columns and
        keeping the relevant columns for EDA and AbstractStateCleaner purposes

        Args:
            df: the contributor dataset

        Returns:
            a pandas dataframe whose columns are appropriately formatted.
        """
        df["AMOUNT"] = df["CONT_AMT_1"] + df["CONT_AMT_2"] + df["CONT_AMT_3"]
        df["RECIPIENT_ID"] = df["RECIPIENT_ID"].astype("str")
        df["DONOR"] = df["DONOR"].astype("str")
        df["DONOR"] = df["DONOR"].str.title()
        df["DONOR_TYPE"] = df["DONOR"].apply(self.classify_contributor)
        df = df.drop(
            columns={
                "ADDRESS_1",
                "ADDRESS_2",
                "CITY",
                "STATE",
                "ZIPCODE",
                "OCCUPATION",
                "E_NAME",
                "E_ADDRESS_1",
                "E_ADDRESS_2",
                "E_CITY",
                "E_STATE",
                "E_ZIPCODE",
                "SECTION",
                "CYCLE",
                "CONT_DATE_1",
                "CONT_AMT_1",
                "CONT_DATE_2",
                "CONT_AMT_2",
                "CONT_DATE_3",
                "CONT_AMT_3",
            }
        )

        if "TIMESTAMP" in df.columns:
            df = df.drop(columns={"TIMESTAMP", "REPORTER_ID"})
            df["DONOR"] = df["DONOR"].apply(lambda x: str(x).title())

        return df

    def pre_process_filer_dataset(self, df: pd.DataFrame):
        """pre-processes a filer dataset by sifting through the columns and
        keeping the relevant columns for EDA and AbstractStateCleaner purposes

        Args:
            df: the filer dataset

        Returns:
            a pandas dataframe whose columns are appropriately formatted.
        """
        df["RECIPIENT_ID"] = df["RECIPIENT_ID"].astype("str")
        df = df.drop(
            columns={
                "YEAR",
                "CYCLE",
                "AMEND",
                "TERMINATE",
                "DISTRICT",
                "ADDRESS_1",
                "ADDRESS_2",
                "CITY",
                "STATE",
                "ZIPCODE",
                "COUNTY",
                "PHONE",
                "BEGINNING",
                "MONETARY",
                "INKIND",
            }
        )
        if "TIMESTAMP" in df.columns:
            df = df.drop(columns={"TIMESTAMP", "REPORTER_ID"})

        df = df.drop_duplicates(subset=["RECIPIENT_ID"])
        df["RECIPIENT_TYPE"] = df.RECIPIENT_TYPE.map(const.PA_FILER_ABBREV_DICT)
        df["RECIPIENT"] = df["RECIPIENT"].apply(lambda x: str(x).title())
        return df

    def pre_process_expense_dataset(self, df: pd.DataFrame):
        """pre-processes an expenditure dataset by sifting through the columns and
        keeping the relevant columns for EDA and AbstractStateCleaner purposes

        Args:
            df: the expenditure dataset

        Returns:
            a pandas dataframe whose columns are appropriately formatted.
        """
        df["DONOR_ID"] = df["DONOR_ID"].astype("str")
        df = df.drop(
            columns={
                "EXPENSE_CYCLE",
                "EXPENSE_ADDRESS_1",
                "EXPENSE_ADDRESS_2",
                "EXPENSE_CITY",
                "EXPENSE_STATE",
                "EXPENSE_ZIPCODE",
                "EXPENSE_DATE",
            }
        )
        if "EXPENSE_REPORTER_ID" in df.columns:
            df = df.drop(columns={"EXPENSE_TIMESTAMP", "EXPENSE_REPORTER_ID"})
        df["PURPOSE"] = df["PURPOSE"].apply(lambda x: str(x).title())
        df["RECIPIENT"] = df["RECIPIENT"].apply(lambda x: str(x).title())
        return df

    def initialize_PA_dataset(self, data_filepath: str, year: int) -> pd.DataFrame:
        """initializes the PA data appropriately based on whether the data contains
        filer, contributor, or expense information

        Args:
            data_filepath: the path in which the data is stored/located.

            year: the year from which the data originates

        Returns:
            a pandas dataframe whose columns are appropriately formatted, and
            any dirty rows with inconsistent columns names dropped.
        """

        df = pd.read_csv(
            data_filepath,
            names=self.assign_col_names(data_filepath, year),
            sep=",",
            encoding="latin-1",
            on_bad_lines="warn",
        )

        df["YEAR"] = year
        dir = data_filepath.split("/")
        file_type = dir[len(dir) - 1]

        if "contrib" in file_type:
            return self.pre_process_contributor_dataset(df)

        elif "filer" in file_type:
            return self.pre_process_filer_dataset(df)

        elif "expense" in file_type:
            return self.pre_process_expense_dataset(df)

        else:
            raise ValueError(
                "This function is currently formatted for filer, \
                expense, and contributor datasets. Make sure your data \
                is from these sources."
            )

    def merge_contrib_filer_datasets(
        self, cont_file: pd.DataFrame, filer_file: pd.DataFrame
    ) -> pd.DataFrame:
        """merges the contributor and filer datasets from the same year using the
        unique filerID
        Args:
            cont_file: The contributor dataset

            filer_file: the filer dataset from the same year as the contributor
            file.
        Returns
            The merged pandas dataframe
        """
        merged_df = pd.merge(cont_file, filer_file, how="left", on="RECIPIENT_ID")
        return merged_df

    def merge_expend_filer_datasets(
        self, expend_file: pd.DataFrame, filer_file: pd.DataFrame
    ) -> pd.DataFrame:
        """merges the expenditure and filer datasets from the same year using the
        unique filerID
        Args:
            expend_file: The expenditure dataset

            filer_file: the filer dataset from the same year as the expenditure file
        Returns
            The merged pandas dataframe
        """
        merged_df = pd.merge(
            expend_file, filer_file, left_on="DONOR_ID", right_on="RECIPIENT_ID"
        ).drop("RECIPIENT_ID", axis=1)
        return merged_df

    def format_contrib_data_for_concat(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reformartes the merged contributor-filer dataset such that it has the
        same columns as the merged expenditure-filer dataset so that concatenation
        can occur

        Args:
            The merged contributor-filer dataset

        Returns:
            A new dataframe with the appropriate column formatting for concatenation
        """
        df["DONOR_ID"] = np.nan
        df["DONOR_PARTY"] = np.nan
        df["DONOR_OFFICE"] = np.nan
        columns = df.columns.to_list()
        columns.sort()
        df = df.loc[:, columns]
        return df

    def format_expend_data_for_concat(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reformartes the merged expenditure-filer dataset such that it has the
        same columns as the merged contributor-filer dataset so that concatenation
        can occur

        Args:
            The merged expenditure-filer dataset

        Returns:
            A new dataframe with the appropriate column formatting for concatenation
        """
        df["RECIPIENT_ID"] = np.nan
        df = df.rename(
            columns={
                "RECIPIENT_x": "RECIPIENT",
                "RECIPIENT_y": "DONOR",
                "RECIPIENT_TYPE": "DONOR_TYPE",
                "RECIPIENT_PARTY": "DONOR_PARTY",
                "RECIPIENT_OFFICE": "DONOR_OFFICE",
            }
        )
        df["RECIPIENT_TYPE"] = np.nan
        df["RECIPIENT_OFFICE"] = np.nan
        df["RECIPIENT_PARTY"] = np.nan
        columns = df.columns.to_list()
        columns.sort()
        df = df.loc[:, columns]
        return df

    def combine_contributor_expenditure_datasets(
        self,
        contrib_ds: list[pd.DataFrame],
        filer_ds: list[pd.DataFrame],
        expend_ds: list[pd.DataFrame],
    ) -> pd.DataFrame:
        """This function takes datasets with information from the contributor,
        filer, and expenditure datasets in each given year, merges the
        contributor and expenditure datasets with pertinent information from the
        filer dataset,and concatenates the 3 datasets into 1 dataset with.

        Args:
            3 datasets: contributor, filer, and expenditure datasets. Each of
            the datasets is a list of dataframes, with each entry in the
            dataframes being a given file from a select year

        Returns:
            A concatenated dataframe with transaction information, contributor
            information, and recipient information.
        """
        merged_cont_datasets_per_yr = []
        merged_exp_dataset_per_yr = []

        for i in range(len(contrib_ds)):
            cont_merged = self.merge_contrib_filer_datasets(contrib_ds[i], filer_ds[i])
            expend_merged = self.merge_expend_filer_datasets(expend_ds[i], filer_ds[i])
            merged_cont_datasets_per_yr.append(cont_merged)
            merged_exp_dataset_per_yr.append(expend_merged)

        merged_cont_datasets_per_yr = pd.concat(merged_cont_datasets_per_yr)
        merged_exp_dataset_per_yr = pd.concat(merged_exp_dataset_per_yr)
        contrib_filer_info = self.format_contrib_data_for_concat(
            merged_cont_datasets_per_yr
        )
        expend_filer_info = self.format_expend_data_for_concat(
            merged_exp_dataset_per_yr
        )
        return pd.concat([contrib_filer_info, expend_filer_info])

    def output_ID_mapping(self, dictionary: dict, df: pd.DataFrame):
        pass

    def make_individuals_table(df: pd.Dataframe) -> pd.DataFrame:
        """This function isolates donors and recipients who are classified as
        individuals and returns a dataframe with strictly individual information
        pertinent to the StateCleaner schema.

        Args:
            df: a pandas dataframe with donor and recipient information
        Returns:
            a pandas dataframe strictly with information regarding individuals
            from the inputted dataframe
        """
        donor_individuals = df.loc[
            (
                (df.DONOR_TYPE == "Individuals")
                | (df.DONOR_TYPE == "Candidate")
                | (df.DONOR_TYPE == "Lobbyist")
            )
        ]
        donor_individuals = donor_individuals[
            ["DONOR", "DONOR_ID", "DONOR_PARTY", "DONOR_TYPE"]
        ]
        donor_individuals = donor_individuals.rename(
            columns={
                "DONOR": "full_name",
                "DONOR_ID": "id",
                "DONOR_PARTY": "party",
                "DONOR_TYPE": "entity_type",
            }
        )

        recipient_individuals = df.loc[
            (
                (df.RECIPIENT_TYPE == "Individuals")
                | (df.RECIPIENT_TYPE == "Candidate")
                | (df.RECIPIENT_TYPE == "Lobbyist")
            )
        ]
        recipient_individuals = recipient_individuals[
            ["RECIPIENT", "RECIPIENT_ID", "RECIPIENT_PARTY", "RECIPIENT_TYPE"]
        ]
        recipient_individuals = recipient_individuals.rename(
            columns={
                "RECIPIENT": "full_name",
                "RECIPIENT_ID": "id",
                "RECIPIENT_PARTY": "party",
                "RECIPIENT_TYPE": "entity_type",
            }
        )

        all_individuals = pd.concat([donor_individuals, recipient_individuals])
        all_individuals = all_individuals.drop_duplicates()
        all_individuals["first_name"] = np.nan
        all_individuals["last_name"] = np.nan
        all_individuals["state"] = "PA"
        all_individuals["company"] = np.nan
        return all_individuals

    def make_organizations_table(organizations_df: pd.DataFrame) -> pd.DataFrame:
        """This function isolates donors and recipients who are classified as
        committees or organizations and returns a dataframe with strictly
        committee/organization information pertinent to the StateCleaner schema.

        Args:
            df: a pandas dataframe with donor and recipient information
        Returns:
            a pandas dataframe strictly with information regarding committess or
            organizations from the inputted dataframe.
        """
        donor_organizations = organizations_df.loc[
            (
                (organizations_df.DONOR_TYPE == "Committee")
                | (organizations_df.DONOR_TYPE == "Organization")
            )
        ]
        donor_organizations = donor_organizations[["DONOR_ID", "DONOR", "DONOR_TYPE"]]
        donor_organizations = donor_organizations.rename(
            columns={"DONOR_ID": "id", "DONOR": "name", "DONOR_TYPE": "entity_type"}
        )
        recipient_organizations = organizations_df.loc[
            (
                (organizations_df.RECIPIENT_TYPE == "Committee")
                | (organizations_df.RECIPIENT_TYPE == "Organization")
            )
        ]
        recipient_organizations = recipient_organizations[
            ["RECIPIENT_ID", "RECIPIENT", "RECIPIENT_TYPE"]
        ]
        recipient_organizations = recipient_organizations.rename(
            columns={
                "RECIPIENT_ID": "id",
                "RECIPIENT": "name",
                "RECIPIENT_TYPE": "entity_type",
            }
        )
        all_organizations = pd.concat([donor_organizations, recipient_organizations])
        all_organizations = all_organizations.drop_duplicates()
        all_organizations["state"] = "PA"

        return all_organizations

    def make_transactions_table(organizations_df: pd.DataFrame) -> pd.DataFrame:
        pass

    def preprocess(self, filepaths_list: list[str]) -> list[pd.DataFrame]:

        contributor_datasets, filer_datasets, expense_datasets = [], [], []
        # incase the provided filepath was not in increasing order, this just
        # makes sure the filepaths numerically line up
        filepaths_list.sort()
        for path in filepaths_list:
            # because of the different formatting for data in different years
            # obstained from the PA website years, I had my webscraper append
            # the year to the filename
            path_split = path.split("_")
            year = path_split[len(path_split) - 1].replace(".txt", "")
            year = int(year)

            df = self.initialize_PA_dataset(path, year)
            if "contrib" in path:
                contributor_datasets.append(df)
            elif "filer" in path:
                filer_datasets.append(df)
            elif "expense" in path:
                expense_datasets.append(df)
            else:
                pass  # do nothing

        merged_dataset = self.combine_contributor_expenditure_datasets(
            contributor_datasets, filer_datasets, expense_datasets
        )
        dictionary, merged_dataset = self.replace_id_with_uuid(
            merged_dataset, "DONOR_ID", "RECIPIENT_ID"
        )
        self.output_ID_mapping(dictionary, merged_dataset)  # return dictionary

        # assign transaction_id
        merged_dataset["TRANSACTION_ID"] = str(uuid.uuid4())
        return merged_dataset

    def create_tables(
        self, standardized_df: pd.Dataframe
    ) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):

        # separate the standardized_df information into the relevant columns for
        # individuals, organizations, and transactions tables

        # Individuals Table:
        individuals_df = standardized_df[
            [
                "DONOR",
                "DONOR_ID",
                "DONOR_PARTY",
                "DONOR_TYPE",
                "RECIPIENT",
                "RECIPIENT_ID",
                "RECIPIENT_PARTY",
                "RECIPIENT_TYPE",
            ]
        ]
        individuals_table = self.make_individuals_table(individuals_df)

        # Organizations Table
        organizations_df = standardized_df[
            [
                "DONOR",
                "DONOR_ID",
                "DONOR_TYPE",
                "RECIPIENT",
                "RECIPIENT_ID",
                "RECIPIENT_TYPE",
            ]
        ]
        organizations_table = self.make_organizations_table(organizations_df)
        # Transactions Table
        transactions_df = standardized_df[
            [
                "AMOUNT",
                "DONOR_ID",
                "DONOR_OFFICE",
                "PURPOSE",
                "RECIPIENT_ID",
                "RECIPIENT_OFFICE",
                "YEAR",
                "TRANSACTION_ID",
            ]
        ]
        transactions_table = self.make_transactions_table(transactions_df)

        return individuals_table, organizations_table, transactions_table
