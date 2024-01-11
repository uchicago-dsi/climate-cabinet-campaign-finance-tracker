import uuid
from pathlib import Path

import pandas as pd

from utils import clean
from utils import constants as const
from utils.constants import repo_root


def assign_PA_column_names(file_name: str, year: int) -> list:
    """Assigns the right column names to the right datasets.

    Args:
        filepath: the path in which the data is stored/located.

        year: to make parsing through the data more manageable, the year
        from which the data originates is also taken.

    Returns:
        a list of the appropriate column names for the dataset
    """
    if "contrib" in file_name:
        if year < 2022:
            return const.PA_CONT_COLS_NAMES_PRE2022
        else:
            return const.PA_CONT_COLS_NAMES_POST2022
    elif "filer" in file_name:
        if year < 2022:
            return const.PA_FILER_COLS_NAMES_PRE2022
        else:
            return const.PA_FILER_COLS_NAMES_POST2022
    elif "expense" in file_name:
        if year < 2022:
            return const.PA_EXPENSE_COLS_NAMES_PRE2022
        else:
            return const.PA_EXPENSE_COLS_NAMES_POST2022


class PennsylvaniaCleaner(clean.StateCleaner):
    def clean_state(self) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
        PA_directory = repo_root / "data" / "raw" / "PA"
        pre_processed_dfs = self.preprocess(PA_directory)
        clean_dfs = self.clean(pre_processed_dfs)
        standardized_dfs = self.standardize(clean_dfs)
        return self.create_tables(standardized_dfs)

    def preprocess(self, directory: str | Path = None) -> list[pd.DataFrame]:
        """Read raw campaign finance files from PA secretary of state

        Files should be stored in directory with format:
        /
        |--YYYY/
        |   |--contrib_*.txt
        |   |--debt_*.txt
        |   |--expense_*.txt
        |   |--filer_*.txt
        |   |--receipt_*.txt
        |--YYYY/
        ...
        """
        contributor_datasets, filer_datasets, expense_datasets = [], [], []
        if directory is None:
            directory = repo_root / "data" / "raw" / "PA"
        else:
            directory = Path(directory)
        for year_directory in directory.iterdir():
            year = int(year_directory.stem)
            for file_path in year_directory.iterdir():
                file_name = file_path.stem
                # only want contributor, filer, and expenditure files:
                if (
                    ("contrib" in file_name)
                    | ("filer" in file_name)
                    | ("expense" in file_name)
                ):
                    df = pd.read_csv(
                        file_path,
                        names=assign_PA_column_names(file_name, year),
                        sep=",",
                        encoding="latin-1",
                        on_bad_lines="warn",
                    )
                    df["YEAR"] = year

                    if "contrib" in file_name:
                        contributor_datasets.append(df)
                    elif "filer" in file_name:
                        filer_datasets.append(df)
                    else:
                        expense_datasets.append(df)
                else:
                    continue

        return contributor_datasets, filer_datasets, expense_datasets

    def clean(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        contributor_datasets, filer_datasets, expense_datasets = [], [], []
        cont_ds, filer_ds, exp_ds = data

        for contrib_df, filer_df, exp_df in zip(cont_ds, filer_ds, exp_ds):
            contributor_datasets.append(
                self.pre_process_contributor_dataset(contrib_df)
            )
            filer_datasets.append(self.pre_process_filer_dataset(filer_df))
            expense_datasets.append(self.pre_process_expense_dataset(exp_df))

        return contributor_datasets, filer_datasets, expense_datasets

    def standardize(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        contributor_ds, filer_ds, expense_ds = data
        merged_dataset = self.combine_contributor_expenditure_datasets(
            contributor_ds, filer_ds, expense_ds
        )
        dictionary, merged_dataset = self.replace_id_with_uuid(
            merged_dataset, "DONOR_ID", "RECIPIENT_ID"
        )
        self.output_ID_mapping(dictionary, merged_dataset)  # return dictionary

        # assign transaction_id
        merged_dataset["TRANSACTION_ID"] = str(uuid.uuid4())
        return merged_dataset

    def create_tables(
        self, standardized_df: pd.DataFrame
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
                "DONOR_TYPE",
                "DONOR_OFFICE",
                "PURPOSE",
                "RECIPIENT_ID",
                "RECIPIENT_TYPE",
                "RECIPIENT_OFFICE",
                "YEAR",
                "TRANSACTION_ID",
            ]
        ]
        transactions_df.columns = map(str.lower, transactions_df.columns)
        transactions_df = transactions_df.rename(
            columns={"recipient_office": "office_sought"}
        )

        return individuals_table, organizations_table, transactions_df

    def make_individuals_table(self, df: pd.DataFrame) -> pd.DataFrame:
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
        ][["DONOR", "DONOR_ID", "DONOR_PARTY", "DONOR_TYPE"]].rename(
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
        ][["RECIPIENT", "RECIPIENT_ID", "RECIPIENT_PARTY", "RECIPIENT_TYPE"]].rename(
            columns={
                "RECIPIENT": "full_name",
                "RECIPIENT_ID": "id",
                "RECIPIENT_PARTY": "party",
                "RECIPIENT_TYPE": "entity_type",
            }
        )

        all_individuals = pd.concat([donor_individuals, recipient_individuals])
        all_individuals = all_individuals.drop_duplicates()

        new_cols = ["first_name", "last_name", "company"]
        all_individuals = all_individuals.assign(**{col: None for col in new_cols})
        all_individuals["state"] = "PA"

        return all_individuals

    def make_organizations_table(self, organizations_df: pd.DataFrame) -> pd.DataFrame:
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
        ][["DONOR_ID", "DONOR", "DONOR_TYPE"]].rename(
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

    def make_transactions_tables(
        self, organizations_df: pd.DataFrame
    ) -> list[pd.DataFrame]:
        """This function takes in donor and recipient information, and
        reformates it into 4 dataframe that indicate transactions flowing from:
        1. Individuals -> Individuals
        2. Individuals -> Organizations:
        3. Organizations -> Individuals
        4. Organizations -> Organizations.

        Args:
            df: a pandas dataframe with donor and recipient information that
            details relevant information about a singular transactions,
            including a transaction ID, donor and recipient IDs, and the
            donation amount
        Returns:
            a list of pandas dataframe with 4 dataframes detailing the 4
            aformentioned transaction types.
        """

        df = organizations_df.copy()
        column_names = list(organizations_df.columns.str.lower())
        df.columns = column_names
        df["transaction_type"] = None
        df["office_sought"] = df.apply(
            lambda x: x["donor_office"]
            if x["recipient_office"] is None
            else x["recipient_office"],
            axis=1,
        )
        df = df.drop(columns={"donor_office", "recipient_office"})

        # now to separate the tables into 4:
        # individuals -> individuals:
        ind_to_ind = df.loc[
            (
                (
                    (df.donor_type == "Individual")
                    | (df.donor_type == "Candidate")
                    | (df.donor_type == "Lobbyist")
                )
                & (
                    (df.recipient_type == "Candidate")
                    | (df.recipient_type == "Individual")
                    | (df.recipient_type == "Lobbyist")
                )
            )
        ]

        # individuals -> Organizations:
        ind_to_org = df.loc[
            (
                (
                    (df.donor_type == "Individual")
                    | (df.donor_type == "Candidate")
                    | (df.donor_type == "Lobbyist")
                )
                & (
                    (df.recipient_type == "Committee")
                    | (df.recipient_type == "Organization")
                )
            )
        ]

        # Organizations -> Individuals
        org_to_ind = df.loc[
            (
                ((df.donor_type == "Committee") | (df.donor_type == "Organization"))
                & (
                    (df.recipient_type == "Candidate")
                    | (df.recipient_type == "Lobbyist")
                )
            )
        ]

        # Organizations -> Organizations:
        org_to_org = df.loc[
            (
                ((df.donor_type == "Committee") | (df.donor_type == "Organization"))
                & (
                    (df.recipient_type == "Committee")
                    | (df.recipient_type == "Organization")
                )
            )
        ]

        return [ind_to_ind, ind_to_org, org_to_ind, org_to_org]

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
        # separate into those with and without original IDs
        ids_1 = set(df[~df[col1].isnull()][col1])
        ids_2 = set(df[~df[col2].isnull()][col2])
        unique_ids = list(ids_1.union(ids_2))

        with_uuid = []
        for id in unique_ids:
            with_uuid.append([id, str(uuid.uuid4())])

        mapped_dict = {lst[0]: (lst[1]) for lst in with_uuid}
        df[col1] = df[col1].map(mapped_dict)
        df[col2] = df[col2].map(mapped_dict)
        mapped_dict = {value: key for key, value in mapped_dict.items()}

        # now generate new IDs for entities that don't have any, and update
        # mapped_dict:
        df[col1] = df[col1].apply(
            (lambda x: str(uuid.uuid4()) if not type(x) is str else x)
        )
        df[col2] = df[col2].apply(
            (lambda x: str(uuid.uuid4()) if not type(x) is str else x)
        )

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
        """pre-processes an expenditure dataset by sifting through the columns
        and keeping the relevant columns for EDA and AbstractStateCleaner
        purposes.

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

    def merge_contrib_filer_datasets(
        self, cont_file: pd.DataFrame, filer_file: pd.DataFrame
    ) -> pd.DataFrame:
        """merges the contributor and filer datasets from the same year using
        the unique filerID.
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
        """merges the expenditure and filer datasets from the same year using
        the unique filerID.
        Args:
            expend_file: The expenditure dataset

            filer_file: the filer dataset from the same year as the expenditure
            file
        Returns
            The merged pandas dataframe
        """
        merged_df = pd.merge(
            expend_file, filer_file, left_on="DONOR_ID", right_on="RECIPIENT_ID"
        ).drop("RECIPIENT_ID", axis=1)
        return merged_df

    def format_contrib_data_for_concat(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reformartes the merged contributor-filer dataset such that it has the
        same columns as the merged expenditure-filer dataset so that
        concatenation can occur.

        Args:
            The merged contributor-filer dataset

        Returns:
            A new dataframe with the appropriate column formatting for
            concatenation
        """
        new_cols = ["DONOR_ID", "DONOR_PARTY", "DONOR_OFFICE"]
        df = df.assign(**{col: None for col in new_cols})

        # There are some recipients whose entity_types isn't specified, so I
        # auto-fill the nan entries with 'Organization.'
        na_free = df.dropna(subset="RECIPIENT_TYPE")
        only_na = df[~df.index.isin(na_free.index)]
        only_na["RECIPIENT_TYPE"] = "Organization"
        df = pd.concat([na_free, only_na])

        columns = df.columns.to_list()
        columns.sort()
        df = df.loc[:, columns]
        return df

    def format_expend_data_for_concat(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reformartes the merged expenditure-filer dataset such that it has the
        same columns as the merged contributor-filer dataset so that
        concatenation can occur.

        Args:
            The merged expenditure-filer dataset

        Returns:
            A new dataframe with the appropriate column formatting for
            concatenation.
        """
        df["RECIPIENT_ID"] = None
        df = df.rename(
            columns={
                "RECIPIENT_x": "RECIPIENT",
                "RECIPIENT_y": "DONOR",
                "RECIPIENT_TYPE": "DONOR_TYPE",
                "RECIPIENT_PARTY": "DONOR_PARTY",
                "RECIPIENT_OFFICE": "DONOR_OFFICE",
            }
        )
        # because recipient information in the expenditure dataset is not
        # provided, for the sake of fitting the schema I code recipient_type as
        # 'Organization'.
        df["RECIPIENT_TYPE"] = "Organization"
        df["RECIPIENT_OFFICE"] = None
        df["RECIPIENT_PARTY"] = None

        # There are some donors whose entity_types isn't specified, so I
        # implement the same classify_contributor function used in the
        # contributors dataset
        na_free = df.dropna(subset="DONOR_TYPE")
        only_na = df[~df.index.isin(na_free.index)]
        only_na["DONOR_TYPE"] = only_na["DONOR"].apply(self.classify_contributor)
        df = pd.concat([na_free, only_na])

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
        merged_cont_datasets_per_yr = [
            self.merge_contrib_filer_datasets(cont, fil)
            for cont, fil in zip(contrib_ds, filer_ds)
        ]

        merged_exp_dataset_per_yr = [
            self.merge_expend_filer_datasets(exp, fil)
            for exp, fil in zip(expend_ds, filer_ds)
        ]

        contrib_filer_info = self.format_contrib_data_for_concat(
            pd.concat(merged_cont_datasets_per_yr)
        )
        expend_filer_info = self.format_expend_data_for_concat(
            pd.concat(merged_exp_dataset_per_yr)
        )
        concat_df = pd.concat([contrib_filer_info, expend_filer_info])
        concat_df = concat_df.reset_index(drop=True)
        return concat_df

    def output_ID_mapping(self, dictionary: dict, df: pd.DataFrame) -> None:
        """Given a dictionary and a dataFrame, this function cross-references
        the data in the dictionary with the dataframe to output a mapped
        dictionary csv. This csv file can be used to map the IDs in the original
        dataset to the generated UUIDs in the current dataset

        Args:
            dictionary: A dictionary mapping generated UUIDs to the original IDs
            df: a dataFrame with information on campaign finance.

        Returns:
        None (outputs a csv file to the output folder in the repository)
        """
        # first split the dataframe into donor and recipient information
        donors = df[["DONOR_TYPE", "DONOR_ID", "YEAR"]]
        donors = donors.drop_duplicates()
        donors["provided_id"] = donors.DONOR_TYPE.map(dictionary)

        recipients = df[["RECIPIENT_TYPE", "RECIPIENT_ID", "YEAR"]]
        recipients = recipients.drop_duplicates()
        recipients["provided_id"] = recipients.RECIPIENT_ID.map(dictionary)

        # combine the two after renaming columns
        donors = donors.rename(
            columns={"DONOR_TYPE": "entity_type", "DONOR_ID": "database_id"}
        )
        recipients = recipients.rename(
            columns={"RECIPIENT_TYPE": "entity_type", "RECIPIENT_ID": "database_id"}
        )

        entities = pd.concat([donors, recipients])
        entities = entities.drop_duplicates()
        entities["state"] = "Pennsylvania"

        entities.to_csv(repo_root / "output" / "PA_IDs.csv", index=False)
