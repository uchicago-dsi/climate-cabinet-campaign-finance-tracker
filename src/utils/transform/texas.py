"""Implements state transformer class for Texas"""

import uuid
from pathlib import Path

import pandas as pd

from utils.constants import BASE_FILEPATH
from utils.transform import Form, clean


class TexasTransformer(clean.StateTransformer):
    """Texas state transformer implementation"""

    name = "Texas"
    stable_id_across_years = True
    TX_directory = BASE_FILEPATH / "data" / "raw" / "TX" / "sample"

    def __init__(self):
        self.contributionFrom = Form.TexasContributionForm()
        self.filerForm = Form.TexasFilerForm()
        self.expenseForm = Form.TexasExpenseForm()

    def clean_state(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Return tables of proper schema"""
        self.read_dataset(self.TX_directory)
        self.clean()
        standardized_dfs = self.standardize()
        return self.create_tables(standardized_dfs)

    def read_dataset(self, directory: str | Path = None) -> None:
        # contributor_datasets, filer_datasets, expense_datasets = None, None, None
        if directory is None:
            directory = self.TX_directory
        else:
            directory = Path(directory)

        contribs_files = [f for f in directory.iterdir() if f.name == "contribs.csv"]
        expense_files = [f for f in directory.iterdir() if f.name == "expend1.csv"]
        filer_files = [f for f in directory.iterdir() if f.name == "filers.csv"]
        try:
            # contributor_datasets = self.contributionFrom.read_table(contribs_files)
            # filer_datasets = self.filerForm.read_table(filer_files)
            # expense_datasets = self.expenseForm.read_table(expense_files)
            self.contributionFrom.read_table(contribs_files)
            self.filerForm.read_table(filer_files)
            self.expenseForm.read_table(expense_files)
        except Exception as e:
            print(f"Error processing file: {e}")
        # return contributor_datasets, filer_datasets, expense_datasets
        return

    def preprocess(self, directory: str | Path = None) -> list[pd.DataFrame]:
        contributor_datasets, filer_datasets, expense_datasets = None, None, None
        if directory is None:
            directory = self.TX_directory
        else:
            directory = Path(directory)

        contribs_files = [f for f in directory.iterdir() if f.name == "contribs.csv"]
        expense_files = [f for f in directory.iterdir() if f.name == "expend1.csv"]
        filer_files = [f for f in directory.iterdir() if f.name == "filers.csv"]
        try:
            contributor_datasets = self.contributionFrom.read_table(contribs_files)
            filer_datasets = self.filerForm.read_table(filer_files)
            expense_datasets = self.expenseForm.read_table(expense_files)
        except Exception as e:
            print(f"Error processing file: {e}")
        return contributor_datasets, filer_datasets, expense_datasets

    def clean(self) -> None:
        self.contributionFrom.preprocess_data()
        self.filerForm.preprocess_data()
        self.expenseForm.preprocess_data()

    # def clean(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:  # noqa: D102
    #     contributor_datasets, filer_datasets, expense_datasets = [], [], []
    #     self.contributionFrom.preprocess_data()
    #     self.filerForm.preprocess_data()
    #     self.expenseForm.preprocess_data()
    #     contributor_datasets.append(self.contributionFrom.get_table())
    #     filer_datasets.append(self.filerForm.get_table())
    #     expense_datasets.append(self.expenseForm.get_table())
    #     return contributor_datasets, filer_datasets, expense_datasets

    # integrate all datasets
    def standardize(self, data: list[pd.DataFrame] = None) -> list[pd.DataFrame]:  # noqa: D102
        merged_dataset = self.combine_contributor_expenditure_datasets(
            [self.contributionFrom.get_table()],
            [self.filerForm.get_table()],
            [self.expenseForm.get_table()],
        )
        # TO CLARIFY: do we need this?
        # merged_dataset = self.replace_id_with_uuid(merged_dataset, "DONOR_ID", "YEAR")
        # merged_dataset = self.replace_id_with_uuid(
        #     merged_dataset, "RECIPIENT_ID", "YEAR"
        # )
        # assign transaction_id
        merged_dataset["TRANSACTION_ID"] = str(uuid.uuid4())
        return merged_dataset

    def create_tables(  # noqa: D102
        self, standardized_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
        transactions_df.rename(columns={"recipient_office": "office_sought"})

        return individuals_table, organizations_table, transactions_df

    def make_individuals_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Returns entiteis that are likely individuals with only relevant columns

        Args:
            df: a pandas dataframe with donor and recipient information
        Returns:
            a pandas dataframe strictly with information regarding individuals
            from the inputted dataframe
        """
        donor_individuals = df.loc[
            (
                (df.DONOR_TYPE == "Individual")
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
                (df.RECIPIENT_TYPE == "Individual")
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
        all_individuals["state"] = "TX"

        return all_individuals

    # TODO: Remove redundant checks
    def make_organizations_table(self, organizations_df: pd.DataFrame) -> pd.DataFrame:
        """Returns entiteis that are likely organizations with only relevant columns

        Args:
            organizations_df: a pandas dataframe with donor and recipient information
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
            columns={
                "DONOR_ID": "id",
                "DONOR": "name",
                "DONOR_TYPE": "entity_type",
            }
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
        all_organizations["state"] = "TX"

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
            organizations_df: a pandas dataframe with donor and recipient information that
                details relevant information about a singular transactions,
                including a transaction ID, donor and recipient IDs, and the
                donation amount
        Returns:
            a list of pandas dataframe with 4 dataframes detailing the 4
            aformentioned transaction types.
        """
        organizations_df = organizations_df.copy()
        column_names = list(organizations_df.columns.str.lower())
        organizations_df.columns = column_names
        organizations_df["transaction_type"] = None
        organizations_df["office_sought"] = organizations_df.apply(
            lambda x: (
                x["donor_office"]
                if x["recipient_office"] is None
                else x["recipient_office"]
            ),
            axis=1,
        )
        organizations_df = organizations_df.drop(
            columns={"donor_office", "recipient_office"}
        )

        # now to separate the tables into 4:
        # individuals -> individuals:
        ind_to_ind = organizations_df.loc[
            (
                (
                    (organizations_df.donor_type == "Individual")
                    | (organizations_df.donor_type == "Candidate")
                    | (organizations_df.donor_type == "Lobbyist")
                )
                & (
                    (organizations_df.recipient_type == "Candidate")
                    | (organizations_df.recipient_type == "Individual")
                    | (organizations_df.recipient_type == "Lobbyist")
                )
            )
        ]

        # individuals -> Organizations:
        ind_to_org = organizations_df.loc[
            (
                (
                    (organizations_df.donor_type == "Individual")
                    | (organizations_df.donor_type == "Candidate")
                    | (organizations_df.donor_type == "Lobbyist")
                )
                & (
                    (organizations_df.recipient_type == "Committee")
                    | (organizations_df.recipient_type == "Organization")
                )
            )
        ]

        # Organizations -> Individuals
        org_to_ind = organizations_df.loc[
            (
                (
                    (organizations_df.donor_type == "Committee")
                    | (organizations_df.donor_type == "Organization")
                )
                & (
                    (organizations_df.recipient_type == "Candidate")
                    | (organizations_df.recipient_type == "Lobbyist")
                )
            )
        ]

        # Organizations -> Organizations:
        org_to_org = organizations_df.loc[
            (
                (
                    (organizations_df.donor_type == "Committee")
                    | (organizations_df.donor_type == "Organization")
                )
                & (
                    (organizations_df.recipient_type == "Committee")
                    | (organizations_df.recipient_type == "Organization")
                )
            )
        ]

        return [ind_to_ind, ind_to_org, org_to_ind, org_to_org]

    def replace_id_with_uuid(
        self, df_with_ids: pd.DataFrame, id_column: str, year_column: int = None
    ) -> pd.DataFrame:
        """For each row, replaces id with UUID. Creates mapping from any previous ids

        If the id_column is na, just replaces it with a uuid. If the id_column is not
        na, creates a UUID for each unique value of the id_column*

        * Some states' ids are not stable across year. This is noted by the
        `stable_id_across_years` attribute. If False, the id is treated as the
        year - id pairing.

        Args:
            df_with_ids: Dataframe that must contain a column representing
                ids for docnor/contributor entities.
            id_column: Column containing raw ids provided by state
            year_column: Column containing year transaction/entity appeared in data.
        """
        na_id_mask = df_with_ids[id_column].isna()
        rows_with_no_id = df_with_ids[na_id_mask]
        rows_with_id = df_with_ids[~na_id_mask]
        # generate and replace na ids with uuids
        rows_with_no_id[id_column] = rows_with_no_id[id_column].apply(
            lambda _: uuid.uuid4()
        )
        # identify unique identifiers
        if self.stable_id_across_years:
            unique_id_year_pairs = [
                (row_id, None) for row_id in rows_with_id[id_column].unique()
            ]
        else:
            unique_id_year_pairs = (
                rows_with_id[[id_column, year_column]].drop_duplicates().to_numpy()
            )
        # generate uuids for existing ids
        uuid_lookup_table_data = []
        for identifier in unique_id_year_pairs:
            uuid_lookup_table_data.append([*identifier, uuid.uuid4()])
        uuid_lookup_table = pd.DataFrame(
            columns=["Raw ID", "Year", "UUID"], data=uuid_lookup_table_data
        )
        uuid_lookup_table["State"] = self.name
        # TODO: save uuid
        # replace ids with uuids
        uuid_lookup_table = uuid_lookup_table.set_index(["Raw ID", "Year"])
        rows_with_id_and_uuid = rows_with_id.merge(
            uuid_lookup_table,
            how="left",
            left_on=[id_column, year_column],
            right_index=True,
        )
        rows_with_id_and_uuid[id_column] = rows_with_id_and_uuid["UUID"]
        rows_with_id_and_uuid = rows_with_id_and_uuid.drop(columns=["UUID"])
        # concatenate dfs and return
        return pd.concat([rows_with_id_and_uuid, rows_with_no_id])

    # TO CLARIFY: do we need two methods for this?
    def merge_contributor_filer_datasets(
        self, contributor_file: pd.DataFrame, filer_file: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge contributor and filer datasets using their IDs

        Args:
            contributor_file: The contributor dataset
            filer_file: the filer dataset from the same year as the contributor
                file.

        Returns:
            The merged pandas dataframe
        """
        merged_df = contributor_file.merge(filer_file, how="left", on="RECIPIENT_ID")
        return merged_df

    def merge_expenditure_filer_datasets(
        self, expenditure_file: pd.DataFrame, filer_file: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge expenditure and filer datasets using their IDs

        Args:
            expenditure_file: The expenditure dataset
            filer_file: the filer dataset from the same year as the expenditure
                file
        Returns
            The merged pandas dataframe
        """
        # TO CLARIFY: no donor in filer

        merged_df = expenditure_file.merge(
            filer_file, left_on="DONOR_ID", right_on="RECIPIENT_ID"
        ).drop("RECIPIENT_ID", axis=1)
        return merged_df

    def format_contributor_filer_dataset(
        self, merged_contributor_filer_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Reformats merged contributor - filer dataset

        Args:
            merged_contributor_filer_df: result of the merge between contributor and
                filer datasets

        Returns:
            A new dataframe with the appropriate column formatting for
            concatenation
        """
        # TO CLARIFY: no donor_party, donor_office in TX?
        new_cols = ["DONOR_ID", "DONOR_PARTY", "DONOR_OFFICE"]
        merged_contributor_filer_df = merged_contributor_filer_df.assign(
            **{col: None for col in new_cols}
        )
        # auto-fill the nan entries with 'Organization.'
        merged_contributor_filer_df["RECIPIENT_TYPE"].fillna(
            "Organization", inplace=True
        )
        columns = merged_contributor_filer_df.columns.to_list()
        columns.sort()
        merged_contributor_filer_df = merged_contributor_filer_df.loc[:, columns]
        return merged_contributor_filer_df

    def format_expense_filer_dataset(
        self, merged_expense_filer_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Reformats merged contributor - filer dataset

        Args:
            merged_expense_filer_df: result of the merge between contributor and
                filer datasets

        Returns:
            A new dataframe with the appropriate column formatting for
            concatenation
        """
        merged_expense_filer_df["RECIPIENT_TYPE"].fillna("Organization", inplace=True)

        merged_expense_filer_df = merged_expense_filer_df.rename(
            columns={
                # "RECIPIENT_x": "RECIPIENT",
                # "RECIPIENT_y": "DONOR",
                "RECIPIENT_TYPE": "DONOR_TYPE",
                "RECIPIENT_PARTY": "DONOR_PARTY",
                "RECIPIENT_OFFICE": "DONOR_OFFICE",
            }
        )
        merged_expense_filer_df = merged_expense_filer_df.reindex(
            sorted(merged_expense_filer_df.columns), axis=1
        )

        return merged_expense_filer_df

    def combine_contributor_expenditure_datasets(
        self,
        contrib_ds: list[pd.DataFrame],
        filer_ds: list[pd.DataFrame],
        expend_ds: list[pd.DataFrame],
    ) -> pd.DataFrame:
        """Merges together contrib, filer, and expense TX campaign finance data

        Args:
            contrib_ds: list of dataframes, with each entry in dataframes being a
                given file from a select year
            filer_ds: list of dataframes, with each entry in dataframes being a
                given file from a select year
            expend_ds: list of dataframes, with each entry in dataframes being a
                given file from a select year

        Returns:
            A concatenated dataframe with transaction information, contributor
            information, and recipient information.
        """
        merged_cont_datasets_per_yr = [
            self.merge_contributor_filer_datasets(cont, fil)
            for cont, fil in zip(contrib_ds, filer_ds)
        ]

        merged_exp_dataset_per_yr = [
            self.merge_expenditure_filer_datasets(exp, fil)
            for exp, fil in zip(expend_ds, filer_ds)
        ]

        contrib_filer_info = self.format_contributor_filer_dataset(
            pd.concat(merged_cont_datasets_per_yr)
        )
        expend_filer_info = self.format_expense_filer_dataset(
            pd.concat(merged_exp_dataset_per_yr)
        )
        concat_df = pd.concat([contrib_filer_info, expend_filer_info])
        concat_df = concat_df.reset_index(drop=True)
        return concat_df
