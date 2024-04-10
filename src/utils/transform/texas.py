"""Implements state transformer class for Texas"""

import uuid
from pathlib import Path
from tkinter.tix import COLUMN

import pandas as pd

from utils.constants import BASE_FILEPATH
from utils.transform import clean
from utils.transform import constants as const


class TexasTransformer(clean.StateTransformer):
    """Texas state transformer implementation"""

    name = "Texas"
    stable_id_across_years = True
    TX_directory = BASE_FILEPATH / "data" / "raw" / "TX"/"sample"

    def clean_state(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Return tables of proper schema"""
        pre_processed_dfs = self.preprocess(self.TX_directory)
        clean_dfs = self.clean(pre_processed_dfs)
        standardized_dfs = self.standardize(clean_dfs)
        return self.create_tables(standardized_dfs)

    def preprocess(self, directory: str | Path = None) -> list[pd.DataFrame]:
        contributor_datasets, filer_datasets, expense_datasets = None,None,None
        if directory is None:
            directory = self.TX_directory
        else:
            directory = Path(directory)
        for file_path in directory.iterdir():
            file_name = file_path.name
            try:
                if file_name == 'contribs.csv':
                    contributor_df = pd.read_csv(file_path,low_memory=False)
                    # contributor_df = self.pre_process_contributor_dataset(contributor_df)
                    contributor_datasets= contributor_df
                if file_name == 'filers.csv':
                    filer_df = pd.read_csv(file_path,low_memory=False)
                    # filer_datasets.append(filer_df)
                    filer_datasets = filer_df
                elif file_name== "expend1.csv":
                    try:
                        expense_df = pd.read_csv(file_path,low_memory=False)
                        # expense_datasets.append(expense_df)
                        expense_datasets= expense_df
                    except Exception as e:
                        print(f"Error processing {file_name}: {e}")
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

        return contributor_datasets, filer_datasets,expense_datasets



    def clean(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:  # noqa: D102
        contributor_datasets, filer_datasets, expense_datasets = [], [], []
        cont_ds, filer_ds, exp_ds = data
        contributor_datasets.append(self.pre_process_contributor_dataset(cont_ds))
        filer_datasets.append(self.pre_process_filer_dataset(filer_ds))
        expense_datasets.append(self.pre_process_expense_dataset(exp_ds))

        # for contrib_df, filer_df, exp_df in zip(cont_ds, filer_ds, exp_ds):
        #     contributor_datasets.append(
        #         self.pre_process_contributor_dataset(contrib_df)
        #     )
        #     filer_datasets.append(self.pre_process_filer_dataset(filer_df))
        #     expense_datasets.append(self.pre_process_expense_dataset(exp_df))

        return contributor_datasets, filer_datasets, expense_datasets

    def standardize(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:  # noqa: D102
        contributor_ds, filer_ds, expense_ds = data
        merged_dataset = self.combine_contributor_expenditure_datasets(
            contributor_ds, filer_ds, expense_ds
        )
        #TO CLARIFY: do we need this?
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
        transactions_df.rename(
            columns={"recipient_office": "office_sought"}
        )

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
    
    #TODO: Remove redundant checks
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
    #Identifies whether an entity is likely an organization or individual

    def classify_contributor(self, PersentTypeCd: str) -> str:
        return "Individual" if PersentTypeCd.lower()=="individual" else "Organization"


    def pre_process_contributor_dataset(
        self, contributor_df: pd.DataFrame
    ) -> pd.DataFrame:
        contributor_df["RECIPIENT_TYPE"] = None
        contributor_df["RECIPIENT_TYPE"] = contributor_df["contributorPersentTypeCd"].apply(
            self.classify_contributor
        )

        contributor_df = contributor_df[const.TX_CONTRIBUTION_COLS]
        contributor_df["DONOR"] = contributor_df.apply(
            lambda row: row["contributorNameLast"] + row["contributorNameFirst"]
            if row["contributorPersentTypeCd"] == "INDIVIDUAL" else row["contributorNameOrganization"],
            axis=1,
        )
        contributor_df["YEAR"] = pd.to_datetime(contributor_df["contributionDt"]).dt.year        
        contributor_df["PURPOSE"] = pd.NA
        contributor_df.rename(
            columns=const.TX_CONTRIBUTION_MAPPING,inplace=True
        )
        contributor_df = contributor_df[const.PA_CONTRIBUTION_COLS]
        contributor_df["RECIPIENT_ID"] =contributor_df["RECIPIENT_ID"].astype("str")

        return contributor_df

    def pre_process_filer_dataset(self, filer_df: pd.DataFrame) -> pd.DataFrame:
        
        filer_df["RECIPIENT_ID"] = filer_df["filerIdent"].astype("str")
        filer_df = filer_df.drop_duplicates(subset=["RECIPIENT_ID"])
        filer_df["RECIPIENT_TYPE"] = filer_df.filerTypeCd.map(
            const.PA_FILER_ABBREV_DICT
        )
        filer_df["RECIPIENT"] = filer_df["filerName"].apply(lambda x: str(x).title())
        filer_df["RECIPIENT_OFFICE"] = pd.NA
        filer_df["RECIPIENT_PARTY"] = pd.NA
        filer_df = filer_df[const.PA_FILER_COLS]
        return filer_df

    def pre_process_expense_dataset(self, expense_df: pd.DataFrame) -> pd.DataFrame:
        # expense_df = expense_df[const.TX_EXPENSE_COLS]
        expense_df["RECIPIENT"]= expense_df.apply(
            lambda row: row["payeeNameLast"] + row["payeeNameFirst"] if row["payeePersentTypeCd"] == "INDIVIDUAL" else row["payeeNameOrganization"],axis=1)
        expense_df["DONOR_ID"] = expense_df["filerIdent"].astype("str")
        expense_df["YEAR"] = pd.to_datetime(expense_df["expendDt"]).dt.year
        expense_df["PURPOSE"] = pd.NA
        expense_df.rename(columns={"expendAmount": "AMOUNT"}, inplace=True)
        expense_df = expense_df[const.PA_EXPENSE_COLS]
        return expense_df

    #TO CLARIFY: do we need two methods for this?
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
        #TO CLARIFY: no donor in filer

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
        #TO CLARIFY: no donor_party, donor_office in TX?
        new_cols = ["DONOR_ID", "DONOR_PARTY", "DONOR_OFFICE"]
        merged_contributor_filer_df = merged_contributor_filer_df.assign(
            **{col: None for col in new_cols}
        )
        # auto-fill the nan entries with 'Organization.'
        merged_contributor_filer_df['RECIPIENT_TYPE'].fillna('Organization', inplace=True)
        columns = merged_contributor_filer_df.columns.to_list()
        columns.sort()
        merged_contributor_filer_df = merged_contributor_filer_df.loc[:, columns]
        return merged_contributor_filer_df

    # def format_expense_filer_dataset(
    #     self, merged_expense_filer_df: pd.DataFrame
    # ) -> pd.DataFrame:
    #     """Reformats merged contributor - filer dataset

    #     Args:
    #         merged_expense_filer_df: result of the merge between contributor and
    #             filer datasets

    #     Returns:
    #         A new dataframe with the appropriate column formatting for
    #         concatenation
    #     """
    #     merged_expense_filer_df["RECIPIENT_ID"] = None
    #     merged_expense_filer_df = merged_expense_filer_df.rename(
    #         columns={
    #             "RECIPIENT_x": "RECIPIENT",
    #             "RECIPIENT_y": "DONOR",
    #             "RECIPIENT_TYPE": "DONOR_TYPE",
    #             "RECIPIENT_PARTY": "DONOR_PARTY",
    #             "RECIPIENT_OFFICE": "DONOR_OFFICE",
    #         }
    #     )
    #     # because recipient information in the expenditure dataset is not
    #     # provided, for the sake of fitting the schema I code recipient_type as
    #     # 'Organization'.
    #     merged_expense_filer_df['RECIPIENT_TYPE'].fillna('Organization', inplace=True)

    #     # There are some donors whose entity_types isn't specified, so I
    #     # implement the same classify_contributor function used in the
    #     # contributors dataset
    #     na_free = merged_expense_filer_df.dropna(subset="DONOR_TYPE")
    #     only_na = merged_expense_filer_df[
    #         ~merged_expense_filer_df.index.isin(na_free.index)
    #     ]
    #     only_na["DONOR_TYPE"] = only_na["DONOR"].apply(self.classify_contributor)
    #     merged_expense_filer_df = pd.concat([na_free, only_na])

    #     columns = merged_expense_filer_df.columns.to_list()
    #     columns.sort()
    #     merged_expense_filer_df = merged_expense_filer_df.loc[:, columns]
    #     return merged_expense_filer_df



    def format_expense_filer_dataset(self,merged_expense_filer_df: pd.DataFrame) -> pd.DataFrame:
        """Reformats merged contributor - filer dataset

        Args:
            merged_expense_filer_df: result of the merge between contributor and
                filer datasets

        Returns:
            A new dataframe with the appropriate column formatting for
            concatenation
        """
        merged_expense_filer_df['RECIPIENT_TYPE'].fillna('Organization', inplace=True)

        merged_expense_filer_df= merged_expense_filer_df.rename(
            columns={
                "RECIPIENT_x": "RECIPIENT",
                "RECIPIENT_y": "DONOR",
                "RECIPIENT_TYPE": "DONOR_TYPE",
                "RECIPIENT_PARTY": "DONOR_PARTY",
                "RECIPIENT_OFFICE": "DONOR_OFFICE",
            }
        )
        merged_expense_filer_df = merged_expense_filer_df.reindex(sorted(merged_expense_filer_df.columns), axis=1)

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
