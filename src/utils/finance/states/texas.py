"""Representations of Texas data sources"""

import pandas as pd

from utils.constants import BASE_FILEPATH, source_metadata_directory
from utils.finance.source import DataSource


class TexasTransactionForm(DataSource):
    """Outlines general structure of Texas transaction tables (expenses + contributions)

    Texas source: https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt
    """

    stable_id_across_years = True

    table_type = "Transaction"

    @property
    def id_columns_to_standardize(self) -> dict:  # noqa D102
        return {
            "donor_id": [],
            "recipient_id": [],
        }

    enum_mapper = {
        "transactor_type": {
            "INDIVIDUAL": "Individual",
            "ENTITY": "Organization",
        }
    }

    def read_table(self, paths: list[str]) -> pd.DataFrame:  # noqa D102
        # Read and concatenate multiple tables (e.g., expense_01.csv, expense_02.csv)
        dtype_dict = self.column_details.set_index("raw_name")["type"].to_dict()
        self.table = pd.read_csv(paths[0], dtype=dtype_dict)
        return self.table


class TexasFilerForm(DataSource):
    """Methods for reading in data about Texan campaign finance filers"""

    table_type = "Transactor"

    default_raw_data_paths = [
        f
        for f in (BASE_FILEPATH / "data" / "raw" / "TX").iterdir()
        if f.name == "filers.csv"
    ]

    @property
    def id_columns_to_standardize(self) -> dict:  # noqa D102
        return {
            "id": [
                "election_result--candidate_id",
                "address--transactor_id",
                "employer--member_id",
            ],
            "election_result--election--id": [],
        }

    enum_mapper = {
        "transactor_type_specific": {
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
        },
        "transactor_type": {
            "INDIVIDUAL": "Individual",
            "ENTITY": "Organization",
        },
        "office_sought": {  # from https://www.ethics.state.tx.us/data/search/cf/CampaignFinanceCSVFileFormat.pdf
            "STATESEN": "State Senator",
            "STATEREP": "State Representative",
            "STATEEDU": "State Board of Education",
            "RRCOMM": "Railroad Commissioner",
            "RRCOMM_UNEXPIRED": "Railroad Commissioner",
            "PRESIDINGJUDGE_COCA": "Criminal Court of Appeals Presiding Judge",
            "PARTYCHAIRCO": "County Party Chair",
            "OTHER": "Other",
            "LTGOVERNOR": "Lieutenant Governor",
            "SOS": "Secretary of State",
            "LANDCOMM": "Land Commissioner",
            "JUDGEDIST": "District Judge",
            "JUSTICE_SC": "Supreme Court Justice",
            "JUSTICE_COA": "Court of Appeals Justice",
            "JUDGEDIST_MULTI": "District Judge",
            "JUDGE_COCA": "Criminal Court of Appeals Judge",
            "GOVERNOR": "Governor",
            "DISTATTY_MULTI_KL_KN": "District Attorney",
            "DISTATTY_MULTI": "District Attorney",
            "DISTATTY_HR": "District Attorney",
            "CRIMINAL_JUDGEDIST_TAR": "Criminal District Court Judge",
            "CRIMINAL_JUDGEDIST_DAL": "Criminal District Court Judge",
            "CRIMINAL_JUDGEDIST_JEF": "Criminal District Court Judge",
            "CRIMINAL_JUDGEDIST": "Criminal District Court Judge",
            "COMPTROLLER": "Comptroller",
            "COL_MULTI_1": "Judge",
            "COL_MULTI_2": "Judge",
            "CHIEFJUSTICE_SC": "Chief Justice of Supreme Court",
            "CHIEFJUSTICE_COA": "Chief Justice of Court of Appeals",
            "ATTYGEN": "Attorney General",
            "AGRICULTUR": "Agriculture Commissioner",
            "X": "Other",
        },
    }

    def _get_additional_columns(self) -> None:
        super()._get_additional_columns()
        # add TX as election state to rows that have election info
        election_columns = [
            col for col in self.table.columns if col.startswith("election_result--")
        ]
        election_info_mask = self.table[election_columns].notna().any(axis=1)
        self.table.loc[election_info_mask, "election_result--election--state"] = "TX"
        self.table.loc[:, "election_result--election--id"] = None
        # self.table["election_result--election--state"] = "TX"

    # @property
    # def enum_mapper(self) -> dict[str, dict]:
    #     """Unused as Texas Filer Form has enums depending on multiple columns."""
    #     return {}

    def read_table(self, paths: list[str]) -> pd.DataFrame:  # noqa D102
        # Read and concatenate multiple tables (e.g., expense_01.csv, expense_02.csv)
        dtype_dict = self.column_details.set_index("raw_name")["type"].to_dict()
        self.table = pd.read_csv(paths[0], dtype=dtype_dict)
        return self.table

    # def _standardize_enums(self) -> None:
    #     super()._standardize_enums()
    #     entity_types_specific = self.table["transactor_type_specific"].map(
    #         self.transactor_type_specific_mapper
    #         # axis=1,  # meta=("ENTITY_TYPE", str) DASK
    #     )
    #     entity_types_general = self.table["transactor_type_general"].map(
    #         self.transactor_general_type_mapper
    #         # axis=1,  # meta=("ENTITY_TYPE", str) DASK
    #     )
    #     specicic_row_type_predicted_general = np.where(
    #         entity_types_specific.isin(self.individual_entity_types),
    #         "Individual",
    #         "Organization",
    #     )
    #     self.table["transactor_type"] = np.where(
    #         specicic_row_type_predicted_general == entity_types_general,
    #         entity_types_specific,
    #         entity_types_general,
    #     )

    @property
    def column_details(self) -> pd.DataFrame:  # noqa D102
        column_details_df = pd.read_csv(
            source_metadata_directory / "TX" / "FilerData.csv"
        )
        return column_details_df


class TexasContributionForm(TexasTransactionForm):
    """Methods for reading in contributions made to filers"""

    default_raw_data_paths = [
        f
        for f in (BASE_FILEPATH / "data" / "raw" / "TX").iterdir()
        if f.name.startswith("contribs")
    ]

    @property
    def column_details(self) -> pd.DataFrame:  # noqa D102
        column_details_df = pd.read_csv(
            source_metadata_directory / "TX" / "ContribData.csv"
        )
        return column_details_df

    @property
    def id_columns_to_standardize(self) -> dict:  # noqa D102
        return {
            "recipient_id": [],
            # "donor--employer--organization--id": [],
        }

    # def _get_additional_columns(self) -> None:
    #     super()._get_additional_columns()
    #     self.table.loc[:, "donor--employer--organization--id"] = None


class TexasExpenseForm(TexasTransactionForm):
    """Methods for reading in contributions made by filers"""

    default_raw_data_paths = [
        f
        for f in (BASE_FILEPATH / "data" / "raw" / "TX").iterdir()
        if f.name.startswith("expend")
    ]

    @property
    def column_details(self) -> pd.DataFrame:  # noqa D102
        column_details_df = pd.read_csv(
            source_metadata_directory / "TX" / "ExpendData.csv"
        )
        return column_details_df

    @property
    def id_columns_to_standardize(self) -> dict:  # noqa D102
        return {
            "donor_id": [],
            # "recipient--employer--organization--id": [],
        }
