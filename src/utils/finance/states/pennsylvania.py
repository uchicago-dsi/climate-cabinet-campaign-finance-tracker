"""Representations of Pennsylvania data sources"""

import re
from pathlib import Path

import pandas as pd

from utils.constants import BASE_FILEPATH, source_metadata_directory
from utils.finance.source import DataSource


class PennsylvaniaForm(DataSource):
    @property
    def form_regex(self) -> str:
        """Raw regex that matches files of this form type"""
        return self._form_regex

    @property
    def form_years(self) -> str:
        """Years for which this form is valid"""
        return self._form_years

    @property
    def column_names(self) -> str:
        """List of column names in order for given form type"""
        return self._column_names

    @property
    def default_raw_data_paths(self) -> list[str | Path]:
        state_base_dir = BASE_FILEPATH / "data" / "raw" / "PA"
        matching_data_files = []
        for year_directory in state_base_dir.iterdir():
            if int(year_directory.name) not in self.form_years:
                continue
            for data_file in year_directory.iterdir():
                if re.match(self.form_regex, data_file.name):
                    matching_data_files.append(data_file)
        return matching_data_files

    enum_mapper = {}

    def read_table(self, paths: list[str]) -> pd.DataFrame:  # noqa D102
        dtype_dict = self.column_details.set_index("raw_name")[
            "type"
        ].to_dict()  # TODO: is dtype dict always the same TODO: error for nan types
        self.table = pd.read_csv(
            paths[0],
            names=self.column_names,
            sep=",",
            encoding="latin-1",
            on_bad_lines="warn",
            dtype=dtype_dict,
        )
        return self.table


class PennsylvaniaFilerPre2022Form(PennsylvaniaForm):
    """Outlines general structure of Pennsylvania filer form"""

    table_type = "Transactor"

    form_years = range(2010, 2022)

    enum_mapper = {
        "office_sought": {
            "STH": "State Representative",
            "STS": "State Senator",
            "GOV": "Governor",
            "LTG": "Lieutenant Governor",
            "DSC": "",
            "RSC": "",
            "USC": "",
        },
        "transactor_type": {
            "1": "Candidate",
            "2": "Committee",
            "3": "Unknown",
        },
    }

    @property
    def column_names(self) -> list[str]:
        return [
            "FILERID",
            "EYEAR",
            "CYCLE",
            "AMMEND",
            "TERMINATE",
            "FILERTYPE",  # integer?
            "FILERNAME",
            "OFFICE",
            "DISTRICT",
            "PARTY",
            "ADDRESS1",
            "ADDRESS2",
            "CITY",
            "STATE",
            "ZIPCODE",
            "COUNTY",
            "PHONE",
            "BEGINNING",
            "MONETARY",
            "INKIND",
        ]

    @property
    def form_regex(self) -> str:
        return "(?i)filer.*txt"

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

    def _get_additional_columns(self) -> None:
        super()._get_additional_columns()
        # add PA as election state to rows that have election info other than year
        election_columns = [
            col
            for col in self.table.columns
            if col.startswith("election_result--") and not col.endswith("year")
        ]
        election_info_mask = self.table[election_columns].notna().any(axis=1)
        self.table.loc[election_info_mask, "election_result--election--state"] = "PA"
        self.table.loc[:, "election_result--election--id"] = None


class PennsylvaniaFilerForm(PennsylvaniaFilerPre2022Form):
    form_years = range(2022, 2025)

    @property
    def column_names(self) -> list[str]:
        updated_columns = super().column_names
        return (
            updated_columns[0:1]
            + ["REPORTID"]
            + updated_columns[1:2]
            + ["TIMESTAMP"]
            + updated_columns[2:]
        )

    @property
    def column_details(self) -> pd.DataFrame:  # noqa D102
        column_details_df = pd.read_csv(source_metadata_directory / "PA" / "filer.csv")
        column_details_df = column_details_df[
            column_details_df["raw_name"].isin(self.column_names)
        ]
        return column_details_df


class PennsylvaniaContributionPre2022Form(PennsylvaniaForm):
    """Outlines general structure of Pennsylvania contribution form"""

    table_type = "Transaction"

    form_years = range(2010, 2022)

    @property
    def form_regex(self) -> str:
        return "(?i)contrib.*txt"

    enum_mapper = {
        "transactor_type": {
            "PAC": "Committee",
        },
    }

    @property
    def column_names(self) -> list[str]:
        return [
            "FILERID",
            "EYEAR",
            "CYCLE",
            "SECTION",
            "CONTRIBUTOR",
            "ADDRESS1",
            "ADDRESS2",
            "CITY",
            "STATE",
            "ZIPCODE",
            "OCCUPATION",
            "ENAME",
            "EADDRESS1",
            "EADDRESS2",
            "ECITY",
            "ESTATE",
            "EZIPCODE",
            "CONTDATE1",
            "CONTAMT1",
            "CONTDATE2",
            "CONTAMT2",
            "CONTDATE3",
            "CONTAMT3",
            "CONTDESC",
        ]

    @property
    def id_columns_to_standardize(self) -> dict:  # noqa D102
        return {
            "donor_id": [],
            "recipient_id": ["recipient--election_result--candidate_id"],
            "reported_election--id": ["recipient--election_result--election--id"],
        }

    @property
    def column_details(self) -> pd.DataFrame:  # noqa D102
        column_details_df = pd.read_csv(
            source_metadata_directory / "PA" / "contrib.csv"
        )
        column_details_df = column_details_df[
            column_details_df["raw_name"].isin(self.column_names)
        ]
        return column_details_df

    def _get_additional_columns(self) -> None:
        super()._get_additional_columns()

        self.table.loc[:, "recipient--election_result--election--year"] = (
            self.table.loc[:, "reported_election--year"]
        )
        self.table.loc[:, "reported_election--id"] = None
        self.table.loc[:, "recipient--election_result--candidate_id"] = None
        self.table.loc[:, "recipient--election_result--election--id"] = None


class PennsylvaniaContribution2023Form(PennsylvaniaContributionPre2022Form):
    form_years = range(2023, 2024)

    @property
    def column_names(self) -> list[str]:
        updated_columns = super().column_names
        return (
            updated_columns[0:1]
            + ["CampaignFinanceID"]
            + ["SubmittedDate"]
            + updated_columns[1:5]
            + ["FILERCODE"]
            + updated_columns[5:]
        )


class PennsylvaniaContributionForm(PennsylvaniaContributionPre2022Form):
    """Outlines current PA contribution form structure"""

    form_years = [2022, 2024]

    @property
    def column_names(self) -> list[str]:
        updated_columns = super().column_names
        return (
            updated_columns[0:1]
            + ["REPORTID"]
            + updated_columns[1:2]
            + ["TIMESTAMP"]
            + updated_columns[2:]
        )


class PennsylvaniaExpensePre2022Form(PennsylvaniaForm):
    """Outlines general structure of Pennsylvania expense form"""

    form_years = range(2010, 2022)

    @property
    def column_names(self) -> list[str]:
        return [
            "FILERID",
            "EYEAR",
            "CYCLE",
            "EXPNAME",
            "ADDRESS1",
            "ADDRESS2",
            "CITY",
            "STATE",
            "ZIPCODE",
            "EXPDATE",
            "EXPAMT",
            "EXPDESC",
        ]

    @property
    def form_regex(self) -> str:
        return "(?i)expens.*txt"

    table_type = "Transaction"

    @property
    def column_details(self) -> pd.DataFrame:  # noqa D102
        column_details_df = pd.read_csv(
            source_metadata_directory / "PA" / "expense.csv"
        )
        column_details_df = column_details_df[
            column_details_df["raw_name"].isin(self.column_names)
        ]
        return column_details_df

    @property
    def id_columns_to_standardize(self) -> dict:  # noqa D102
        return {
            "donor_id": ["donor--election_result--candidate_id"],
            "recipient_id": [],
            "reported_election--id": ["donor--election_result--election_id"],
        }

    def _get_additional_columns(self) -> None:
        super()._get_additional_columns()

        self.table.loc[:, "donor--election_result--election--year"] = self.table.loc[
            :, "reported_election--year"
        ]
        self.table.loc[:, "donor--election_result--candidate_id"] = None
        self.table.loc[:, "donor--election_result--election_id"] = None
        self.table.loc[:, "reported_election--id"] = None


class PennsylvaniaExpenseForm(PennsylvaniaExpensePre2022Form):
    """Outlines general structure of Pennsylvania expense form"""

    form_years = range(2022, 2025)

    @property
    def column_names(self) -> list[str]:
        updated_columns = super().column_names
        return (
            updated_columns[0:1]
            + ["REPORTID"]
            + updated_columns[1:2]
            + ["TIMESTAMP"]
            + updated_columns[2:]
        )
