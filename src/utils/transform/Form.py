import abc

import pandas as pd

from utils.transform import constants as const


class Form(abc.ABC):
    def __init__(self, required_columns: list[str], column_mapper: dict):
        self.required_columns = required_columns
        self.column_mapper = column_mapper
        self.table = None

    @abc.abstractmethod
    def read_table(self, paths: list[str]) -> pd.DataFrame:
        """Read table(s) into a DataFrame"""
        pass

    def map_columns(self) -> None:
        """Map and filter columns of the DataFrame"""
        self.table.rename(columns=self.column_mapper, inplace=True)
        self.table = self.table[self.required_columns]
        return

    def get_table(self) -> pd.DataFrame:
        return self.table


class ContributionForm(Form):
    def __init__(self, column_mapper=None):
        required_columns = [
            "RECIPIENT_ID",
            "DONOR_ID",
            "DONOR",
            "AMOUNT",
            "YEAR",
            "PURPOSE",
            "DONOR_FIRST_NAME",
            "DONOR_LAST_NAME",
            "DONOR_ADDRESS_LINE_1",
            "DONOR_ZIP_CODE",
            "DONOR_EMPLOYER",
            "DONOR_OCCUPATION",
            "DONOR_TYPE",
        ]

        super().__init__(required_columns, column_mapper)

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., expense_01.csv, expense_02.csv)
        tables = [pd.read_csv(path) for path in paths]
        self.table = pd.concat(tables)
        print("Reading contribution table...")
        return self.table

    def map_columns(self) -> None:
        """Refine the mapping and selection of columns specific to Texas contributions."""
        # Call the parent implementation of map_columns which automatically handles the mapping and filtering based on 'column_mapper' and 'required_columns'
        super().map_columns()
        self.table["RECIPIENT_ID"] = self.table["RECIPIENT_ID"].astype("str")
        return


class ExpenseForm(Form):
    def __init__(self, column_mapper=None):
        required_columns = [
            "DONOR_ID",
            "RECIPIENT",
            "PURPOSE",
            "AMOUNT",
            "YEAR",
        ]

        super().__init__(required_columns, column_mapper)

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., expense_01.csv, expense_02.csv)
        tables = [pd.read_csv(path) for path in paths]
        self.table = pd.concat(tables)
        return self.table

    def map_columns(self) -> None:
        """Refine the mapping and selection of columns specific to Texas contributions."""
        # Call the parent implementation of map_columns which automatically handles the mapping and filtering based on 'column_mapper' and 'required_columns'
        super().map_columns()
        self.table["DONOR_ID"] = self.table["DONOR_ID"].astype("str")
        return


class FilerForm(Form):
    def __init__(self, column_mapper=None):
        required_columns = [
            "RECIPIENT_ID",
            "RECIPIENT_TYPE",
            "RECIPIENT",
            "RECIPIENT_OFFICE",
            "RECIPIENT_PARTY",
        ]

        super().__init__(required_columns, column_mapper)

    def read_table(self, paths: list[str]) -> pd.DataFrame:
        # Read and concatenate multiple tables (e.g., filer_01.csv, filer_02.csv)
        try:
            tables = [pd.read_csv(path) for path in paths]
            self.table = pd.concat(tables)
        except Exception as e:
            print(f"Error reading table: {e}")
        return self.table

    def map_columns(self) -> None:
        """Refine the mapping and selection of columns specific to Texas contributions."""
        super().map_columns()
        self.table["RECIPIENT_ID"] = self.table["RECIPIENT_ID"].astype("str")
        self.table.drop_duplicates(subset=["RECIPIENT_ID"], inplace=True)
        return


class TexasContributionForm(ContributionForm):
    def __init__(self):
        # column_mapper = {"filerIdent": "RECIPIENT_ID", "contributionAmount": "AMOUNT"}
        column_mapper = {
            "contributorNameFirst": "DONOR_FIRST_NAME",
            "contributorNameLast": "DONOR_LAST_NAME",
            "contributorStreetCity": "DONOR_ADDRESS_LINE_1",
            "contributorStreetPostalCode": "DONOR_ZIP_CODE",
            "contributorEmployer": "DONOR_EMPLOYER",
            "contributorOccupation": "DONOR_OCCUPATION",
            "filerIdent": "RECIPIENT_ID",
            "contributionAmount": "AMOUNT",
        }
        super().__init__(column_mapper)

    def type_classifier(self, PersentTypeCd: str) -> str:
        return "Individual" if PersentTypeCd.upper() == "INDIVIDUAL" else "Organization"

    def get_additional_columns(self) -> None:
        """Enhance and prepare the dataset for final output."""
        # self.table["RECIPIENT_TYPE"] = None
        # ##?
        # self.table["RECIPIENT_TYPE"] = self.table["contributorPersentTypeCd"].apply(
        #     self.type_classifier
        # )

        self.table["DONOR"] = self.table.apply(
            lambda row: f"{row['contributorNameLast']}, {row['contributorNameFirst']}"
            if "contributorPersentTypeCd" in row
            and row["contributorPersentTypeCd"] == "INDIVIDUAL"
            else row.get("contributorNameOrganization", ""),
            axis=1,
        )
        self.table["DONOR_TYPE"] = self.table["contributorPersentTypeCd"].apply(
            self.type_classifier
        )
        self.table["DONOR_ID"] = pd.NA
        self.table["YEAR"] = pd.to_datetime(self.table["contributionDt"]).dt.year
        self.table["PURPOSE"] = pd.NA
        return

    def preprocess_data(self) -> None:
        self.get_additional_columns()
        self.map_columns()
        return


class TexasFilerForm(FilerForm):
    def __init__(self):
        column_mapper = {
            "filerIdent": "RECIPIENT_ID",
            # "filerName": "RECIPIENT",
            # "filerTypeCd": "RECIPIENT_TYPE",
        }

        super().__init__(column_mapper=column_mapper)

    def get_additional_columns(self) -> None:
        self.table["RECIPIENT_TYPE"] = self.table.filerTypeCd.map(
            const.PA_FILER_ABBREV_DICT
        )
        self.table["RECIPIENT"] = self.table["filerName"].apply(
            lambda x: str(x).title()
        )
        self.table["RECIPIENT_OFFICE"] = pd.NA
        self.table["RECIPIENT_PARTY"] = pd.NA

    def preprocess_data(self) -> None:
        """Preprocess additional data if necessary."""
        self.get_additional_columns()
        self.map_columns()
        return


class TexasExpenseForm(ExpenseForm):
    def __init__(self):
        column_mapper = {"expendAmount": "AMOUNT", "filerIdent": "DONOR_ID"}
        super().__init__(column_mapper=column_mapper)

    def get_additional_columns(self) -> None:
        self.table["RECIPIENT"] = self.table.apply(
            lambda row: row["payeeNameLast"] + row["payeeNameFirst"]
            if row["payeePersentTypeCd"] == "INDIVIDUAL"
            else row["payeeNameOrganization"],
            axis=1,
        )
        self.table["YEAR"] = pd.to_datetime(self.table["expendDt"]).dt.year
        self.table["PURPOSE"] = pd.NA

    def preprocess_data(self) -> None:
        """Preprocess additional data if necessary."""
        self.get_additional_columns()
        self.map_columns()
        return
