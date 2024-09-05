"""Implements state transformer class for Texas"""

from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from utils.constants import BASE_FILEPATH
from utils.transform import Form, clean


class TexasTransformer(clean.StateTransformer):
    """Texas state transformer implementation"""

    def preprocess(self):
        pass

    name = "Texas"
    stable_id_across_years = True
    TX_directory = BASE_FILEPATH / "data" / "raw" / "TX"  # / "sample"

    def __init__(self):
        self.contributionFrom = Form.TexasContributionForm()
        self.filerForm = Form.TexasFilerForm()
        self.expenseForm = Form.TexasExpenseForm()
        self.forms = [self.filerForm, self.expenseForm, self.contributionFrom]

    def clean_state(
        self, directory=None
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Return tables of proper schema"""
        if directory is None:
            directory = self.TX_directory
        else:
            directory = Path(directory)

        contribs_files = [
            f for f in directory.iterdir() if f.name.startswith("contribs")
        ]
        expense_files = [f for f in directory.iterdir() if f.name.startswith("expend")]
        filer_files = [f for f in directory.iterdir() if f.name == "filers.csv"]

        self.filerForm.transform_data(filer_files)
        self.contributionFrom.transform_data(contribs_files)
        self.expenseForm.transform_data(expense_files)

        individual_tables = []
        organization_tables = []
        transaction_tables = []
        id_maps = []

        for form in self.forms:
            individual_tables.append(form.transformed_data["individuals"])
            organization_tables.append(form.transformed_data["organizations"])
            transaction_tables.append(form.transformed_data["transactions"])
            id_maps.append(form.uuid_lookup_table)

        return (
            dd.concat(individual_tables),
            dd.concat(organization_tables),
            dd.concat(transaction_tables),
            dd.concat(id_maps),
        )

    def clean(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        return super().clean(data)

    def create_tables(
        self, data: list[pd.DataFrame]
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        return super().create_tables(data)

    def standardize(self, data: list[pd.DataFrame]) -> list[pd.DataFrame]:
        return super().standardize(data)
