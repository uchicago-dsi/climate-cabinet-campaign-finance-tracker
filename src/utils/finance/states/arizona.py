"""Representations of Pennsylvania data sources"""

import pandas as pd

from utils.finance.data_source_registry import register_data_source
from utils.finance.source import (
    ConfigHandler,
    DataSourceStandardizationPipeline,
    DataStandardizer,
)

state_code = "az"
form_codes = ["transactor", "transaction"]


class ArizonaTransactorsStandardizer(DataStandardizer):
    """Special Handling of Arizona Transactors

    Arizona 'OfficeName' contains office sought and district
    """

    def _standardize_office_names(
        self, standard_schema_table: pd.DataFrame
    ) -> pd.DataFrame:
        """AZ lists office names as office sought and district"""
        candidate_with_district_mask = standard_schema_table[
            "election_result--election--office_sought"
        ].str.contains(" - District")
        extracted_names = standard_schema_table.loc[
            candidate_with_district_mask, "election_result--election--office_sought"
        ].str.extract(r"^(?P<office_sought>.*) - District (No\.)? (?P<district>\d+)$")
        extracted_names = extracted_names.rename(
            columns={"office_sought": "election_result--election--office_sought"}
        )
        extracted_names.columns = [
            f"election_result--election--{col}" for col in extracted_names.columns
        ]
        standard_schema_table = standard_schema_table.merge(
            extracted_names, how="left", left_index=True, right_index=True
        )
        return standard_schema_table

    def standardize_data(  # noqa: ANN201, D102
        self,
        standard_schema_table,  # noqa: ANN001
        enum_mapper=None,  # noqa: ANN001
        column_to_date_format=None,  # noqa: ANN001
    ):
        standard_schema_table = self._standardize_office_names(standard_schema_table)
        return super().standardize_data(
            standard_schema_table, enum_mapper, column_to_date_format
        )


az_config_handler = ConfigHandler("transactor", state_code=state_code)
az_transactor_standardizer = ArizonaTransactorsStandardizer(az_config_handler)
register_data_source(
    state_code,
    DataSourceStandardizationPipeline(
        state_code=state_code,
        form_code="transactor",
        data_standardizer=az_transactor_standardizer,
    ),
)
register_data_source(
    state_code,
    DataSourceStandardizationPipeline(
        state_code=state_code,
        form_code="transaction",
    ),
)
