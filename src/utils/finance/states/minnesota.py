"""Representations of Pennsylvania data sources"""

import pandas as pd

from utils.finance.data_source_registry import register_data_source
from utils.finance.source import (
    ConfigHandler,
    DataSourceStandardizationPipeline,
    DataStandardizer,
)

state_code = "mn"
form_codes = ["contributions", "expenditures"]


class MinnesotaContributionsStandardizer(DataStandardizer):
    """Special Handling of Minnesota Contributions

    Needed because Minnesota places multiple details about candidates
    in a single column in a very structured way. Separating them must
    be handled with custom code.
    """

    def _standardize_committee_names(
        self, standard_schema_table: pd.DataFrame
    ) -> pd.DataFrame:
        r"""MN lists candidate committees in a very specific format

        All Candidates are listed as:
            (Last Name), (First Name) (\(Preferred Name\))? (Middle Initial.)?
            (Office Sought) Committee
        """
        pattern = (
            r"^(?P<last_name>[^,]+),\s+"  # last name (everything before the comma)
            r"(?P<first_name>(?:[A-Za-z]+\s+)*[A-Za-z]+)"  # first name: one or more words (e.g., "Mary Jo")
            r'(?:\s+(?P<name_preferred>\([^)]*\)|"[^"]*"))?'  # optional preferred name in () or ""
            r"(?:\s+(?P<middle_name>[A-Z]{1,2}))?"  # optional middle name (one or two capital letters)
            r"(?:\s+(?P<name_suffix>(?:Jr\.?|Sr\.?|II|III|IV|V)))?"  # optional name suffix
            r"\s+(?P<office_sought>(?:Atty Gen|Sup Court|Dist Court|Sec of State|State Aud|App Court|Senate|House|Gov))"
            # office_sought must match one of the fixed options
            r"\s+(?P<committee>\S+)$"  # committee is the last token (no spaces)
        )
        candidate_mask = standard_schema_table["recipient--transactor_type"] == "PCC"
        extracted_names = standard_schema_table.loc[
            candidate_mask, "recipient--full_name"
        ].str.extract(pattern)
        extracted_names = extracted_names.rename(
            columns={"office_sought": "election_result--election--office_sought"}
        )
        extracted_names = extracted_names.drop(columns="committee")
        extracted_names.columns = [
            f"recipient--{col}" for col in extracted_names.columns
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
        standard_schema_table = self._standardize_committee_names(standard_schema_table)
        return super().standardize_data(
            standard_schema_table, enum_mapper, column_to_date_format
        )


mn_config_handler = ConfigHandler("contributions", state_code=state_code)
mn_contribution_standardizer = MinnesotaContributionsStandardizer(mn_config_handler)
register_data_source(
    state_code,
    DataSourceStandardizationPipeline(
        state_code=state_code,
        form_code="contributions",
        data_standardizer=mn_contribution_standardizer,
    ),
)
