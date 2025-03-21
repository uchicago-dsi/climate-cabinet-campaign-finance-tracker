"""Representations of Pennsylvania data sources"""

from utils.finance.data_source_registry import register_data_source
from utils.finance.source import DataSourceStandardizationPipeline

# TODO: this is all determined form config file, make a default version
state_code = "pa"
form_codes = [
    "filer_through_2022",
    "filer_2023",
    "filer_post_2023",
    "contributions_through_2022",
    "contributions_2023",
    "contributions_post_2023",
    "expense_through_2022",
    "expense_2023",
    "expense_post_2023",
]

for standard_form_code in form_codes:
    register_data_source(
        state_code, DataSourceStandardizationPipeline(state_code, standard_form_code)
    )
