"""Representations of Michigan data sources"""

from utils.finance.data_source_registry import register_data_source
from utils.finance.source import DataSourceStandardizationPipeline

state_code = "mi"
form_codes = ["transaction"]

for standard_form_code in form_codes:
    register_data_source(
        state_code, DataSourceStandardizationPipeline(state_code, standard_form_code)
    )
