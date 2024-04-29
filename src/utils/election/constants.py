"""Constants to be used in building up election pipeline."""

import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
from utils.constants import BASE_FILEPATH

HV_FILEPATH = BASE_FILEPATH / "data" / "raw" / "HV" / "196slers1967to2016_20180908.dta"

INDIVIDUALS_FILEPATH = BASE_FILEPATH / "output" / "cleaned" / "individuals_table.csv"

HV_INDIVIDUAL_COLS = [
    "year",
    "month",
    "day",
    "sab",
    "cname",
    "ddez",
    "dname",
    "dno",
    "geopost",
    "mmdpost",
    "candid",
    "vote",
    "termz",
    "cand",
    "sen",
    "partyt",
    "dseats",
    "outcome",
    "last",
    "first",
    "v19_20171211",
]

election_settings = {
    "link_type": "dedupe_only",
    "comparison_columns": [
        {
            "col_name": "first_name",
            "data_type": "string",
            "case_expression": "lower(first_name)",
            "string_metric": "jarowinkler",
            "threshold": 0.85,
            "weight": 2
        },
        {
            "col_name": "last_name",
            "data_type": "string",
            "case_expression": "lower(last_name)",
            "string_metric": "jarowinkler",
            "threshold": 0.85,
            "weight": 2
        },
        {
            "col_name": "full_name",
            "data_type": "string",
            "case_expression": "lower(full_name)",
            "string_metric": "jarowinkler",
            "threshold": 0.85,
            "weight": 3
        },
        {
            "col_name": "state",
            "data_type": "string",
            "case_expression": "upper(state)",
            "string_metric": "levenshtein",
            "threshold": 0.95,
            "weight": 1
        },
        {
            "col_name": "party",
            "data_type": "string",
            "case_expression": "upper(party)",
            "string_metric": "levenshtein",
            "threshold": 0.95,
            "weight": 1
        }
    ],
    "additional_columns_to_retain": ["year", "district_number", "vote", "outcome", "unique_id"],
    "em_convergence": 0.01,
    "max_iterations": 10
}


election_blocking = [
    "l.first_name = r.first_name",
    "l.single_last_name = r.single_last_name",
]