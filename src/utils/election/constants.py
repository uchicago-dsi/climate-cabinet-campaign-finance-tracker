"""Constants to be used in building up election pipeline."""

import numpy as np
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
from utils.constants import BASE_FILEPATH
import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl
from splink.duckdb.blocking_rule_library import block_on


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

type_mapping = {
    "year": "int",
    "month": "int",
    "day": "int", 
    "state": "string",
    "county": "string",
    "district_designation_ballot": "string",
    "district": "string",
    "district_number": "int",
    "geographic_post": "int",
    "mmd_post": "int",
    "candidate_id": "int",
    "vote": "int",
    "term": "float",
    "full_name": "string",
    "senate": "int",
    "party": "string",
    "district_seat_number": "int",
    "outcome": "string",
    "last_name": "string",
    "first_name": "string",
}
party_map = {"d": "democratic", "r": "republican", "partymiss": np.nan}

settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        block_on(["first_name", "last_name", "year","month","county"]),
    ],
    "comparisons": [
        ctl.name_comparison("first_name", term_frequency_adjustments=True),
        ctl.name_comparison("last_name", term_frequency_adjustments=True),
        cl.exact_match("year", term_frequency_adjustments=True),
        cl.exact_match("month",  term_frequency_adjustments=True),
        cl.exact_match("county",  term_frequency_adjustments=True),
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "max_iterations": 10,
    "em_convergence": 0.01
}
blocking = [
    "l.first_name = r.first_name and l.last_name = r.last_name and l.county = r.county and l.year = r.year and l.month = r.month"
]