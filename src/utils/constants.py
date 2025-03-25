"""Constants to be used in various parts of the project."""

from pathlib import Path

import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl

BASE_FILEPATH = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIRECTORY = BASE_FILEPATH / "data" / "raw"
# returns the base_path to the directory

source_metadata_directory = BASE_FILEPATH / "src" / "utils" / "static"

COMPANY_TYPES = {
    "CORP": "CORPORATION",
    "CO": "CORPORATION",
    "LLC": "LIMITED LIABILITY COMPANY",
    "PTNR": "PARTNERSHIP",
    "LP": "LIMITED PARTNERSHIP",
    "LLP": "LIMITED LIABILITY PARTNERSHIP",
    "SOLE PROP": "SOLE PROPRIETORSHIP",
    "SP": "SOLE PROPRIETORSHIP",
    "NPO": "NONPROFIT ORGANIZATION",
    "PC": "PROFESSIONAL CORPORATION",
    "CO-OP": "COOPERATIVE",
    "LTD": "LIMITED COMPANY",
    "JSC": "JOINT STOCK COMPANY",
    "HOLDCO": "HOLDING COMPANY",
    "PLC": "PUBLIC LIMITED COMPANY",
    "PVT LTD": "PRIVATE LIMITED COMPANY",
    "INC": "INCORPORATED",
    "ASSOC": "ASSOCIATION",
    "FDN": "FOUNDATION",
    "TR": "TRUST",
    "SOC": "SOCIETY",
    "CONSORT": "CONSORTIUM",
    "SYND": "SYNDICATE",
    "GRP": "GROUP",
    "CORP SOLE": "CORPORATION SOLE",
    "JV": "JOINT VENTURE",
    "SUB": "SUBSIDIARY",
    "FRANCHISE": "FRANCHISE",
    "PA": "PROFESSIONAL ASSOCIATION",
    "CIC": "COMMUNITY INTEREST COMPANY",
    "PAC": "POLITICAL ACTION COMMITTEE",
}

individuals_settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.first_name = r.first_name",
        "l.last_name = r.last_name",
    ],
    "comparisons": [
        ctl.name_comparison("full_name"),
        cl.exact_match("entity_type", term_frequency_adjustments=True),
        cl.jaro_winkler_at_thresholds(
            "state", [0.9, 0.8]
        ),  # threshold will catch typos and shortenings
        cl.jaro_winkler_at_thresholds("party", [0.9, 0.8]),
        cl.jaro_winkler_at_thresholds("company", [0.9, 0.8]),
    ],
    # DEFAULT
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "max_iterations": 10,
    "em_convergence": 0.01,
}

individuals_blocking = [
    "l.first_name = r.first_name",
    "l.last_name = r.last_name",
]

organizations_settings = {
    "link_type": "dedupe_only",
    "blocking_rules_to_generate_predictions": [
        "l.name = r.name",
    ],
    "comparisons": [
        cl.exact_match("entity_type", term_frequency_adjustments=True),
        cl.jaro_winkler_at_thresholds(
            "state", [0.9, 0.8]
        ),  # threshold will catch typos and shortenings
        # Add more comparisons as needed
    ],
    "retain_matching_columns": True,
    "retain_intermediate_calculation_columns": True,
    "max_iterations": 10,
    "em_convergence": 0.01,
}

organizations_blocking = ["l.name = r.name"]

suffixes = [
    "sr",
    "jr",
    "i",
    "ii",
    "iii",
    "iv",
    "v",
    "vi",
    "vii",
    "viii",
    "ix",
    "x",
]

titles = [
    "mr",
    "ms",
    "mrs",
    "miss",
    "prof",
    "dr",
    "doctor",
    "sir",
    "madam",
    "professor",
]
