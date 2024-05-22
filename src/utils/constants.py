"""Constants to be used in various parts of the project."""

from pathlib import Path

import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl

BASE_FILEPATH = Path(__file__).resolve().parent.parent.parent
# returns the base_path to the directory

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
    "link_type": "link_only",
    "blocking_rules_to_generate_predictions": [
        "l.company_name[0] = r.company_name[0] AND l.zipcode = r.zipcode",
    ],
    "comparisons": [
        cl.jaro_winkler_at_thresholds(
            "company_name", [0.9, 0.6]
        ),  # threshold will catch typos and shortenings,
        cl.exact_match(
            "zipcode"
        ),  # want to get rid of this actually bc too many false positives
        cl.jaro_winkler_at_thresholds("address", [0.9, 0.6]),
    ],
}

# where left is the organizations table and right is the classified company table
organizations_blocking = "l.zipcode = r.zipcode"


FFF_company_classification_settings = {
    "link_type": "dedupe_only",
    "probability_two_random_records_match": 0.001,
    "blocking_rules_to_generate_predictions": [
        "l.company_name[0] = r.company_name[0]",
    ],
    "comparisons": [
        {
            "output_column_name": "company_name",
            "comparison_levels": [
                {
                    "sql_condition": "company_name_l IS NULL OR company_name_r IS NULL",
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                {
                    "sql_condition": "company_name_l = company_name_r",
                    "label_for_charts": "Exact match",
                    "tf_adjustment_column": "company_name",
                    "tf_minimum_u_value": 0.001,
                },
                {
                    "sql_condition": "jaro_winkler_similarity(company_name_l, company_name_r) > 0.5",
                    "label_for_charts": "similar",
                    "tf_adjustment_column": "company_name",
                    "tf_minimum_u_value": 0.003,
                    "tf_adjustment_weight": 0.5,
                },
                {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
            ],
        },
        {
            "output_column_name": "stock_symbol",
            "comparison_levels": [
                {
                    "sql_condition": "stock_symbol_l IS NULL OR stock_symbol_r IS NULL",
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                {
                    "sql_condition": "stock_symbol_l = stock_symbol_r",
                    "label_for_charts": "Exact match",
                    "tf_adjustment_column": "stock_symbol",
                    "tf_minimum_u_value": 0.001,
                },
                {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
            ],
        },
        {
            "output_column_name": "legal_company_name",
            "comparison_levels": [
                {
                    "sql_condition": "legal_name_l IS NULL OR legal_name_r IS NULL",
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                {
                    "sql_condition": "legal_name_l = legal_name_r",
                    "label_for_charts": "Exact match",
                    "tf_adjustment_column": "legal_name",
                    "tf_minimum_u_value": 0.001,
                },
                {
                    "sql_condition": "jaro_winkler_similarity(legal_name_l, legal_name_r) > 0.5",
                    "label_for_charts": "similar",
                    "tf_adjustment_column": "legal_name",
                    "tf_minimum_u_value": 0.003,
                    "tf_adjustment_weight": 0.5,
                },
                {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
            ],
        },
    ],
}

FFF_company_classification_blocking = "l.stock_symbol = r.stock_symbol"

InfoGroup_company_classification_settings = {
    "link_type": "dedupe_only",
    "probability_two_random_records_match": 0.0001,
    "blocking_rules_to_generate_predictions": [
        "l.company_name[0] = r.company_name[0] AND l.zipcode = r.zipcode AND l.primary_SIC_code = r.primary_SIC_code"
    ],
    "comparisons": [
        {
            "output_column_name": "company_name",
            "comparison_levels": [
                {
                    "sql_condition": "company_name_l IS NULL OR company_name_r IS NULL",
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                {
                    "sql_condition": "company_name_l = company_name_r",
                    "label_for_charts": "Exact match",
                    "tf_adjustment_column": "company_name",
                    "tf_minimum_u_value": 0.001,
                },
                {
                    "sql_condition": "jaro_winkler_similarity(company_name_l, company_name_r) > 0.5",
                    "label_for_charts": "similar",
                    "tf_adjustment_column": "company_name",
                    "tf_minimum_u_value": 0.003,
                    "tf_adjustment_weight": 0.5,
                },
                {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
            ],
        },
        {
            "output_column_name": "zipcode",
            "comparison_levels": [
                {
                    "sql_condition": "zipcode_l IS NULL OR zipcode_r IS NULL",
                    "label_for_charts": "Null",
                    "is_null_level": True,
                },
                {
                    "sql_condition": "zipcode_l = zipcode_r",
                    "label_for_charts": "Exact match",
                    "tf_adjustment_column": "zipcode",
                    "tf_minimum_u_value": 0.001,
                },
                {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
            ],
        },
    ],
}

InfoGroup_company_classification_blocking = "l.zipcode = r.zipcode"

# individuals compnay f names
f_companies = [
    "exxon",
    "chevron",
    "southwest gas",
    "petroleum",
    "koch industries",
    "koch companies",
    "oil & gas",
    "marathon oil",
    "shell oil",
]

# organizations f names
f_org_names = [
    "koch industries",
    "koch pac",
    "kochpac",
    "southwest gas az",
    "pinnacle west",
    "americans for prosperity",
    "energy transfer",
]

# organizations c names
c_org_names = [
    "clean energy",
    "vote solar action",
    "renewable",
    "pattern energy",
    "beyond carbon",
    "lcv victory",
    "league of conservation",
]

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

company_classification_output_schema = {
    "company_name": str,
    "stock_symbol": str,
    "legal_name": str,
    "address": str,
    "city": str,
    "state": str,
    "zipcode": str,
    "area_code": str,
    "primary_SIC_code": int,
    "SIC6_description": str,
    "SIC_code": float,
    "primary_NAICS_code": str,
    "NAICS8_description": str,
    "parent_company_name": str,
    "parent_company_ABI": int,
    "classification": str,
    "fossil_fuel_primary": bool,
    "clean_energy_primary": bool,
    "fossil_fuel_secondary": bool,
    "clean_energy_secondary": bool,
}

# columns of interest in the InfoGroup DataFrame
relevant_InfoGroup_columns = [
    "COMPANY",
    "ADDRESS LINE 1",
    "CITY",
    "STATE",
    "ZIPCODE",
    "AREA CODE",
    "PRIMARY SIC CODE",
    "SIC6_DESCRIPTIONS (SIC)",
    "PRIMARY NAICS CODE",
    "NAICS8 DESCRIPTIONS",
    "classification",
    "ABI",
    "PARENT NUMBER",
]

InfoGroup_column_mapper = {
    "COMPANY": "company_name",
    "ADDRESS LINE 1": "address",
    "CITY": "city",
    "STATE": "state",
    "ZIPCODE": "zipcode",
    "AREA CODE": "area_code",
    "PRIMARY SIC CODE": "primary_SIC_code",
    "SIC6_DESCRIPTIONS (SIC)": "SIC6_description",
    "PRIMARY NAICS CODE": "primary_NAICS_code",
    "NAICS8 DESCRIPTIONS": "NAICS8_description",
    "classification": "classification",
    "PARENT NUMBER": "parent_company_ABI",
    "ABI": "ABI",
}
