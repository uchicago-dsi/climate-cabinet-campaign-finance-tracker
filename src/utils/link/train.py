"""Code for training and running splink on dataset

Based on:
https://moj-analytical-services.github.io/splink/demos/tutorials/00_Tutorial_Introduction.html
"""
#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import duckdb
import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from utils.link.predict import load_linker


def create_random_individuals_subset(con: duckdb.DuckDBPyConnection, size: int) -> None:
    """Create a temp table that is a random subset of individuals"""
    con.execute(f"""
        CREATE TEMP TABLE random_subset AS
        SELECT *
        FROM transactor_detailed_view
        where transactor_type='Individual'
        USING SAMPLE {size} ROWS (RESERVOIR)
    """)


def train_splink(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    output_file: str,
    checkpoint_path: str | None = None,
    resume_from_checkpoint: bool = False,
) -> None:
    """Train splink linker and save to json

    Args:
        con: duckdb connection to database that has table table_name with arguments
            including first_name, last_name, address_city, address_street_name,
            name_suffix, name_prefix, employer_full_name, employer_role, phone_number
            and an id column with unique ids.
        table_name: name of a table in database
        output_file: where to save splink settings
        checkpoint_path: path to save/load checkpoint after first EM training
        resume_from_checkpoint: if True, load from checkpoint and skip initial training
    """
    checkpoint_exists = checkpoint_path and Path(checkpoint_path).exists()

    if resume_from_checkpoint and checkpoint_exists:
        print(f"Resuming from checkpoint: {checkpoint_path}")
        linker = load_linker(con, checkpoint_path, table_name)
    else:
        if resume_from_checkpoint and not checkpoint_exists:
            print(
                f"Warning: --resume-from-checkpoint specified but checkpoint not found at {checkpoint_path}"
            )
            print("Starting training from scratch")

        db_api = DuckDBAPI(con)
        comparisons = [
            cl.ForenameSurnameComparison("first_name", "last_name"),
            cl.NameComparison("address_city").configure(
                term_frequency_adjustments=True
            ),
            cl.NameComparison("address_street_name").configure(
                term_frequency_adjustments=True
            ),
            cl.NameComparison("name_suffix").configure(term_frequency_adjustments=True),
            cl.NameComparison("name_prefix").configure(term_frequency_adjustments=True),
            cl.NameComparison("employer_full_name").configure(
                term_frequency_adjustments=True
            ),
            cl.NameComparison("employer_role").configure(
                term_frequency_adjustments=True
            ),
        ]
        if (
            "phone_number"
            in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        ):
            comparisons.append(cl.LevenshteinAtThresholds("phone_number", 1))

        settings = SettingsCreator(
            link_type="dedupe_only",
            unique_id_column_name="id",
            comparisons=comparisons,
            blocking_rules_to_generate_predictions=[
                block_on("first_name", "last_name"),
                block_on("address_city", "address_street_name"),
            ],
            retain_intermediate_calculation_columns=False,
        )

        linker = Linker(table_name, settings, db_api=db_api)

        deterministic_rules = [
            block_on("first_name", "last_name", "address_city"),
            "jaro_winkler_similarity(l.first_name, r.first_name) >= 0.94 and l.last_name = r.last_name and l.address_street_name = r.address_street_name",
        ]

        linker.training.estimate_probability_two_random_records_match(
            deterministic_rules, recall=0.9
        )
        linker.training.estimate_u_using_random_sampling(max_pairs=1e8)
        training_blocking_rule = block_on("first_name", "last_name")

        linker.training.estimate_parameters_using_expectation_maximisation(
            training_blocking_rule
        )

        if checkpoint_path:
            linker.misc.save_model_to_json(checkpoint_path, overwrite=True)
            print(f"Saved checkpoint: {checkpoint_path}")

    training_blocking_rule_address = block_on("address_city", "address_street_name")
    linker.training.estimate_parameters_using_expectation_maximisation(
        training_blocking_rule_address
    )

    linker.misc.save_model_to_json(output_file, overwrite=True)
    print(f"Saved final model: {output_file}")


def create_splink_visualizations(linker: Linker) -> None:
    """Create and save splink visualizations for analysis"""
    linker.visualisations.match_weights_chart()
    linker.visualisations.m_u_parameters_chart()
    linker.visualisations.parameter_estimate_comparisons_chart()
    linker.evaluation.unlinkables_chart()


def create_linker_dashboard(
    linker: Linker, df_predictions: pd.DataFrame, output_file: str
) -> None:
    """Create and save splink comparison dashboard"""
    linker.visualisations.comparison_viewer_dashboard(
        df_predictions, output_file, overwrite=True
    )


def create_clustering_chart(linker: Linker, df_predictions: pd.DataFrame) -> None:
    """Create splink clustering chart"""
    df_clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predictions, threshold_match_probability=0.5
    )

    linker.visualisations.cluster_studio_dashboard(
        df_predictions,
        df_clusters,
        "cluster_studio.html",
        sampling_method="by_cluster_size",
        overwrite=True,
    )
