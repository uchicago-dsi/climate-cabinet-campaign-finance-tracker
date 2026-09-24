from collections import Counter
from pathlib import Path

import pytest
import yaml
from utils.standardize.source import DataSourceStandardizationPipeline

# read configs from the repo so tests work when the package is not installed
# in editable mode (as in CI)
CONFIG_DIRECTORY = (
    Path(__file__).parents[1] / "src" / "utils" / "standardize" / "finance" / "config"
)
# synthetic raw files that mirror each state's real file layouts
RAW_FIXTURE_DIRECTORY = Path(__file__).parent / "data" / "standardize" / "raw"
ROWS_PER_FIXTURE_FILE = 3
STATES_WITH_FIXTURES = sorted(p.name for p in RAW_FIXTURE_DIRECTORY.iterdir())


def standard_forms(state):
    with (CONFIG_DIRECTORY / f"{state}.yaml").open() as f:
        config = yaml.safe_load(f)
    return [form for form, details in config.items() if not details.get("meta")]


@pytest.mark.parametrize("state", STATES_WITH_FIXTURES)
def test_each_fixture_file_matches_exactly_one_form(state):
    state_directory = RAW_FIXTURE_DIRECTORY / state
    matches = Counter()
    for form in standard_forms(state):
        pipeline = DataSourceStandardizationPipeline(
            state_code=state,
            form_code=form,
            config_file=CONFIG_DIRECTORY / f"{state}.yaml",
        )
        matches.update(pipeline._raw_data_file_paths(state_directory))
    fixture_files = [p for p in state_directory.rglob("*") if p.is_file()]
    assert {f: matches[f] for f in fixture_files} == {f: 1 for f in fixture_files}


@pytest.mark.parametrize(
    ("state", "form"),
    [(state, form) for state in STATES_WITH_FIXTURES for form in standard_forms(state)],
)
def test_form_loads_every_row(state, form):
    state_directory = RAW_FIXTURE_DIRECTORY / state
    pipeline = DataSourceStandardizationPipeline(
        state_code=state,
        form_code=form,
        config_file=CONFIG_DIRECTORY / f"{state}.yaml",
    )
    n_files = len(pipeline._raw_data_file_paths(state_directory))
    table = pipeline.load_and_standardize_data_source(
        state_data_directory=state_directory
    )
    assert len(table) == n_files * ROWS_PER_FIXTURE_FILE


ALL_STATES = sorted(p.stem for p in CONFIG_DIRECTORY.glob("*.yaml"))


def is_id_column(standard_name):
    return standard_name == "id" or standard_name.endswith("_id")


@pytest.mark.parametrize("state", ALL_STATES)
def test_every_id_column_declares_a_source(state):
    """Raw ids are only meaningful with the id system they come from"""
    with (CONFIG_DIRECTORY / f"{state}.yaml").open() as f:
        config = yaml.safe_load(f)
    missing = [
        (form, column["raw_name"])
        for form, details in config.items()
        for column in details.get("column_details", []) or []
        if is_id_column(column.get("standard_name", "")) and "id_source" not in column
    ]
    assert missing == []


@pytest.mark.parametrize(
    ("state", "form"),
    [(state, form) for state in STATES_WITH_FIXTURES for form in standard_forms(state)],
)
def test_fixture_ids_get_sources(state, form):
    state_directory = RAW_FIXTURE_DIRECTORY / state
    pipeline = DataSourceStandardizationPipeline(
        state_code=state,
        form_code=form,
        config_file=CONFIG_DIRECTORY / f"{state}.yaml",
    )
    table = pipeline.load_and_standardize_data_source(
        state_data_directory=state_directory
    )
    id_columns = [column for column in table.columns if is_id_column(column)]
    assert id_columns
    for id_column in id_columns:
        assert table[id_column].notna().all()
        assert table[f"{id_column}_source"].notna().all()
