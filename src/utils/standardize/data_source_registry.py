"""Stores registry of all data source pipelines by state

For standard states (ones that only require yaml configuration),
they will be registered by the `register_all_standard_data_sources` function.

For special states (ones that require additional python configuration),
they will be registered by the `register_all_special_data_sources` function.
"""

import importlib
import pkgutil

import yaml

from utils.constants import STATE_FINANCE_CONFIG_DIRECTORY
from utils.standardize.source import DataSourceStandardizationPipeline

DATA_SOURCE_REGISTRY: dict[str, list[DataSourceStandardizationPipeline]] = {}


def register_data_source(
    state: str, data_source: DataSourceStandardizationPipeline
) -> None:
    """Add data source pipeline to registry

    If a data source pipeline with this form_code is already registered,
    it will be skipped.

    Args:
        state: two letter state abbreviation
        data_source: pipeline defining how to standardize data source
    """
    if state not in DATA_SOURCE_REGISTRY:
        DATA_SOURCE_REGISTRY[state] = []
    if data_source.form_code not in [
        pipeline.form_code for pipeline in DATA_SOURCE_REGISTRY[state]
    ]:
        DATA_SOURCE_REGISTRY[state].append(data_source)


def get_registered_sources() -> dict[str, list[DataSourceStandardizationPipeline]]:
    """Retrieve dictionary mapping state abbreviations to their data source pipelines"""
    return DATA_SOURCE_REGISTRY


def register_all_data_source_pipelines(
    states: list[str] = None,
) -> None:
    """Dynamically loads all state modules to ensure they register their pipelines.

    Args:
        states: list of state abbreviations to get data sources for.
            If not provided, all available states will be included.
    """
    register_all_special_data_sources(states)
    register_all_standard_data_sources(states)


def register_all_special_data_sources(
    states: list[str] = None,
) -> None:
    """Register all data sources pipelines requiring special configuration.

    Args:
        states: list of state abbreviations to get data sources for.
            If not provided, all available states will be included.
    """
    package_name = "utils.standardize.finance"
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        if states and module_name not in states:
            continue
        importlib.import_module(f"{package_name}.{module_name}")


def register_all_standard_data_sources(
    states: list[str] = None,
) -> list[DataSourceStandardizationPipeline]:
    """Register all data sources pipelines requiring no special configuration.

    Args:
        states: list of state abbreviations to get data sources for.
            If not provided, all available states will be included.
    """
    for state_configuration in STATE_FINANCE_CONFIG_DIRECTORY.iterdir():
        state = state_configuration.stem
        if states and state not in states:
            continue
        with state_configuration.open() as f:
            config = yaml.safe_load(f)
            for form_code in config.keys():
                if config[form_code].get("meta", False):
                    continue
                pipeline = DataSourceStandardizationPipeline(
                    state_code=state,
                    form_code=form_code,
                )
                register_data_source(state, pipeline)
