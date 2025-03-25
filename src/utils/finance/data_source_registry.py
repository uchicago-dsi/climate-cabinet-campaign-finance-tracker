"""Stores registry of all data source pipelines by state"""

import importlib
import pkgutil

from utils.finance.source import DataSourceStandardizationPipeline

DATA_SOURCE_REGISTRY: dict[str, list[DataSourceStandardizationPipeline]] = {}


def register_data_source(
    state: str, data_source: DataSourceStandardizationPipeline
) -> None:
    """Add data source pipeline to registry

    Args:
        state: two letter state abbreviation
        data_source: pipeline defining how to standardize data source
    """
    if state not in DATA_SOURCE_REGISTRY:
        DATA_SOURCE_REGISTRY[state] = []
    DATA_SOURCE_REGISTRY[state].append(data_source)


def get_registered_sources() -> dict[str, list[DataSourceStandardizationPipeline]]:
    """Retrieve dictionary mapping state abbreviations to their data source pipelines"""
    return DATA_SOURCE_REGISTRY


def load_state_modules(package_name: str = "utils.finance.states") -> None:
    """Dynamically loads all state modules to ensure they register their pipelines.

    Args:
        package_name: Location of where state pipeline modules are defined
    """
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        importlib.import_module(f"{package_name}.{module_name}")
