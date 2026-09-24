"""Registry of identifier systems that raw ids come from

Each raw id in the pipeline is namespaced by the id system ("source") that
issued it, so the same number from two id systems maps to two entities. The
valid sources and their formats are listed in sources.yaml.
"""

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from yaml import safe_load

from utils.constants import DEFAULT_SOURCES_PATH

LEGACY_SOURCE_SUFFIX = "_legacy"


@dataclass(frozen=True)
class Source:
    """An id system, such as a state agency's committee registration numbers"""

    name: str
    state: str | None = None
    agency: str | None = None
    description: str | None = None
    url: str | None = None
    pattern: re.Pattern | None = None
    null_values: frozenset[str] = field(default_factory=frozenset)
    case: str | None = None

    def normalize(self, source_id: str) -> str:
        """Put source_id in its canonical form (e.g. upper case) before use"""
        if self.case == "upper":
            return source_id.upper()
        if self.case == "lower":
            return source_id.lower()
        return source_id

    def is_null(self, source_id: str) -> bool:
        """Whether source_id is a placeholder that means the id is missing"""
        return source_id in self.null_values

    def matches(self, source_id: str) -> bool:
        """Whether source_id has this source's format"""
        return self.pattern is None or bool(self.pattern.fullmatch(source_id))


def legacy_source(reported_state: str | None) -> str:
    """Source for raw ids whose id system was not recorded

    Used for ids migrated from id_mapping.tsv and for id columns without a
    declared source, so both map to the same entities.
    """
    state = reported_state.lower() if isinstance(reported_state, str) else "unknown"
    return f"{state}{LEGACY_SOURCE_SUFFIX}"


def is_legacy_source(source_name: str) -> bool:
    """Whether source_name was created by legacy_source"""
    return source_name.endswith(LEGACY_SOURCE_SUFFIX)


class SourceRegistry:
    """The set of valid sources, loaded from a registry yaml file"""

    def __init__(self, registry_path: Path | str = DEFAULT_SOURCES_PATH) -> None:
        """Load a source registry

        Args:
            registry_path: yaml file mapping source names to their properties
        """
        with Path(registry_path).open() as f:
            raw_registry = safe_load(f) or {}
        self._sources = {
            name: Source(
                name=name,
                state=properties.get("state"),
                agency=properties.get("agency"),
                description=properties.get("description"),
                url=properties.get("url"),
                pattern=(
                    re.compile(properties["pattern"])
                    if properties.get("pattern")
                    else None
                ),
                null_values=frozenset(
                    str(value) for value in properties.get("null_values", [])
                ),
                case=properties.get("case"),
            )
            for name, properties in raw_registry.items()
        }

    def __contains__(self, source_name: str) -> bool:
        """Whether source_name is registered or a legacy source"""
        return source_name in self._sources or is_legacy_source(source_name)

    def __getitem__(self, source_name: str) -> Source:
        """Get a source by name. Legacy sources have no format restrictions."""
        if source_name in self._sources:
            return self._sources[source_name]
        if is_legacy_source(source_name):
            return Source(name=source_name)
        raise KeyError(
            f"Unknown id source '{source_name}'. Add it to sources.yaml. "
            f"Registered sources: {', '.join(sorted(self._sources))}"
        )

    @property
    def names(self) -> list[str]:
        """Names of all registered sources"""
        return list(self._sources)


@lru_cache
def get_default_registry() -> SourceRegistry:
    """Registry loaded from the default sources.yaml, loaded once"""
    return SourceRegistry(DEFAULT_SOURCES_PATH)
