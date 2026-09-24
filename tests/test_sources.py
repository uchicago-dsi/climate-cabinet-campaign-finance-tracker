import pytest
from utils.sources import (
    SourceRegistry,
    get_default_registry,
    is_legacy_source,
    legacy_source,
)


@pytest.fixture
def registry():
    return get_default_registry()


def test_legacy_source():
    assert legacy_source("MN") == "mn_legacy"
    assert legacy_source(None) == "unknown_legacy"
    assert is_legacy_source("mn_legacy")
    assert not is_legacy_source("mn_cfb_registration")


def test_unknown_source_raises(registry):
    assert "not_a_source" not in registry
    with pytest.raises(KeyError, match="not_a_source"):
        registry["not_a_source"]


def test_legacy_sources_are_known_and_unrestricted(registry):
    assert "az_legacy" in registry
    assert registry["az_legacy"].matches("anything at all")


def test_registry_from_file(tmp_path):
    registry_path = tmp_path / "sources.yaml"
    registry_path.write_text(
        "demo_source:\n  state: zz\n  pattern: '^\\d+$'\n  null_values: [0]\n"
    )
    registry = SourceRegistry(registry_path)
    assert registry.names == ["demo_source"]
    assert registry["demo_source"].matches("123")
    assert not registry["demo_source"].matches("12a")
    assert registry["demo_source"].is_null("0")


# Formats observed in the raw data for each id system
@pytest.mark.parametrize(
    ("source", "source_id", "valid"),
    [
        ("mn_cfb_registration", "15677", True),
        ("mn_cfb_registration", "500", False),
        ("mn_cfb_lobbyist", "500", True),
        ("mn_cfb_unregistered", "-2139646094", True),
        ("az_sos_committee", "1001", True),
        ("az_sos_committee", "201200487", True),
        ("az_sos_name", "-7", True),
        ("az_sos_name", "3007195", True),
        ("pa_dos_filer", "8200022", True),
        ("pa_dos_filer", "20110236", True),
        ("pa_dos_filer", "2011C0051", True),
        ("pa_dos_filer", "10014", False),
        ("pa_dos_candidate_filer", "2011-10014", True),
        ("pa_dos_candidate_filer", "-10014", False),
        ("tx_ethics_filer", "00015654", True),
        ("tx_ethics_filer", "someone@example.com", False),
        ("mi_sos_committee", "508821", True),
        ("fec_committee", "C00236489", True),
        ("fec_committee", "C000311043", False),
        ("fec_committee", "0.00", False),
        ("fec_committee", "11/2/2016", False),
        ("mn_sos_candidacy", "20221108-0255-0401", True),
    ],
)
def test_registered_source_patterns(registry, source, source_id, valid):
    assert registry[source].matches(source_id) == valid


def test_case_insensitive_source_ids_are_normalized(registry):
    source = registry["pa_dos_filer"]
    assert source.normalize("2012c1014") == "2012C1014"
    assert source.matches(source.normalize("2012c1014"))
    assert registry["tx_ethics_filer"].normalize("00015654") == "00015654"


@pytest.mark.parametrize(
    ("source", "placeholder"),
    [("mn_cfb_registration", "12345"), ("mn_cfb_lobbyist", "0")],
)
def test_registered_null_values(registry, source, placeholder):
    assert registry[source].is_null(placeholder)
