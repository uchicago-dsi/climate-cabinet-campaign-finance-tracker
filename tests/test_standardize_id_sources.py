import pandas as pd
import pytest
from utils.standardize.config import ConfigHandler, declared_id_sources
from utils.standardize.source import SchemaTransformer

CONFIG_TEMPLATE = """
demo:
  state_code: zz
  table_name: Transaction
  path_pattern: "^demo\\\\.csv$"
  state_code_columns:
    - reported_state
  column_details:
    - raw_name: RegNum
      type: str
      standard_name: recipient_id
      id_source:
        - source: mn_cfb_unregistered
          when: '^-\\d+$'
        - source: mn_cfb_lobbyist
          when: '^\\d{{1,4}}$'
        - source: mn_cfb_registration
    - raw_name: FilerId
      type: str
      standard_name: donor_id
      id_source:
        - source: pa_dos_candidate_filer
          when: '^\\d{{1,6}}$'
          source_id_format: '{{reported_election_year}}-{{value}}'
        - source: pa_dos_filer
    - raw_name: Year
      type: Int16
      standard_name: reported_election_year
{extra_columns}
"""


def make_config(tmp_path, extra_columns=""):
    config_path = tmp_path / "zz.yaml"
    config_path.write_text(CONFIG_TEMPLATE.format(extra_columns=extra_columns))
    return config_path


@pytest.fixture
def transformer(tmp_path):
    return SchemaTransformer(
        ConfigHandler("demo", config_file_path=make_config(tmp_path))
    )


def test_id_source_rules_parsed(tmp_path):
    handler = ConfigHandler("demo", config_file_path=make_config(tmp_path))
    assert list(handler.id_source_rules) == ["recipient_id", "donor_id"]
    assert [rule.source for rule in handler.id_source_rules["recipient_id"]] == [
        "mn_cfb_unregistered",
        "mn_cfb_lobbyist",
        "mn_cfb_registration",
    ]


def test_assign_id_sources(transformer, capsys):
    raw_table = pd.DataFrame(
        {
            "RegNum": ["15677", "500", "-2139646094", "12345", "0", "123456789", None],
            "FilerId": ["8200022", "10014", "2011c0051", None, None, "bad id", None],
            "Year": pd.array([2010, 2011, 2012, 2013, 2014, 2015, 2016], dtype="Int16"),
        }
    )
    table = transformer.standardize_schema(raw_table)

    assert table["recipient_id"].to_list() == [
        "15677",
        "500",
        "-2139646094",
        pd.NA,  # placeholder
        pd.NA,  # placeholder
        pd.NA,  # matches no source's format
        pd.NA,
    ]
    assert table["recipient_id_source"].to_list() == [
        "mn_cfb_registration",
        "mn_cfb_lobbyist",
        "mn_cfb_unregistered",
        pd.NA,
        pd.NA,
        pd.NA,
        pd.NA,
    ]
    # short PA ids are reassigned across elections, so the year is part of the id
    assert table["donor_id"].to_list()[:3] == ["8200022", "2011-10014", "2011C0051"]
    assert table["donor_id_source"].to_list()[:3] == [
        "pa_dos_filer",
        "pa_dos_candidate_filer",
        "pa_dos_filer",
    ]
    assert pd.isna(table.loc[5, "donor_id"])

    output = capsys.readouterr().out
    assert "1 values in recipient_id" in output
    assert "1 values in donor_id" in output


def test_numeric_raw_ids_become_strings(tmp_path):
    config_path = make_config(tmp_path)
    config_path.write_text(
        config_path.read_text().replace(
            "raw_name: RegNum\n      type: str", "raw_name: RegNum\n      type: Int32"
        )
    )
    transformer = SchemaTransformer(ConfigHandler("demo", config_file_path=config_path))
    raw_table = pd.DataFrame(
        {
            "RegNum": pd.array([15677, 500], dtype="Int32"),
            "FilerId": [None, None],
            "Year": pd.array([2010, 2011], dtype="Int16"),
        }
    )
    table = transformer.standardize_schema(raw_table)
    assert table["recipient_id"].to_list() == ["15677", "500"]


def test_unregistered_source_fails_config(tmp_path):
    config_path = make_config(tmp_path)
    config_path.write_text(
        config_path.read_text().replace("source: pa_dos_filer", "source: not_a_source")
    )
    with pytest.raises(KeyError, match="not_a_source"):
        ConfigHandler("demo", config_file_path=config_path)


def test_unreachable_rule_fails_config(tmp_path):
    config_path = make_config(tmp_path)
    config_path.write_text(
        config_path.read_text().replace("          when: '^-\\d+$'\n", "")
    )
    with pytest.raises(ValueError, match="can never apply"):
        ConfigHandler("demo", config_file_path=config_path)


def test_two_id_columns_with_same_standard_name_fail_config(tmp_path):
    extra = """    - raw_name: OtherRegNum
      type: str
      standard_name: recipient_id
      id_source: mn_cfb_registration
"""
    with pytest.raises(ValueError, match="recipient_id"):
        ConfigHandler("demo", config_file_path=make_config(tmp_path, extra))


def test_id_source_without_standard_name_fails_config(tmp_path):
    extra = """    - raw_name: DroppedId
      type: str
      id_source: mn_cfb_registration
"""
    with pytest.raises(ValueError, match="DroppedId"):
        ConfigHandler("demo", config_file_path=make_config(tmp_path, extra))


def test_declared_id_sources(tmp_path):
    make_config(tmp_path)
    sources = [rule.source for rule in declared_id_sources("zz", tmp_path)]
    assert sources == [
        "mn_cfb_unregistered",
        "mn_cfb_lobbyist",
        "mn_cfb_registration",
        "pa_dos_candidate_filer",
        "pa_dos_filer",
    ]
