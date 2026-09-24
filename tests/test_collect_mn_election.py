from http import HTTPStatus
from pathlib import Path

import pytest
from utils.collect.election.minnesota import (
    Election,
    download_result_file,
    parse_result_file,
    standardize_results,
)

GENERAL_ROWS = [
    "MN;;;0121;State Senator District 1;1;0301;Mark Johnson;;;R;315;315;27320;97.05;28150",
    "MN;;;0121;State Senator District 1;1;9901;WRITE-IN;;;WI;315;315;830;2.95;28150",
    "MN;;;0123;State Senator District 3;3;0301;Andrea Zupancich;;;R;132;132;21349;49.15;43439",
    "MN;;;0123;State Senator District 3;3;0401;Grant Hauschild;;;DFL;132;132;22052;50.77;43439",
]

PRIMARY_ROWS = [
    "MN;;;0121;State Senator District 1;1;0301;Mark Johnson;;;R;315;315;6363;85.73;7422",
    "MN;;;0121;State Senator District 1;1;0302;Dave Hughes;;;R;315;315;1059;14.27;7422",
    "MN;;;0121;State Senator District 1;1;0401;Jane Doe;Jr.;;DFL;315;315;900;100.00;900",
]


@pytest.fixture
def write_result_file(tmp_path: Path):
    def _write(rows: list[str]) -> Path:
        file_path = tmp_path / "stsenate.txt"
        file_path.write_bytes("\r\n".join(rows).encode("latin-1"))
        return file_path

    return _write


def test_parse_result_file(write_result_file):
    results = parse_result_file(write_result_file(GENERAL_ROWS))
    assert len(results) == len(GENERAL_ROWS)
    assert results.loc[0, "candidate_name"] == "Mark Johnson"
    assert results.loc[0, "votes"] == 27320
    assert results.loc[0, "vote_percentage"] == 97.05


def test_parse_result_file_latin1(write_result_file):
    row = "MN;;;0188;State Representative District 1A;1A;0301;José Núñez;;;R;1;1;5;100.00;5"
    results = parse_result_file(write_result_file([row]))
    assert results.loc[0, "candidate_name"] == "José Núñez"


def test_standardize_general_picks_one_winner_per_race(write_result_file):
    results = parse_result_file(write_result_file(GENERAL_ROWS))
    standardized = standardize_results(
        results, Election("20221108", "general"), "State Senator"
    )
    assert "WRITE-IN" not in standardized["candidate--full_name"].to_list()
    winners = standardized[standardized["win"]]
    assert winners["candidate--full_name"].to_list() == [
        "Mark Johnson",
        "Grant Hauschild",
    ]
    assert (standardized["election--year"] == 2022).all()
    assert standardized.loc[0, "vote_share"] == pytest.approx(0.9705)


def test_standardize_primary_picks_winner_per_party(write_result_file):
    results = parse_result_file(write_result_file(PRIMARY_ROWS))
    standardized = standardize_results(
        results, Election("20220809", "primary"), "State Senator"
    )
    winners = standardized[standardized["win"]]
    assert winners["candidate--full_name"].to_list() == ["Mark Johnson", "Jane Doe Jr."]
    assert (standardized["election--election_type"] == "primary").all()


def test_download_result_file_missing_returns_none(mocker, tmp_path):
    response = mocker.Mock(status_code=HTTPStatus.NOT_FOUND)
    mocker.patch("utils.collect.election.minnesota.requests.get", return_value=response)
    assert (
        download_result_file(Election("20180814", "primary"), "stsenate.txt", tmp_path)
        is None
    )


def test_download_result_file_rejects_html(mocker, tmp_path):
    response = mocker.Mock(
        status_code=HTTPStatus.OK, headers={"Content-Type": "text/html"}
    )
    mocker.patch("utils.collect.election.minnesota.requests.get", return_value=response)
    with pytest.raises(ValueError, match="HTML"):
        download_result_file(Election("20221108", "general"), "stsenate.txt", tmp_path)


def test_download_result_file_saves_by_date(mocker, tmp_path):
    response = mocker.Mock(
        status_code=HTTPStatus.OK,
        headers={"Content-Type": "text/plain"},
        content=GENERAL_ROWS[0].encode(),
    )
    mocker.patch("utils.collect.election.minnesota.requests.get", return_value=response)
    file_path = download_result_file(
        Election("20221108", "general"), "stsenate.txt", tmp_path
    )
    assert file_path == tmp_path / "20221108" / "stsenate.txt"
    assert file_path.read_bytes() == GENERAL_ROWS[0].encode()
