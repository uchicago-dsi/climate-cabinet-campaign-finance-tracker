"""Download and standardize election results from the Minnesota Secretary of State.

Results come from the Secretary of State's "media files", which are
semicolon-delimited text files without a header, published per election date
and office at https://electionresultsfiles.sos.mn.gov/<YYYYMMDD>/<file>.txt.
The layout of each row is described by RESULT_FILE_COLUMNS. The interactive
results site (electionresults.sos.mn.gov) sits behind a captcha, so elections
cannot be discovered automatically and are listed explicitly in ELECTIONS.
"""

import argparse
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

import pandas as pd
import requests

from utils.constants import DATA_DIR

BASE_URL = "https://electionresultsfiles.sos.mn.gov"
REQUEST_TIMEOUT = 30

RESULT_FILE_COLUMNS = [
    "state",
    "county_id",
    "precinct_name",
    "office_id",
    "office_name",
    "district",
    "candidate_order_code",
    "candidate_name",
    "suffix",
    "incumbent_code",
    "party",
    "precincts_reporting",
    "total_precincts",
    "votes",
    "vote_percentage",
    "total_votes_for_office",
]

# Maps media file name to the office_sought enum value in table.yaml
RESULT_FILES = {
    "stsenate.txt": "State Senator",
    "LegislativeByDistrict.txt": "State Representative",
    "Governor.txt": "Governor",
}

WRITE_IN_PARTY = "WI"


@dataclass(frozen=True)
class Election:
    """A single Minnesota election date with published result files."""

    date: str
    election_type: str

    @property
    def year(self) -> int:
        """Year the election took place."""
        return int(self.date[:4])


# Statewide primaries and generals with media files on electionresultsfiles.
# 2014 and earlier are not hosted there.
ELECTIONS = [
    Election("20160809", "primary"),
    Election("20161108", "general"),
    Election("20180814", "primary"),
    Election("20181106", "general"),
    Election("20200811", "primary"),
    Election("20201103", "general"),
    Election("20220809", "primary"),
    Election("20221108", "general"),
    Election("20240813", "primary"),
    Election("20241105", "general"),
]


def download_result_file(
    election: Election, file_name: str, output_directory: Path
) -> Path | None:
    """Download one results media file for an election.

    Args:
        election: Election to download results for.
        file_name: Name of the media file, e.g. "stsenate.txt".
        output_directory: Directory raw files are saved to. Files are saved
            as <output_directory>/<YYYYMMDD>/<file_name>.

    Returns:
        Path to the downloaded file, or None if the file does not exist for
        this election (for example, no State Senate races in 2018).
    """
    url = f"{BASE_URL}/{election.date}/{file_name}"
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    if response.status_code == HTTPStatus.NOT_FOUND:
        return None
    response.raise_for_status()
    if response.headers.get("Content-Type", "").startswith("text/html"):
        raise ValueError(f"Expected a results text file but got HTML from {url}")

    file_path = output_directory / election.date / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(response.content)
    return file_path


def parse_result_file(file_path: Path) -> pd.DataFrame:
    """Read a results media file into a DataFrame.

    Args:
        file_path: Path to a downloaded media file.

    Returns:
        DataFrame with one row per candidate per office, columns named by
        RESULT_FILE_COLUMNS.
    """
    results = pd.read_csv(
        file_path,
        sep=";",
        header=None,
        names=RESULT_FILE_COLUMNS,
        dtype=str,
        keep_default_na=False,
        encoding="latin-1",
    )
    for column in ["votes", "total_votes_for_office"]:
        results[column] = results[column].astype(int)
    results["vote_percentage"] = results["vote_percentage"].astype(float)
    return results


def standardize_results(
    results: pd.DataFrame, election: Election, office_sought: str
) -> pd.DataFrame:
    """Convert parsed media file rows to ElectionResult rows.

    Winners are the top vote getter in each race. In primaries each party
    has its own race for the same office, so winners are chosen per party.
    Aggregate write-in rows are dropped since they are not a candidate.

    Args:
        results: Output of parse_result_file.
        election: Election the results are from.
        office_sought: office_sought enum value for every race in the file.

    Returns:
        DataFrame of candidate results using the project's nested column
        naming, plus election date/type and vote share metadata.
    """
    results = results[results["party"] != WRITE_IN_PARTY].copy()

    race_columns = ["office_id"]
    if election.election_type == "primary":
        race_columns.append("party")
    max_votes = results.groupby(race_columns)["votes"].transform("max")
    results["win"] = results["votes"] == max_votes

    full_names = results["candidate_name"] + " " + results["suffix"]
    return pd.DataFrame(
        {
            "election--year": election.year,
            "election--date": pd.to_datetime(election.date, format="%Y%m%d"),
            "election--election_type": election.election_type,
            "election--office_sought": office_sought,
            "election--office_name": results["office_name"],
            "election--district": results["district"].replace("", pd.NA),
            "election--state": "MN",
            "candidate--full_name": full_names.str.strip(),
            "candidate--party": results["party"],
            "votes_received": results["votes"],
            "vote_share": results["vote_percentage"] / 100,
            "total_votes_for_office": results["total_votes_for_office"],
            "win": results["win"],
            "reported_state": "MN",
        }
    ).reset_index(drop=True)


def collect_election_results(
    elections: list[Election], output_directory: Path
) -> pd.DataFrame:
    """Download and standardize results for all offices in the given elections.

    Args:
        elections: Elections to collect.
        output_directory: Directory raw media files are saved to.

    Returns:
        Standardized results for every election and office found.
    """
    standardized = []
    for election in elections:
        for file_name, office_sought in RESULT_FILES.items():
            file_path = download_result_file(election, file_name, output_directory)
            if file_path is None:
                print(f"No {file_name} for {election.date}, skipping")
                continue
            results = parse_result_file(file_path)
            standardized.append(standardize_results(results, election, office_sought))
            print(f"Collected {len(results)} rows from {election.date}/{file_name}")
    return pd.concat(standardized, ignore_index=True)


def main(output_directory: Path | None = None) -> None:
    """Collect all Minnesota election results and save them as a csv.

    Args:
        output_directory: Directory for raw files and the combined csv.
            Defaults to DATA_DIR/raw/mn/elections.
    """
    if output_directory is None:
        output_directory = DATA_DIR / "raw" / "mn" / "elections"
    output_directory = Path(output_directory).resolve()

    election_results = collect_election_results(ELECTIONS, output_directory)
    output_path = output_directory / "ElectionResults.csv"
    election_results.to_csv(output_path, index=False)
    print(f"Saved {len(election_results)} candidate results to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_directory",
        type=str,
        default=None,
        help="Output directory for results. Defaults to DATA_DIR/raw/mn/elections",
    )
    args = parser.parse_args()
    main(args.output_directory)
