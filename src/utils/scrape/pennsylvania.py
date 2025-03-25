"""This modules provides functions to scrape Pennsylvannia campaign finance data"""

import zipfile
from http import HTTPStatus
from io import BytesIO
from pathlib import Path

import requests

from utils.constants import BASE_FILEPATH


def download_PA_data(
    start_year: int, end_year: int, output_directory: Path = None
) -> None:
    """Downloads PA datasets from specified years to a local directory

    Args:
        start_year: The first year in the range of desired years to extract data
        end_year: The last year in the range of desired years to extract data.
        output_directory: desired output location. Defaults to 'data/raw/PA'
    Modifies:
        Saves raw files from dos.pa.gov to output_directory with a separate directory
        for each year's files.
    """
    if output_directory is None:
        output_directory = BASE_FILEPATH / "data" / "raw" / "PA"

    else:
        output_directory = Path(output_directory).resolve()
    pa_url = "https://www.pa.gov/content/dam/copapwp-pagov/en/dos/resources/voting-and-elections/campaign-finance/campaign-finance-data/"  # noqa

    for year in range(start_year, end_year + 1):
        link = f"{pa_url}{year}.zip"

        response = requests.get(link, timeout=10)
        if response.status_code != HTTPStatus.OK:
            print(f"Pennsylvania data from {year} returned {response.reason}")

        year_directory = output_directory / str(year)
        year_directory.mkdir(exist_ok=True, parents=True)
        zippedfiles = zipfile.ZipFile(BytesIO(response.content))
        for zippedfile in zippedfiles.infolist():
            # some years have all contents in a single directory named after the
            # year by default
            if zippedfile.filename.startswith(f"{year}/"):
                zippedfiles.extract(zippedfile, output_directory)
            else:
                zippedfiles.extract(zippedfile, year_directory)


if __name__ == "__main__":
    download_PA_data(2024, 2024)
