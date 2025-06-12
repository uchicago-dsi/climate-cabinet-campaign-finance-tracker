"""This module provides functions to scrape Michigan campaign finance data.

Data is retireved form the Legacy Downloads provided by the Michigan Secretary of State.
It is accessed from the following URL:
https://www.michigan.gov/sos/elections/disclosure/cfr/committee-search/intro/welcome-to-the-michigan-campaign-finance-searchable-database

For full historical data, you can search here:
https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page=page.miboeContributionPublicSearch.

This has known issues and does not allow for large downloads at this time. Michigan SOS
is working on it with status being updated here:
https://www.michigan.gov/sos/elections/disclosure/mitn-information

Previously data was available here:
https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/

This is no longer available without a login.
"""

import argparse
import io
import re
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urljoin

import py7zr
import requests
from bs4 import BeautifulSoup

from utils.constants import DATA_DIR


def download_MI_data(output_directory: Path = None) -> None:
    """Downloads Michigan legacy campaign finance datasets to a local directory

    Args:
        output_directory: desired output location. Defaults to 'data/raw/MI'
    Modifies:
        Saves extracted files from michigan.gov to output_directory with a separate
        directory for each year's files.
    """
    if output_directory is None:
        output_directory = DATA_DIR / "raw" / "MI" / "LegacyDownloads"
    else:
        output_directory = Path(output_directory).resolve()

    base_url = "https://www.michigan.gov"
    search_url = (
        "https://www.michigan.gov/sos/elections/disclosure/cfr/committee-search/"
        "intro/welcome-to-the-michigan-campaign-finance-searchable-database"
    )

    cookies = {
        "shell#lang": "en",
        "sxa_site": "sos",
        "browserChecked": "True",
        "sos#lang": "en",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.michigan.gov/en/sos/elections/Disclosure/cfr/committee-search",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Sec-GPC": "1",
    }

    try:
        # Get the page containing legacy data links
        response = requests.get(
            search_url, cookies=cookies, headers=headers, timeout=30
        )
        if response.status_code != HTTPStatus.OK:
            print(f"Failed to retrieve page: {response.status_code} {response.reason}")
            return

        # Parse HTML to find legacy data links
        soup = BeautifulSoup(response.text, "html.parser")
        legacy_links = []

        # Find all links that contain 'Legacy-Data' and end with '.7z'
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "Legacy-Data" in href and ".7z" in href:
                # Extract year from filename using regex
                year_match = re.search(r"(\d{4})_mi_cfr\.7z", href)
                if year_match:
                    year = year_match.group(1)
                    full_url = urljoin(base_url, href)
                    legacy_links.append((year, full_url))

        if not legacy_links:
            print("No legacy data links found on the page")
            return

        print(f"Found {len(legacy_links)} legacy data files")

        # Download and extract each file
        for year, url in sorted(legacy_links):
            print(f"Processing Michigan data for {year}...")

            try:
                # Download the 7z file
                file_response = requests.get(url, timeout=60, headers=headers)
                if file_response.status_code != HTTPStatus.OK:
                    print(f"Michigan data from {year} returned {file_response.reason}")
                    continue

                # Create year directory
                year_directory = output_directory / year
                year_directory.mkdir(exist_ok=True, parents=True)

                # Extract 7z archive directly from memory
                with py7zr.SevenZipFile(
                    io.BytesIO(file_response.content), mode="r"
                ) as archive:
                    archive.extractall(path=year_directory)

                print(f"Successfully extracted Michigan data for {year}")

            except Exception as e:
                print(f"Error processing {year} data: {e}")
                continue

        print(f"Michigan data download completed. Files saved to {output_directory}")

    except requests.RequestException as e:
        print(f"Network error occurred: {e}")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download Michigan campaign finance legacy data"
    )
    parser.add_argument(
        "--output_directory",
        type=str,
        default=None,
        help="Output directory for downloaded data. Defaults to DATA_DIR/raw/MI",
    )
    args = parser.parse_args()

    download_MI_data(args.output_directory)
