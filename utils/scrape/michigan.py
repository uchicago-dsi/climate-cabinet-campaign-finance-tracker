"""This modules provides functions to scrape Pennsylvannia campaign finance data"""
import datetime
import shutil
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup

from utils.constants import HEADERS, MAX_TIMEOUT, MI_CON_FILEPATH, MI_EXP_FILEPATH

MI_SOS_URL = "https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/"


def scrape_and_download_mi_data() -> None:
    """Scrapes and Downloads MI data

    Web scraper that navigates to the MI Secretary of State page and downloads
    the contribution and expenditure data and README
    """
    create_directory()
    year_lst = get_year_range()
    contribution_urls, expenditure_urls = capture_data(year_lst)

    for url in contribution_urls:
        make_request(url)
    print("Michigan Campaign Contribution Data Downloaded")

    for url in expenditure_urls:
        make_request(url)
    print("Michigan Campaign Expenditure Data Downloaded")


def get_year_range() -> list:
    """Returns year range for webscraper

    Inputs: None

    Returns: year_range (lst): Range of years to pull
    """
    current_year = datetime.now().year
    year_range = list(range(2018, current_year + 1))

    return year_range


def capture_data(year_lst: list) -> tuple[list, list]:
    """Makes a request and saves the urls directly to the MI  data

    Inputs: year_lst: list of years to capture data from

    Returns: (contribution_urls, expenditure_urls) (tuple): tuple with two
            lists of urls to MI data
    """
    contribution_urls = []
    expenditure_urls = []

    response = requests.get(MI_SOS_URL, headers=HEADERS, timeout=MAX_TIMEOUT)
    if response.status_code == HTTPStatus.OK:
        # create beautiful soup object to parse the table for contributions
        soup = BeautifulSoup(response.content, "html.parser")

        table = soup.find("table")

        for anchor in table.find_all("a"):
            anchor_text = anchor.get_text(strip=True)
            if "contributions" in anchor_text and any(
                str(year) in anchor_text for year in year_lst
            ):
                href = MI_SOS_URL + anchor["href"]
                contribution_urls.append(href)
            elif "expenditures" in anchor_text.lower() and any(
                str(year) in anchor_text for year in year_lst
            ):
                href = MI_SOS_URL + anchor["href"]
                expenditure_urls.append(href)
            else:
                continue

    else:
        # print the status code if the response failed to retrive the page
        print(f"Failed to retrieve page. Status code: {response.status_code}")
    return (contribution_urls, expenditure_urls)


def make_request(url: str) -> None:
    """Make a request and download the campaign contributions zip files

    Inputs: url (str): URL to the MI campaign zip file

    Returns: zip_file (io.BytesIO): An in-memory ZIP file as a BytesIO stream
    """
    response = requests.get(url, headers=HEADERS, timeout=MAX_TIMEOUT)

    if response.status_code == HTTPStatus.OK and "contribution" in url:
        zip_file = BytesIO(response.content)
        unzip_file(zip_file, MI_CON_FILEPATH)
    elif response.status_code == HTTPStatus.OK and "expenditure" in url:
        zip_file = BytesIO(response.content)
        unzip_file(zip_file, MI_EXP_FILEPATH)

    else:
        print(f"Failed to retrieve page. Status code: {response.status_code}")


def unzip_file(zip_file: BytesIO, directory: str) -> None:
    """Unzips the zip file and reads the file into the directory

    Inputs: zipfile (io.BytesIO): An in-memory ZIP file as a BytesIO stream
            directory (str): directory for the files to be saved

    Returns: None
    """
    with ZipFile(zip_file, "r") as zip_reference:
        file_name = zip_reference.namelist()[0]
        with zip_reference.open(file_name) as target_zip_file:
            content = target_zip_file.read()
            target_zip_file_path = Path(directory) / file_name
            with target_zip_file_path.open("wb") as f:
                f.write(content)

    print(f"Extracted and saved: {file_name}")


def create_directory() -> None:
    """Creates the directory for the MI contributions data

    Inputs: FILEPATH (str): filepath to the directory
    """
    FILEPATHS = [MI_CON_FILEPATH, MI_EXP_FILEPATH]

    for path in FILEPATHS:
        if path.exists():
            # remove existing MI campaign data
            shutil.rmtree(path)
            print(f"Deleted existing directory: {path}")

            path.mkdir(parents=True)
            print(f"Created directory: {path}")
        else:
            path.mkdir(parents=True)
            print(f"Created directory: {path}")


if __name__ == "__main__":
    scrape_and_download_mi_data()
