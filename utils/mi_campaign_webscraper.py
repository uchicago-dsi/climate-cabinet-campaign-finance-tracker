import os
import shutil
from io import BytesIO
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup

from utils.constants import FILEPATH, URL


def scrape_and_download_mi_data():
    """
    Web scraper that navigates to the MI Secretary of State page and downloads
    the contribution data and README
    """
    create_directory()
    url_lst = capture_contributions_data()

    for urls in url_lst:
        url, file_name = urls
        full_file_path = url + file_name
        make_request(full_file_path, file_name)
    print("Michigan Campaign Data Downloaded")


def capture_contributions_data():
    """
    Makes a request and saves the urls directly to the MI contributions data

    Inputs: None

    Returns: url_lst (lst): list of urls to MI contributions data
    """
    url_lst = []

    response = requests.get(URL)

    if response.status_code == 200:
        # create beautiful soup object to parse the table for contributions
        soup = BeautifulSoup(response.content, "html.parser")

        table = soup.find("table")

        for anchor in table.find_all("a"):
            anchor_text = anchor.get_text(strip=True)
            if "contribution" in anchor_text.lower():
                href = URL + anchor["href"]
                url_lst.append((href, anchor_text))
    else:
        # print the status code if the response failed to retrive the page
        print(f"Failed to retrieve page. Status code: {response.status_code}")

    return url_lst


def make_request(url, file_name):
    """
    Make a request and download the campaign contributions zip files

    Inputs: url (str): URL to the contributions zip file

    Returns: zip_file (io.BytesIO): An in-memory ZIP file as a BytesIO stream
    """
    response = requests.get(url)
    if response.status_code == 200:
        zip_file = BytesIO(response.content)
        unzip_file(zip_file, file_name, FILEPATH)

    else:
        print(f"Failed to retrieve page. Status code: {response.status_code}")


def unzip_file(zip_file, file_name, directory):
    """
    Unzips the zip file and reads the file into the directory

    Inputs: zipfile (io.BytesIO): An in-memory ZIP file as a BytesIO stream
            file_name (str)
            directory (str): directory for the files to be saved
    """
    with ZipFile(zip_file, "r") as zip_reference:
        with zip_reference.open(file_name) as target_zip_file:
            content = target_zip_file.read()
            target_zip_file_path = os.path.join(directory, file_name)
            with open(target_zip_file_path, "wb") as f:
                f.write(content)

    print(f"Extracted and saved: {file_name}")


def create_directory():
    """
    Creates the directory for the MI contributions data
    """
    FILEPATH = "data/Contributions"
    if os.path.exists(FILEPATH):
        shutil.rmtree(FILEPATH)
        print(f"Deleted existing directory: {FILEPATH}")
    else:
        os.makedirs(FILEPATH)
        print(f"Created directory: {FILEPATH}")
