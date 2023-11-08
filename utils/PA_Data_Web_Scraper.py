import zipfile
from io import BytesIO

import numpy as np
import requests
from bs4 import BeautifulSoup as BS

from utils import constants as const


def make_request(website_url: str) -> object:
    """makes a HTTML request to the specified url, whose data is pulled out into
    a Beautiful Soup

    Args:
        website_url: the url link to the campaign finance reports on PA's
        government website

    Returns: A parsed BeautifulSoup document
    """
    return BS(requests.get(website_url).text, "html.parser")


def download_PA_data(start_year: int, end_year: int):
    """downloads PA datasets from specified years to a local directory
    Args:
        start_year: The first year in the range of desired years to extract data

        end_year: The last year in the range of desired years to extract data.
    Returns:
        unzipped .txt files (that are really csvs) stored in the 'data'
        directory
    """

    years = np.arange(start_year, end_year + 1)
    for year in years:
        link = const.PA_MAIN_URL + const.PA_ZIPPED_URL + str(year) + ".zip"
        req = requests.get(link)

        zippedfiles = zipfile.ZipFile(BytesIO(req.content))
        zippedfiles.extractall("../data")
