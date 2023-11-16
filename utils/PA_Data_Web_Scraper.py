import zipfile
from io import BytesIO
from pathlib import Path

import numpy as np
import requests
from bs4 import BeautifulSoup as BS

from utils import constants as const

# resolve to the path of the current python file
curr_python_file = Path(__file__).resolve()
# repo_root will be the absolute path to the root of the repository,
# no matter where the repository is.
# For pathlib Path objects, `.parent` gets the parent directory and `/`
# can be used like `/` in unix style paths.
repo_root = curr_python_file.parent.parent


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
        for zippedfile in zippedfiles.infolist():
            zippedfile.filename = zippedfile.filename.replace(
                ".txt", "_" + str(year) + ".txt"
            )
            zippedfiles.extract(zippedfile, "../data")
