import zipfile
from io import BytesIO

import numpy as np
import requests
from bs4 import BeautifulSoup as BS

from utils import PA_constants as const


def make_request(website_url: str):
    """makes a HTTML request to the specified url, whose data is pulled out into
    a Beautiful Soup

    Args: string --> website url
    Returns: A parsed BeautifulSoup document
    """
    return BS(requests.get(website_url).text, "html.parser")


def download_PA_data(start_year: int, end_year: int):
    """downloads PA datasets from specified years to a local directory
    Args:
        The desired years
    Returns:
        .csv files"""

    years = np.arange(start_year, end_year + 1)
    for year in years:
        link = const.main_url + const.zipped_url + str(year) + ".zip"
        req = requests.get(link)

        zippedfiles = zipfile.ZipFile(BytesIO(req.content))
        zippedfiles.extractall("../data")