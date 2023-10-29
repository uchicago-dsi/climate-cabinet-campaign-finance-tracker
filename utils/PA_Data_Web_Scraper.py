from bs4 import BeautifulSoup as BS
from io import BytesIO
from utils import PA_constants as const
import requests
import zipfile
import numpy as np


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
        .csv files
    """
    years = np.arange(start_year, end_year + 1)
    for year in years:
        link = const.main_url + const.zipped_url + str(year) + ".zip"
        req = requests.get(link)

        zippedfiles = zipfile.ZipFile(BytesIO(req.content))

        #only interested in contribution and filer datasets...
        for zippedfile in zippedfiles.namelist():
            if('contrib' in zippedfile) or ('filer' in zippedfile):
                zippedfiles.extract(zippedfile,"../data")