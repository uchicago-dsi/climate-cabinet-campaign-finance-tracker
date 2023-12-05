import zipfile
from io import BytesIO

import numpy as np
import requests

from utils import constants as const


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
        link = f"{const.PA_MAIN_URL}{const.PA_ZIPPED_URL}{year}.zip"

        req = requests.get(link)

        zippedfiles = zipfile.ZipFile(BytesIO(req.content))
        for zippedfile in zippedfiles.infolist():
            zippedfile.filename = zippedfile.filename.replace(
                ".txt", "_" + str(year) + ".txt"
            )
            zippedfiles.extract(zippedfile, "../data/raw/PA")


def main():
    """"""
    text = input(
        "Provide a range of desired years to extract data. Format is \
                 year1, year2. Ex: 2018, 2023"
    )
    years = text.split(",")
    download_PA_data(years[0], years[1])


if __name__ == "__main__":
    main()
