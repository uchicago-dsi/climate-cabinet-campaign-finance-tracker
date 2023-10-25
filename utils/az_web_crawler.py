import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument("--headless=new")


# note, right now only does the first 1000 rows, create an iterator
def az_individual_base_scraper(
    # fmt: off
        start_year=2023, end_year=2023, t=5, *args, **kwargs: int
):
    # fmt: on
    """Scrape Arizona individual donors table

    This function takes a link to the main page of the
    arizona individual donors section, scrapes the table
    within a certain span of years, and returns it as a
    pandas dataframe while also returning the links

    Kwargs: start_year: the earliest year for which to scrape data (inclusive)
    end_year: the last year for which to scrape data (inclusive)
    t: time in seconds to wait for the page to load, defaults to 5

    Returns: a dataframe of all columns and relevant rows,
    and a list of links leading to the detailed transactions pages
    for each entity. The links within can be processed by
    arizona_individual_sub_scraper
    """

    url = (
        """https://seethemoney.az.gov/Reporting/Explore#JurisdictionId
    =0%7CPage|Page=7|startYear="""
        + str(start_year)
        + """|end
    Year="""
        + str(end_year)
        + """|IsLessActive=false|ShowOfficeHolder=
    false|View=Detail|TablePage=1|TableLength=1000"""
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    driver.get(url)

    time.sleep(t)

    data = []
    links = []
    # fmt: off
    rows = driver.find_elements(By.XPATH,
                                '//tbody[@id="IndividualInformationData"]/tr')
    # fmt: on
    for row in rows:
        name = row.find_element(By.XPATH, ".//td[1]").text
        number = row.find_element(By.XPATH, ".//td[2]/a").text
        data.append([name, number])

        link_element = row.find_element(By.XPATH, ".//td[2]/a")
        link = link_element.get_attribute("href")
        links.append(link)

    df = pd.DataFrame(data, columns=["Name", "Number"])

    driver.quit()

    return df, links


def az_individual_sub_scraper(
    url,
    table_page=1,
    start_year=2023,
    end_year=2023,
    t=5,
    table_length=100,
    *args: str,
    **kwargs: int,
):
    """Scrape detailed transactions for individual contributors

    This function takes a link in to an amount in
    the arizona individual donors table and scrapes
    the table found at that link, within the selected time frame,
    and turns them into a pandas dataframe

    Args: url: the link to the transactions page. Can be
    provided by arizona_individual_base_scraper

    Kwargs: table_page: the numbered page of the tabular results.
    This is manipulated by arizona_individual_sub_scraper_iterator
    start_year: the earliest year for which to scrape data (inclusive)
    end_year: the last year for which to scrape data (inclusive)
    t: time in seconds to wait for the page to load, defaults to 5
    table_length: the length of the table called for each page

    Returns: a pandas dataframe of all transactions for that entity
    within the specified time frame
    The amount of time spent waiting for the page to load is determined by t
    Note that time t may need to be adjusted upwards in the
    event of empty results
    """

    url = re.sub(r"startYear=\d+", f"startYear={start_year}", url)
    url = re.sub(r"endYear=\d+", f"endYear={end_year}", url)
    url = re.sub(r"TableLength=\d+", f"TableLength={table_length}", url)
    url = re.sub(r"TablePage=\d+", f"TablePage={table_page}")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    driver.get(url)

    time.sleep(t)

    table_element = driver.find_element(By.ID, "IndividualAmountTable")

    table_html = table_element.get_attribute("outerHTML")

    df = pd.read_html(table_html)[0].drop(columns={"Unnamed: 0"})

    driver.quit()

    print(url)

    return df


def az_individual_sub_scraper_iterator(
    url,
    start_year=2023,
    end_year=2023,
    t=5,
    table_length=100,
    start_page=1,
    end_page=1,
    *args: str,
    **kwargs: int,
):
    """Iterate the sub scraper through many pages

    This function iterates arizona_individual_sub_scraper through multiple pages
    sourced from a single url, and returns the outputs as a list

    Args: url: the link to the transactions page. Can be
    provided by arizona_individual_base_scraper

    Kwargs: start_year: the earliest year for which to scrape data (inclusive)
    end_year: the last year for which to scrape data (inclusive)
    t: time in seconds to wait for the page to load, defaults to 5
    table_length: the length of the table called for each page
    start_page: the page one wishes to begin scraping (inclusive)
    end_page: the page one wishes to finish scraping (inclusive)
    """
    res = []
    for i in range(start_page, end_page + 1):
        res.append(
            # fmt: off
            az_individual_sub_scraper(
                url, i, start_year, end_year, t, table_length)
            # fmt: on
        )
