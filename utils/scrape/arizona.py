"""This module provides functions to scrape Arizona Campaign Finance data.

Arizona has relational database with an endpoint `GetNEWTableData` that seems most
relevant and has what would usually be multiple different endpoints separated by
a `page` parameter. The page parameter has entities:
    1 - Candidate
    2 - PAC
    3 - Political Party
    4 - Organizations
    5 - Independent Expenditures
    6 - Ballot Measures
    7 - Individual Contributors
    8 - Vendors

That mainly just list the names of all entities and their IDs. Candidate usefully
includes a candidate committee name, which seems to be the conduit for all AZ
contributions.

Each entity type has pages in a GetNEWDetailedTableData endpoint that have transaction
level details, starting with the digit that is *one more* than their page digit.
(Candidate details start with 2, PAC details start with 3, etc.). The next digit
represents one of the following, always in the same order, but not always all present:
    Income
    Expense
    IEFor
    IEAgainst
    Ballot Measure Expenditure For
    Ballot Measure Expenditure Against
    All Transactions
So, to get candidate income, look at page 20, Political party all transactions is
42 because political parties don't have IE or Ballot Measure endpoints.

"""

from pathlib import Path
from typing import Any

import pandas as pd
import requests

from utils.constants import HEADERS, MAX_TIMEOUT, AZ_pages_dict, repo_root

BASE_URL = "https://seethemoney.az.gov/Reporting"
BASE_ENDPOINT = "GetNEWTableData"
DETAILED_ENDPOINT = "GetNEWDetailedTableData"
INFO_ENDPOINT = "GetDetailedInformation"
AZ_SEARCH_DATA = {
    "draw": "2",
    "order[0][column]": "0",
    "order[0][dir]": "asc",
    "start": "0",
    "length": "500000",
    "search[value]": "",
    "search[regex]": "false",
}
AZ_HEADER = HEADERS.update(
    {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.5",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://seethemoney.az.gov",
        "Connection": "keep-alive",
        "Referer": "https://seethemoney.az.gov/Reporting/Explore",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
)
BASIC_TYPE_PAGE = 10
NAME_INFO_PAGE = 11
MAX_DETAILED_PAGE = 20
AZ_valid_detailed_pages = [v for v in AZ_pages_dict.values() if v >= MAX_DETAILED_PAGE]
all_transactions_pages = [24, 36, 42, 54, 62, 72, 80, 90]


def scrape_and_download_az_data(
    start_year: int, end_year: int, output_directory: Path = None
) -> None:
    """Collect and download all arizona data within range"""
    if output_directory is None:
        output_directory = repo_root / "data" / "raw" / "AZ2"
        output_directory.mkdir(parents=True, exist_ok=True)
    for page in AZ_pages_dict:
        formatted_page = page.replace("/", "-").replace(" ", "-")
        if AZ_pages_dict[page] not in all_transactions_pages:
            continue
        transaction_data, filer_data = scrape_az_page_data(page, start_year, end_year)
        transaction_data.to_csv(output_directory / f"{formatted_page}-transactions.csv")
        filer_data.to_csv(output_directory / f"{formatted_page}-details.csv")


def scrape_az_page_data(
    page: str,
    start_year: int = 2023,
    end_year: int = 2023,
) -> pd.DataFrame:
    """Scrape data from arizona database at https://seethemoney.az.gov/

    This function retrieves and compiles the data from a given table
    from the arizona database, whether aggregate or detailed,
    within the given time period
    NOTE: Empty returns are to be expected for some inputs,
    as some tables are empty or near empty even for long spans of time.

    Args:
        page: the name of a basic or detailed page in the
            Arizona dataset, excluding the Name page.
        start_year: earliest year to include scraped data, inclusive
        end_year: last year to include scraped data, inclusive

    Returns: two pandas dataframes and two lists. The first dataframe
    contains the requested transactions data. The following two lists
    contain either the entity or committee name, and the final dataframe
    contains the information on those entities or committees
    """
    page = AZ_pages_dict[page]

    # page is a basic type, showing entity information
    if page < BASIC_TYPE_PAGE:
        return scrape_wrapper(page, start_year, end_year)
    elif page == NAME_INFO_PAGE:
        raise ValueError("'Name' endpoint unimplemented")
    # page is detailed, showing transaction information. Get relevant entity
    # information first and then get transaction details for each entity
    else:
        base_page = get_base_page_code(page)
        agg_df = scrape_wrapper(base_page, start_year, end_year)
        entities = agg_df["EntityID"]

        return detailed_scrape_wrapper(entities, page, start_year, end_year)


def get_base_page_code(page: int) -> int:
    """Get the base page code from a seethemoney.az.gov DetailedTable page code

    Args:
        page: 2-digit seethemoney GetNEWDetailedPageData page

    Returns: an integer representing the parent page
    """
    if page not in AZ_valid_detailed_pages:
        raise ValueError("not a valid detailed page number")
    return int(str(page)[0]) - 1


def scrape_wrapper(page: int, start_year: int, end_year: int) -> pd.DataFrame:
    """Create parameters and scrape an aggregate table

    This function is called by az_wrapper() to create the parameters and
    call the basic scraper for a certain basic page. To scrape the detailed
    pages, use detailed_scrape_wrapper() instead.

    Args:
        page: the one-digit number representing one of the eight
            basic pages in the arizona dataset, such as Candidates, PAC,
            Individual Contributions, etc. Refer to AZ_pages_dict
        start_year: earliest year to include scraped data, inclusive
        end_year: last year to include scraped data, inclusive

    Returns: a pandas dataframe containing the table data for
    the selected timeframe
    """
    if page < BASIC_TYPE_PAGE:
        raise ValueError(f"Page should be less than 10, was {page}")
    params = parametrize(page, start_year, end_year)
    res = scrape(BASE_ENDPOINT, params)
    results = res.json()
    raw_table = pd.DataFrame(data=results["data"])
    raw_table = raw_table.reset_index().drop(columns={"index"})

    return raw_table


def get_keys_from_value(d: dict, val: Any) -> str: # noqa ANN401
    """Returns first key from dict with value 'val'"""
    return [k for k, v in d.items() if v == val][0]


def detailed_scrape_wrapper(
    entities: pd.core.series.Series, page: int, start_year: int, end_year: int
) -> pd.DataFrame:
    """Create parameters and scrape an aggregate table

    This function is called by az_wrapper() to create the parameters and
    call the detailed scraper for a certain detailed page. To scrape the
    basic pages, use scrape_wrapper() instead.

    Args: page: the two-digit number representing a sub-page of
    one of the eight basic pages, such as Candidates/Income,
    PAC/All Transactions, etc. Refer to AZ_pages_dict
    start_year: earliest year to include scraped data, inclusive
    end_year: last year to include scraped data, inclusive

    Returns: 2 pandas dataframes with transaction information and filer information
    """
    max_per_entity_type = 10
    entities = entities[:max_per_entity_type]
    entity_detail_params = []
    info_params = []

    for entity in entities:
        entity_detailed_parameters = detailed_parametrize(
            entity, page, start_year, end_year
        )
        name_details = detailed_parametrize(
            entity, NAME_INFO_PAGE, start_year, end_year
        )

        entity_detail_params.append(entity_detailed_parameters)
        info_params.append(name_details)

    detail_dfs = []
    info_dfs = []

    entity_type_code = int(str(page)[0]) - 1

    entity_type = get_keys_from_value(AZ_pages_dict, entity_type_code)

    for d_param, entity in zip(entity_detail_params, entities):
        res = scrape(DETAILED_ENDPOINT, d_param)
        results = res.json()

        detail_df = pd.DataFrame(data=results["data"])
        detail_df["retrieved_id"] = entity
        detail_df["entity_type"] = entity_type

        detail_dfs.append(detail_df)
    for info_param in info_params:
        info = scrape(INFO_ENDPOINT, info_param)
        info_table = info.json()
        if info_table == "":
            continue
        info_dfs.append(pd.DataFrame(data=info_table)[["ReportFilerInfo"]])

    info_complete = info_process(
        pd.concat(info_dfs).reset_index().drop(columns={"index"})
    )
    info_complete["retrieved_id"] = entities
    info_complete["entity_type"] = entity_type
    return (
        pd.concat(detail_dfs).reset_index().drop(columns={"index"}),
        info_complete,
    )


def scrape(
    endpoint: str, params: dict, headers: dict = None, data: dict = None
) -> requests.models.Response:
    """Scrape a table from the main arizona site

    This function takes in the header and base provided
    elsewhere, and parameters generated by parametrize(),
    to locate and scrape data from one of the eight
    aggregate tables on the Arizona database.

    Args:
        endpoint: which of the seethemoney endpoints to call
            (either GetNEWTableData or GetNEWDetailedTableData)
        params: created from parametrize(), containing
            the page, start and end years, table page, and table length.
            Note that 'page' encodes the page to be scraped, such as
            Candidates, IndividualContributions, etc. Refer to the
            attached Pages dictionary for details.
        headers: headers for https post, standard defaults provided
        data: data for https post, defaults defined as constant

    returns: request response containing aggregate information
    """
    if headers is None:
        headers = AZ_HEADER
    if data is None:
        data = AZ_SEARCH_DATA

    return requests.post(
        f"{BASE_URL}/{endpoint}",
        params=params,
        headers=headers,
        data=data,
        timeout=MAX_TIMEOUT
    )


def parametrize(
    page: int = 1,
    start_year: int = 2023,
    end_year: int = 2025,
    table_page: int = 1,
    table_length: int = 500000,
) -> dict:
    """Input parameters for scrape and return as dict

    This function takes in parameters to scrape a
    given section of the arizona database, and turns
    them into a dictionary to be fed into scrape() as params

    Args:
        page: encodes the page to be scraped, such as
            Candidates, Individual Contributions, etc. Refer to the
            AZ_pages_dict dictionary for details.
        start_year: earliest year to include scraped data, inclusive
        end_year: last year to include scraped data, inclusive
        table_page: the numbered page to be accessed. Only necessary
            to iterate on this if accessing large quantities of Individual
            Contributions data, as all other data will be captured whole by
            the default table_length
        table_length: the length of the table to be scraped. The default
            setting should scrape the entirety of the desired data unless
            looking at Individual Contributions

    Returns: a dictionary of the parameters, to be fed into scrape()
    """
    return {
        "Page": str(page),  # refers to the overall page, like candidates
        # or individual expenditures
        "startYear": str(start_year),
        "endYear": str(end_year),
        "JurisdictionId": "0|Page",
        "TablePage": str(table_page),
        "TableLength": str(table_length),
        "ChartName": str(page),
        "IsLessActive": "false",
        "ShowOfficeHolder": "false",
    }


def detailed_parametrize(
    entity_id: str,
    page: int = 1,
    start_year: int = 2023,
    end_year: int = 2025,
    table_page: int = 1,
    table_length: int = 500000,
) -> dict:
    """Input parameters for detailed_scrape and return as dict

    This function takes in a similar list of parameters as parametrize()
    and creates the params dictionary used by detailed_scrape.

    Args:
        entity_id: the unique id given to eahc entity in the
        page: encodes the page to be scraped, such as
            Candidates, Individual Contributions, etc. Refer to the
            AZ_pages_dict dictionary for details.
        start_year: earliest year to include scraped data, inclusive
        end_year: last year to include scraped data, inclusive
        table_page: the numbered page to be accessed. Only necessary
            to iterate on this if accessing large quantities of Individual
            Contributions data, as all other data will be captured whole by
            the default table_length
        table_length: the length of the table to be scraped. The default
            setting should scrape the entirety of the desired data unless
            looking at Individual Contributions
    """
    default_parameters = parametrize(
        page, start_year, end_year, table_page, table_length
    )
    default_parameters.update(
        {
            "CommitteeId": str(entity_id),
            "NameId": str(entity_id),
            "Name": "1~" + str(entity_id),  # these two get used
            "entityId": str(entity_id),  # when scraping detailed data
        }
    )
    return default_parameters


def info_process(info_df: pd.DataFrame) -> pd.DataFrame:
    """Processes detailed entity information

    This function takes in the concatenated dataframes
    of detailed entity information, processes them, and
    returns them in a more readable and searchable form

    args: concatenation of info dataframes created from
    the info_scrape() response

    returns: reprocessed info dataframe
    """
    l2 = []
    for i in range(int(len(info_df) / 20)):
        it = 20 * i
        lst = []

        for i in range(20):
            lst.append(info_df[it : it + 20].to_numpy()[i][0])
        l2.append(lst)

    dat = pd.DataFrame(l2)
    dat.columns = [
        "candidate",
        "candidate_email",
        "candidate_phone",
        "chairman",
        "committee_address",
        "committee_name",
        "committee_type_name",
        "county_name",
        "designee",
        "email",
        "last_amended_date",
        "last_filed_date",
        "mailing_address",
        "master_committee_id",
        "office_name",
        "party_name",
        "phone_number",
        "registration_date",
        "status",
        "treasurer",
    ]
    return dat


if __name__ == "__main__":
    scrape_and_download_az_data(2022, 2022)
