"""
Scraping function for Arizona

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

Each entity type has pages that have transaction level details, starting with the 
digit that is *one more* than their page digit. (Candidate details start with 2, 
PAC details start with 3, etc.). The next digit represents one of the following,
always in the same order, but not always all present:
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


import pandas as pd
import requests
from utils.constants import (
    AZ_base_data,
    AZ_head,
    AZ_pages_dict,
    AZ_valid_detailed_pages,
    HEADERS,
)

"""
1 - Candidate info and their committees
2 - pacs
3 - political parties


"""


def scrape_and_download_az_data(start_year, end_year):
    """Collect and download all arizona data within range"""
    pages = []
    for page in AZ_pages_dict:
        page_data = scrape_az_page_data(page, start_year, end_year)
        pages.append(page_data)
    return pages


def scrape_az_page_data(
    page: str, start_year=2023, end_year=2023, *args, **kwargs: int
) -> pd.DataFrame:
    """Scrape data from arizona database at https://seethemoney.az.gov/

    This function retrieves and compiles the data from a given table
    from the arizona database, whether aggregate or detailed,
    within the given time period
    NOTE: Empty returns are to be expected for some inputs,
    as some tables are empty or near empty even for long spans of time.

    Args: page: the name of a basic or detailed page in the
    Arizona dataset, excluding the Name page.
    start_year: earliest year to include scraped data, inclusive
    end_year: last year to include scraped data, inclusive

    Returns: two pandas dataframes and two lists. The first dataframe
    contains the requested transactions data. The following two lists
    contain either the entity or committee name, and the final dataframe
    contains the information on those entities or committees
    """

    page = AZ_pages_dict[page]

    if page < 10:
        return scrape_wrapper(page, start_year, end_year)

    else:
        det = detailed_wrapper_director(page)
        agg_df = scrape_wrapper(det, start_year, end_year)
        entities = agg_df["EntityID"]

        return detailed_scrape_wrapper(entities, page, start_year, end_year)


def detailed_wrapper_director(page: int) -> int:
    """Direct az_wrapper to base page from detailed page

    This function takes as input the number of the page input to
    scrape_wrapper in the course of az_wrapper, and derives the
    number of the parent page which must be scraped for
    entities first

    Args: page: the two-digit page number belonging to a
    detailed page as shown in AZ_pages_dict.

    Returns: an integer representing the parent page
    """
    # if page not in AZ_valid_detailed_pages:
    #     raise ValueError("not a valid detailed page number")

    return int(str(page)[0]) - 1


def scrape_wrapper(page, start_year, end_year, *args: int) -> pd.DataFrame:
    """Create parameters and scrape an aggregate table

    This function is called by az_wrapper() to create the parameters and
    call the basic scraper for a certain basic page. To scrape the detailed
    pages, use detailed_scrape_wrapper() instead.

    Args: page: the one-digit number representing one of the eight
    basic pages in the arizona dataset, such as Candidates, PAC,
    Individual Contributions, etc. Refer to AZ_pages_dict
    start_year: earliest year to include scraped data, inclusive
    end_year: last year to include scraped data, inclusive

    Returns: a pandas dataframe containing the table data for
    the selected timeframe
    """

    params = parametrize(page, start_year, end_year)
    res = scrape(params, AZ_head, AZ_base_data)
    results = res.json()
    df = pd.DataFrame(data=results["data"])
    df = df.reset_index().drop(columns={"index"})

    return df


def get_keys_from_value(d, val):
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

    Returns: a pandas dataframe containing the table data for
    the selected timeframe
    """

    d_params = []
    info_params = []

    for entity in entities:
        ent = detailed_parametrize(entity, page, start_year, end_year)
        name_details = detailed_parametrize(entity, 11, start_year, end_year)

        d_params.append(ent)
        info_params.append(name_details)

    detail_dfs = []
    entity_names = []
    info_dfs = []

    entity_type_code = int(str(page)[0]) - 1

    entity_type = get_keys_from_value(AZ_pages_dict, entity_type_code)

    for d_param, entity in zip(d_params, entities):
        res = detailed_scrape(d_param)
        results = res.json()

        detail_df = pd.DataFrame(data=results["data"])
        detail_df["retrieved_id"] = entity
        detail_df["entity_type"] = entity_type

        detail_dfs.append(detail_df)

    for info_param in info_params:
        entity_name, info = info_scrape(info_param)
        info_table = info.json()
        entity_names.append(entity_name)  #
        info_dfs.append(pd.DataFrame(data=info_table)[["ReportFilerInfo"]])

    info_complete = info_process(
        pd.concat(info_dfs).reset_index().drop(columns={"index"})
    )

    info_complete["retrieved_name"] = entity_names

    info_complete["retrieved_id"] = entities

    info_complete["entity_type"] = entity_type

    return (
        pd.concat(detail_dfs).reset_index().drop(columns={"index"}),
        info_complete,
    )


def scrape(params: dict, headers: dict, data: dict) -> requests.models.Response:
    """Scrape a table from the main arizona site

    This function takes in the header and base provided
    elsewhere, and parameters generated by parametrize(),
    to locate and scrape data from one of the eight
    aggregate tables on the Arizona database.

    Args: params: created from parametrize(), containing
    the page, start and end years, table page, and table length.
    Note that 'page' encodes the page to be scraped, such as
    Candidates, IndividualContributions, etc. Refer to the
    attached Pages dictionary for details.
    headers: necessary for calling the response, provided above
    data: necessary for calling the response, provided above

    returns: request response containing aggregate information
    """

    return requests.post(
        "https://seethemoney.az.gov/Reporting/GetNEWTableData/",
        params=params,
        headers=HEADERS,
        data=AZ_base_data,
    )


def detailed_scrape(detailed_params: dict) -> requests.models.Response:
    """Scrape a sub-table from the arizona database

    This function takes an entity number, which can be
    gathered from an aggregate table using scrape() or
    inputted manually, and gathers that entity's detailed
    information within the specified time frame from
    one of the sub-tables.

    Args: detailed_params: created from detailed_parametrize(),
    containing the entity_id, page, start and end years, table page,
    and table length.
    Note that 'page' encodes the page to be scraped, such as
    Candidates, IndividualContributions, etc. Refer to the
    AZ_pages_dict dictionary for details.
    headers: necessary for calling the response, provided above
    data: necessary for calling the response, provided above

    returns: request response containing entity details
    """

    return requests.post(
        "https://seethemoney.az.gov/Reporting/GetNEWDetailedTableData/",
        params=detailed_params,
        headers=AZ_head,
        data=AZ_base_data,
    )


def parametrize(
    page=1,
    start_year=2023,
    end_year=2025,
    table_page=1,
    table_length=500000,
    **kwargs: int
) -> dict:
    """Input parameters for scrape and return as dict

    This function takes in parameters to scrape a
    given section of the arizona database, and turns
    them into a dictionary to be fed into scrape() as params

    Kwargs: page: encodes the page to be scraped, such as
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
    entity_id,
    page=1,
    start_year=2023,
    end_year=2025,
    table_page=1,
    table_length=500000,
    *args: int,
    **kwargs: int
) -> dict:
    """Input parameters for detailed_scrape and return as dict

    This function takes in a similar list of parameters as parametrize()
    and creates the params dictionary used by detailed_scrape.

    Args: entity_id: the unique id given to eahc entity in the
    arizona database
    Kwargs: page: encodes the page to be scraped, such as
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

    return {
        "CommitteeId": str(entity_id),
        "NameId": str(entity_id),
        "Page": str(page),  # refers to the overall page, like candidates
        # or individual expenditures
        "startYear": str(start_year),
        "endYear": str(end_year),
        "JurisdictionId": "0|Page",
        "TablePage": str(table_page),
        "TableLength": str(table_length),
        "Name": "1~" + str(entity_id),  # these two get used
        "entityId": str(entity_id),  # when scraping detailed data
        "ChartName": str(page),
        "IsLessActive": "false",
        "ShowOfficeHolder": "false",  # have yet to experiment with these
    }


def info_scrape(detailed_params: dict) -> requests.models.Response:
    """Scrape the name and filer information of individuals and organizations

    This function takes in detailed scrape parameters created by
    detailed_parametrize(), scrapes entity names and their
    detailed information, and returns both the list of entity
    names and the dataframe of entity details.

    args: detailed parameters dictionary

    returns: two request responses

    """

    entity_name_response = requests.get(
        "https://seethemoney.az.gov/Reporting/GetEntityName/",
        params=detailed_params,
        headers=AZ_head,
    ).json()

    if str(entity_name_response) == "  ":
        entity_name_response = None

    else:
        entity_name_response = entity_name_response.strip()

    info_response = requests.post(
        "https://seethemoney.az.gov/Reporting/GetDetailedInformation",
        params=detailed_params,
        headers=AZ_head,
    )

    return entity_name_response, info_response


def info_process(info_df: pd.DataFrame) -> pd.DataFrame:
    """processes detailed entity information

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
            lst.append(info_df[it : it + 20].values[i][0])
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
    # scrape_and_download_az_data(2020, 2023)
    parameters = detailed_parametrize(101502, 24, 2020, 2025)
    resp = detailed_scrape(parameters)
    x = 1
