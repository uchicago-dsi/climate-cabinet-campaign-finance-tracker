"""Scripts to scrape Arizona Campaign Finance data"""

import datetime
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils.constants import DATA_DIR

BASE_URL = "https://seethemoney.az.gov/Reporting/AdvancedSearch/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.5",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://seethemoney.az.gov",
    "Referer": "https://seethemoney.az.gov/Reporting/AdvancedSearch/",
    "Connection": "keep-alive",
}

# Category mappings
CATEGORY_TYPES = {
    "contributions": "Income",
    "expenditures": "Expenditures",
    "independent_expenditures": "IndependentExpenditures",
    "ballot_measures": "BallotMeasures",
}

# Filer type mappings
FILER_TYPES = {
    "candidate": "130",
    "committee": "131",
    "party": "132",
    "office_holder": "96",
}

TOO_MANY_REQUESTS = 429


def _create_session() -> requests.Session:
    """Create and initialize a requests session with proper headers and cookies."""
    session = requests.Session()
    session.headers.update(HEADERS)
    # Make initial GET request to establish session and get cookies
    session.get(BASE_URL, timeout=30)
    return session


def _simulate_form_interaction(session: requests.Session) -> None:
    """Simulate form interaction to establish proper session state.

    This may be required before POST requests will return data.
    """
    # Make a search request with minimal parameters to establish session state
    form_data = {
        "JurisdictionId": "0",
        "CommiteeReportId": "",
        "CategoryType": "Income",
        "CycleId": "",
        "StartDate": "",
        "EndDate": "",
        "FilerName": "",
        "FilerId": "",
        "BallotName": "",
        "BallotMeasureId": "",
        "FilerTypeId": "130",  # candidate
        "OfficeTypeId": "",
        "OfficeId": "",
        "PartyId": "",
        "ContributorName": "",
        "VendorName": "",
        "StateId": "",
        "City": "",
        "Employer": "",
        "Occupation": "",
        "CandidateName": "",
        "CandidateFilerId": "",
        "Position": "Support",
        "LowAmount": "",
        "HighAmount": "",
    }

    # Make a POST request to simulate form submission
    session.post(BASE_URL, data=form_data, timeout=30)


def get_available_election_cycles(
    session: requests.Session | None = None,
) -> dict[str, dict[str, str]]:
    """Get available election cycles from the Arizona SeeTheMoney website.

    Args:
        session: Optional requests session to use. If None, creates a new one.

    Returns:
        Dictionary mapping cycle names to cycle info containing 'id' and 'date_range'
    """
    if session is None:
        session = _create_session()

    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    cycle_select = soup.find("select", {"id": "CycleId"})

    if not cycle_select:
        raise ValueError("Could not find CycleId select element on page")

    cycles = {}
    for option in cycle_select.find_all("option"):
        value = option.get("value", "").strip()
        text = option.get_text().strip()

        if value and text and text != "Please Select an Election Cycle":
            # Parse the cycle ID to extract date range
            start_date, end_date = _extract_date_range_from_cycle_id(value)
            cycles[text] = {"id": value, "start_date": start_date, "end_date": end_date}

    return cycles


def get_all_available_filer_types(
    session: requests.Session | None = None,
) -> dict[str, str]:
    """Get all available filer types from the Arizona SeeTheMoney website.

    Args:
        session: Optional requests session to use. If None, creates a new one.
    """
    if session is None:
        session = _create_session()

    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    filer_type_select = soup.find("select", {"id": "FilerTypeId"})

    if not filer_type_select:
        raise ValueError("Could not find FilerTypeId select element on page")

    filer_types = {}
    for option in filer_type_select.find_all("option"):
        value = option.get("value", "").strip()
        # skip blank value because this is the 'All' option and we must specify
        # at least one option to filter by
        if not value:
            continue
        text = option.get_text().strip()
        filer_types[text] = value

    return filer_types


def _extract_date_range_from_cycle_id(cycle_id: str) -> tuple[str, str]:
    """Extract start and end dates from a cycle ID string.

    Args:
        cycle_id: Cycle ID in format like "44~1/1/2025 12:00:00 AM~12/31/2026 11:59:59 PM"

    Returns:
        Tuple of start and end dates in YYYY-MM-DD format
    """
    expected_tilde_sections = 2
    if cycle_id.count("~") != expected_tilde_sections:
        raise ValueError(f"Invalid cycle ID: {cycle_id}")

    parts = cycle_id.split("~")
    start_part = parts[1].strip()
    end_part = parts[2].strip()

    start_date = _parse_date_from_datetime_string(start_part)
    end_date = _parse_date_from_datetime_string(end_part)

    return start_date, end_date


def _parse_date_from_datetime_string(datetime_str: str) -> datetime.date:
    """Parse date from datetime string like '1/1/2025 12:00:00 AM' to 'YYYY-MM-DD'."""
    try:
        # Extract just the date part before the time
        date_part = datetime_str.split(" ")[0]
        month, day, year = date_part.split("/")
        return datetime.date(int(year), int(month), int(day))
    except ValueError as e:
        raise ValueError(f"Invalid datetime string: {datetime_str}") from e


def get_cycle_info_by_year(year: int) -> dict[str, str] | None:
    """Get cycle information for a specific year.

    Args:
        year: The election year to find cycle info for

    Returns:
        Dictionary with cycle info or None if not found
    """
    cycles = get_available_election_cycles()

    for cycle_name, cycle_info in cycles.items():
        if str(year) in cycle_name:
            return cycle_info

    return None


def _build_form_data(
    category_type: str,
    cycle_id: str,
    start_date: str,
    end_date: str,
    filer_type_id: str,
    start: int = 0,
    length: int = 10,
) -> str:
    """Build form data for Arizona Campaign Finance API request."""
    # DataTables parameters
    datatables_params = (
        f"draw=1&"
        f"columns[0][data]=TransactionDate&columns[0][name]=&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]=&columns[0][search][regex]=false&"
        f"columns[1][data]=CommitteeName&columns[1][name]=&columns[1][searchable]=true&columns[1][orderable]=true&columns[1][search][value]=&columns[1][search][regex]=false&"
        f"columns[2][data]=Amount&columns[2][name]=&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]=&columns[2][search][regex]=false&"
        f"columns[3][data]=TransactionName&columns[3][name]=&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&"
        f"columns[4][data]=TransactionType&columns[4][name]=&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]=&columns[4][search][regex]=false&"
        f"columns[5][data]=City&columns[5][name]=&columns[5][searchable]=true&columns[5][orderable]=true&columns[5][search][value]=&columns[5][search][regex]=false&"
        f"columns[6][data]=State&columns[6][name]=&columns[6][searchable]=true&columns[6][orderable]=true&columns[6][search][value]=&columns[6][search][regex]=false&"
        f"columns[7][data]=ZipCode&columns[7][name]=&columns[7][searchable]=true&columns[7][orderable]=true&columns[7][search][value]=&columns[7][search][regex]=false&"
        f"columns[8][data]=Memo&columns[8][name]=&columns[8][searchable]=true&columns[8][orderable]=true&columns[8][search][value]=&columns[8][search][regex]=false&"
        f"order[0][column]=0&order[0][dir]=asc&"
        f"start={start}&length={length}&"
        f"search[value]=&search[regex]=false"
    )

    # Search parameters
    search_params = (
        f"JurisdictionId=0&"
        f"CommiteeReportId=&"
        f"CategoryType={category_type}&"
        f"CycleId={cycle_id}&"
        f"StartDate={start_date}&"
        f"EndDate={end_date}&"
        f"FilerName=&"
        f"FilerId=&"
        f"BallotName=&"
        f"BallotMeasureId=&"
        f"FilerTypeId={filer_type_id}&"
        f"OfficeTypeId=&"
        f"OfficeId=&"
        f"PartyId=&"
        f"ContributorName=&"
        f"VendorName=&"
        f"StateId=&"
        f"City=&"
        f"Employer=&"
        f"Occupation=&"
        f"CandidateName=&"
        f"CandidateFilerId=&"
        f"Position=Support&"
        f"LowAmount=&"
        f"HighAmount="
    )

    return f"{datatables_params}&{search_params}"


def _get_cycle_ids_in_range(
    start_date: str | None, end_date: str | None, session: requests.Session
) -> list[str]:
    """Get cycle IDs in the range of start_date and end_date.

    Args:
        start_date: Start date for data collection in YYYY-MM-DD format
            If None, there will be no lower bound on the cycle start date
        end_date: End date for data collection in YYYY-MM-DD format
            If None, there will be no upper bound on the cycle end date
        session: Requests session to use for API calls

    Returns:
        List of cycle IDs in the range of start_date and end_date
    """
    if start_date is not None:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    if end_date is not None:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    cycles = get_available_election_cycles(session)
    cycle_ids = []
    for _, cycle_info in cycles.items():
        if (start_date is None or cycle_info["start_date"] <= end_date) and (
            end_date is None or cycle_info["end_date"] >= start_date
        ):
            cycle_ids.append(cycle_info["id"])
    return cycle_ids


def _fetch_arizona_data_page(
    category_type: str,
    cycle_id: str,
    start_date: str,
    end_date: str,
    filer_type_id: str,
    session: requests.Session,
    start: int = 0,
    length: int = 1000,
) -> dict:
    """Fetch a single page of Arizona Campaign Finance data.

    Args:
        category_type: Category type (Contributions, Expenditures, IndependentExpenditures)
        cycle_id: Election cycle ID with date range
        start_date: Start date for data collection in YYYY-MM-DD format
        end_date: End date for data collection in YYYY-MM-DD format
        filer_type_id: ID of the filer type to filter by
        session: Requests session to use for API calls
        start: Start index for pagination
        length: Number of records to fetch
    """
    form_data = _build_form_data(
        category_type=category_type,
        cycle_id=cycle_id,
        start_date=start_date,
        end_date=end_date,
        filer_type_id=filer_type_id,
        start=start,
        length=length,
    )

    response = session.post(BASE_URL, data=form_data, timeout=30)

    if response.status_code == TOO_MANY_REQUESTS:
        print("Rate limited. Retrying after delay...")
        time.sleep(10)
        return _fetch_arizona_data_page(
            category_type,
            cycle_id,
            start_date,
            end_date,
            filer_type_id,
            session,
            start,
            length,
        )

    response.raise_for_status()
    return response.json()


def get_arizona_transaction_data(
    start_date: str | None = None,
    end_date: str | None = None,
    filer_types: list[str] | None = None,
    report_categories: list[str] | None = None,
    output_dir: str | None = None,
) -> dict[str, pd.DataFrame]:
    """Get all Arizona Campaign Finance transaction data.

    This gets all income, expense, and independent expenditures data as separate
    dataframes.

    This function is a wrapper around the get_arizona_data_by_parameters function.
    There are other parameters in the API that are not exposed here. Only those
    required to make a valid API request are exposed (filer type, report category,
    and election cycle).

    Args:
        start_date: Start date for data collection in YYYY-MM-DD format
        end_date: End date for data collection in YYYY-MM-DD format
        filer_types: Filer types to filter by. If None, all filer types will be included.
        report_categories: Report categories to filter by. If None, all report
            categories will be included.
        output_dir: Directory to save the dataframes to. If None, the dataframes
            will be saved to DATA_DIR / "raw" / "AZ".

    Returns:
        Dictionary with report category as key and DataFrame as value
    """
    if output_dir is None:
        output_dir = DATA_DIR / "raw" / "AZ"
    output_dir.mkdir(parents=True, exist_ok=True)

    session = _create_session()
    _simulate_form_interaction(session)

    if filer_types is None:
        filer_types = get_all_available_filer_types(session)
    else:
        filer_types = {
            filer_type: FILER_TYPES[filer_type] for filer_type in filer_types
        }

    if report_categories is None:
        report_categories = CATEGORY_TYPES
    else:
        report_categories = {
            report_category: CATEGORY_TYPES[report_category]
            for report_category in report_categories
        }

    all_data = {}
    cycle_ids = _get_cycle_ids_in_range(start_date, end_date, session)

    for category_name, category_type in report_categories.items():
        all_category_data = []
        for filer_type, filer_type_id in filer_types.items():
            for cycle_id in cycle_ids:
                print(f"Fetching {category_name} data for cycle {cycle_id}...")
                partial_df = get_arizona_data_by_parameters(
                    report_category=category_type,
                    election_cycle=cycle_id,
                    filer_type_id=filer_type_id,
                    session=session,
                    output_dir=output_dir,
                )
                partial_df["filer_type"] = filer_type
                all_category_data.append(partial_df)
                time.sleep(1)  # Avoid hitting rate limits

        all_data[category_name] = pd.concat(all_category_data, ignore_index=True)

    return all_data


def get_arizona_data_by_parameters(
    report_category: str,
    election_cycle: str,
    filer_type_id: str,
    session: requests.Session,
    output_dir: str | None = None,
) -> pd.DataFrame:
    """Collect Arizona Campaign Finance transaction data by parameters.

    Args:
        report_category: Category type (Contributions, Expenditures, IndependentExpenditures)
        election_cycle: Election cycle ID with date range
        filer_type_id: ID of the filer type to filter by
        session: Requests session to use for API calls
        output_dir: Directory to save the dataframes to. If None, the dataframes
            will be saved to DATA_DIR / "raw" / "AZ".

    Returns:
        DataFrame containing filtered transaction data
    """
    start_date, end_date = _extract_date_range_from_cycle_id(election_cycle)
    output_file = output_dir / f"{report_category}.csv"

    all_records = []
    start = 0
    page_size = 1000
    progress_bar = None

    while True:
        try:
            response_data = _fetch_arizona_data_page(
                category_type=report_category,
                cycle_id=election_cycle,
                start_date=start_date,
                end_date=end_date,
                filer_type_id=filer_type_id,
                session=session,
                start=start,
                length=page_size,
            )

            if "data" not in response_data or not response_data["data"]:
                break

            # Add records to in memory dataframe and append to output file
            records = response_data["data"]
            all_records.extend(records)
            records_df = pd.DataFrame(records)
            records_df.to_csv(output_file, index=False, mode="a")

            # Initialize progress bar after first successful request
            total_records = response_data.get("recordsTotal", 0)
            if progress_bar is None and total_records > 0:
                progress_bar = tqdm(
                    total=total_records,
                    desc=f"Fetching {report_category} data for {election_cycle} and filer type {filer_type_id}",
                    unit="records",
                )
            if progress_bar:
                progress_bar.update(len(records))

            if start + page_size >= total_records:
                break
            start += page_size
            time.sleep(0.5)  # Rate limiting

        except Exception as e:
            print(f"Error fetching data at offset {start}: {e}")
            break

    # Close progress bar
    if progress_bar:
        progress_bar.close()

    if not all_records:
        print("No records found")
        return pd.DataFrame()

    complete_paramater_table = pd.DataFrame(all_records)
    print(f"Retrieved {len(complete_paramater_table)} total records")
    return complete_paramater_table


if __name__ == "__main__":
    transactions_tables = get_arizona_transaction_data(
        start_date="2023-01-01", end_date="2023-12-31"
    )
    print(f"Retrieved {len(transactions_tables)} total records")
    for category, category_df in transactions_tables.items():
        print(f"{category}: {len(category_df)} records")

    # Save to CSV files
    for category, category_df in transactions_tables.items():
        filename = f"arizona_{category}_data.csv"
        category_df.to_csv(filename, index=False)
        print(f"Saved {filename}")
