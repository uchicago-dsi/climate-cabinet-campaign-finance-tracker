"""Scripts to scrape Arizona Campaign Finance data"""

import argparse
import datetime
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils.constants import DATA_DIR

ADVANCED_SEARCH_URL = "https://seethemoney.az.gov/Reporting/AdvancedSearch/"
TRANSACTOR_DETAILS_URL = "https://seethemoney.az.gov/Reporting/GetDetailedInformation"
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
CATEGORY_TYPES = [
    "Income",
    "Expenditures",
    "IndependentExpenditures",
    "BallotMeasures",
]

# Filer type mappings
FILER_TYPES = {
    "Candidate": "130",
    "Committee": "131",
    "Party": "132",
    "Office Holder": "96",
}

TRANSACTOR_TYPES_TO_DETAILED_INFO_ID = {
    "Candidate": 4,
    "Committee": 3,
    "Party": 5,
    "Office Holder": 6,
}

TOO_MANY_REQUESTS = 429


class ArizonaAPI:
    """Handles all Arizona Campaign Finance API interactions."""

    def __init__(self, wait_time: float = 0.2, timeout: float = 30) -> None:
        """Initialize the API client with a session."""
        self.wait_time = wait_time
        self.timeout = timeout
        self.session = self._create_session()
        self._simulate_form_interaction()

    def _create_session(self) -> requests.Session:
        """Create and initialize a requests session with proper headers and cookies."""
        session = requests.Session()
        session.headers.update(HEADERS)
        time.sleep(self.wait_time)
        session.get(ADVANCED_SEARCH_URL, timeout=self.timeout)
        return session

    def _simulate_form_interaction(self) -> None:
        """Simulate form interaction to establish proper session state."""
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
            "FilerTypeId": "130",
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
        time.sleep(self.wait_time)
        self.session.post(ADVANCED_SEARCH_URL, data=form_data, timeout=self.timeout)

    def get_available_election_cycles(self) -> dict[str, dict[str, str]]:
        """Get available election cycles from the Arizona website.

        Returns:
            Dictionary mapping cycle names to cycle info containing 'id' and date range
        """
        time.sleep(self.wait_time)
        response = self.session.get(ADVANCED_SEARCH_URL, timeout=self.timeout)
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
                start_date, end_date = self._extract_date_range_from_cycle_id(value)
                cycles[text] = {
                    "id": value,
                    "start_date": start_date,
                    "end_date": end_date,
                }

        return cycles

    def get_available_filer_types(self) -> dict[str, str]:
        """Get all available filer types from the Arizona website."""
        time.sleep(self.wait_time)
        response = self.session.get(ADVANCED_SEARCH_URL, timeout=self.timeout)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        filer_type_select = soup.find("select", {"id": "FilerTypeId"})

        if not filer_type_select:
            raise ValueError("Could not find FilerTypeId select element on page")

        filer_types = {}
        for option in filer_type_select.find_all("option"):
            value = option.get("value", "").strip()
            if not value:  # Skip 'All' option
                continue
            text = option.get_text().strip()
            filer_types[text] = value

        return filer_types

    def _extract_date_range_from_cycle_id(
        self, cycle_id: str
    ) -> tuple[datetime.date, datetime.date]:
        """Extract start and end dates from a cycle ID string.

        Args:
            cycle_id: Cycle ID in format "44~1/1/2025 12:00:00 AM~12/31/2026 11:59:59 PM"

        Returns:
            Tuple of start and end dates
        """
        expected_tilde_sections = 2
        if cycle_id.count("~") != expected_tilde_sections:
            raise ValueError(f"Invalid cycle ID: {cycle_id}")

        parts = cycle_id.split("~")
        start_part = parts[1].strip()
        end_part = parts[2].strip()

        start_date = self._parse_date_from_datetime_string(start_part)
        end_date = self._parse_date_from_datetime_string(end_part)

        return start_date, end_date

    def _parse_date_from_datetime_string(self, datetime_str: str) -> datetime.date:
        """Parse date from datetime string like '1/1/2025 12:00:00 AM'."""
        try:
            date_part = datetime_str.split(" ")[0]
            month, day, year = date_part.split("/")
            return datetime.date(int(year), int(month), int(day))
        except ValueError as e:
            raise ValueError(f"Invalid datetime string: {datetime_str}") from e

    def _build_form_data(
        self,
        category_type: str,
        cycle_id: str,
        start_date: str,
        end_date: str,
        filer_type_id: str,
        start: int = 0,
        length: int = 10,
    ) -> str:
        """Build form data for Arizona Campaign Finance API request."""
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

    def fetch_transaction_data_page(
        self,
        category_type: str,
        cycle_id: str,
        start_date: str,
        end_date: str,
        filer_type_id: str,
        start: int = 0,
        length: int = 1000,
    ) -> dict:
        """Fetch a single page of Arizona Campaign Finance transaction data.

        Args:
            category_type: Category type (Income, Expenditures, etc.)
            cycle_id: Election cycle ID with date range
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            filer_type_id: ID of the filer type to filter by
            start: Start index for pagination
            length: Number of records to fetch
        """
        form_data = self._build_form_data(
            category_type=category_type,
            cycle_id=cycle_id,
            start_date=start_date,
            end_date=end_date,
            filer_type_id=filer_type_id,
            start=start,
            length=length,
        )

        time.sleep(self.wait_time)
        response = self.session.post(
            ADVANCED_SEARCH_URL, data=form_data, timeout=self.timeout
        )

        if response.status_code == TOO_MANY_REQUESTS:
            print("Rate limited. Retrying after delay...")
            time.sleep(self.timeout)
            return self.fetch_transaction_data_page(
                category_type,
                cycle_id,
                start_date,
                end_date,
                filer_type_id,
                start,
                length,
            )

        response.raise_for_status()
        return response.json()

    def fetch_transactor_data(self, transactor_id: str, transactor_type: str) -> dict:
        """Get Arizona Campaign Finance transactor data by transactor ID.

        Args:
            transactor_id: ID of the transactor to get data for
            transactor_type: Type of the transactor (Committee, Candidate, etc.)

        Returns:
            Dictionary containing transactor data
        """
        time.sleep(self.wait_time)
        response = self.session.post(
            TRANSACTOR_DETAILS_URL,
            params={
                "Page": 11,
                "startYear": 2000,
                "endYear": 2100,
                "JurisdictionId": 0,
                "TablePage": 1,
                "TableLength": 10,
                "Name": f"3~{transactor_id}",
            },
            headers=HEADERS,
            timeout=self.timeout,
        )
        response.raise_for_status()

        if "ReportFilerInfo" not in response.json():
            raise ValueError(
                f"Unexpected response from API for {transactor_id}: {response.json()}"
            )

        transactor_data = response.json()["ReportFilerInfo"]
        transactor_data[f"{transactor_type}ID"] = transactor_id
        return transactor_data


class ArizonaDataProcessor:
    """Handles Arizona Campaign Finance data processing and file I/O operations."""

    def __init__(
        self,
        output_path: Path | str,
        override_existing_data: bool = False,
        save_in_batches: bool = True,
        batch_size: int = 1000,
        early_stop: int | None = None,
    ) -> None:
        """Initialize the data processor.

        Args:
            output_path: Directory to save processed data
            override_existing_data: If True, overwrite existing data files
            save_in_batches: If True, save data in batches during processing
            batch_size: Number of records per batch when save_in_batches is True
            early_stop: If not None, stop processing for each type after this many records
        """
        self.output_path = Path(output_path)
        self.override_existing_data = override_existing_data
        self.save_in_batches = save_in_batches
        self.batch_size = batch_size
        self.early_stop = early_stop
        self.output_path.mkdir(parents=True, exist_ok=True)

    def _get_output_file_path(
        self, report_category: str, filer_type_id: str, cycle_id: str
    ) -> Path:
        """Generate output file path for transaction data."""
        filename = f"{report_category}-{filer_type_id}-{cycle_id.split('~')[0]}.csv"
        return self.output_path / filename

    def _get_transactor_file_path(self, transactor_type: str) -> Path:
        """Generate output file path for transactor data."""
        return self.output_path / f"{transactor_type}.csv"

    def _get_resume_position(self, file_path: Path) -> int:
        """Get the position to resume from based on existing data."""
        if not self.override_existing_data and file_path.exists():
            try:
                existing_df = pd.read_csv(file_path)
                return len(existing_df)
            except Exception as e:
                print(f"Error reading existing file {file_path}: {e}")
                return 0
        return 0

    def _save_batch_data(self, data: list[dict], file_path: Path) -> None:
        """Save a batch of data to file."""
        if not data:
            return

        batch_df = pd.DataFrame(data)
        batch_df.to_csv(
            file_path,
            index=False,
            mode="a",
            header=not file_path.exists(),
        )

    def process_transaction_data(
        self,
        api: ArizonaAPI,
        report_category: str,
        election_cycle: str,
        filer_type_id: str,
    ) -> pd.DataFrame:
        """Process transaction data for given parameters.

        Args:
            api: ArizonaAPI instance for making API calls
            report_category: Category type (Income, Expenditures, etc.)
            election_cycle: Election cycle ID with date range
            filer_type_id: ID of the filer type to filter by

        Returns:
            DataFrame containing processed transaction data
        """
        start_date, end_date = api._extract_date_range_from_cycle_id(election_cycle)
        output_file = self._get_output_file_path(
            report_category, filer_type_id, election_cycle
        )

        start_position = self._get_resume_position(output_file)
        all_records = []
        page_size = self.batch_size
        progress_bar = None

        while True:
            try:
                response_data = api.fetch_transaction_data_page(
                    category_type=report_category,
                    cycle_id=election_cycle,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    filer_type_id=filer_type_id,
                    start=start_position,
                    length=page_size,
                )

                if "data" not in response_data or not response_data["data"]:
                    break

                records = response_data["data"]
                all_records.extend(records)

                # Save in batches if enabled
                if self.save_in_batches:
                    self._save_batch_data(records, output_file)

                # Initialize progress bar after first successful request
                total_records = response_data.get("recordsTotal", 0)
                if progress_bar is None and total_records > 0:
                    progress_bar = tqdm(
                        total=total_records - start_position,
                        desc=f"Processing {report_category} data for cycle {election_cycle} and filer type {filer_type_id}",
                        unit="records",
                    )
                if progress_bar:
                    progress_bar.update(len(records))

                if start_position + page_size >= total_records:
                    break
                start_position += page_size

                if self.early_stop and len(all_records) >= self.early_stop:
                    break

            except Exception as e:
                print(f"Error fetching data at offset {start_position}: {e}")
                break

        if progress_bar:
            progress_bar.close()

        if not all_records:
            print("No records found")
            return pd.DataFrame()

        # Save final data if not saving in batches
        if not self.save_in_batches:
            self._save_batch_data(all_records, output_file)

        complete_data = pd.DataFrame(all_records)
        print(f"Retrieved {len(complete_data)} total records")
        return complete_data

    def process_transactor_data(
        self,
        api: ArizonaAPI,
        transactor_ids_by_type: dict[str, set[str]],
        transactor_batch_size: int = 100,
    ) -> dict[str, pd.DataFrame]:
        """Process transactor data for given transactor IDs.

        Args:
            api: ArizonaAPI instance for making API calls
            transactor_ids_by_type: Dictionary of transactor type to set of transactor IDs
            transactor_batch_size: Number of transactor IDs to process at a time

        Returns:
            Dictionary of transactor type to DataFrame
        """
        all_transactor_data = {}
        total_transactors = sum(len(ids) for ids in transactor_ids_by_type.values())

        with tqdm(
            total=total_transactors,
            desc="Processing all transactor types",
            unit="transactor",
        ) as overall_pbar:
            for transactor_type, transactor_ids in transactor_ids_by_type.items():
                if not transactor_ids:
                    continue

                output_file = self._get_transactor_file_path(transactor_type)
                if self.override_existing_data and output_file.exists():
                    output_file.unlink()

                all_transactor_data[transactor_type] = []
                transactor_ids_list = list(transactor_ids)

                with tqdm(
                    total=len(transactor_ids_list),
                    desc=f"Processing {transactor_type} transactors",
                    unit="transactor",
                    leave=False,
                ) as type_pbar:
                    for i in range(0, len(transactor_ids_list), transactor_batch_size):
                        batch_transactor_ids = transactor_ids_list[
                            i : i + transactor_batch_size
                        ]
                        batch_results = []

                        with tqdm(
                            total=len(batch_transactor_ids),
                            desc=f"Processing batch {i // transactor_batch_size + 1}",
                            unit="transactor",
                            leave=False,
                        ) as batch_pbar:
                            for transactor_id in batch_transactor_ids:
                                try:
                                    single_transactor_details = (
                                        api.fetch_transactor_data(
                                            transactor_id, transactor_type
                                        )
                                    )
                                    batch_results.append(single_transactor_details)
                                except (ValueError, requests.HTTPError) as e:
                                    print(
                                        f"Error fetching data for {transactor_id}: {e}"
                                    )
                                finally:
                                    batch_pbar.update(1)
                                    type_pbar.update(1)
                                    overall_pbar.update(1)

                        if batch_results:
                            if self.save_in_batches:
                                self._save_batch_data(batch_results, output_file)
                            all_transactor_data[transactor_type].extend(batch_results)

        return all_transactor_data

    def get_transactor_ids_from_transactions(self) -> dict[str, set[str]]:
        """Get transactor IDs from existing transaction files."""
        committee_ids = set()
        for file in self.output_path.glob("*.csv"):
            if file.name.startswith(tuple(CATEGORY_TYPES)):
                try:
                    transaction_df = pd.read_csv(file)
                    if "CommitteeID" in transaction_df.columns:
                        committee_ids.update(set(transaction_df["CommitteeID"]))
                except Exception as e:
                    print(f"Error reading file {file}: {e}")
        return {"Committee": committee_ids}

    def get_previously_scraped_transactor_ids(self) -> dict[str, set[str]]:
        """Get previously scraped transactor IDs from existing files."""
        previously_scraped = {}
        for filer_type in FILER_TYPES.keys():
            file_path = self._get_transactor_file_path(filer_type)
            if file_path.exists():
                try:
                    transactions_df = pd.read_csv(file_path)
                    id_column = f"{filer_type}ID"
                    if id_column in transactions_df.columns:
                        previously_scraped[filer_type] = set(transactions_df[id_column])
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
                    previously_scraped[filer_type] = set()
            else:
                previously_scraped[filer_type] = set()
        return previously_scraped

    def load_existing_transaction_data(self) -> dict[str, pd.DataFrame]:
        """Load existing transaction data from files."""
        transaction_data = {}
        for category in CATEGORY_TYPES:
            for file in self.output_path.glob(f"{category}*.csv"):
                try:
                    transaction_data[category] = pd.read_csv(file)
                except Exception as e:
                    print(f"Error loading {file}: {e}")
                    transaction_data[category] = pd.DataFrame()

        return transaction_data

    def get_all_transaction_data(
        self,
        api: ArizonaAPI,
        start_date: str | None = None,
        end_date: str | None = None,
        filer_types: list[str] | None = None,
        report_categories: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Get all Arizona Campaign Finance transaction data.

        Args:
            api: ArizonaAPI instance for making API calls
            start_date: Start date for data collection in YYYY-MM-DD format
            end_date: End date for data collection in YYYY-MM-DD format
            filer_types: Filer types to filter by. If None, all filer types will be included
            report_categories: Report categories to filter by. If None, all categories included

        Returns:
            Dictionary with report category as key and DataFrame as value
        """
        if filer_types is None:
            filer_types = api.get_available_filer_types()
        else:
            filer_types = {
                filer_type: FILER_TYPES[filer_type] for filer_type in filer_types
            }

        if report_categories is None:
            report_categories = CATEGORY_TYPES

        all_data = {}
        cycle_ids = self._get_cycle_ids_in_range(start_date, end_date, api)

        for category_name in report_categories:
            all_category_data = []
            for filer_type, filer_type_id in filer_types.items():
                for cycle_id in cycle_ids:
                    partial_df = self.process_transaction_data(
                        api=api,
                        report_category=category_name,
                        election_cycle=cycle_id,
                        filer_type_id=filer_type_id,
                    )
                    partial_df["filer_type"] = filer_type
                    all_category_data.append(partial_df)
                    time.sleep(1)  # Avoid hitting rate limits

            all_data[category_name] = pd.concat(all_category_data, ignore_index=True)

        return all_data

    def _get_cycle_ids_in_range(
        self, start_date: str | None, end_date: str | None, api: ArizonaAPI
    ) -> list[str]:
        """Get cycle IDs in the range of start_date and end_date."""
        if start_date is not None:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        if end_date is not None:
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

        cycles = api.get_available_election_cycles()
        cycle_ids = []
        for _, cycle_info in cycles.items():
            if (start_date is None or cycle_info["start_date"] <= end_date) and (
                end_date is None or cycle_info["end_date"] >= start_date
            ):
                cycle_ids.append(cycle_info["id"])
        return cycle_ids


def get_cycle_info_by_year(year: int, api: ArizonaAPI) -> dict[str, str] | None:
    """Get cycle information for a specific year."""
    cycles = api.get_available_election_cycles()

    for cycle_name, cycle_info in cycles.items():
        if str(year) in cycle_name:
            return cycle_info

    return None


def get_all_arizona_data(
    start_date: str | None = None,
    end_date: str | None = None,
    output_dir: str | None = None,
    override_existing_data: bool = False,
    save_in_batches: bool = True,
    batch_size: int = 1000,
) -> dict[str, pd.DataFrame]:
    """Get all Arizona Campaign Finance data.

    Args:
        start_date: Start date for data collection in YYYY-MM-DD format
        end_date: End date for data collection in YYYY-MM-DD format
        output_dir: Directory to save the dataframes to
        override_existing_data: If True, existing data will be overwritten
        save_in_batches: If True, save data in batches during processing
        batch_size: Number of records per batch when save_in_batches is True

    Returns:
        Dictionary containing both transaction and transactor data
    """
    if output_dir is None:
        output_dir = DATA_DIR / "raw" / "AZ" / "AdvancedSearch"

    api = ArizonaAPI()
    processor = ArizonaDataProcessor(
        output_path=output_dir,
        override_existing_data=override_existing_data,
        save_in_batches=save_in_batches,
        batch_size=batch_size,
    )

    # Get transaction data
    transaction_data = processor.get_all_transaction_data(
        api=api,
        start_date=start_date,
        end_date=end_date,
    )

    # Get transactor data
    transactor_ids = processor.get_transactor_ids_from_transactions()
    transactor_data = processor.process_transactor_data(
        api=api, transactor_ids_by_type=transactor_ids
    )

    return {**transaction_data, **transactor_data}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Arizona Campaign Finance data")
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=False)
    parser.add_argument("--override_existing_data", action="store_true", default=False)
    parser.add_argument("--save_in_batches", action="store_true", default=True)
    parser.add_argument("--batch_size", type=int, default=1000)
    args = parser.parse_args()

    get_all_arizona_data(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        override_existing_data=args.override_existing_data,
        save_in_batches=args.save_in_batches,
        batch_size=args.batch_size,
    )
