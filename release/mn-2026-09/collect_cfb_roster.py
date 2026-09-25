"""Collect the MN Campaign Finance Board candidate roster by office, district, year.

Uses the Board's district viewer endpoint (the same request its web page makes):
POST /reports-and-data/viewers/campaign-finance/districts-constitutional-offices/api
with office, district, year, tabname=information, after an ordinary page load
to obtain a session cookie. Responses are cached as JSON so reruns resume.
"""

import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://cfb.mn.gov/reports-and-data/viewers/campaign-finance/districts-constitutional-offices"
YEARS = [2016, 2018, 2020, 2022, 2024]
SENATE_YEARS = [2016, 2020, 2022]
SENATE_SPECIALS = [("13", 2018), ("45", 2024)]
STATEWIDE = {"GC": "Governor", "AG": "Attorney General", "SS": "Secretary of State", "SA": "Auditor General"}
STATEWIDE_YEARS = [2018, 2022]
DELAY_SECONDS = 3.0
FLAG_ROWS = {
    "Incumbent": "incumbent",
    "Filed to be on the ballot": "filed_for_ballot",
    "Primary election winner (or no primary)": "primary_winner",
    "General election winner": "general_winner",
}
VALUE_ROWS = {
    "Registration number": "registration_number",
    "Registration date": "registration_date",
    "Termination date": "termination_date",
    "Party": "party",
}


def requests_to_make() -> list[tuple[str, str, str, int]]:
    """(office code, office_sought, district, year) for every request"""
    out = []
    for year in YEARS:
        out += [("House", "State Representative", f"{n}{s}", year) for n in range(1, 68) for s in "AB"]
    # the viewer returns the previous election's field for years without an
    # election, so query the Senate only for regular elections and the specials
    # held on general election dates
    for year in SENATE_YEARS:
        out += [("Senate", "State Senator", str(n), year) for n in range(1, 68)]
    out += [("Senate", "State Senator", district, year) for district, year in SENATE_SPECIALS]
    for year in STATEWIDE_YEARS:
        out += [(code, office, "", year) for code, office in STATEWIDE.items()]
    return out


def parse_information(tabcontent: str) -> list[dict]:
    """One dict per candidate column in the information tab"""
    soup = BeautifulSoup(tabcontent, "html.parser")
    names = {
        box["value"]: label.get_text(strip=True)
        for label in soup.select("form.filter_candidates label")
        if (box := label.find("input"))
    }
    running = {
        box["value"]
        for box in soup.select("form.filter_candidates input.running")
    }
    candidates = {reg: {"registration_number": reg, "name": name, "running": reg in running} for reg, name in names.items()}
    for row in soup.select("table tr"):
        header = row.find("th")
        if header is None:
            continue
        label = header.get_text(strip=True)
        for cell in row.find_all("td"):
            match = re.search(r"col-(\d+)", " ".join(cell.get("class", [])))
            if not match or match.group(1) not in candidates:
                continue
            candidate = candidates[match.group(1)]
            if label in FLAG_ROWS:
                icon = cell.find("i")
                candidate[FLAG_ROWS[label]] = bool(icon and "fa-check-square-o" in icon.get("class", []))
            elif label in VALUE_ROWS and label != "Registration number":
                candidate[VALUE_ROWS[label]] = cell.get_text(strip=True) or None
    return list(candidates.values())


def new_session() -> requests.Session:
    """Session with the cookie the viewer's API requires"""
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0"
    session.get(f"{BASE}/House/2022", timeout=30)
    return session


def fetch(session: requests.Session | None, office: str, district: str, year: int) -> tuple[requests.Session | None, str | None]:
    """POST one request with retries; returns (session, response text or None)"""
    for attempt in range(5):
        try:
            if session is None:
                session = new_session()
            response = session.post(
                f"{BASE}/api",
                data={"office": office, "district": district, "year": year, "tabname": "information"},
                timeout=60,
            )
            time.sleep(DELAY_SECONDS)
            if response.status_code == 200:
                return session, response.text
            print(f"status {response.status_code} for {office} {district} {year}", flush=True)
        except requests.exceptions.RequestException as error:
            print(f"retrying {office} {district} {year} after {error}", flush=True)
        session = None  # start a fresh session after backing off
        time.sleep(30 * (attempt + 1))
    return session, None


def main(cache_directory: Path, output_path: Path) -> None:
    """Fetch (or read cached) responses and write the roster table"""
    cache_directory.mkdir(parents=True, exist_ok=True)
    session = None
    rows, failed = [], []
    todo = requests_to_make()
    for i, (office, office_sought, district, year) in enumerate(todo):
        cache = cache_directory / f"{office}_{district or 'statewide'}_{year}.json"
        if not cache.exists():
            session, text = fetch(session, office, district, year)
            if text is None:
                failed.append((office, district, year))
                continue
            cache.write_text(text)
        payload = json.loads(cache.read_text())
        for candidate in parse_information(payload.get("tabcontent", "")):
            rows.append({"year": year, "office_code": office, "office_sought": office_sought, "district": district or None, **candidate})
        if i % 50 == 0:
            print(f"{i}/{len(todo)}", flush=True)
    if failed:
        print(f"FAILED {len(failed)} requests (rerun to fill): {failed}", flush=True)
        sys.exit(1)
    roster = pd.DataFrame(rows)
    roster.to_parquet(output_path, index=False)
    print(f"Saved {len(roster)} roster rows to {output_path}")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
