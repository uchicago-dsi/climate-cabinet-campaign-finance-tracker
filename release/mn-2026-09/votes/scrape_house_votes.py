"""Scrape Minnesota House recorded roll-call floor votes from house.mn.gov/Votes.

For each session key: POST /Votes/GetVoteSummary to list bills with roll calls,
GET /Votes/Details?SessionKey=..&BillNumber=.. for each bill (cached as HTML),
and parse every roll call and the members listed as voting yea and nay.

Usage: scrape_house_votes.py <session_key> [<session_key> ...]
Outputs (per session key, in output/): roll_calls_<key>.csv, member_votes_<key>.csv
"""

import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = "https://www.house.mn.gov/Votes"
DELAY_SECONDS = 1.0
HERE = Path(__file__).parent
CACHE = HERE / "cache"
OUTPUT = HERE / "output"


def get_with_retries(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    """Request with retries and backoff on connection errors and 5xx responses"""
    for attempt in range(5):
        try:
            response = session.request(method, url, timeout=60, **kwargs)
            time.sleep(DELAY_SECONDS)
            if response.status_code < 500:
                response.raise_for_status()
                return response
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as error:
            print(f"retrying {url}: {error}", flush=True)
        time.sleep(15 * (attempt + 1))
    raise RuntimeError(f"failed after retries: {url}")


def bill_list(session: requests.Session, session_key: int) -> list[dict]:
    """Bills with recorded roll calls in a session (cached)"""
    cache = CACHE / f"summary_{session_key}.json"
    if not cache.exists():
        response = get_with_retries(
            session, "POST", f"{BASE}/GetVoteSummary",
            data=json.dumps({"SessionKey": session_key, "sortOption": "BillNumber"}),
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        cache.write_text(response.text)
    return json.loads(cache.read_text())


def details_html(session: requests.Session, session_key: int, bill: str) -> str:
    """Details page of one bill (cached)"""
    cache = CACHE / str(session_key) / f"{bill}.html"
    if not cache.exists():
        cache.parent.mkdir(parents=True, exist_ok=True)
        response = get_with_retries(
            session, "GET", f"{BASE}/Details", params={"SessionKey": session_key, "BillNumber": bill}
        )
        cache.write_text(response.text)
    return cache.read_text()


def names_after(label: str, panel_html: str) -> list[str]:
    """Member names in the table following the text `label` in a panel's raw HTML

    The site leaves <td> tags unclosed, so names are read with a regex on the raw
    HTML rather than from a parsed tree (which nests the cells).
    """
    start = panel_html.find(label)
    if start == -1:
        return []
    end = panel_html.find("</table>", start)
    return [
        name.strip()
        for name in re.findall(r"<td>([^<]*)", panel_html[start:end])
        if name.strip()
    ]


def parse_details(html: str, session_key: int, bill: str) -> tuple[list[dict], list[dict]]:
    """Roll calls and member votes from one bill's details page"""
    roll_calls, member_votes = [], []
    results_html = html[html.find('id="DisplayResults"'):]
    panel_chunks = results_html.split('<div class="collapsible-panel"')[1:]
    for index, panel_html in enumerate(panel_chunks):
        panel = BeautifulSoup('<div class="collapsible-panel"' + panel_html, "html.parser")
        cells = panel.select("div.panel-header table tbody tr td")
        if len(cells) < 7:
            continue
        roll_call_id = f"{session_key}-{bill}-{index:03d}"
        description = " | ".join(s.strip() for s in cells[1].stripped_strings)
        yeas_listed = names_after("voted in the affirmative", panel_html)
        nays_listed = names_after("voted in the negative", panel_html)
        roll_calls.append({
            "roll_call_id": roll_call_id,
            "session_key": session_key,
            "bill_number": bill,
            "description": description,
            "amendment": " ".join(cells[2].stripped_strings) or None,
            "yeas": int(cells[3].get_text(strip=True) or 0),
            "nays": int(cells[4].get_text(strip=True) or 0),
            "journal_page": cells[5].get_text(strip=True) or None,
            "date": pd.to_datetime(cells[6].get_text(strip=True), format="%m/%d/%Y").date(),
            "yeas_listed": len(yeas_listed),
            "nays_listed": len(nays_listed),
        })
        member_votes += [{"roll_call_id": roll_call_id, "member": n, "vote": "yes"} for n in yeas_listed]
        member_votes += [{"roll_call_id": roll_call_id, "member": n, "vote": "no"} for n in nays_listed]
    return roll_calls, member_votes


def scrape(session_key: int) -> None:
    """Scrape one session and write its CSVs"""
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0"
    bills = bill_list(session, session_key)
    all_roll_calls, all_votes = [], []
    for i, bill in enumerate(bills):
        roll_calls, votes = parse_details(details_html(session, session_key, bill["Number"]), session_key, bill["Number"])
        for roll_call in roll_calls:
            roll_call["bill_short_description"] = bill["ShortDescription"]
        all_roll_calls += roll_calls
        all_votes += votes
        if i % 25 == 0:
            print(f"{session_key}: {i}/{len(bills)} bills", flush=True)
    OUTPUT.mkdir(exist_ok=True)
    roll_calls = pd.DataFrame(all_roll_calls)
    roll_calls.to_csv(OUTPUT / f"roll_calls_{session_key}.csv", index=False)
    pd.DataFrame(all_votes).to_csv(OUTPUT / f"member_votes_{session_key}.csv", index=False)
    mismatched = roll_calls[(roll_calls.yeas != roll_calls.yeas_listed) | (roll_calls.nays != roll_calls.nays_listed)]
    print(f"{session_key}: {len(bills)} bills, {len(roll_calls)} roll calls, {len(all_votes)} member votes, "
          f"{len(mismatched)} roll calls where listed names != counts", flush=True)


if __name__ == "__main__":
    CACHE.mkdir(exist_ok=True)
    for key in sys.argv[1:]:
        scrape(int(key))
