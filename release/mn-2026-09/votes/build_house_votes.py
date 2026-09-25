"""Build the Minnesota House vote tables for the student data release.

Inputs:
- house_scrape/output/: roll calls and member votes scraped from house.mn.gov/Votes
- matched/: member votes matched to Open States person ids and districts
  (match_members.py, with member_overrides.csv)
- openstates/MN/: Open States session CSVs (bill titles, subjects, Revisor URLs)
- people/: Open States people repository (party, name)
- the linked release on Box: CandidateRegistration (to link members to
  candidate and committee ids)

Outputs (parquet, in out/): HouseBill, HouseRollCall, HouseVote, HouseMember

Usage: build_house_votes.py <linked release mn dir> <out dir>
"""

import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import yaml

HERE = Path(__file__).parent

# House site session key -> (session label, Open States session, election year
# whose winners hold the seats)
SESSIONS = {
    243: ("2015-2016 Regular Session", None, 2014),
    245: ("2015 Special Session", None, 2014),
    246: ("2017-2018 Regular Session", "2017-2018", 2016),
    248: ("2017 Special Session", "2017s1", 2016),
    249: ("2019-2020 Regular Session", "2019-2020", 2018),
    251: ("2019 Special Session", "2019s1", 2018),
    252: ("2020 First Special Session", "2020s1", 2018),
    253: ("2020 Second Special Session", "2020s2", 2018),
    254: ("2020 Third Special Session", "2020s3", 2018),
    255: ("2020 Fourth Special Session", "2020s4", 2018),
    256: ("2020 Fifth Special Session", "2020s5", 2018),
    258: ("2020 Sixth Special Session", "2020s6", 2018),
    259: ("2020 Seventh Special Session", "2020s7", 2018),
    257: ("2021-2022 Regular Session", "2021-2022", 2020),
    299: ("2021 Special Session", "2021s1", 2020),
    300: ("2023-2024 Regular Session", "2023-2024", 2022),
    302: ("2025-2026 Regular Session", "2025-2026", 2024),
    304: ("2025 Special Session", "2025s1", 2024),
}


SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}
PARTY_CODES = {"RPM": "R", "Republican": "R", "DFL": "DFL", "Democratic-Farmer-Labor": "DFL"}


def shares_name_word(name: str, member_words: list[str]) -> bool:
    """Whether a winner's name shares a word (3+ letters, not a suffix) with a member's

    Names appear as "First Last", "Last, First M", or with suffixes, so any shared
    word counts. This only confirms the district and year match; each district
    has one winner per election.
    """
    words = {w for w in fold(str(name)).split() if len(w) >= 3 and w not in SUFFIXES}
    return bool(words & set(member_words))


def fold(text: str) -> str:
    """Lower case, no accents, apostrophes, or punctuation"""
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z ]", "", text.replace("-", " ").lower()).strip()


def bill_label(number: str) -> str:
    """House site bill number (HF0007) as Open States identifier (HF 7)"""
    match = re.match(r"^([A-Z]+)0*(\d+)$", number)
    return f"{match.group(1)} {match.group(2)}" if match else number


def load_scraped(keys: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Roll calls and matched member votes for the given session keys"""
    roll_calls = pd.concat(
        pd.read_csv(HERE / "house_scrape" / "output" / f"roll_calls_{k}.csv") for k in keys
    )
    votes = pd.concat(pd.read_csv(HERE / "matched" / f"member_votes_{k}.csv") for k in keys)
    return roll_calls, votes


def open_states_bills() -> pd.DataFrame:
    """Bill title, subjects, and Revisor URL from Open States, keyed by session and identifier"""
    frames = []
    for session_dir in (HERE / "openstates" / "MN").iterdir():
        bills_path = session_dir / f"MN_{session_dir.name}_bills.csv"
        if not bills_path.exists():
            continue
        bills = pd.read_csv(bills_path)
        sources_path = session_dir / f"MN_{session_dir.name}_bill_sources.csv"
        if sources_path.exists():
            urls = pd.read_csv(sources_path).groupby("bill_id").url.first()
            bills["revisor_url"] = bills["id"].map(urls)
        frames.append(bills.assign(os_session=session_dir.name))
    bills = pd.concat(frames)
    return bills[["os_session", "identifier", "title", "subject", "revisor_url"]].rename(
        columns={"subject": "subjects"}
    )


def people_party(people_dir: Path) -> dict[str, str]:
    """Most recent party of each Open States person"""
    parties = {}
    for path in people_dir.glob("data/mn/*/*.yml"):
        person = yaml.safe_load(path.read_text())
        party = person.get("party") or []
        if party:
            parties[person["id"]] = party[0].get("name")
    return parties


def link_members(members: pd.DataFrame, release_dir: Path) -> pd.DataFrame:
    """Attach candidate, committee, and registration to each member-session

    A member is linked to the Secretary of State general-election winner for the
    member's district in the session's election year, if the winner's surname
    matches the member's. The committee and registration come from
    CandidateRegistration for that person, year, and district. Members seated in
    a special election are linked to the winner of the next general election if
    it is the same person. Members of 2015-2016, before our election data, are
    not linked.
    """
    results = pd.read_parquet(release_dir / "ElectionResult.parquet")
    elections = pd.read_parquet(release_dir / "Election.parquet")
    people = pd.read_parquet(
        release_dir / "Transactor.parquet", columns=["id", "full_name", "first_name", "last_name"]
    )
    # some linked people have no full_name, only the first and last name parsed
    # from their committee's name
    people["full_name"] = people.full_name.fillna(
        people.first_name.fillna("") + " " + people.last_name.fillna("")
    )
    registrations = pd.read_parquet(release_dir / "CandidateRegistration.parquet")
    winners = (
        results[results.win]
        .merge(elections, left_on="election_id", right_on="id", suffixes=("", "_election"))
        .query("election_type == 'general' and office_sought == 'State Representative'")
        .merge(people, left_on="candidate_id", right_on="id", suffixes=("", "_person"))
        [["year", "district", "candidate_id", "full_name"]]
        .rename(columns={"year": "election_year", "full_name": "winner_name"})
    )
    registration = registrations[registrations.office_sought == "State Representative"][
        ["year", "district", "candidate_id", "registration_number", "committee_id", "party"]
    ].rename(columns={"year": "election_year"}).drop_duplicates(["election_year", "district", "candidate_id"])
    winners = winners.merge(registration, on=["election_year", "district", "candidate_id"], how="left")
    columns = ["winner_name", "party", "registration_number", "candidate_id", "committee_id"]
    first_year = int(elections.year.min())
    rows = []
    for member in members.itertuples(index=False):
        member = member._asdict()
        member_words = fold(member["name"]).split()

        def same_person(year: int) -> pd.DataFrame:
            found = winners[(winners.district == member["district"]) & (winners.election_year == year)]
            return found[found.winner_name.map(lambda w: shares_name_word(w, member_words))]

        if member["election_year"] < first_year:
            match, status = None, "no_election_data"
        elif len(at_election := same_person(member["election_year"])):
            match, status = at_election.iloc[0], "election_winner"
        elif len(next_election := same_person(member["election_year"] + 2)):
            match, status = next_election.iloc[0], "next_election_winner"
        else:
            match, status = None, "no_matching_winner"
        member.update({c: (match[c] if match is not None else None) for c in columns})
        member["link_status"] = status
        rows.append(member)
    linked = pd.DataFrame(rows)
    linked["party"] = linked.party.map(lambda p: PARTY_CODES.get(p, p) if isinstance(p, str) else p)
    return linked


def main(release_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    keys = [k for k in SESSIONS if (HERE / "matched" / f"member_votes_{k}.csv").exists()]
    missing = sorted(set(SESSIONS) - set(keys))
    if missing:
        print(f"Warning: sessions not yet scraped and matched: {missing}")
    roll_calls, votes = load_scraped(keys)
    session_info = pd.DataFrame(
        [(k, *v) for k, v in SESSIONS.items()],
        columns=["session_key", "session", "os_session", "election_year"],
    )

    # HouseRollCall
    roll_calls = roll_calls.merge(session_info, on="session_key")
    roll_calls["bill_id"] = roll_calls.session_key.astype(str) + "-" + roll_calls.bill_number
    house_roll_call = roll_calls[[
        "roll_call_id", "bill_id", "session", "date", "description", "amendment",
        "yeas", "nays", "journal_page",
    ]].rename(columns={"description": "motion", "yeas": "yes_count", "nays": "no_count"})
    house_roll_call["date"] = pd.to_datetime(house_roll_call.date)

    # HouseBill
    bills = roll_calls.drop_duplicates("bill_id")[[
        "bill_id", "session", "os_session", "bill_number", "bill_short_description",
    ]].copy()
    bills["bill"] = bills.bill_number.map(bill_label)
    enrich = open_states_bills()
    bills = bills.merge(
        enrich, left_on=["os_session", "bill"], right_on=["os_session", "identifier"], how="left"
    )
    bills["house_votes_url"] = (
        "https://www.house.mn.gov/Votes/Details?SessionKey="
        + bills.bill_id.str.split("-").str[0] + "&BillNumber=" + bills.bill_number
    )
    house_bill = bills[[
        "bill_id", "session", "bill", "bill_short_description", "title", "subjects",
        "revisor_url", "house_votes_url",
    ]]

    # HouseVote
    house_vote = votes.rename(columns={"person_id": "person_id"})[
        ["roll_call_id", "person_id", "district", "vote"]
    ]

    # HouseMember: one row per person per session
    votes = votes.merge(roll_calls[["roll_call_id", "session_key", "session", "election_year"]], on="roll_call_id")
    members = (
        votes.groupby(["session_key", "session", "election_year", "person_id", "person_name", "district"])
        .size().rename("n_votes").reset_index()
        .rename(columns={"person_name": "name"})
    )
    members = link_members(members, release_dir)
    parties = people_party(HERE / "people")
    members["party"] = members.party.fillna(
        members.person_id.map(parties).map(lambda p: PARTY_CODES.get(p, p))
    )
    house_member = members[[
        "session", "person_id", "name", "district", "party", "n_votes", "election_year",
        "candidate_id", "committee_id", "registration_number", "link_status",
    ]]

    for name, table in {
        "HouseBill": house_bill, "HouseRollCall": house_roll_call,
        "HouseVote": house_vote, "HouseMember": house_member,
    }.items():
        table.to_parquet(out_dir / f"{name}.parquet", index=False)
        print(f"{name}: {len(table)} rows")
    print("members by session and link status:")
    print(house_member.groupby(["session", "link_status"]).size().unstack(fill_value=0).to_string())
    print(f"bills with Open States title: {house_bill.title.notna().mean():.1%}")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
