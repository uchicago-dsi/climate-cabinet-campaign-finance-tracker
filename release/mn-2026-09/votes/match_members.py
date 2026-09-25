"""Match member names from House vote listings to legislators and districts.

Vote listings name members by surname, adding a first initial when two members
share a surname ("Hansen, R."). Candidates come from the Open States people
repository (data/mn/legislature and data/mn/retired): every House (lower) role
with its district and start/end dates. A listed name matches the one member of
the session's roster whose family name (or a listed alternate name) fits.

Usage: match_members.py <people repo dir> <member_votes.csv> <roll_calls.csv> <out.csv> [overrides.csv]

Overrides (session_key, member, person_name, district, evidence) resolve names the
Open States data cannot, e.g. members whose older House roles are missing.
"""

import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import yaml


def fold(text: str) -> str:
    """Lower case, no accents, apostrophes, or extra spaces"""
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", text.replace("'", "").replace(".", "").lower()).strip()


def house_roles(people_dir: Path) -> pd.DataFrame:
    """One row per House role: person, name parts, district, start, end"""
    rows = []
    for path in sorted(people_dir.glob("data/mn/*/*.yml")):
        person = yaml.safe_load(path.read_text())
        family = person.get("family_name") or person["name"].split()[-1]
        given = person.get("given_name") or person["name"].split()[0]
        alternates = {fold(n["name"]) for n in person.get("other_names", [])}
        for role in person.get("roles", []):
            if role.get("type") != "lower":
                continue
            rows.append({
                "person_id": person["id"], "name": person["name"], "family": fold(family),
                "given_initial": fold(given)[:1], "alternates": alternates,
                "district": str(role.get("district")),
                "start": pd.to_datetime(role["start_date"]) if role.get("start_date") else pd.Timestamp.min,
                "end": pd.to_datetime(role.get("end_date")) if role.get("end_date") else pd.Timestamp.max,
            })
    return pd.DataFrame(rows)


def match(label: str, roster: pd.DataFrame) -> pd.DataFrame:
    """Roster rows matching a listed member name"""
    surname, _, initial = (part.strip() for part in label.partition(","))
    surname, initial = fold(surname), fold(initial)[:1]
    label_words = set(surname.replace("-", " ").split())

    def compound_match(family: str) -> bool:
        """Surname words contained in the family name or vice versa (name changes,
        e.g. 'Kunesh-Podein' for Kunesh, 'Neu' for Neu Brindley)"""
        family_words = set(family.replace("-", " ").split())
        return bool(label_words) and (label_words <= family_words or family_words <= label_words)

    hits = roster[
        (roster.family == surname)
        | roster.alternates.map(lambda a: fold(label) in a or surname in a)
        | roster.family.map(compound_match)
    ]
    if initial:
        hits = hits[hits.given_initial == initial]
    if len(hits) > 1:  # prefer exact family-name matches over alternate names
        exact = hits[hits.family == surname]
        hits = exact if len(exact) else hits
    return hits


def main(people_dir: Path, votes_path: Path, roll_calls_path: Path, out_path: Path,
         overrides_path: Path | None = None) -> None:
    roles = house_roles(people_dir)
    overrides = pd.read_csv(overrides_path) if overrides_path else pd.DataFrame(
        columns=["session_key", "member", "person_name", "district", "evidence"])
    ids_by_name = {
        person["name"]: person["id"]
        for person in (yaml.safe_load(f.read_text()) for f in people_dir.glob("data/mn/*/*.yml"))
    }
    votes = pd.read_csv(votes_path)
    roll_calls = pd.read_csv(roll_calls_path, parse_dates=["date"])
    votes = votes.merge(roll_calls[["roll_call_id", "date", "session_key"]], on="roll_call_id")
    session_key = int(votes["session_key"].iloc[0])
    overridden = overrides[overrides.session_key == session_key].set_index("member")
    resolved = {}
    for (label, date), _ in votes.groupby(["member", "date"]):
        if label in overridden.index:
            row = overridden.loc[label]
            person_id = ids_by_name.get(row["person_name"])
            resolved[(label, date)] = (person_id, row["person_name"], row["district"], "override")
            continue
        roster = roles[(roles.start <= date) & (roles.end >= date)]
        hits = match(label, roster)
        resolved[(label, date)] = (
            (hits.iloc[0]["person_id"], hits.iloc[0]["name"], hits.iloc[0]["district"], "matched")
            if len(hits) == 1 else (None, None, None, f"{len(hits)} candidates")
        )
    keys = list(zip(votes.member, votes.date))
    votes["person_id"], votes["person_name"], votes["district"], votes["match_status"] = zip(*(resolved[k] for k in keys))
    votes.drop(columns=["date", "session_key"]).to_csv(out_path, index=False)
    status = votes.match_status.value_counts()
    print(f"{out_path.name}: {len(votes)} member votes; {status.to_dict()}")
    unmatched = votes[~votes.match_status.isin(["matched", "override"])].groupby(["member", "match_status"]).size()
    if len(unmatched):
        print("unmatched names:", unmatched.to_dict())


if __name__ == "__main__":
    main(*(Path(a) for a in sys.argv[1:6]))
