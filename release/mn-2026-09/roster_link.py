"""Link SOS election results to campaign finance committees with the CFB roster.

Steps (documented on issue #153):
1. Match each SOS ElectionResult to a CFB roster candidate in the same race
   (year, office, district) by name, using party as a tiebreaker.
2. The roster's registration number identifies the candidate committee
   (SourceIdentifier mn_cfb_registration). All candidate records of a committee
   (Membership type Candidate) and the SOS candidates matched to it are one
   person: merge them (union-find across committees) into one canonical id.
3. Add Membership(Candidate) rows for matched committees without one, with
   earliest/latest_known_date from the roster years.
4. Collapse Transactor to one row per id, dedupe Membership and Address.
5. Save the roster (with committee_id, candidate_id) as CandidateRegistration.

Usage: roster_link.py <linked.duckdb> <roster.parquet> <prelink dir> <out dir>
"""

import re
import shutil
import sys
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd
from rapidfuzz import fuzz

# Columns that reference Transactor.id, from table.yaml (utils.ids.get_all_id_references)
TRANSACTOR_REFERENCES = {
    "Transactor": ["id"],
    "Transaction": ["donor_id", "recipient_id", "affected_party_id"],
    "ElectionResult": ["candidate_id"],
    "Address": ["transactor_id"],
    "Membership": ["member_id", "organization_id"],
}


def replace_ids_with_canonical(con, table_name, id_columns, mapping_table):
    """Same as utils.link.predict.replace_ids_with_canonical (PR #152)"""
    for id_column in id_columns:
        con.execute(f"""
            UPDATE "{table_name}" AS t SET {id_column} = m.canonical_id
            FROM {mapping_table} AS m WHERE t.{id_column} = m.old_id
        """)


def update_source_identifiers(con, entity_table, mapping_table):
    """Same as utils.link.predict.update_source_identifiers (PR #152)"""
    con.execute(f"""
        UPDATE SourceIdentifier AS s SET entity_id = m.canonical_id
        FROM {mapping_table} AS m
        WHERE s.entity_table = '{entity_table}' AND s.entity_id = m.old_id
    """)
    con.execute("CREATE OR REPLACE TABLE SourceIdentifier AS SELECT DISTINCT * FROM SourceIdentifier")

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "dr"}
PARTY_TO_SOS = {"RPM": "R", "DFL": "DFL"}


def tokens(name: str) -> list[str]:
    """Lower-case name tokens without punctuation or suffixes"""
    name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    name = re.sub(r"['’]", "", name.lower())
    name = re.sub(r"[^a-z\s-]", " ", name).replace("-", " ")
    return [t for t in name.split() if t not in SUFFIXES]


def roster_parts(name: str) -> tuple[list[str], list[str]]:
    """(last-name tokens, first/middle/nickname tokens) from 'Last, First M (Nick)'"""
    last, _, rest = str(name).partition(",")
    return tokens(last), tokens(rest)


def sos_parts(name: str) -> list[str]:
    """Tokens of an SOS ballot name; for tickets keep the first candidate"""
    return tokens(re.split(r"\s+and\s+", str(name))[0])


def match_race(sos_rows: pd.DataFrame, roster_rows: pd.DataFrame) -> list[dict]:
    """Match SOS candidates to roster candidates within one race"""
    matches = []
    for sos in sos_rows.itertuples():
        sos_tokens = sos_parts(sos.full_name)
        scored = []
        for roster in roster_rows.itertuples():
            last, first = roster_parts(roster.name)
            if not last or not sos_tokens:
                continue
            last_match = sos_tokens[-len(last):] == last or " ".join(last) in " ".join(sos_tokens)
            first_score = max((fuzz.ratio(sos_tokens[0], f) for f in first), default=0)
            full_score = fuzz.token_set_ratio(" ".join(sos_tokens), " ".join(first + last))
            party_match = PARTY_TO_SOS.get(roster.party, roster.party) == sos.party
            scored.append((roster, last_match, first_score, full_score, party_match))
        by_last = [s for s in scored if s[1]]
        if len(by_last) == 1:
            best, method = by_last[0], "last_name"
        elif len(by_last) > 1:
            best = max(by_last, key=lambda s: (s[4], s[2]))
            method = "last_name+first_name"
        else:
            fuzzy = [s for s in scored if s[3] >= 85]
            # last names can change (e.g. marriage): accept a unique same-party
            # candidate in the race with the same first name
            same_first = [s for s in scored if s[4] and s[2] >= 90]
            if fuzzy:
                best, method = max(fuzzy, key=lambda s: (s[4], s[3])), "fuzzy_full_name"
            elif len(same_first) == 1:
                best, method = same_first[0], "first_name+party"
            else:
                matches.append({"election_result_id": sos.er_id, "sos_candidate_id": sos.candidate_id, "method": "unmatched"})
                continue
        roster, _, first_score, full_score, party_match = best
        matches.append({
            "election_result_id": sos.er_id,
            "sos_candidate_id": sos.candidate_id,
            "registration_number": roster.registration_number,
            "roster_name": roster.name,
            "method": method,
            "full_name_score": full_score,
            "party_match": party_match,
            "roster_general_winner": roster.general_winner,
        })
    return matches


def union_find(pairs: list[tuple[str, str]]) -> dict[str, str]:
    """Map every id to the smallest id in its connected component"""
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in pairs:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    return {x: find(x) for x in parent}


def main(linked_db: Path, roster_path: Path, prelink_dir: Path, out_dir: Path) -> None:
    final_db = out_dir.parent / "final.duckdb"
    out_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(linked_db, final_db)
    con = duckdb.connect(str(final_db))

    roster = pd.read_parquet(roster_path)
    roster["registration_number"] = roster["registration_number"].astype(str)
    prelink_el = pd.read_parquet(prelink_dir / "Election.parquet")
    prelink_er = pd.read_parquet(prelink_dir / "ElectionResult.parquet")

    # SOS results with race keys (election metadata comes from the pre-link tables,
    # which keep columns the DuckDB loader drops)
    sos = con.sql("SELECT id AS er_id, candidate_id, election_id FROM ElectionResult").df()
    sos = sos.merge(prelink_el[["id", "year", "office_sought", "district", "election_type"]],
                    left_on="election_id", right_on="id").drop(columns="id")
    people = con.sql("SELECT id, any_value(full_name) AS full_name, any_value(party) AS party FROM Transactor GROUP BY id").df()
    sos = sos.merge(people, left_on="candidate_id", right_on="id", how="left").drop(columns="id")

    # 1. match within races
    filed = roster[roster["filed_for_ballot"]]
    matches = []
    for (year, office, district), sos_rows in sos.groupby(["year", "office_sought", sos["district"].fillna("")]):
        race = filed[(filed.year == year) & (filed.office_sought == office) & (filed.district.fillna("") == district)]
        if race.empty:
            race = roster[(roster.year == year) & (roster.office_sought == office) & (roster.district.fillna("") == district)]
        if race.empty:
            matches += [{"election_result_id": r, "method": "no_roster_for_race"} for r in sos_rows.er_id]
            continue
        race_matches = match_race(sos_rows, race)
        # the "filed for the ballot" flag is sometimes missing (e.g. specials):
        # retry unmatched candidates against everyone registered for the race
        unmatched = [m["election_result_id"] for m in race_matches if m["method"] == "unmatched"]
        if unmatched:
            everyone = roster[(roster.year == year) & (roster.office_sought == office) & (roster.district.fillna("") == district)]
            retried = {m["election_result_id"]: m for m in match_race(sos_rows[sos_rows.er_id.isin(unmatched)], everyone)}
            race_matches = [retried.get(m["election_result_id"], m) if m["method"] == "unmatched" else m for m in race_matches]
        matches += race_matches
    matches = pd.DataFrame(matches)

    # 2. committees and person merges
    committees = con.sql("""
        SELECT source_id AS registration_number, entity_id AS committee_id
        FROM SourceIdentifier WHERE source = 'mn_cfb_registration' AND entity_table = 'Transactor'
    """).df()
    members = con.sql("SELECT DISTINCT organization_id, member_id FROM Membership WHERE membership_type = 'Candidate'").df()
    matched = matches.dropna(subset=["registration_number"]).merge(committees, on="registration_number", how="left")
    regs = set(matched["registration_number"]) | set(roster["registration_number"])
    reg_committee = committees[committees.registration_number.isin(regs)]
    pairs = []
    anchor = {}  # registration number -> one person id in its group
    for reg, committee_id in reg_committee.itertuples(index=False):
        for member_id in members.loc[members.organization_id == committee_id, "member_id"]:
            anchor.setdefault(reg, member_id)
            pairs.append((anchor[reg], member_id))
    for row in matched.itertuples():
        anchor.setdefault(row.registration_number, row.sos_candidate_id)
        pairs.append((anchor[row.registration_number], row.sos_candidate_id))
    canonical = union_find(pairs)
    mapping = pd.DataFrame([(old, new) for old, new in canonical.items() if old != new], columns=["old_id", "canonical_id"])
    con.register("candidate_mapping_df", mapping)
    con.execute("CREATE OR REPLACE TABLE candidate_linkage AS SELECT * FROM candidate_mapping_df")
    for table_name, id_columns in TRANSACTOR_REFERENCES.items():
        replace_ids_with_canonical(con, table_name, id_columns, mapping_table="candidate_linkage")
    update_source_identifiers(con, "Transactor", mapping_table="candidate_linkage")
    component_sizes = pd.Series(canonical.values()).value_counts()

    # 3. candidate memberships for matched committees, with roster years
    reg_person = {reg: canonical.get(pid, pid) for reg, pid in anchor.items()}
    years = roster.groupby("registration_number").year.agg(["min", "max"])
    new_memberships = []
    for reg, committee_id in reg_committee.itertuples(index=False):
        if reg in reg_person:
            new_memberships.append({
                "membership_type": "Candidate", "member_id": reg_person[reg], "organization_id": committee_id,
                "earliest_known_date": f"{years.loc[reg, 'min']}-01-01", "latest_known_date": f"{years.loc[reg, 'max']}-12-31",
                "reported_state": "mn",
            })
    new_memberships = pd.DataFrame(new_memberships)
    con.register("new_memberships_df", new_memberships)
    con.execute("""
        CREATE OR REPLACE TABLE Membership AS
        WITH existing AS (SELECT * FROM Membership WHERE NOT (membership_type = 'Candidate' AND organization_id IN (SELECT organization_id FROM new_memberships_df)))
        SELECT DISTINCT * FROM existing
        UNION ALL BY NAME SELECT * FROM new_memberships_df
    """)

    # 4. one row per Transactor id: keep the most complete record
    cols = [r[1] for r in con.execute("PRAGMA table_info('Transactor')").fetchall()]
    completeness = " + ".join(f"({c} IS NOT NULL)::INT" for c in cols)
    con.execute(f"""
        CREATE OR REPLACE TABLE Transactor AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY id ORDER BY ({completeness}) DESC, full_name) AS rn FROM Transactor
        ) WHERE rn = 1
    """)
    con.execute("CREATE OR REPLACE TABLE Address AS SELECT DISTINCT * FROM Address")

    # 5. roster with ids
    roster = roster.merge(committees, on="registration_number", how="left")
    roster["candidate_id"] = roster["registration_number"].map(reg_person)
    er_by_reg = matched.groupby(["registration_number"]).election_result_id.apply(list)
    con.register("roster_df", roster)
    con.execute("CREATE OR REPLACE TABLE CandidateRegistration AS SELECT * FROM roster_df")
    con.register("matches_df", matches)
    con.execute("CREATE OR REPLACE TABLE election_result_roster_match AS SELECT * FROM matches_df")

    # export, reattaching columns the DuckDB loader dropped
    exports = ["Transaction", "Transactor", "Membership", "Address", "Election", "ElectionResult", "SourceIdentifier", "CandidateRegistration"]
    for table in exports:
        df = con.sql(f'SELECT * FROM "{table}"').df()
        if table == "Election":
            extra = prelink_el[["id", "date", "election_type", "office_name"]]
            df = df.merge(extra, on="id", how="left")
        if table == "ElectionResult":
            extra = prelink_er[["id", "vote_share", "total_votes_for_office"]]
            df = df.merge(extra, on="id", how="left")
        df.to_parquet(out_dir / f"{table}.parquet", index=False)

    # metrics
    print("match methods:", matches.method.value_counts().to_dict())
    print("merged person ids:", len(mapping), "| component sizes:", component_sizes.value_counts().sort_index().to_dict())
    print("new candidate memberships:", len(new_memberships))
    matches.to_parquet(out_dir.parent / "election_result_roster_match.parquet", index=False)


if __name__ == "__main__":
    main(*(Path(a) for a in sys.argv[1:5]))
