"""Pre-link fixes on the cleaned MN tables (documented on issue #153)."""
import sys
from pathlib import Path
import pandas as pd

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
dst.mkdir(parents=True, exist_ok=True)
tables = {p.stem: pd.read_parquet(p) for p in src.glob("*.parquet")}

# B9: expenditure vendor names were standardized to a non-schema `name` column
tx = tables["Transactor"]
vendor = tx["full_name"].isna() & tx["name"].notna()
tx.loc[vendor, "full_name"] = tx.loc[vendor, "name"]
tables["Transactor"] = tx.drop(columns=["name"])
print(f"B9: moved {vendor.sum()} vendor names into full_name")

# G6: office-only Election/ElectionResult rows parsed from committee names are not
# results (no year, district, or votes) and carry swapped offices (B2); drop them.
el, er = tables["Election"], tables["ElectionResult"]
placeholder_elections = set(el.loc[el["year"].isna(), "id"])
drop_results = er["election_id"].isin(placeholder_elections)
tables["Election"] = el[~el["id"].isin(placeholder_elections)]
tables["ElectionResult"] = er[~drop_results]
print(f"G6: dropped {len(placeholder_elections)} placeholder elections and {drop_results.sum()} placeholder results")

# B7: one spelling of reported_state
for name, table in tables.items():
    if "reported_state" in table.columns:
        table["reported_state"] = table["reported_state"].str.lower()

for name, table in tables.items():
    table.to_parquet(dst / f"{name}.parquet", index=False)
    print(f"{name}: {len(table)} rows")
