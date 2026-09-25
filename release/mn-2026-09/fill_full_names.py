"""Fill empty Transactor.full_name from first_name and last_name.

Some linked people keep only the record parsed from their committee name, which
has first and last name but no full_name. Applied to the published release.

Usage: fill_full_names.py <release mn dir>
"""

import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

if __name__ == "__main__":
    path = Path(sys.argv[1]) / "Transactor.parquet"
    schema = pq.read_schema(path)
    people = pd.read_parquet(path)
    fill = people.full_name.isna() & people.last_name.notna()
    people.loc[fill, "full_name"] = (
        people.loc[fill, "first_name"].fillna("") + " " + people.loc[fill, "last_name"]
    ).str.strip()
    people.to_parquet(path, index=False, schema=schema)
    print(f"Filled {fill.sum()} names; {people.full_name.isna().sum()} still empty")
