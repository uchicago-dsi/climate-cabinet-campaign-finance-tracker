"""Stage the MN election collector output as a standardized ElectionResult table.

The election collector (src/utils/collect/election/minnesota.py) writes
ElectionResults.csv, but no standardize form reads it yet (#153 G1), so it is
copied into the standardized directory for `cft normalize`.

Usage: stage_election_results.py <ElectionResults.csv> <standardized/mn dir>
"""

import sys
from pathlib import Path

import pandas as pd

if __name__ == "__main__":
    source, state_dir = Path(sys.argv[1]), Path(sys.argv[2])
    results = pd.read_csv(
        source, dtype={"id": str, "election_id": str, "election--district": str}
    )
    results.to_parquet(state_dir / "ElectionResult.parquet", index=False)
    print(f"Staged {len(results)} results to {state_dir / 'ElectionResult.parquet'}")
