import pandas as pd

from utils.constants import BASE_FILEPATH
from utils.linkage import deduplicate_perfect_matches

# inds_sample = pd.read_csv(
#    BASE_FILEPATH / "output" / "complete_individuals_table.csv",
#    low_memory=False,
# )
orgs_sample = pd.read_csv(
    BASE_FILEPATH / "output" / "complete_organizations_table.csv"
)

# deduplicated_inds = deduplicate_perfect_matches(inds_sample)
deduplicated_orgs = deduplicate_perfect_matches(orgs_sample)

output_dedup_ids = pd.read_csv(
    BASE_FILEPATH / "output" / "deduplicated_UUIDs.csv"
)
# outpud_ids should have all the ids that deduplicated_inds and deduplicated_orgs
# has

# dedup_inds_id = set(deduplicated_inds.id.tolist())
dedup_orgs_id = set(deduplicated_orgs.id.tolist())
unique_ids = set(output_dedup_ids.duplicated_uuids.tolist())

# assert dedup_inds_id.issubset(unique_ids)
assert dedup_orgs_id.issubset(unique_ids)
