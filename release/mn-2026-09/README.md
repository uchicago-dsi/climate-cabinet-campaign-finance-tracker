# MN student data release, September 2026

The code behind the Minnesota data release on Box (`dsi-core/2026-fall-climate-cabinet/data/`). The release was built from this branch, which is PRs #144 → #152 → #147 plus the fixes committed on top. It also needed steps outside the `cft` pipeline, which are in this folder. **This branch records how the release was made; it is not meant to be merged.** Issues found along the way, and what belongs in the pipeline, are tracked in #153.

## Steps

Run from the repository root with the environment installed (`pip install -e .`, or `uv`). `DATA_DIR` is a scratch data directory.

**1. Collect and standardize** (pipeline):
```bash
cft collect --states mn -d $DATA_DIR
python src/utils/collect/election/minnesota.py --output_directory $DATA_DIR/raw/mn/elections
cft standardize --states mn -d $DATA_DIR
python release/mn-2026-09/stage_election_results.py \
    $DATA_DIR/raw/mn/elections/ElectionResults.csv $DATA_DIR/standardized/mn
```
The last step stages election results by hand, because no standardize form reads them yet (#153 G1).

**2. Normalize and clean** (pipeline):
```bash
cft normalize --states mn -d $DATA_DIR
cft clean --states mn -d $DATA_DIR
```

**3. Pre-link fixes:** vendor names (B9), dropping placeholder elections (G6), and `reported_state` case (B7).
```bash
python release/mn-2026-09/prelink.py $DATA_DIR/cleaned/mn $DATA_DIR/prelink/mn
```

**4. Link** (pipeline). Uses the ZIP-code training rules committed on this branch (B4, B5):
```bash
cft link -i $DATA_DIR/prelink -d $DATA_DIR --database-path $DATA_DIR/linked.duckdb \
    --model-path $DATA_DIR/link_model.json --train --overwrite
```

**5. Candidate roster and roster linkage:**
- Collect the roster from the Campaign Finance Board's district viewer: about 880 requests at 2 seconds each, cached and resumable.
- Match SOS results to it and merge each candidate's records into one person.
```bash
python release/mn-2026-09/collect_cfb_roster.py $DATA_DIR/roster_cache $DATA_DIR/cfb_roster.parquet
python release/mn-2026-09/roster_link.py $DATA_DIR/linked.duckdb $DATA_DIR/cfb_roster.parquet \
    $DATA_DIR/prelink/mn $DATA_DIR/release/mn
python release/mn-2026-09/fill_full_names.py $DATA_DIR/release/mn
```

**6. House votes** (`votes/`):
1. **Scrape** each session from house.mn.gov (session keys are listed in `build_house_votes.py`).
2. **Match members:** match listed names to legislators with the [openstates/people](https://github.com/openstates/people) repository (commit `bf4caf1`) and `member_overrides.csv`.
3. **Build:** produce the House tables and link them to the release. Bill titles come from Open States session CSVs, which need an account to download.

`build_house_votes.py` expects this layout next to it:
- `house_scrape/output/`: the scraper's output;
- `matched/`: the matcher's output;
- `openstates/MN/<session>/`: the unzipped Open States CSVs;
- `people/`: a checkout of `openstates/people` with `data/mn`.

```bash
python votes/scrape_house_votes.py 243 245 246 248 249 251 252 253 254 255 256 257 258 259 299 300 302 304
python votes/match_members.py people house_scrape/output/member_votes_<key>.csv \
    house_scrape/output/roll_calls_<key>.csv matched/member_votes_<key>.csv votes/member_overrides.csv
python votes/build_house_votes.py $DATA_DIR/release/mn $DATA_DIR/release/mn
```

**7. Publish:**
- Copy `$DATA_DIR/release/mn/*.parquet` to Box, then write the data README.
- Use `shutil.copyfile`, not `copy`: the Box mount doesn't allow setting permissions.

## Tests

Before the ZIP training commit (`827e449`), the test suite passes. That commit makes 3 tests in `tests/test_link_train.py` fail, because their fixtures have no `address_zipcode` column. This is expected for a release-only change; the pipeline fix should choose blocking rules from the columns a state actually has (#153 B4, B5).

## Known issues in the release

Documented in the data README on Box and in #153:
- **Duplicates:** 20,166 exact duplicate transactions are kept.
- **Contribution type:** contributions have no `transaction_type`.
- **Donor merging:** contributor and vendor deduplication is approximate.
- **Missing names:** 623 people have no name.
- **2015–16 House members** are not linked to money or results.
- **Dangling references:** 11 `Membership` rows reference `Transactor` ids that don't exist (from `roster_link.py`).
