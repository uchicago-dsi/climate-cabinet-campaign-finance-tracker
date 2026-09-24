# Proposal: Source Identifiers

**Status:** Draft for discussion
**Scope:** `table.yaml`, a new source registry, `utils/ids.py`, standardize, normalize, link

## Summary

Record every identifier a source provides in a new `SourceIdentifier` table. Examples: a campaign finance registration number, a secretary of state candidacy id, a Texas filer id, an FEC committee id. Each row says which entity the identifier points to and which id system issued it.

- **Id systems are listed in a registry file** that standardize validates against.
- **Entity ids stay UUIDs** (`Transactor.id` and friends). Linking updates `SourceIdentifier` so every source id always points at the current canonical entity.
- **Replaces `id_mapping.tsv`.** That file already records this information, but only as a side file, keyed without an id system, and it goes stale after linking.

## Motivation

We are about to link data from more than one source for the same state. For Minnesota:

| Source | Native id | Identifies |
|---|---|---|
| Campaign Finance Board (CFB) finance files | registration number, e.g. `15677` | a candidate *committee* (an Organization) |
| CFB district viewer (candidate roster) | registration number + office/district/year + name | the bridge between a committee and a candidacy |
| Secretary of State (SOS) results files | office id + candidate order code, e.g. `0255`/`0401` | one *candidacy* in one election |

Linking these reliably is a rule-based join on source ids, not a probabilistic match. Today that is hard, for four reasons:

1. **Source ids disappear from the data.** `handle_existing_ids` swaps each raw id for a UUID. The raw id survives only in `DATA_DIR/normalized/<state>/id_mapping.tsv`, which is not part of the database and is not loaded by the link step.
2. **The mapping goes stale after linking.** `link/predict.py` rewrites Transactor ids to canonical cluster ids in place (`replace_ids_with_canonical`). It records `old_id -> canonical_id` in the DuckDB `linkage_mapping` table but never updates `id_mapping.tsv`, so a raw id still maps to the pre-link UUID.
3. **Ids from different id systems collide.** The mapping key is `(raw_id, year, reported_state, table_name)`, which has no notion of which id system a raw id came from. This already causes a real collision (see [Evidence](#evidence-id-behavior-in-the-raw-data)):
   - `az.yaml` maps both Arizona `CommitteeID` and `NameID` into Transactor ids, and 706 values appear in both systems.
   - `1001` is the Arizona State AFL-CIO PAC as a CommitteeID but Gail Carey as a NameID.
   - The existing `normalized/az/id_mapping.tsv` has a single UUID for raw id `1001`.
4. **One entity can have many source ids.** A person can have one committee for a House run and another for a later Senate run. An Arizona committee has both a CommitteeID and its own NameID. A fixed column per source on `Transactor` cannot represent this.

## Evidence: id behavior in the raw data

The raw data in `raw/` was checked for ids reused across years for different entities, and for collisions between id systems. Method: for each id, build its distinct `(id, year, normalized name)` rows and compare the names with fuzzy matching. Renames of the same entity don't count as reuse.

| State | Ids checked | Finding |
|---|---|---|
| MN | 3,812 across all six reg num columns | No reuse. `0` and `12345` are placeholders attached to many unrelated names and should be treated as null. `Contrib Reg Num` mixes lobbyist numbers (0–9,992) with committee reg nums (≥10,054); the ranges don't overlap today. |
| PA | 15,533 `FILERID`s, 2000–2024 filer files | **Reuse in short candidate ids.** 1–6 digit ids, almost all FILERTYPE 1 (candidate), are reassigned across elections: 58 of 296 recurring short ids name different people, typically ten years apart. Example: `10014` is Mary Jane Bowes (2001, Superior Court), then Farley Toothman (2011). Standard 7–9 character ids behave as permanent; name changes there are renames or repurposed committees. |
| TX | 10,864 `filerIdent`s, full pass of all contribution and expenditure files | No reuse. Name changes are renames or mergers (BB&T → Truist PAC). Some ids cover several related entities in different filer roles, e.g. `00015654` is both Texas Association of Business and TAB PAC. |
| MI | 2,907 `cfr_com_id`s | Can't tell: the export stamps each committee's current name on every year. |
| AZ | Access DB and AdvancedSearch | No reuse across years. **CommitteeID/NameID collision** as described above. AdvancedSearch uses the same CommitteeID system as the Access DB (512 of 513 shared ids have matching names). |

**Conclusion:** `year` doesn't belong in the key. For four states it would split every committee into one entity per year and fix nothing. The actual collisions come from mixing id systems, which the key doesn't model. The one case of reuse across years (PA short candidate ids) is a property of that one id system, so that id system handles it (see [Source registry](#2-source-registry)).

## Proposal

### 1. New table

```yaml
SourceIdentifier:
  required_attributes:
    - entity_id
    - entity_table
    - source
    - source_id
  attributes:
    - entity_id      # uuid of the row this identifier points to
    - entity_table   # Transactor, Election, ElectionResult, Transaction, ...
    - source         # which id system issued source_id; must be in sources.yaml
    - source_id      # the identifier as the source provides it (string)
    - reported_state
    - earliest_known_date
    - latest_known_date
```

- **Invariant:** `(source, source_id, entity_table)` is unique, and it maps to exactly one `entity_id` at any time. One entity can have any number of rows.
- **The key has no year.** `source` replaces both `reported_state` and `year` in the key. `reported_state` is kept as an attribute for filtering.
- **Dates:** `earliest_known_date` / `latest_known_date` follow the convention already used by `Address` and `Membership`.

### 2. Source registry

The valid `source` values live in a registry file, `src/utils/sources.yaml`. They are not an enum in `table.yaml`, for two reasons:

- **Enum values are never checked.** `table.yaml` enums are declared but not validated against data today (`transactor_type_specific` already contains values outside its enum, such as "Political Fund").
- **A source needs more than a name.** It also needs documentation and a format.

Excerpt (the full initial list is in the implementation plan):

```yaml
mn_cfb_registration:
  state: mn
  agency: Minnesota Campaign Finance and Public Disclosure Board
  description: Registration number of a committee, party unit, or fund
  url: https://cfb.mn.gov/
  pattern: '^[1-9]\d{4}$'
  null_values: ['0', '12345']   # placeholders seen in the raw data

mn_cfb_lobbyist:
  state: mn
  agency: Minnesota Campaign Finance and Public Disclosure Board
  description: Lobbyist registration number (appears in Contrib Reg Num)
  pattern: '^\d{1,4}$'

mn_sos_candidacy:
  state: mn
  agency: Minnesota Secretary of State
  description: One candidate in one race, as <election date>-<office id>-<candidate order code>
  pattern: '^\d{8}-\d{4}-\d{4}$'

az_sos_committee:
  state: az
  description: CommitteeID in the Access DB export and AdvancedSearch
az_sos_name:
  state: az
  description: NameID in the Access DB export (people and organizations, including committees)

pa_dos_filer:
  state: pa
  description: Standard FILERID (7-9 characters, encodes registration year)
  pattern: '^(\d{7,8}|\d{4}C\d{4})$'
pa_dos_candidate_filer:
  state: pa
  description: >-
    Short candidate FILERID (1-6 digits). Reassigned across elections, so
    source_id is <EYEAR>-<FILERID>.
  pattern: '^\d{4}-\d{1,6}$'

fec_committee:
  state: null
  agency: Federal Election Commission
  description: FEC committee id of a federal PAC or candidate committee
  url: https://www.fec.gov/data/browse-data/?tab=bulk-data
  pattern: '^C\d{8}$'
```

- **Naming:** `<state>_<agency>_<id type>`, lower snake case. Federal id systems have no state prefix.
- **Validation:** standardize fails if a config references a source that isn't in the registry. It sets ids matching `null_values` to null, and counts and logs ids that don't match `pattern` (dropping them as ids, not the row).
- **Ids reused across years** are handled by the source that has the problem, by building `source_id` to be unique (as `pa_dos_candidate_filer` does). The key stays the same for every source.

### 3. `ElectionResult` gets an `id`; candidacy ids attach there

SOS candidacy ids identify an `ElectionResult` row, not the candidate person.

- **It's one-to-one.** A candidacy id and a result row describe the same thing: one candidate in one race. Attaching it to the person would give each candidate one row per election and mix identifiers for events into a table meant for identifying people.
- **Re-collection is safe.** Re-running the collector (certified results, added specials) updates existing result rows instead of duplicating them.
- **Merging people still works.** When linking merges candidate people, `ElectionResult.candidate_id` is updated by the existing `replace_ids_with_canonical` path, because `get_all_id_references` already includes every column that references Transactor.
- **Cost:** add `id` to `ElectionResult` attributes in `table.yaml`. `handle_id_column` already assigns UUIDs to any table whose schema has an `id` attribute.

Who a candidate is comes from the finance side. The CFB registration number identifies the committee, and the roster's `Membership(Candidate)` row connects the committee to the candidate person.

### 4. `Organization.ein` becomes an FEC committee id

`ein` (Employer Identification Number, the IRS's 9-digit tax id for organizations) is only populated from Texas `contributorPacFein`. The Texas CFS ReadMe describes that field as the "FEC ID of out-of-state PAC contributor". Its values in the current data (4,207 distinct):

- 1,764 are FEC committee ids (`C########`, e.g. `C00236489` KOCH-PAC, `C00366559` NRG Energy PAC);
- 101 are EIN-shaped;
- 2,342 are junk: dates, `0.00`, `USA`.

**Proposal:**
- Map `contributorPacFein` to `source: fec_committee`. The registry pattern keeps only valid FEC ids.
- Remove `ein` from `Organization`.
- If real EINs turn up in a future source, add an `irs_ein` source rather than a column.

FEC ids are more useful than EINs here:
- They identify the same federal PAC across every state, which helps cross-state linking.
- The FEC committee master file lists each PAC's connected (sponsoring) organization, which is direct input for industry classification.

### 5. Entity ids stay UUIDs

`Transactor.id` is **not** replaced by one of the source ids.

- **Linking changes the entity.** When two records are merged, whichever source id was "the" id would change for some rows. That breaks references and anything downstream that saved an id.
- **Some entities have no source id.** Most individual donors have no id in any source.
- **Source ids collide across systems,** as the Arizona case shows.

### 6. Lifecycle

- **Standardize:**
  - Each id column in a state config declares its source, e.g. `id_source: mn_cfb_registration` on `Recipient reg num` in `mn.yaml`.
  - A column that mixes id systems declares a rule instead, e.g. MN `Contrib Reg Num` below 10,000 is `mn_cfb_lobbyist`, otherwise `mn_cfb_registration`; PA `FILERID` by length.
  - Standardize validates ids against the registry and emits a companion column per id column (e.g. `recipient_id_source`), so the source travels with the raw id.
- **Normalize:**
  - When `handle_existing_ids` meets a raw id, it looks up `(source, source_id, entity_table)` in `SourceIdentifier`.
  - If found, it reuses that `entity_id`. If not, it creates a UUID and appends a row.
  - The companion `*_id_source` columns are then dropped.
  - The chunked loop in `run_normalize` accumulates the table as it accumulates `id_mapping` today, and writes it with the other tables instead of to a TSV.
- **Link:**
  - After clustering, `SourceIdentifier.entity_id` is updated with the same `old_id -> canonical_id` mapping used for the other id columns.
  - `linkage_mapping` stays as the audit trail.
  - Deterministic links (below) run before Splink and write through the same path.

### 7. Deterministic linking with source ids

With source ids in the database, cross-source links become joins. For Minnesota:

1. **Finance files:** the committee Transactor gets `(mn_cfb_registration, 15677)`.
2. **CFB roster:**
   - House 34B, 2022 gives Hortman, registration `15677`, DFL, incumbent, general winner.
   - This becomes a `Membership(member = candidate Individual, organization = committee, membership_type = Candidate)` pointing at the committee found through `(mn_cfb_registration, 15677)`.
3. **SOS results:**
   - The 2022-11-08 House 34B result row for "Melissa Hortman" gets `(mn_sos_candidacy, 20221108-0255-0401)` with `entity_table = ElectionResult`.
   - Its candidate is matched to the roster candidate within the same office, district and year, usually two or three candidates.
   - Names differ between sources ("Scott Simmons" in SOS vs. "Simmons, Scott R" in the CFB roster), so the match must be scoped to the race rather than global.
   - The matched roster person becomes `ElectionResult.candidate_id`.

The result: `ElectionResult -> candidate Individual -> Membership(Candidate) -> committee -> Transaction.recipient_id`, with every hop auditable through a source id.

## Alternatives considered

- **One id column per source on each table** (`campaign_finance_id`, `election_results_id`, ...):
  - Simple to query, but can't hold more than one id per source per entity (the House-then-Senate case, or an AZ committee's CommitteeID and NameID).
  - It also needs a schema change for every new source.
  - A pivot over `SourceIdentifier` gives the same shape when it is convenient.
- **Use a source id as the entity id:** rejected for the reasons in section 5.
- **Keep `year` in the key:**
  - It fixes PA short candidate ids, but splits every committee in the other states into one entity per year.
  - It does nothing about collisions between id systems.
- **Enum in `table.yaml` for `source`:** rejected in section 2.
- **Keep `id_mapping.tsv` and add a `source` column:**
  - This fixes collisions, but the mapping stays outside the database and still goes stale after linking.
  - It is a reasonable first step if the full change is too large right now.

## Implementation plan

1. **`table.yaml`:**
   - Add `SourceIdentifier`.
   - Add `id` to `ElectionResult`.
   - Remove `ein` from `Organization`.
2. **`src/utils/sources.yaml`** plus a loader and validator.
   - Initial entries: `mn_cfb_registration`, `mn_cfb_lobbyist`, `mn_sos_candidacy`, `az_sos_committee`, `az_sos_name`, `pa_dos_filer`, `pa_dos_candidate_filer`, `tx_ethics_filer`, `mi_sos_committee`, `fec_committee`.
3. **`utils/ids.py`:**
   - Change the mapping key to `(source, source_id, entity_table)`.
   - Hold the mapping as a DataFrame with `SourceIdentifier` columns instead of a dict.
   - Replace `save_id_mapping` / `load_id_mapping` with table I/O.
   - Keep `normalize_id_to_string` for `source_id`.
4. **Standardize configs** (`az`, `mi`, `mn`, `pa`, `tx`):
   - Add `id_source`, or a rule for mixed columns, to each raw id column.
   - Map TX `contributorPacFein` to `fec_committee`.
   - Emit the `*_id_source` companion columns.
5. **`normalize/_core.py` and `cli/core.py`:** pass sources through `handle_id_column` / `handle_existing_ids`, and accumulate and save `SourceIdentifier` per chunk.
6. **`link/predict.py`:** update `SourceIdentifier.entity_id` alongside the other Transactor references in `run_linkage_pipeline`.
7. **Migration:**
   - Convert existing `id_mapping.tsv` files, preserving current UUIDs.
   - Where the source can be inferred from the config, use it; otherwise use `<state>_legacy`.
   - AZ ids that collide can't be split from the old mapping and are re-keyed on the next normalize run.
8. **Tests:** extend `tests/test_ids.py` and `tests/test_normalization.py` for:
   - collisions between id systems: the AZ case, where the same raw id from two sources gives two entities;
   - PA short-id composition;
   - registry validation (unknown source, `null_values`, pattern failures);
   - reuse across chunks;
   - post-link updates.

This touches files also changed in #144 (`cli/core.py`, link). It is best implemented after #144 merges.

## Resolved questions

1. **Does any state reuse ids across years?** Only PA short candidate FILERIDs. Handled by that source's `source_id` format, not a year in the key. See [Evidence](#evidence-id-behavior-in-the-raw-data).
2. **Candidacy ids: person or result row?** Result row. `ElectionResult` gets an `id` (section 3).
3. **Enum or registry for `source`?** A documented, validated registry file (section 2).
4. **`ein`?** It's actually an FEC committee id in practice. It becomes the `fec_committee` source (section 4).

## Open questions

1. **TX multi-role filers:** should Texas filer roles (e.g. `00015654` as both Texas Association of Business and TAB PAC) be separate entities (`source_id = <filerIdent>-<role>`) or one? Separate matches how money flows; one matches how the state assigns ids.
2. **MI:** the MI export can't show whether `cfr_com_id`s are ever reused. Treat them as permanent unless MI documentation says otherwise.
