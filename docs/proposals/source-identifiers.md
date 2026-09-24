# Proposal: Source Identifiers

**Status:** Draft for discussion
**Scope:** `table.yaml`, `utils/ids.py`, normalize, link, standardize configs

## Summary

Record every identifier a source provides (a campaign finance registration number, a secretary of state candidate id, a Texas filer id, ...) in a new `SourceIdentifier` table. Each row says which entity the identifier points to and which source issued it. Entity ids (`Transactor.id` and friends) stay UUIDs. Linking updates `SourceIdentifier` so every source id always points at the current canonical entity.

This replaces `id_mapping.tsv`. That file already records this information, but only as a side file, without a source, and it goes stale after linking.

## Motivation

We are about to link data from more than one source for the same state. For Minnesota:

| Source | Native id | Identifies |
|---|---|---|
| Campaign Finance Board (CFB) finance files | registration number, e.g. `15677` | a candidate *committee* (an Organization) |
| CFB district viewer (candidate roster) | registration number + office/district/year + name | the bridge between a committee and a candidacy |
| Secretary of State (SOS) results files | office id + candidate order code, e.g. `0121`/`0301` | one *candidacy* in one election |

Linking these reliably is a rule-based join on source ids, not a probabilistic match. Today that is hard, for four reasons:

1. **Source ids disappear from the data.** `handle_existing_ids` swaps each raw id for a UUID. The raw id survives only in `DATA_DIR/normalized/<state>/id_mapping.tsv`, which is not part of the database and is not loaded by the link step.
2. **The mapping goes stale after linking.** `link/predict.py` rewrites Transactor ids to canonical cluster ids in place (`replace_ids_with_canonical`). It records `old_id -> canonical_id` in the DuckDB `linkage_mapping` table but never updates `id_mapping.tsv`. A raw id therefore still maps to the pre-link UUID.
3. **Ids have no namespace.** The mapping key is `(raw_id, year, reported_state, table_name)`. Two id systems in the same state and table would collide, for example if a state has two sources (as Arizona does) or when a second agency's ids are added (as SOS ids would be for Minnesota). The key also carries `year`, which is always empty for Transactor (the table has no `year` column) and is empty in every existing `id_mapping.tsv`.
4. **One entity can have many source ids.** A person can have one committee for a House run and another for a later Senate run. A committee has a CFB id, and its candidate may also have SOS candidacy ids for each election. A fixed column per source on `Transactor` cannot represent this.

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
    - entity_table   # Transactor, Election, Transaction, ...
    - source         # which id system issued source_id, see naming below
    - source_id      # the identifier exactly as the source provides it (string)
    - reported_state
    - earliest_known_date
    - latest_known_date
```

- **Invariant:** `(source, source_id, entity_table)` is unique, and it maps to exactly one `entity_id` at any time. One entity can have any number of rows.
- **Source naming:** `<state>_<agency>_<id type>`, lower snake case. Examples: `mn_cfb_registration`, `mn_sos_candidacy`, `tx_ethics_filer`, `pa_dos_filer`. The source replaces both `reported_state` and `year` in the mapping key. `reported_state` is kept as an attribute for filtering.
- **Dates:** `earliest_known_date` / `latest_known_date` follow the convention already used by `Address` and `Membership`. Some ids are only meaningful for a period, such as an SOS candidacy id, which is only valid within one election.

### 2. Entity ids stay UUIDs

`Transactor.id` is **not** replaced by one of the source ids.

- **Linking changes the entity.** When two records are merged, whichever source id was "the" id would change for some rows. That breaks references and anything downstream that saved an id.
- **Some entities have no source id.** Most individual donors have no id in any source.
- **Source ids collide across systems.** CFB `15677` and some other agency's `15677` are unrelated. A UUID plus namespaced source ids avoids that.

This keeps the property the current design is aiming for: an entity's id is independent of where we first saw it, and every id a source gave us can be found.

### 3. Lifecycle

- **Standardize:**
  - Each id column in a state config declares its source, e.g. `id_source: mn_cfb_registration` on `Recipient reg num` in `mn.yaml`.
  - Standardize emits a companion column per id column, e.g. `recipient_id_source`, so the source travels with the raw id.
- **Normalize:**
  - When `handle_existing_ids` meets a raw id, it looks up `(source, source_id, entity_table)` in `SourceIdentifier`.
  - If found, it reuses that `entity_id`. If not, it creates a UUID and appends a row.
  - The companion `*_id_source` columns are then dropped.
  - The chunked loop in `run_normalize` accumulates the table exactly as it accumulates `id_mapping` today, and writes it with the other tables instead of to a TSV.
- **Link:**
  - After clustering, `SourceIdentifier.entity_id` is updated with the same `old_id -> canonical_id` mapping used for the other id columns.
  - `linkage_mapping` stays as the audit trail.
  - Deterministic links (below) run before Splink and write through the same path.

### 4. Deterministic linking with source ids

With source ids in the database, cross-source links become joins. For Minnesota:

1. **Finance files:** the committee Transactor gets `(mn_cfb_registration, 15677)`.
2. **CFB roster:**
   - House 34B, 2022 gives Hortman, registration `15677`, DFL, incumbent, general winner.
   - This becomes a `Membership(member = candidate Individual, organization = committee, membership_type = Candidate)` pointing at the committee found through `(mn_cfb_registration, 15677)`.
3. **SOS results:**
   - The 2022-11-08 House 34B result for "Melissa Hortman" gets `(mn_sos_candidacy, 20221108-0255-0401)`, the election date plus office id plus candidate order code.
   - It is matched to the roster candidate within the same office, district and year, usually two or three candidates.
   - Names differ between sources ("Scott Simmons" in SOS vs. "Simmons, Scott R" in the CFB roster), so the match must be scoped to the race rather than global.
   - The two Individuals are merged, and both source ids now point at one entity.

The result: `ElectionResult -> candidate Individual -> Membership(Candidate) -> committee -> Transaction.recipient_id`, with every hop auditable through a source id.

## Alternatives considered

- **One id column per source on each table** (`campaign_finance_id`, `election_results_id`, ...):
  - Simple to query, but can't hold more than one id per source per entity (the House-then-Senate case).
  - It also needs a schema change for every new source or state.
  - A pivot over `SourceIdentifier` gives the same shape when it is convenient.
- **Use a source id as the entity id** (e.g. `Transactor.id = mn_cfb_registration:15677`): rejected for the reasons in section 2.
- **Keep `id_mapping.tsv` and add a `source` column:**
  - This fixes collisions, but the mapping stays outside the database and still goes stale after linking.
  - It is a reasonable first step if the full change is too large right now.

## Implementation plan

1. **`table.yaml`:** add `SourceIdentifier`.
2. **`utils/ids.py`:**
   - Change the mapping key to `(source, source_id, entity_table)`.
   - Hold the mapping as a DataFrame with `SourceIdentifier` columns instead of a dict.
   - Replace `save_id_mapping` / `load_id_mapping` with table I/O.
   - Keep `normalize_id_to_string` for `source_id`.
3. **Standardize configs** (`az`, `mi`, `mn`, `pa`, `tx`): add `id_source` to each raw id column, and emit the `*_id_source` companion columns.
4. **`normalize/_core.py` and `cli/core.py`:** pass sources through `handle_id_column` / `handle_existing_ids`, and accumulate and save `SourceIdentifier` per chunk.
5. **`link/predict.py`:** update `SourceIdentifier.entity_id` alongside the other Transactor references in `run_linkage_pipeline`.
6. **Migration:** convert existing `id_mapping.tsv` files, using `source = <state>_legacy` where the source isn't known, so current UUIDs are preserved.
7. **Tests:** extend `tests/test_ids.py` and `tests/test_normalization.py` for:
   - namespaced collisions: the same raw id from two sources gives two entities;
   - reuse across chunks;
   - post-link updates.

This touches files also changed in #144 (`cli/core.py`, link). It is best implemented after #144 merges.

## Open questions

1. **Year in the key:** does any state reuse ids across years for different entities? If so, that source should put the year into `source_id` rather than into the key for every source.
2. **ElectionResult has no `id`:** should SOS candidacy ids identify the candidate Individual (as above) or an `ElectionResult` row, which would need an `id`?
3. **Enum or registry:** should `source` be an enum in `table.yaml`, or a documented registry, to avoid a schema change per new source?
4. **Documented source ids that are not keys:** EIN, which `Organization` already has, could also live here, making `ein` a derived convenience column.
